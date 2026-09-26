#!/usr/bin/env python3
"""Give every delivered device its warranty, whatever made it miss (v2.12).

The warranty workflow ("Warranty · Activate on Delivery", flow 4762248403)
enrols an order when its stage CHANGES to Delivered or Completed. An order
that reaches HubSpot already in one of those stages never changes stage
there, so it never enrols: catalog-held orders released days later, orders
re-created after a repair, orders created while the live engine caught up.
On 2026-09-27 that left 4,783 Salla orders with a device and no warranty.

This runs the SAME code the workflow runs (warranty_engine_action.py is a
byte-for-byte copy of the deployed action), order by order, so every record
it makes is the record the workflow would have made: same keys (order:item:
unit, so a re-run or a later enrolment upserts onto the same records), same
dates, same stage. Two passes:

  missing   Salla orders in Delivered or Completed with a device line item
            and no warranty: the engine action is run for each
  stale     warranties still Active or Expiring Soon on an order that is
            now Cancelled or Returned: voided with the matching reason

An order without a delivery_date is dated from its stage history (the day
it entered Delivered or Completed in HubSpot) before the action runs: the
action would otherwise fall back to the last-modified date, which for an old
order is wrong. One with no such history entry is skipped and counted.
Imported Zid orders are skipped by the action itself.

Dry run by default: every write the action would make is intercepted and
counted. Ledger mirror/warranty_sweep.csv.

    venv/bin/python3 tools/warranty_sweep.py --config config.live.json --days 3        # dry
    venv/bin/python3 tools/warranty_sweep.py --config config.live.json --days 3 --apply
    venv/bin/python3 tools/warranty_sweep.py --config config.live.json --ids-file ids.txt --apply
"""

import argparse
import csv
import importlib.util
import json
import logging
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, HubSpot, apply_portal_config, dig, now_str, setup_logging

log = logging.getLogger("backfill")
ROOT = Path(__file__).resolve().parent.parent
LEDGER = Path("mirror/warranty_sweep.csv")
WARRANTY = "2-252148104"
RIYADH = timezone(timedelta(hours=3))
W_ACTIVE, W_EXPIRING, W_VOIDED = "5913821399", "5913821400", "5913821402"


def load_action(token):
    """The deployed action as a module, its token set before import (the
    action reads WARRANTY_ENGINE_TOKEN at import time)."""
    os.environ["WARRANTY_ENGINE_TOKEN"] = token
    spec = importlib.util.spec_from_file_location("warranty_engine_action",
                                                  ROOT / "warranty_engine_action.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body = status, body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class Pacer:
    """One shared pace for every thread: HubSpot allows 190 calls per 10 s
    per app; the live engine shares the app, so the sweep keeps to ~8/s."""

    def __init__(self, per_s=8.0):
        self.gap, self.lock, self.next = 1.0 / per_s, threading.Lock(), 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next)
            self.next = t + self.gap
        if t > now:
            time.sleep(t - now)


def wrap_http(mod, live, pacer, planned):
    """Retry 429/5xx with backoff; in a dry run intercept the writes (the
    upsert and the association creates) and answer them as HubSpot would."""
    real_get, real_post = mod._get, mod._post

    def retrying(fn, *a):
        for attempt in range(6):
            pacer.wait()
            try:
                r = fn(*a)
            except Exception as e:
                if attempt == 5:
                    raise
                log.warning("warranty sweep HTTP error %s, retrying", e)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(float(r.headers.get("Retry-After") or 2 ** attempt) if hasattr(r, "headers") else 2 ** attempt)
                continue
            return r
        return r

    def get(path, params=None):
        return retrying(real_get, path, params)

    def post(path, body):
        is_write = path.endswith("/batch/upsert") or ("/associations/" in path and path.endswith("/batch/create"))
        if is_write and not live:
            if path.endswith("/batch/upsert"):
                res = []
                for i, inp in enumerate(body.get("inputs") or []):
                    planned.append(inp.get("id"))
                    res.append({"id": f"DRY-{len(planned)}", "properties": inp.get("properties") or {}})
                return FakeResponse(200, {"results": res})
            return FakeResponse(201, {"results": []})
        return retrying(real_post, path, body)

    mod._get, mod._post = get, post


def search_all(hs, filters, props, cap=100000):
    out, after = [], None
    while len(out) < cap:
        body = {"filterGroups": [{"filters": filters}], "properties": props, "limit": 100}
        if after:
            body["after"] = after
        d = hs.search("/crm/v3/objects/orders/search", body, "warranty sweep")
        out += d.get("results") or []
        after = dig(d, "paging.next.after")
        if not after:
            return out
    return out


def assoc_map(hs, frm, to, ids):
    out = {}
    for i in range(0, len(ids), 1000):
        st, d = hs._req("POST", f"/crm/v4/associations/{frm}/{to}/batch/read",
                        {"inputs": [{"id": x} for x in ids[i:i + 1000]]}, what="warranty sweep assoc")
        if st not in (200, 207):
            raise RuntimeError(f"association read {frm}->{to}: HTTP {st}")
        for r in d.get("results") or []:
            out[str(dig(r, "from.id"))] = [str(t.get("toObjectId")) for t in r.get("to") or []]
    return out


def candidates(hs, cfg, days, ids):
    """Delivered/Completed Salla orders with no warranty: explicit ids, or
    those touched in the last `days` days (a stage change or a late create
    both move hs_lastmodifieddate)."""
    stage_map = cfg.status_stage_map or {}
    stages = [stage_map.get("delivered"), stage_map.get("completed")]
    props = ["salla_order_id", "hs_pipeline_stage", "delivery_date", "hs_source_store"]
    if ids:
        rows = []
        for i in range(0, len(ids), 100):
            st, d = hs._req("POST", "/crm/v3/objects/orders/batch/read",
                            {"properties": props, "inputs": [{"id": x} for x in ids[i:i + 100]]},
                            what="warranty sweep read")
            rows += (d or {}).get("results") or []
    else:
        since = str(int((datetime.now(RIYADH) - timedelta(days=days)).timestamp() * 1000))
        rows = []
        for stage in stages:
            if stage:
                rows += search_all(hs, [
                    {"propertyName": "hs_source_store", "operator": "EQ", "value": "Salla"},
                    {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": stage},
                    {"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": since}], props)
    rows = [r for r in rows if dig(r, "properties.hs_source_store") != "Zid"
            and dig(r, "properties.hs_pipeline_stage") in stages]
    has = assoc_map(hs, "orders", WARRANTY, [r["id"] for r in rows])
    return [r for r in rows if not has.get(str(r["id"]))]


def ledger(rows):
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "pass", "hs_order_id", "salla_order_id", "status", "warranties", "detail"])
        for r in rows:
            w.writerow([now_str()] + r)


def delivered_on(hs, cfg, hs_id):
    """The day the order entered Delivered or Completed in HubSpot, from its
    stage history (the earliest such entry), as YYYY-MM-DD in Riyadh time.
    For an order created already delivered that is its creation day."""
    stage_map = cfg.status_stage_map or {}
    done = {stage_map.get("delivered"), stage_map.get("completed")}
    st, o = hs._req("GET", f"/crm/v3/objects/orders/{hs_id}?propertiesWithHistory=hs_pipeline_stage",
                    what="warranty sweep history")
    hist = dig(o, "propertiesWithHistory.hs_pipeline_stage") or [] if st == 200 else []
    when = sorted(h.get("timestamp", "") for h in hist if h.get("value") in done and h.get("timestamp"))
    if not when:
        return None
    t = datetime.fromisoformat(when[0].replace("Z", "+00:00")).astimezone(RIYADH)
    return t.strftime("%Y-%m-%d")


def sweep_missing(hs, cfg, mod, live, days, ids, workers):
    todo = candidates(hs, cfg, days, ids)
    run = [r for r in todo if dig(r, "properties.delivery_date")]
    no_date, dated = [], []
    for r in todo:
        if dig(r, "properties.delivery_date"):
            continue
        # the action would fall back to the last-modified date, which for an
        # old order is wrong: date it from the stage history instead
        day = delivered_on(hs, cfg, r["id"])
        if not day:
            no_date.append(r)
            continue
        if live:
            st, d = hs._write("PATCH", f"/crm/v3/objects/orders/{r['id']}",
                              {"properties": {"delivery_date": day}}, "warranty sweep delivery_date")
            if st not in (200, 201):
                no_date.append(r)
                continue
        dated.append((str(r["id"]), day))
        run.append(r)
    log.info("WARRANTY missing: %d delivered/completed order(s) without a warranty; %d dated from "
             "their stage history; %d skipped (no date found); running the action on %d",
             len(todo), len(dated), len(no_date), len(run))
    outcome, rows, lock = Counter(), [], threading.Lock()

    def one(r):
        try:
            out = mod.main({"object": {"objectId": str(r["id"])}}).get("outputFields") or {}
        except Exception as e:
            out = {"status": f"exception {type(e).__name__}", "detail": str(e)[:120]}
        with lock:
            outcome[out.get("status")] += 1
            if out.get("status") == "ok":
                outcome["warranties"] += int(out.get("warranties_created") or 0)
            rows.append(["missing", str(r["id"]), dig(r, "properties.salla_order_id"), out.get("status"),
                         out.get("warranties_created", 0), str(out.get("detail") or out.get("skipped_no_term") or "")[:120]])
            if len(rows) % 250 == 0:
                log.info("  %d / %d orders done %s", len(rows), len(run), dict(outcome))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, run))
    rows += [["missing", str(r["id"]), dig(r, "properties.salla_order_id"), "skipped_no_delivery_date", 0, ""]
             for r in no_date]
    rows += [["dated", oid, "", "delivery_date from stage history", 0, day] for oid, day in dated]
    if live:
        ledger(rows)
    return {"orders": len(todo), "dated_from_history": len(dated), "no_delivery_date": len(no_date),
            **dict(outcome)}


def sweep_stale(hs, cfg, live, days):
    stage_map = cfg.status_stage_map or {}
    reasons = {stage_map.get("canceled"): "cancelled", stage_map.get("restored"): "returned"}
    since = str(int((datetime.now(RIYADH) - timedelta(days=days)).timestamp() * 1000))
    orders = []
    for stage in reasons:
        if stage:
            orders += search_all(hs, [
                {"propertyName": "hs_source_store", "operator": "EQ", "value": "Salla"},
                {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": stage},
                {"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": since}],
                ["salla_order_id", "hs_pipeline_stage"])
    wmap = assoc_map(hs, "orders", WARRANTY, [o["id"] for o in orders])
    wids = sorted({w for v in wmap.values() for w in v})
    stage_of = {}
    for i in range(0, len(wids), 100):
        st, d = hs._req("POST", f"/crm/v3/objects/{WARRANTY}/batch/read",
                        {"properties": ["hs_pipeline_stage"], "inputs": [{"id": x} for x in wids[i:i + 100]]},
                        what="warranty sweep stale read")
        for r in (d or {}).get("results") or []:
            stage_of[str(r["id"])] = dig(r, "properties.hs_pipeline_stage")
    upd, rows = [], []
    for o in orders:
        why = reasons.get(dig(o, "properties.hs_pipeline_stage"))
        for w in wmap.get(str(o["id"]), []):
            if stage_of.get(w) in (W_ACTIVE, W_EXPIRING):
                upd.append({"id": w, "properties": {"hs_pipeline_stage": W_VOIDED, "void_reason": why}})
                rows.append(["stale", str(o["id"]), dig(o, "properties.salla_order_id"), f"voided ({why})", 1, w])
    log.info("WARRANTY stale: %d active warranty(ies) on cancelled/returned orders%s", len(upd),
             "" if live else " (dry run)")
    if live and upd:
        for i in range(0, len(upd), 100):
            st, d = hs._write("POST", f"/crm/v3/objects/{WARRANTY}/batch/update",
                              {"inputs": upd[i:i + 100]}, "warranty sweep void")
            if st not in (200, 201):
                raise RuntimeError(f"void batch HTTP {st}: {json.dumps(d)[:200]}")
        ledger(rows)
    return {"voided": len(upd)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--days", type=int, default=3, help="orders touched in the last N days")
    ap.add_argument("--ids-file", default="", help="HubSpot order ids, one per line (backfill)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--no-stale", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="warranty_sweep.log")
    if Path("STOP.warranty").exists():
        log.info("STOP.warranty present -- not running")
        return
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not token:
        sys.exit("HUBSPOT_ACCESS_TOKEN missing")
    hs = HubSpot(cfg, token, live=args.apply)
    mod = load_action(token)
    planned = []
    wrap_http(mod, args.apply, Pacer(), planned)
    ids = [x.strip() for x in open(args.ids_file).read().split()] if args.ids_file else []
    t0 = time.time()
    res = {"missing": sweep_missing(hs, cfg, mod, args.apply, args.days, ids, args.workers)}
    if not args.no_stale:
        res["stale"] = sweep_stale(hs, cfg, args.apply, max(args.days, 30))
    if not args.apply:
        res["missing"]["planned_records"] = len(planned)
    log.info("WARRANTY SWEEP %s in %.0fs: %s", "APPLIED" if args.apply else "DRY RUN", time.time() - t0,
             json.dumps(res))


if __name__ == "__main__":
    main()
