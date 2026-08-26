#!/usr/bin/env python3
"""Replay a month's plan into HubSpot through the batch endpoints.

This is the WRITE side of the Zid import. Everything before it is preparation:
zed_normalize built the canonical orders, zed_plan ran the REAL engine offline
and recorded every write it would make, and the equivalence test proved those
recorded writes match what the live engine produces, property by property.
This tool sends them, 100 records per call, so ~974k orders cost ~40k calls
instead of the ~9M a per-order replay would.

The plan's synthetic ids (§N) are resolved through a symbol table built from
the RESPONSES: an order is matched back by its salla_order_id and a line item
by its salla_order_item_id, both present in every create body and unique in
the portal. Response ORDER is never relied on, because HubSpot does not
promise it.

One fold, from the original design: the engine records a follow-up PATCH
setting last_salla_sync_status; the emitter folds that into the order create
body. The equivalence test validated the fold (created-and-patched equals
created-with), and it removes ~974k calls.

Idempotency is layered:
  * a per-month append-only ledger (mirror/zed_emitted/YYYY-MM.csv) records
    every order as CREATED the moment its batch returns, and DONE once its
    line items and associations are in. Re-runs skip DONE orders entirely.
  * an order CREATED but not DONE is repaired, not re-created: its existing
    line items are read back by salla_order_id, only the missing ones are
    created, and associations are re-sent (association creates are idempotent
    in HubSpot; a duplicate input is a no-op).
  * beneath both, salla_order_item_id is a UNIQUE line-item property in this
    portal, so even a logic error cannot mint a duplicate line item: the
    create would 400.

Safety rails:
  * dry run by default; --live to write.
  * refuses a plan file older than its month's normalised data.
  * refuses any op shape it does not recognise, so a stale plan format fails
    loudly instead of being half-understood.
  * honours STOP.zed_emit between chunks; single instance via flock.
  * yields to live sync exactly like the backfill engine does.

Usage:
    python3 zed_emit.py --month 2023-10            # dry summary
    python3 zed_emit.py --month 2023-10 --live
    python3 zed_emit.py --all --live               # oldest month first
"""

import argparse
import csv
import fcntl
import glob
import json
import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

from backfill import (Config, HubSpot, apply_portal_config, now_str,
                      setup_logging)

log = logging.getLogger("backfill")

PLANS = Path("mirror/zed_plans")
NORM = Path("mirror/zed")
LEDGERS = Path("mirror/zed_emitted")
STOP = Path("STOP.zed_emit")
LOCK = Path("mirror/zed_emit.lock")

CHUNK_ORDERS = 100          # orders per emit chunk == one batch-create call


class MonthLedger:
    """Append-only, flushed per write: a crash loses nothing already sent."""

    def __init__(self, month):
        self.path = LEDGERS / f"{month}.csv"
        self.state = {}                    # oid -> (status, hs_id)
        if self.path.exists():
            with open(self.path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.state[row["salla_order_id"]] = (row["status"],
                                                         row["hs_order_id"])
        # the file handle opens LAZILY on the first mark(): a dry run must
        # leave no footprint at all, not even a header
        self.f = self.w = None

    def mark(self, oid, hs_id, li_expected, status):
        if self.f is None:
            LEDGERS.mkdir(parents=True, exist_ok=True)
            new = not self.path.exists()
            self.f = open(self.path, "a", newline="", encoding="utf-8")
            self.w = csv.writer(self.f)
            if new:
                self.w.writerow(["ts", "salla_order_id", "hs_order_id",
                                 "li_expected", "status"])
        self.w.writerow([now_str(), oid, hs_id, li_expected, status])
        self.f.flush()
        self.state[str(oid)] = (status, str(hs_id))


def load_plan(month):
    """plan ops grouped per order, validated against the shapes we emit."""
    pf = PLANS / f"{month}.plan.jsonl"
    nf = NORM / f"{month}.jsonl.gz"
    if not pf.exists():
        sys.exit(f"no plan for {month}; run zed_plan.py --month {month}")
    if nf.exists() and pf.stat().st_mtime < nf.stat().st_mtime:
        sys.exit(f"plan for {month} is OLDER than its normalised data; "
                 f"regenerate with zed_plan.py --month {month}")

    orders = {}
    for line in open(pf, encoding="utf-8"):
        r = json.loads(line)
        o = orders.setdefault(r["order_id"], {"create": None, "status": None,
                                              "lis": [], "assoc": []})
        if r["op"] == "POST" and r["path"] == "/crm/v3/objects/orders":
            o["create"] = r
        elif r["op"] == "PATCH" and r["path"].startswith("/crm/v3/objects/orders/"):
            props = r["body"].get("properties") or {}
            if set(props) - {"last_salla_sync_status"}:
                sys.exit(f"unrecognised order PATCH for {r['order_id']}: "
                         f"{sorted(props)}")
            o["status"] = props.get("last_salla_sync_status")
        elif r["op"] == "POST" and r["path"] == "/crm/v3/objects/line_items":
            o["lis"].append(r)
        elif r["op"] == "POST" and r["path"].startswith("/crm/v4/associations/"):
            o["assoc"].append(r)
        else:
            sys.exit(f"unrecognised plan op: {r['op']} {r['path']}")
    bad = [oid for oid, o in orders.items() if not o["create"]]
    if bad:
        sys.exit(f"{len(bad)} orders in plan without a create op: {bad[:5]}")
    return orders


def yield_to_live():
    """Same convention as the backfill engine: live wins."""
    return bool(glob.glob("mirror/live_active*.json"))


def emit_chunk(hs, month, ledger, chunk, live):
    """One chunk: batch order create -> batch LI create -> batch assoc.

    Returns (created, repaired, li_count).
    """
    # ---- phase 1: orders --------------------------------------------------
    to_create, sym_of_oid = [], {}
    for oid, o in chunk:
        body = dict(o["create"]["body"])
        props = dict(body.get("properties") or {})
        if o["status"]:
            props["last_salla_sync_status"] = o["status"]   # the fold
        body["properties"] = props
        sym_of_oid[oid] = o["create"]["sym"]
        to_create.append(body)

    symtab = {}
    if live:
        st, data = hs._req("POST", "/crm/v3/objects/orders/batch/create",
                           body={"inputs": to_create}, what="emit orders")
        if st not in (200, 201):
            raise RuntimeError(f"order batch create HTTP {st}: "
                               f"{json.dumps(data)[:300]}")
        by_salla = {}
        for r in data.get("results", []):
            sid = (r.get("properties") or {}).get("salla_order_id")
            if sid:
                by_salla[str(sid)] = str(r["id"])
        for oid, o in chunk:
            hs_id = by_salla.get(str(oid))
            if not hs_id:
                raise RuntimeError(f"order {oid} missing from batch response")
            symtab[sym_of_oid[oid]] = hs_id
            ledger.mark(oid, hs_id, len(o["lis"]), "created")
    else:
        for oid, o in chunk:
            symtab[sym_of_oid[oid]] = f"DRY-{oid}"

    # ---- phase 2: line items ---------------------------------------------
    li_bodies, li_sym_by_item = [], {}
    for oid, o in chunk:
        for r in o["lis"]:
            body = dict(r["body"])
            li_bodies.append(body)
            item_id = (body.get("properties") or {}).get("salla_order_item_id")
            li_sym_by_item[str(item_id)] = r["sym"]
    if live:
        for i in range(0, len(li_bodies), 100):
            st, data = hs._req("POST", "/crm/v3/objects/line_items/batch/create",
                               body={"inputs": li_bodies[i:i + 100]},
                               what="emit line items")
            if st not in (200, 201):
                raise RuntimeError(f"LI batch create HTTP {st}: "
                                   f"{json.dumps(data)[:300]}")
            for r in data.get("results", []):
                iid = (r.get("properties") or {}).get("salla_order_item_id")
                sym = li_sym_by_item.get(str(iid))
                if sym:
                    symtab[sym] = str(r["id"])
    else:
        for iid, sym in li_sym_by_item.items():
            symtab[sym] = f"DRY-LI-{iid}"

    # ---- phase 3: associations -------------------------------------------
    assoc_inputs = []
    for oid, o in chunk:
        for r in o["assoc"]:
            for inp in r["body"].get("inputs", []):
                inp = json.loads(json.dumps(inp))     # deep copy
                for side in ("from", "to"):
                    v = str(inp[side]["id"])
                    if v.startswith("§"):
                        real = symtab.get(v)
                        if not real:
                            raise RuntimeError(f"unresolved symbol {v} "
                                               f"on order {oid}")
                        inp[side]["id"] = real
                assoc_inputs.append(inp)
    if live and assoc_inputs:
        for i in range(0, len(assoc_inputs), 100):
            st, data = hs._req(
                "POST", "/crm/v4/associations/order/line_items/batch/create",
                body={"inputs": assoc_inputs[i:i + 100]}, what="emit assoc")
            if st not in (200, 201):
                raise RuntimeError(f"assoc batch HTTP {st}: "
                                   f"{json.dumps(data)[:300]}")
    if live:
        for oid, o in chunk:
            ledger.mark(oid, symtab[sym_of_oid[oid]], len(o["lis"]), "done")
    return len(chunk), 0, len(li_bodies)


def repair_order(hs, month, ledger, oid, o, hs_id):
    """CREATED but not DONE: finish it without duplicating anything."""
    st, data = hs._req(
        "POST", "/crm/v3/objects/line_items/search",
        body={"filterGroups": [{"filters": [
                {"propertyName": "salla_order_id", "operator": "EQ",
                 "value": str(oid)}]}],
              "properties": ["salla_order_item_id"], "limit": 100},
        what="repair read")
    have = {str((r.get("properties") or {}).get("salla_order_item_id")): str(r["id"])
            for r in (data or {}).get("results", [])}
    missing = [r for r in o["lis"]
               if str((r["body"].get("properties") or {})
                      .get("salla_order_item_id")) not in have]
    for i in range(0, len(missing), 100):
        st, data = hs._req("POST", "/crm/v3/objects/line_items/batch/create",
                           body={"inputs": [r["body"] for r in missing[i:i+100]]},
                           what="repair LI create")
        if st not in (200, 201):
            raise RuntimeError(f"repair LI create HTTP {st}")
        for r in (data or {}).get("results", []):
            iid = (r.get("properties") or {}).get("salla_order_item_id")
            have[str(iid)] = str(r["id"])
    inputs = [{"from": {"id": hs_id}, "to": {"id": li},
               "types": [{"associationCategory": "HUBSPOT_DEFINED",
                          "associationTypeId": 513}]}
              for li in have.values()]
    for i in range(0, len(inputs), 100):
        hs._req("POST", "/crm/v4/associations/order/line_items/batch/create",
                body={"inputs": inputs[i:i + 100]}, what="repair assoc")
    ledger.mark(oid, hs_id, len(o["lis"]), "done")


def emit_month(hs, month, live):
    orders = load_plan(month)
    ledger = MonthLedger(month)

    done = {oid for oid, (s, _) in ledger.state.items() if s == "done"}
    dirty = {oid: hid for oid, (s, hid) in ledger.state.items()
             if s == "created"}
    todo = [(oid, o) for oid, o in orders.items()
            if oid not in done and oid not in dirty]
    log.info("%s: %d in plan, %d done, %d dirty (repair), %d to emit",
             month, len(orders), len(done), len(dirty), len(todo))

    for oid, hid in dirty.items():
        if oid in orders and live:
            log.info("repairing %s -> %s", oid, hid)
            repair_order(hs, month, ledger, oid, orders[oid], hid)

    t0, sent, lis = time.time(), 0, 0
    for i in range(0, len(todo), CHUNK_ORDERS):
        if STOP.exists():
            log.warning("STOP.zed_emit present; halting cleanly after %d "
                        "orders", sent)
            return False
        while yield_to_live():
            log.info("live sync active; yielding 30s")
            time.sleep(30)
        chunk = todo[i:i + CHUNK_ORDERS]
        c, _, l = emit_chunk(hs, month, ledger, chunk, live)
        sent += c
        lis += l
        if sent % 1000 < CHUNK_ORDERS:
            rate = sent / max(1, time.time() - t0)
            log.info("%s: %d/%d orders (%.0f/min), %d line items",
                     month, sent, len(todo), rate * 60, lis)
    log.info("%s %s: %d orders, %d line items in %.1f min", month,
             "EMITTED" if live else "DRY RUN", sent, lis,
             (time.time() - t0) / 60)
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--month")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="zed_emit.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    LOCK.parent.mkdir(parents=True, exist_ok=True)
    lockf = open(LOCK, "w")
    try:
        fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("another zed_emit is running; refusing a second instance")

    if args.live:
        print(f"LIVE EMIT to portal {cfg.hubspot_base}. Type RUN to proceed: ",
              end="", flush=True)
        if sys.stdin.readline().strip() != "RUN":
            sys.exit("aborted")

    hs = HubSpot(cfg, token, live=args.live)
    months = ([args.month] if args.month else
              sorted(p.name.split(".")[0] for p in PLANS.glob("*.plan.jsonl")))
    if not months:
        sys.exit("no plans found; run zed_plan.py first")
    if not args.all and len(months) > 1:
        sys.exit(f"{len(months)} plans present; pass --month or --all")
    for m in months:
        if not emit_month(hs, m, args.live):
            break
    return 0


if __name__ == "__main__":
    sys.exit(main())
