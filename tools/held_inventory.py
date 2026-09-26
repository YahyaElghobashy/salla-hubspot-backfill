#!/usr/bin/env python3
"""Everything that is stuck, in one read (v2.12, Repair Batch 1.0).

Read-only. Walks every place a contact, an order, a delivery status, a
warranty or a consent flag can be left waiting, and prints one section per
place with counts and samples. Writes mirror/held_inventory.json so the
fixes and the documentation quote the same numbers.

  queues       Live, Status and Customer Queue rows that are not finished:
               queued/processing older than 30 min, error, deferred, held,
               exhausted (attempts at the cap), grouped by the reason in
               their note
  queue log    the catalog drain's Queue Log rows still Queued or Error,
               grouped by blocking product
  exceptions   Delivery Status Exceptions rows of the last 14 days by reason
  ledgers      errors.csv rows of the last 30 days whose order is not in the
               created ledger (never finished) by stage
  orders       Salla orders stamped partial/failed; Salla orders whose Zid
               item was merged in (salla_store "Zid" on a Salla source)
  contacts     phone-only contacts (no Salla id, no first name) created by
               the integration since 10 Aug; Salla contacts without the
               consent flag (last 7 days and all)
  warranties   Salla orders delivered or completed since 31 Aug with a
               device line item and no warranty; warranties still Active on
               a cancelled or returned order
  make         unresolved stored runs on every active scenario, and webhook
               queue depth for every active scenario

    venv/bin/python3 tools/held_inventory.py --config config.live.json
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, CreatedLedger, GoogleIO, HubSpot, apply_portal_config, dig, setup_logging

log = logging.getLogger("backfill")
OUT = Path("mirror/held_inventory.json")
RIYADH = timezone(timedelta(hours=3))
WARRANTY = "2-252148104"
DELIVERED, COMPLETED = "3725360f-519b-4b18-a593-494d60a29c9f", "5656450292"
CANCELLED, RETURNED = "3c85a297-e9c3-4f63-a8d5-b0a4b3c9e4d1", "5656470775"
W_ACTIVE, W_EXPIRING = "5913821399", "5913821400"
FINAL = {"done", "gone", "superseded", "error-final"}


def now():
    return datetime.now(RIYADH).replace(tzinfo=None)


def age_min(ts):
    try:
        return (now() - datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
    except Exception:
        return None


def reason(note):
    n = str(note or "")
    for k in ("catalog gate", "zid collision", "partial HS", "relay fetch miss", "order not in HS",
              "payload unreadable", "lookup", "held", "retries exhausted", "PATCH failed",
              "order absent", "unmapped status", "verify unavailable", "processing failed"):
        if k.lower() in n.lower():
            return k
    return n[:40] or "(no note)"


def queue_section(gio, cfg, tab, cap):
    rows = gio.queue_read_all(cfg.queue_spreadsheet_id, tab=tab)
    by_state = Counter(str(r.get("status") or "queued") for r in rows)
    stuck = defaultdict(list)
    for r in rows:
        st = str(r.get("status") or "queued")
        if st in FINAL:
            continue
        a = age_min(r.get("received_at"))
        if st in ("queued", "processing") and (a is None or a < 30):
            continue                       # in flight
        key = st
        if st == "error" and int(r.get("attempts") or 0) >= cap:
            key = "error (exhausted)"
        stuck[f"{key}: {reason(r.get('note'))}"].append(r)
    return {"rows": len(rows), "by_state": dict(by_state),
            "stuck": {k: {"count": len(v), "oldest": min(str(x.get("received_at")) for x in v),
                          "sample": [str(x.get("order_id")) for x in v[:12]]}
                      for k, v in sorted(stuck.items(), key=lambda kv: -len(kv[1]))}}


def queue_log_section(cfg):
    import queue_drain
    dg = queue_drain.DrainGoogleIO(cfg, enabled=True)
    rows = dg.qlog_read()
    by = Counter(r["status"] or "(blank)" for r in rows)
    open_rows = [r for r in rows if r["status"] not in queue_drain.TERMINAL]
    blockers = Counter()
    for r in open_rows:
        note = (r.get("notes") or r.get("reason") or "")
        blockers[note.split("@")[0].replace("blocked:", "").strip()[:60] or "(none)"] += 1
    return {"rows": len(rows), "by_status": dict(by), "open": len(open_rows),
            "open_by_blocker": dict(blockers.most_common(15)),
            "sample": [r["order_id"] for r in open_rows[:15]]}


def exceptions_section(gio, cfg, days=14):
    v = gio._gexec(gio.sheets.values().get(spreadsheetId=cfg.spreadsheet_id,
                                           range="'Delivery Status Exceptions'!A2:H"),
                   "exceptions read", gio.sheets_rl)
    rows = v.get("values") or []
    since = (now() - timedelta(days=days)).strftime("%Y-%m-%d")
    recent = [r for r in rows if r and str(r[0])[:10] >= since]
    why = Counter((r[5] if len(r) > 5 else "?") for r in recent)
    orders = defaultdict(set)
    for r in recent:
        if len(r) > 5:
            orders[r[5]].add(str(r[1]))
    return {"rows": len(rows), "recent_days": days, "recent": len(recent),
            "recent_by_reason": dict(why.most_common()),
            "distinct_orders_by_reason": {k: len(v) for k, v in orders.items()},
            "sample_by_reason": {k: sorted(v)[:10] for k, v in orders.items()}}


def ledger_section(days=30):
    led = CreatedLedger("mirror")
    since = (now() - timedelta(days=days)).strftime("%Y-%m-%d")
    open_ = defaultdict(set)
    with open("mirror/errors.csv", newline="", encoding="utf-8", errors="ignore") as f:
        for r in csv.DictReader(f):
            if str(r.get("ts", ""))[:10] < since:
                continue
            oid = str(r.get("salla_order_id") or "")
            if oid and not led.get(oid):
                open_[str(r.get("stage"))].add(oid)
    return {"days": days, "unfinished_by_stage": {k: len(v) for k, v in open_.items()},
            "unfinished_ids": {k: sorted(v) for k, v in open_.items()}}


def total(hs, obj, filters):
    d = hs.search(f"/crm/v3/objects/{obj}/search",
                  {"filterGroups": [{"filters": filters}], "properties": ["hs_object_id"], "limit": 1},
                  "inventory count")
    return int(d.get("total") or 0)


def listing(hs, obj, filters, props, cap=200):
    out, after = [], None
    while len(out) < cap:
        body = {"filterGroups": [{"filters": filters}], "properties": props, "limit": 100}
        if after:
            body["after"] = after
        d = hs.search(f"/crm/v3/objects/{obj}/search", body, "inventory list")
        out += d.get("results") or []
        after = dig(d, "paging.next.after")
        if not after:
            break
    return out[:cap]


def EQ(p, v): return {"propertyName": p, "operator": "EQ", "value": v}
def IN(p, v): return {"propertyName": p, "operator": "IN", "values": v}
def HAS(p): return {"propertyName": p, "operator": "HAS_PROPERTY"}
def NOT(p): return {"propertyName": p, "operator": "NOT_HAS_PROPERTY"}
def GTE(p, v): return {"propertyName": p, "operator": "GTE", "value": v}


def ms(y, m, d):
    return str(int(datetime(y, m, d, tzinfo=RIYADH).timestamp() * 1000))


def orders_section(hs):
    bad = listing(hs, "orders", [EQ("hs_source_store", "Salla"), IN("last_salla_sync_status", ["partial", "failed"])],
                  ["salla_order_id", "last_salla_sync_status", "hs_createdate"], cap=300)
    merged = total(hs, "orders", [EQ("salla_store", "Zid"), EQ("hs_source_store", "Salla")])
    return {"salla_partial_or_failed": len(bad),
            "partial_or_failed_sample": [dig(r, "properties.salla_order_id") for r in bad[:20]],
            "partial_or_failed_ids": [r["id"] for r in bad],
            "salla_orders_labelled_zid": merged}


def contacts_section(hs):
    since = ms(2026, 8, 10)
    phone_only = listing(hs, "contacts", [NOT("salla_customer_id"), NOT("firstname"), HAS("main_phone_number"),
                                          GTE("createdate", since)],
                         ["main_phone_number", "createdate", "hs_object_source_id", "hs_object_source_label"], cap=500)
    by_src = Counter(str(dig(r, "properties.hs_object_source_id") or dig(r, "properties.hs_object_source_label"))
                     for r in phone_only)
    week = ms(*(now() - timedelta(days=7)).timetuple()[:3])
    return {"phone_only_since_aug10": len(phone_only), "phone_only_by_source": dict(by_src),
            "phone_only_ids": [r["id"] for r in phone_only],
            "consent_missing_last7d": total(hs, "contacts", [HAS("salla_customer_id"), NOT("salla_consent_status"),
                                                             GTE("createdate", week)]),
            "consent_missing_all": total(hs, "contacts", [HAS("salla_customer_id"), NOT("salla_consent_status")])}


def _assoc_batch(hs, frm, to, ids):
    """{from_id: [to_ids]} through the v4 batch association read."""
    out = {}
    for i in range(0, len(ids), 1000):
        st, d = hs._req("POST", f"/crm/v4/associations/{frm}/{to}/batch/read",
                        {"inputs": [{"id": x} for x in ids[i:i + 1000]]}, what="inventory assoc")
        if st not in (200, 207):
            raise RuntimeError(f"assoc {frm}->{to}: HTTP {st}")
        for r in d.get("results") or []:
            out[str(dig(r, "from.id"))] = [str(t.get("toObjectId")) for t in r.get("to") or []]
    return out


def warranty_section(hs):
    since = ms(2026, 8, 31)
    delivered = []
    for stage in (DELIVERED, COMPLETED):
        # split by day windows: search stops at 10,000 results
        day = datetime(2026, 8, 31)
        while day.date() <= now().date():
            nxt = day + timedelta(days=1)
            delivered += listing(hs, "orders", [
                EQ("hs_source_store", "Salla"), EQ("hs_pipeline_stage", stage),
                GTE("hs_lastmodifieddate", str(int(day.replace(tzinfo=RIYADH).timestamp() * 1000))),
                {"propertyName": "hs_lastmodifieddate", "operator": "LT",
                 "value": str(int(nxt.replace(tzinfo=RIYADH).timestamp() * 1000))}],
                ["salla_order_id", "hs_pipeline_stage", "delivery_date", "hs_createdate"], cap=10000)
            day = nxt
    ids = sorted({r["id"] for r in delivered})
    has_w = _assoc_batch(hs, "orders", WARRANTY, ids)
    without = [i for i in ids if not has_w.get(i)]
    lis = _assoc_batch(hs, "orders", "line_items", without)
    li_ids = sorted({x for v in lis.values() for x in v})
    props = {}
    for i in range(0, len(li_ids), 100):
        st, d = hs._req("POST", "/crm/v3/objects/line_items/batch/read",
                        {"properties": ["product_class", "warranty_months", "sale_context", "hs_product_id"],
                         "inputs": [{"id": x} for x in li_ids[i:i + 100]]}, what="inventory LI")
        for r in (d or {}).get("results") or []:
            props[str(r["id"])] = r.get("properties") or {}
    # device class may sit only on the product (line items created before tagging)
    pids = sorted({p.get("hs_product_id") for p in props.values()
                   if p.get("hs_product_id") and not p.get("product_class")})
    pclass = {}
    for i in range(0, len(pids), 100):
        st, d = hs._req("POST", "/crm/v3/objects/products/batch/read",
                        {"properties": ["product_class"], "inputs": [{"id": x} for x in pids[i:i + 100]]},
                        what="inventory products")
        for r in (d or {}).get("results") or []:
            pclass[str(r["id"])] = dig(r, "properties.product_class")
    missing = []
    for o in without:
        for li in lis.get(o, []):
            p = props.get(li, {})
            if p.get("sale_context") in ("bundle_parent", "needs_review"):
                continue
            if (p.get("product_class") or pclass.get(str(p.get("hs_product_id")))) == "device":
                missing.append(o)
                break
    # active warranties on cancelled / returned orders
    stale = []
    for stage in (CANCELLED, RETURNED):
        rows = listing(hs, "orders", [EQ("hs_source_store", "Salla"), EQ("hs_pipeline_stage", stage),
                                      GTE("hs_lastmodifieddate", since)], ["salla_order_id"], cap=10000)
        oids = [r["id"] for r in rows]
        wmap = _assoc_batch(hs, "orders", WARRANTY, oids)
        wids = sorted({w for v in wmap.values() for w in v})
        wst = {}
        for i in range(0, len(wids), 100):
            st, d = hs._req("POST", f"/crm/v3/objects/{WARRANTY}/batch/read",
                            {"properties": ["hs_pipeline_stage"], "inputs": [{"id": x} for x in wids[i:i + 100]]},
                            what="inventory warranties")
            for r in (d or {}).get("results") or []:
                wst[str(r["id"])] = dig(r, "properties.hs_pipeline_stage")
        for o, ws in wmap.items():
            if any(wst.get(w) in (W_ACTIVE, W_EXPIRING) for w in ws):
                stale.append(o)
    return {"delivered_or_completed_since_aug31": len(ids), "without_warranty": len(without),
            "device_orders_without_warranty": len(missing), "device_missing_ids": missing,
            "active_warranty_on_cancelled_or_returned": len(stale), "stale_ids": stale}


def make_section():
    import credit_watch as cw
    out = {"stored_runs": {}, "hook_queues": {}}
    sc = cw._get("/scenarios?teamId=1523932&pg[limit]=100").get("scenarios") or []
    for s in sc:
        if not s.get("isActive"):
            continue
        d = cw._get(f"/dlqs?scenarioId={s['id']}&status=unresolved&pg[limit]=50")
        n = len(d.get("dlqs") or [])
        if n:
            out["stored_runs"][f"{s['id']} {s['name']}"] = n
    hooks = cw._get("/hooks?teamId=1523932&pg[limit]=100").get("hooks") or []
    active = {s["id"] for s in sc if s.get("isActive")}
    for h in hooks:
        if h.get("scenarioId") in active and int(h.get("queueCount") or 0) > 0:
            out["hook_queues"][f"{h.get('scenarioId')} {h.get('name')}"] = h.get("queueCount")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--skip", default="", help="comma list of sections to skip")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="held_inventory.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    hs = HubSpot(cfg, os.environ["HUBSPOT_ACCESS_TOKEN"], live=False)
    gio = GoogleIO(cfg, enabled=True)
    cap = int(getattr(cfg, "live_max_attempts", 8))
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    sections = [
        ("live_queue", lambda: queue_section(gio, cfg, cfg.live_queue_tab, cap)),
        ("status_queue", lambda: queue_section(gio, cfg, cfg.status_queue_tab, cap)),
        ("customer_queue", lambda: queue_section(gio, cfg, cfg.customer_queue_tab,
                                                 int(getattr(cfg, "realtime_max_attempts", 12)))),
        ("queue_log", lambda: queue_log_section(cfg)),
        ("exceptions", lambda: exceptions_section(gio, cfg)),
        ("ledgers", ledger_section),
        ("orders", lambda: orders_section(hs)),
        ("contacts", lambda: contacts_section(hs)),
        ("warranties", lambda: warranty_section(hs)),
        ("make", make_section),
    ]
    result = {"ts": now().strftime("%Y-%m-%d %H:%M:%S")}
    for name, fn in sections:
        if name in skip:
            continue
        t = time.time()
        try:
            result[name] = fn()
        except Exception as e:
            log.exception("section %s failed", name)
            result[name] = {"error": f"{type(e).__name__}: {e}"[:300]}
        short = {k: v for k, v in result[name].items() if not k.endswith("_ids") and k != "unfinished_ids"}
        log.info("== %s (%.0fs)\n%s", name, time.time() - t, json.dumps(short, indent=1, ensure_ascii=False)[:3500])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=1, ensure_ascii=False))
    log.info("written %s", OUT)


if __name__ == "__main__":
    main()
