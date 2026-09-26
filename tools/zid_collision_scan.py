#!/usr/bin/env python3
"""Find every Salla order that collided with a Zid order number (v2.12).

The Zid import (Aug 2026) stored each Zid order NUMBER in the unique
salla_order_id and keyed its line items "Z<number>-<n>". A Salla order whose
id equals a Zid number was, before the v2.12 guard:

  ledger_on_zid   matched to the Zid order by search, judged complete by a
                  line-item count, and ledgered as synced: the Salla order was
                  never created (silent loss)
  partial_on_zid  the same, but flagged partial because the Zid order had
                  fewer line items (loud, never created either)
  status_on_zid   a Salla status event applied to the Zid order's stage
  salla_items_on_zid
                  line items with Salla keys sitting on a Zid order

and, in the other direction, during the import itself (--deep):

  zid_items_on_salla
                  Zid line items attached to a Salla order: the import's
                  lost-response recovery reads a unique-value 400 as "already
                  created by me" and finishes the order it finds

Read-only: HubSpot reads and local ledgers only. Writes
mirror/zid_collision_scan.csv (one row per finding) and prints a summary.

    venv/bin/python3 tools/zid_collision_scan.py --config config.live.json
    venv/bin/python3 tools/zid_collision_scan.py --config config.live.json --deep
"""

import argparse
import csv
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import (ZID_ITEM_KEY, Config, HubSpot, apply_portal_config, dig,
                      is_zid_order, setup_logging)

log = logging.getLogger("backfill")
OUT = Path("mirror/zid_collision_scan.csv")
HS_ID = re.compile(r"HS (\d{6,})")
PROPS = ["salla_order_id", "salla_store", "hs_source_store", "hs_order_name", "hs_pipeline_stage",
         "hs_external_created_date", "hs_lastmodifieddate"]


def read_csv(path):
    p = Path(path)
    if not p.exists():
        log.warning("%s missing -- skipped", p)
        return []
    with open(p, newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


def batch_orders(hs, ids):
    """{hs_id: properties} for every id, 100 per batch read."""
    out, ids = {}, sorted(set(i for i in ids if i))
    for n in range(0, len(ids), 100):
        chunk = ids[n:n + 100]
        st, data = hs._req("POST", "/crm/v3/objects/orders/batch/read",
                           {"properties": PROPS, "inputs": [{"id": i} for i in chunk]},
                           what="scan orders")
        if st not in (200, 207):
            raise RuntimeError(f"batch read HTTP {st}")
        for r in data.get("results") or []:
            out[str(r["id"])] = r.get("properties") or {}
        if n and n % 10000 == 0:
            log.info("  read %d / %d orders", n, len(ids))
    return out


def item_keys(hs, hs_order_id):
    st, data = hs._req("GET", f"/crm/v4/objects/orders/{hs_order_id}/associations/"
                       "line_items?limit=100", what="scan LI assoc")
    ids = [str(r.get("toObjectId")) for r in (data or {}).get("results") or []] if st == 200 else []
    if not ids:
        return []
    st, data = hs._req("POST", "/crm/v3/objects/line_items/batch/read",
                       {"properties": ["salla_order_item_id"],
                        "inputs": [{"id": i} for i in ids]}, what="scan LI read")
    return [str(dig(r, "properties.salla_order_item_id") or "")
            for r in (data or {}).get("results") or []]


def stage_history(hs, hs_order_id):
    st, data = hs._req("GET", f"/crm/v3/objects/orders/{hs_order_id}"
                       "?propertiesWithHistory=hs_pipeline_stage", what="scan history")
    hist = (dig(data, "propertiesWithHistory.hs_pipeline_stage") or []) if st == 200 else []
    return [(h.get("timestamp", ""), h.get("value", ""), h.get("sourceType", ""),
             h.get("sourceId", "")) for h in hist]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--deep", action="store_true",
                    help="also search line items of every ledgered Salla order for Zid keys")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="zid_collision_scan.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not tok:
        sys.exit("HUBSPOT_ACCESS_TOKEN missing")
    hs = HubSpot(cfg, tok, live=False)

    created = read_csv("mirror/created.csv")
    status = read_csv("mirror/status_applied.csv")
    errors = read_csv("mirror/errors.csv")
    ledger = {}                                    # salla id -> (ts, hs id), last wins
    for r in created:
        ledger[str(r.get("salla_order_id"))] = (r.get("ts", ""), str(r.get("hubspot_order_id")))
    err_hs = defaultdict(set)                       # hs id -> salla ids flagged
    for r in errors:
        m = HS_ID.search(r.get("detail") or "")
        if m and r.get("stage") in ("partial", "duplicate_create"):
            err_hs[m.group(1)].add(str(r.get("salla_order_id")))
    all_ids = ({h for _, h in ledger.values()} | {str(r.get("hs_order_id")) for r in status}
               | set(err_hs))
    log.info("ledgers: %d created, %d status events, %d flagged errors -> %d HS orders to read",
             len(ledger), len(status), len(err_hs), len(all_ids))
    props = batch_orders(hs, all_ids)
    zid = {h for h, p in props.items() if is_zid_order(p)}
    log.info("read %d orders; %d are Zid-import orders", len(props), len(zid))

    findings = []
    for sid, (ts, h) in ledger.items():
        if h in zid:
            findings.append({"kind": "ledger_on_zid", "salla_order_id": sid, "hs_order_id": h,
                             "ts": ts, "detail": props[h].get("hs_order_name", "")})
    for h, sids in err_hs.items():
        if h in zid:
            for sid in sorted(sids):
                findings.append({"kind": "partial_on_zid", "salla_order_id": sid,
                                 "hs_order_id": h, "ts": "",
                                 "detail": props[h].get("hs_order_name", "")})
    by_order = defaultdict(list)
    for r in status:
        h = str(r.get("hs_order_id"))
        if h in zid:
            by_order[h].append(r)
    for h, rows in by_order.items():
        for r in rows:
            findings.append({"kind": "status_on_zid", "salla_order_id": r.get("order_id"),
                             "hs_order_id": h, "ts": r.get("ts", ""),
                             "detail": f"{r.get('slug')} -> stage {r.get('stage')}"})
    touched = sorted({f["hs_order_id"] for f in findings})
    for h in touched:
        keys = item_keys(hs, h)
        salla_keys = [k for k in keys if k and not ZID_ITEM_KEY.match(k)]
        if salla_keys:
            findings.append({"kind": "salla_items_on_zid", "salla_order_id": props[h].get("salla_order_id"),
                             "hs_order_id": h, "ts": "", "detail": " ".join(salla_keys[:20])})
        if h in by_order:
            hist = stage_history(hs, h)
            findings.append({"kind": "zid_stage_history", "salla_order_id": props[h].get("salla_order_id"),
                             "hs_order_id": h, "ts": "",
                             "detail": " | ".join(f"{t[:19]} {v} {st}:{si}" for t, v, st, si in hist[:6])})

    if args.deep:
        salla_ids = sorted(s for s, (_, h) in ledger.items() if h in props and h not in zid)
        log.info("deep: searching line items of %d Salla orders for Zid keys", len(salla_ids))
        for n in range(0, len(salla_ids), 100):
            chunk = salla_ids[n:n + 100]
            after = None
            while True:
                body = {"filterGroups": [{"filters": [
                            {"propertyName": "salla_order_id", "operator": "IN", "values": chunk}]}],
                        "properties": ["salla_order_id", "salla_order_item_id"], "limit": 200}
                if after:
                    body["after"] = after
                data = hs.search("/crm/v3/objects/line_items/search", body, "deep scan")
                for r in data.get("results") or []:
                    p = r.get("properties") or {}
                    k = str(p.get("salla_order_item_id") or "")
                    if ZID_ITEM_KEY.match(k):
                        sid = str(p.get("salla_order_id"))
                        findings.append({"kind": "zid_items_on_salla", "salla_order_id": sid,
                                         "hs_order_id": ledger.get(sid, ("", ""))[1],
                                         "ts": "", "detail": f"line item {r['id']} key {k}"})
                after = dig(data, "paging.next.after")
                if not after:
                    break
            if n and n % 10000 == 0:
                log.info("  deep %d / %d", n, len(salla_ids))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "salla_order_id", "hs_order_id", "ts", "detail"])
        w.writeheader()
        w.writerows(findings)
    counts = Counter(f["kind"] for f in findings)
    log.info("SUMMARY %s", dict(counts) or "no collisions found")
    log.info("distinct Salla orders affected: %d; distinct Zid orders touched: %d",
             len({f["salla_order_id"] for f in findings if f["kind"] in
                  ("ledger_on_zid", "partial_on_zid", "status_on_zid")}),
             len({f["hs_order_id"] for f in findings if f["hs_order_id"] in zid}))
    log.info("written: %s", OUT)


if __name__ == "__main__":
    main()
