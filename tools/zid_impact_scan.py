#!/usr/bin/env python3
"""Measure what the Zid/Salla order-number collision touched (v2.12).

Read-only. Two passes over what tools/zid_collision_scan.py found:

  impact   for every Zid-import order a Salla order collided with: its current
           stage and sync status, its stage history, and every Device Warranty
           record on it (object 2-252148104, association 113) with its stage,
           origin, key and creation time, so warranties minted or voided by a
           Salla status event that landed on the Zid order can be told apart
           from the import's own
  idspace  every Salla order id the engine has seen anywhere (Status Queue
           sheet, Live Queue sheet and its trim archives, mirror ledgers,
           archived order JSON names) searched in HubSpot by salla_order_id,
           100 per call; any hit on a Zid-import order that the first scan did
           not already list is a collision that left no ledger trace

Writes mirror/zid_impact.csv and prints a summary. No HubSpot, Make or sheet
writes; no Salla or Make calls (the id space comes from local and sheet data).

    venv/bin/python3 tools/zid_impact_scan.py --config config.live.json
    venv/bin/python3 tools/zid_impact_scan.py --config config.live.json --no-idspace
"""

import argparse
import csv
import glob
import gzip
import io
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import (Config, GoogleIO, HubSpot, apply_portal_config, dig,
                      is_zid_order, setup_logging)

log = logging.getLogger("backfill")
SCAN = Path("mirror/zid_collision_scan.csv")
OUT = Path("mirror/zid_impact.csv")
WARRANTY = "2-252148104"
W_PROPS = ["warranty_key", "origin", "hs_pipeline_stage", "void_reason",
           "warranty_start_date", "warranty_end_date", "hs_createdate"]
O_PROPS = ["salla_order_id", "hs_order_name", "hs_pipeline_stage", "salla_store",
           "hs_source_store", "last_salla_sync_status", "delivery_date",
           "hs_lastmodifieddate"]
ARCHIVE_ID = re.compile(r"order_RID[^_]*_(\d+)_")


def zid_holders():
    rows = list(csv.DictReader(open(SCAN, newline="", encoding="utf-8")))
    out = {}
    for r in rows:
        if r["kind"] in ("ledger_on_zid", "partial_on_zid", "status_on_zid"):
            out.setdefault(r["hs_order_id"], set()).add(r["salla_order_id"])
    return out, rows


def impact(hs, holders):
    found = []
    for h in sorted(holders):
        st, o = hs._req("GET", f"/crm/v3/objects/orders/{h}?properties={','.join(O_PROPS)}"
                        "&propertiesWithHistory=hs_pipeline_stage", what="impact order")
        if st != 200:
            log.error("order %s: HTTP %s", h, st)
            continue
        p = o.get("properties") or {}
        if not is_zid_order(p):
            continue                      # a genuine Salla order: not a Zid holder
        hist = dig(o, "propertiesWithHistory.hs_pipeline_stage") or []
        found.append({"kind": "zid_holder", "hs_order_id": h,
                      "salla_order_id": ";".join(sorted(holders[h])),
                      "detail": (f"stage={p.get('hs_pipeline_stage')} sync={p.get('last_salla_sync_status')} "
                                 f"delivery={p.get('delivery_date')} history="
                                 + " | ".join(f"{x.get('timestamp','')[:19]} {x.get('value')} "
                                              f"{x.get('sourceType')}:{x.get('sourceId')}"
                                              for x in hist[:8]))})
        st, a = hs._req("GET", f"/crm/v4/objects/orders/{h}/associations/{WARRANTY}?limit=500",
                        what="impact warranty assoc")
        wids = [str(r.get("toObjectId")) for r in (a or {}).get("results") or []] if st == 200 else []
        if not wids:
            continue
        st, b = hs._req("POST", f"/crm/v3/objects/{WARRANTY}/batch/read",
                        {"properties": W_PROPS, "inputs": [{"id": w} for w in wids]},
                        what="impact warranty read")
        for r in (b or {}).get("results") or []:
            q = r.get("properties") or {}
            found.append({"kind": "zid_warranty", "hs_order_id": h,
                          "salla_order_id": ";".join(sorted(holders[h])),
                          "detail": (f"warranty {r['id']} key={q.get('warranty_key')} "
                                     f"stage={q.get('hs_pipeline_stage')} origin={q.get('origin')} "
                                     f"created={(q.get('hs_createdate') or '')[:19]} "
                                     f"void={q.get('void_reason') or ''} "
                                     f"start={q.get('warranty_start_date')} end={q.get('warranty_end_date')}")})
    return found


def sheet_ids(gio, qsid, tab):
    try:
        return {str(r.get("order_id")) for r in gio.queue_read_all(qsid, tab=tab) if r.get("order_id")}
    except Exception as e:
        log.warning("could not read %s: %s", tab, e)
        return set()


def archive_ids():
    ids = set()
    for f in glob.glob("mirror/archive/*.csv.gz"):
        try:
            with gzip.open(f, "rt", encoding="utf-8", errors="ignore") as g:
                for row in csv.reader(g):
                    if len(row) > 1 and row[1].isdigit():
                        ids.add(row[1])
        except Exception as e:
            log.warning("archive %s unreadable: %s", f, e)
    for f in glob.glob("archive/*.json"):
        m = ARCHIVE_ID.search(os.path.basename(f))
        if m:
            ids.add(m.group(1))
    return ids


def ledger_ids():
    ids = set()
    for path, col in (("mirror/created.csv", "salla_order_id"),
                      ("mirror/status_applied.csv", "order_id"),
                      ("mirror/errors.csv", "salla_order_id")):
        if Path(path).exists():
            with open(path, newline="", encoding="utf-8", errors="ignore") as f:
                ids |= {str(r.get(col)) for r in csv.DictReader(f) if str(r.get(col) or "").isdigit()}
    return ids


def idspace(cfg, hs, known):
    gio = GoogleIO(cfg, enabled=True)
    ids = (sheet_ids(gio, cfg.queue_spreadsheet_id, getattr(cfg, "status_queue_tab", "Status Queue"))
           | sheet_ids(gio, cfg.queue_spreadsheet_id, getattr(cfg, "live_queue_tab", "Live Queue"))
           | archive_ids() | ledger_ids())
    ids = sorted(i for i in ids if i.isdigit())
    log.info("idspace: %d distinct Salla order ids to check", len(ids))
    found = []
    for n in range(0, len(ids), 100):
        chunk = ids[n:n + 100]
        data = hs.search("/crm/v3/objects/orders/search", {
            "filterGroups": [{"filters": [{"propertyName": "salla_order_id",
                                           "operator": "IN", "values": chunk}]}],
            "properties": ["salla_order_id", "salla_store", "hs_source_store", "hs_order_name"],
            "limit": 100}, "idspace")
        for r in data.get("results") or []:
            p = r.get("properties") or {}
            if is_zid_order(p) and str(r["id"]) not in known:
                found.append({"kind": "idspace_zid_holder", "hs_order_id": str(r["id"]),
                              "salla_order_id": str(p.get("salla_order_id")),
                              "detail": p.get("hs_order_name", "")})
        if n and n % 20000 == 0:
            log.info("  idspace %d / %d", n, len(ids))
    return found, len(ids)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--no-idspace", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="zid_impact_scan.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not tok:
        sys.exit("HUBSPOT_ACCESS_TOKEN missing")
    if not SCAN.exists():
        sys.exit("run tools/zid_collision_scan.py first")
    hs = HubSpot(cfg, tok, live=False)
    holders, _ = zid_holders()
    rows = impact(hs, holders)
    zid = {r["hs_order_id"] for r in rows if r["kind"] == "zid_holder"}
    log.info("impact: %d Zid holders, %d warranty records on them", len(zid),
             sum(1 for r in rows if r["kind"] == "zid_warranty"))
    checked = 0
    if not args.no_idspace:
        extra, checked = idspace(cfg, hs, zid)
        rows += extra
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "hs_order_id", "salla_order_id", "detail"])
        w.writeheader()
        w.writerows(rows)
    log.info("SUMMARY %s (idspace ids checked: %d)", dict(Counter(r["kind"] for r in rows)), checked)
    log.info("written: %s", OUT)


if __name__ == "__main__":
    main()
