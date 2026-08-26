#!/usr/bin/env python3
"""Archive the duplicate product records the verified pass confirmed.

Six records, every one a twin of a product that already exists under its real
SKU, every one confirmed to have ZERO line items attached before archiving.
HubSpot archive is a soft delete (restorable from the recycle bin for 90
days), and the ledger records what was archived and why, so the operation is
reversible twice over.

The Multi-Head Brush twin (429789091060) is deliberately NOT here: the client
identified it as the Black colour variant C029, so it survives and gets a SKU
instead. That reverses the earlier plan on his explicit finding.

    python3 tools/archive_duplicates.py            # dry run
    python3 tools/archive_duplicates.py --live
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")
LEDGER = Path("approvals") / "archived_products.csv"

# hs_sku -> survivor it duplicates
ARCHIVE = {
    "LGCY-CH14": "the barcode record being renamed to CH14",
    "LGCY-C6": "brushes",
    "LGCY-C011": "C11",
    "LGCY-C021": "C21",
    "LGCY-C18CH11C45C46": "C18CH11C45C46 (salla 1018965024)",
    "LGCY-C41CH7": "C41CH7 (salla 1209728408)",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    setup_logging(False, logfile="archive_duplicates.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, token, live=args.live)

    # resolve by SKU against the LIVE portal, not a snapshot
    targets = {}
    for sku in ARCHIVE:
        st, d = hs._req("POST", "/crm/v3/objects/products/search",
                        body={"filterGroups": [{"filters": [
                                {"propertyName": "hs_sku", "operator": "EQ",
                                 "value": sku}]}],
                              "properties": ["hs_sku", "name"], "limit": 2},
                        what=f"find {sku}")
        rs = (d or {}).get("results") or []
        if len(rs) != 1:
            log.warning("%s: found %d records, skipping", sku, len(rs))
            continue
        targets[sku] = rs[0]

    # hard gate: a record with line items is NOT a safe archive
    for sku, rec in list(targets.items()):
        st, d = hs._req("GET",
                        f"/crm/v4/objects/products/{rec['id']}/associations/line_items?limit=1",
                        what="li check")
        if (d or {}).get("results"):
            log.error("%s has line items attached; REFUSING to archive", sku)
            targets.pop(sku)

    if args.live:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        new = not LEDGER.exists()
        lf = open(LEDGER, "a", newline="", encoding="utf-8")
        lw = csv.writer(lf)
        if new:
            lw.writerow(["ts", "hubspot_id", "hs_sku", "name", "duplicate_of"])

    n = 0
    for sku, rec in targets.items():
        why = ARCHIVE[sku]
        if not args.live:
            log.info("DRY RUN archive %s (%s) duplicate of %s",
                     sku, rec["id"], why)
            n += 1
            continue
        st, _ = hs._req("DELETE", f"/crm/v3/objects/products/{rec['id']}",
                        what=f"archive {sku}")
        if st in (200, 204):
            n += 1
            log.info("archived %s (%s)", sku, rec["id"])
            lw.writerow([now_str(), rec["id"], sku,
                         (rec.get("properties") or {}).get("name", "")[:60], why])
            lf.flush()
        else:
            log.error("DELETE %s failed HTTP %s", sku, st)
    if args.live:
        lf.close()
    log.info("%s: %d of %d archived", "APPLIED" if args.live else "DRY RUN",
             n, len(ARCHIVE))
    return 0


if __name__ == "__main__":
    sys.exit(main())
