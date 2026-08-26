#!/usr/bin/env python3
"""Create the 40 products the verified sheet confirmed but HubSpot never had.

These are Salla-era bundle codes (C41CH11CH16, C19CH8, C51C26...) that sold on
real order lines yet have no product record, so those lines can never inherit
a classification. The client confirmed each one's identity, class and term on
the verified sheet, and several exist in Salla today as hidden products with a
NULL SKU, which is exactly why the sync never carried them across.

Created under their REAL codes, not LGCY-: the codes are canonical, the orders
carry them verbatim, and prefixing would break resolution.

Class and warranty term go IN the create call: a record born classified needs
no follow-up pass, and any future line item created against it inherits from
day one.

Idempotency tests EXISTENCE, not approval. The earlier check that tested
approval could not see unapproved twins and minted duplicates; that trap is
closed here.

    python3 tools/create_verified_missing.py            # dry run
    python3 tools/create_verified_missing.py --live
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")
LEDGER = Path("approvals") / "created_products.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--input", default="/tmp/to_create.json")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    setup_logging(False, logfile="create_verified_missing.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, token, live=args.live)

    rows = json.loads(Path(args.input).read_text())
    if args.live:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        new = not LEDGER.exists()
        lf = open(LEDGER, "a", newline="", encoding="utf-8")
        lw = csv.writer(lf)
        if new:
            lw.writerow(["ts", "hs_sku", "original_zid_sku", "hubspot_id", "name"])

    made = skipped = bad = 0
    for r in rows:
        sku = str(r.get("SKU") or "").strip()
        cls = str(r.get("Product class") or "").strip().lower()
        mo = str(r.get("Warranty months") or "").strip()
        if mo:
            mo = str(int(float(mo)))
        name = str(r.get("Product") or sku).strip()
        if not sku or cls not in ("device", "consumable", "accessory",
                                  "bundle", "other"):
            log.error("row %r: unusable sku/class, skipping", sku or name[:30])
            bad += 1
            continue
        # EXISTENCE check, any approval state, either spelling
        st, d = hs._req("POST", "/crm/v3/objects/products/search",
                        body={"filterGroups": [
                                {"filters": [{"propertyName": "hs_sku",
                                              "operator": "EQ", "value": v}]}
                                for v in (sku, f"LGCY-{sku}")],
                              "properties": ["hs_sku"], "limit": 1},
                        what=f"exists {sku}")
        if ((d or {}).get("total") or 0) > 0:
            log.info("%s already exists, skipping", sku)
            skipped += 1
            continue
        props = {"name": name, "hs_sku": sku,
                 "catalog_approval_status": "approved",
                 "product_class": cls,
                 "description": ("Created from the client-verified catalogue "
                                 "sheet of 2026-08-25. Sold on historical "
                                 "order lines; had no product record."),
                 }
        if mo:
            props["warranty_months"] = mo
        comps = str(r.get("What is inside (components)") or "").strip()
        if comps:
            props["description"] += f" Contents: {comps}."
        if not args.live:
            log.info("DRY RUN create %-18s class=%-10s months=%-3s %s",
                     sku, cls, mo or "-", name[:40])
            made += 1
            continue
        hid = hs.create_product(props, f"verified missing {sku}")
        if hid:
            made += 1
            lw.writerow([now_str(), sku, sku, hid, name[:60]])
            lf.flush()
            log.info("created %s -> %s", sku, hid)
        else:
            bad += 1
    if args.live:
        lf.close()
    log.info("%s: %d created, %d existed, %d unusable",
             "APPLIED" if args.live else "DRY RUN", made, skipped, bad)
    return 0


if __name__ == "__main__":
    sys.exit(main())
