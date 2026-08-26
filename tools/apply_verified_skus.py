#!/usr/bin/env python3
"""Apply the client's verified SKU corrections to the HubSpot catalogue.

From the VERIFIED sheet (2026-08-25) and its GUI checklist: seven products
carry a BARCODE as their SKU on Salla and Zid both, and the letter code is the
real one. The records are renamed rather than duplicated, so the catalogue
converges on the codes the client actually uses.

The Multi-Head Brush pair is the opposite case: two records that LOOKED like
duplicates are the product's two colour variants (Salla 2039567592 has a NULL
product-level SKU, variants C050 default / C029 Black). Both survive, each
gets its variant SKU, and the unapproved one gets approved. This reverses the
earlier plan to archive one of them, on the client's explicit finding.

Order matters portfolio-wide: these renames must land BEFORE the Zid corpus is
re-normalised and re-gated, because the alias direction flipped (barcodes now
fold INTO letter codes) and the gate can only match what the records carry.

    python3 tools/apply_verified_skus.py            # dry run
    python3 tools/apply_verified_skus.py --live
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
LEDGER = Path("approvals") / "sku_renames.csv"

# hubspot_id -> (new_sku, why)
RENAMES = {
    # ids resolved from the live snapshot, not assumed
    "419301615837": ("CH11", "Volumizing Mousse, was barcode 6287032431307"),
    "419244000451": ("CH10", "Hair Gloss Spray, was barcode 6287032431314"),
    "419285801201": ("CH09", "Dry Shampoo Spray, was barcode 6287032431321"),
    "419301257425": ("CH15", "Repair & Shine Gloss, was barcode 6287032432366"),
    "419249198305": ("CH14", "Hair Styling Wax, was barcode 6287032432144"),
    "419224993979": ("CP1", "Hair Perfume | C, was barcode 6287032431734"),
    "419285784791": ("CH16", "Flexible Setting Hairspray, was barcode 6287032432151"),
    "429785448673": ("C050", "Multi-Head Brush default variant, had no SKU"),
    "429789091060": ("C029", "Multi-Head Brush Black variant, had no SKU"),
}
APPROVE = {"429789091060"}      # the C029 twin was left unapproved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    setup_logging(False, logfile="apply_verified_skus.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, token, live=args.live)

    ids = list(RENAMES)
    st, data = hs._req("POST", "/crm/v3/objects/products/batch/read",
                       body={"inputs": [{"id": i} for i in ids],
                             "properties": ["hs_sku", "name",
                                            "catalog_approval_status"]},
                       what="rename read")
    if st != 200:
        sys.exit(f"batch read HTTP {st}")
    before = {str(r["id"]): (r.get("properties") or {})
              for r in data.get("results", [])}
    missing = [i for i in ids if i not in before]
    if missing:
        sys.exit(f"ids not found, refusing: {missing}")

    # a rename must not collide with a SKU already in use elsewhere
    for hid, (sku, _) in RENAMES.items():
        n = hs.gate_search_product_by_sku([sku])
        if n and str(before[hid].get("hs_sku") or "").strip().upper() != sku:
            sys.exit(f"target SKU {sku} already exists on another record; "
                     f"resolve that first")

    if args.live:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        new = not LEDGER.exists()
        lf = open(LEDGER, "a", newline="", encoding="utf-8")
        lw = csv.writer(lf)
        if new:
            lw.writerow(["ts", "hubspot_id", "was_sku", "now_sku",
                         "was_status", "now_status", "why"])

    changed = 0
    for hid, (sku, why) in RENAMES.items():
        prev = before[hid]
        props = {"hs_sku": sku}
        newst = prev.get("catalog_approval_status") or ""
        if hid in APPROVE and newst != "approved":
            props["catalog_approval_status"] = "approved"
            newst = "approved"
        if str(prev.get("hs_sku") or "").strip() == sku and \
                "catalog_approval_status" not in props:
            log.info("%s already %s, skipping", hid, sku)
            continue
        if not args.live:
            log.info("DRY RUN %s: sku %r -> %r  status %r -> %r  (%s)", hid,
                     prev.get("hs_sku"), sku,
                     prev.get("catalog_approval_status"), newst, why)
            changed += 1
            continue
        st, _ = hs._req("PATCH", f"/crm/v3/objects/products/{hid}",
                        body={"properties": props}, what=f"rename {sku}")
        if st in (200, 201):
            changed += 1
            log.info("renamed %s -> %s", hid, sku)
            lw.writerow([now_str(), hid, prev.get("hs_sku") or "", sku,
                         prev.get("catalog_approval_status") or "", newst, why])
            lf.flush()
        else:
            log.error("PATCH %s failed HTTP %s", hid, st)
    if args.live:
        lf.close()
    log.info("%s: %d records", "APPLIED" if args.live else "DRY RUN", changed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
