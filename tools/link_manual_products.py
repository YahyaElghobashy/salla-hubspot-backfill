#!/usr/bin/env python3
"""Link hand-made, approved HubSpot products to their Salla product id.

On 2026-08-26 two "set" products were created by hand in HubSpot (approved,
hs_sku filled, salla_product_id empty) so the gate's SKU fallback [M211S]
would release their orders. That worked for the gate, but the Make scenario
"Salla | Product Updated" upserts by salla_product_id: with the link missing
it POSTs a second record carrying the same hs_sku, HubSpot rejects the write
because hs_sku is unique, and Make deactivates the scenario. That is exactly
the 2026-08-30 11:35 kill:

    [400] Cannot set PropertyValueCoordinates{... propertyName=hs_sku,
    value=CH91011C45} on 431077827799. 430591605992 already has that value.

Writing salla_product_id on the existing record makes the upsert PATCH it
instead of creating a duplicate. Approval status, SKU and price are untouched,
so the gate's answer for these products does not change.

Guards (each one refuses the row rather than guessing):
  * the HubSpot record must exist, be approved and carry the expected hs_sku
  * it must not already have a salla_product_id
  * no other product may already carry the Salla id

Every applied change is appended to mirror/manual_product_links.csv with the
old values, so it can be reverted with one PATCH per row.

    python3 tools/link_manual_products.py            # dry run, default
    python3 tools/link_manual_products.py --apply
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

LEDGER = Path("mirror/manual_product_links.csv")

# (hubspot_product_id, salla_product_id, expected hs_sku, human label)
LINKS = [
    ("430591605992", "1866446350", "CH91011C45",
     "Hair Styling Set / مجموعة تصفيف الشعر"),
    ("430636308724", "771366295", "CH4CH6CH7CH10CH15",
     "Eid care set / مجموعة عناية العيد"),
]

READ_PROPS = ("name,hs_sku,salla_product_id,catalog_approval_status,"
              "source_store,salla_admin_url")


def ledger_write(rows):
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "hubspot_product_id", "salla_product_id", "hs_sku",
                        "old_salla_product_id", "old_source_store",
                        "old_salla_admin_url", "label"])
        for r in rows:
            w.writerow([now_str()] + r)


def link_one(hs, hid, sid, sku, label, apply_):
    status, data = hs._req("GET", f"/crm/v3/objects/products/{hid}?properties={READ_PROPS}",
                           what="read manual product")
    if status != 200:
        log.error("  %s: HTTP %s reading the record -- refusing", hid, status)
        return None
    p = data.get("properties") or {}
    have_sku = (p.get("hs_sku") or "").strip()
    if have_sku.upper() != sku.upper():
        log.error("  %s: carries hs_sku %r, expected %r -- refusing", hid, have_sku, sku)
        return None
    if p.get("catalog_approval_status") != "approved":
        log.error("  %s: catalog_approval_status is %r, not approved -- refusing",
                  hid, p.get("catalog_approval_status"))
        return None
    old_sid = (p.get("salla_product_id") or "").strip()
    if old_sid == sid:
        log.info("  %s already linked to %s -- nothing to do", hid, sid)
        return None
    if old_sid:
        log.error("  %s: already linked to a different Salla id %r -- refusing", hid, old_sid)
        return None

    d = hs.search("/crm/v3/objects/products/search", {
        "filterGroups": [{"filters": [
            {"propertyName": "salla_product_id", "operator": "EQ", "value": sid}]}],
        "properties": ["name", "hs_sku"], "limit": 5}, "find products by salla id")
    others = d.get("results") or []
    if others:
        log.error("  %s: Salla id %s already on %s -- refusing",
                  hid, sid, [r["id"] for r in others])
        return None

    props = {"salla_product_id": sid, "source_store": "Salla",
             "salla_admin_url": f"https://s.salla.sa/products/{sid}"}
    log.info("  %s %s (%s) -> salla_product_id=%s  [%s]",
             "PATCH" if apply_ else "WOULD PATCH", hid, sku, sid, label)
    if not apply_:
        return None

    status, _ = hs._write("PATCH", f"/crm/v3/objects/products/{hid}",
                          {"properties": props}, "link manual product")
    if status not in (200, 201):
        log.error("        PATCH failed: HTTP %s", status)
        return None

    status, data = hs._req("GET", f"/crm/v3/objects/products/{hid}?properties={READ_PROPS}",
                           what="readback manual product")
    q = (data.get("properties") or {}) if status == 200 else {}
    ok = (q.get("salla_product_id") == sid and (q.get("hs_sku") or "") == have_sku
          and q.get("catalog_approval_status") == "approved")
    log.info("        readback: salla_product_id=%s hs_sku=%s approval=%s -> %s",
             q.get("salla_product_id"), q.get("hs_sku"), q.get("catalog_approval_status"),
             "OK" if ok else "MISMATCH")
    if not ok:
        return None
    return [hid, sid, have_sku, old_sid, p.get("source_store") or "",
            p.get("salla_admin_url") or "", label]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--apply", action="store_true", help="write to HubSpot (default: dry run)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="link_manual_products.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not tok:
        sys.exit("HUBSPOT_ACCESS_TOKEN missing")
    hs = HubSpot(cfg, tok, live=args.apply)

    log.info("%s -- %d link(s)", "APPLY" if args.apply else "DRY RUN", len(LINKS))
    rows = []
    for hid, sid, sku, label in LINKS:
        r = link_one(hs, hid, sid, sku, label, args.apply)
        if r:
            rows.append(r)
    if rows:
        ledger_write(rows)
        log.info("ledger: %s (+%d)", LEDGER, len(rows))
    log.info("done: %d linked", len(rows))


if __name__ == "__main__":
    main()
