#!/usr/bin/env python3
"""Create the missing parent product for named Salla bundles (v2.15).

Why: the Make scenario "Salla | Bundle Template Sync" (6243670), the one the
approval sheet's "Push to HubSpot" calls, first looks for a HubSpot product
whose salla_product_id is the BUNDLE's own Salla id and sets it inactive
(modules 1602 -> 1605 -> 1606). Since the scenario's 15 Jul 2026 edit that
step sits inline behind a "product exists" filter, so a bundle with no parent
product stops there: Make still answers "Accepted", the run logs "Success",
and no component records are ever built. The three Multi Styler bundles
below have no parent product, so their pushes have built nothing, and their
orders got no dryer line and no warranty.

The fix copies the pattern of a bundle that works (754169291, product
431539660995): a parent product created by the integration, with
catalog_approval_status "pending_review". The catalog gate counts APPROVED
products only, so a pending parent changes nothing for orders; the next push
finds it, sets it inactive (as it does for 754169291) and goes on to build the
components. No hs_sku is set: a duplicate hs_sku is what stopped Product
Updated for 23 days in August.

Refuses a bundle that already has a product with its salla_product_id. Every
created id goes to mirror/bundle_parent_products.csv; rollback is archiving
those ids. Dry run by default.

    python3 tools/create_bundle_parents.py --config config.live.json
    python3 tools/create_bundle_parents.py --config config.live.json --apply
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backfill import Config, HubSpot, now_str

LEDGER = Path("mirror/bundle_parent_products.csv")
STATUS = "pending_review"           # as 754169291's parent; the gate counts approved only

# (salla bundle id, name as on its HubSpot bundle template, price SAR)
BUNDLES = [
    ("1448268221", "الاستشوار متعدد الاستخدام و رغوة التصفيف و بخاخ التثبيت", "590"),
    ("770385112", "الاستشوار متعدد الاستخدام و كريم الترميم و الحماية و سيروم اللمعان", "595"),
    ("119765676", "الاستشوار متعدد الاستخدام و بخاخ مثبت الشعر", "540"),
]


def existing(hs, sid):
    data = hs.search("/crm/v3/objects/products/search", {
        "filterGroups": [{"filters": [{"propertyName": "salla_product_id",
                                       "operator": "EQ", "value": sid}]}],
        "properties": ["name", "salla_product_id", "catalog_approval_status"],
        "limit": 5}, "bundle parent lookup")
    return data.get("results") or []


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    cfg = Config.load(args.config)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, tok, live=args.apply)

    rows, failed = [], 0
    for sid, name, price in BUNDLES:
        found = existing(hs, sid)
        if found:
            print(f"SKIP   {sid}: parent product already exists "
                  f"({', '.join(r['id'] for r in found)})")
            continue
        props = {"name": name, "salla_product_id": sid, "price": price,
                 "catalog_approval_status": STATUS}
        print(f"{'CREATE' if args.apply else 'WOULD '} {sid}: {name} | {price} SAR | {STATUS}")
        if not args.apply:
            continue
        status, resp = hs._write("POST", "/crm/v3/objects/products",
                                 {"properties": props}, "create bundle parent")
        pid = str((resp or {}).get("id") or "")
        # the search index lags a create by a few seconds: retry before calling it failed
        back = []
        for wait in (0, 3, 6, 10):
            time.sleep(wait)
            back = existing(hs, sid) if pid else []
            if any(r.get("id") == pid for r in back):
                break
        ok = status in (200, 201) and any(r.get("id") == pid for r in back)
        print(f"       {'created ' + pid if ok else 'FAILED (HTTP %s)' % status}"
              f"{', readback matches' if ok else ''}")
        if not ok:
            failed += 1
        rows.append([now_str(), sid, pid, name, price, STATUS, "ok" if ok else f"fail {status}"])
    if rows:
        new = not LEDGER.exists()
        LEDGER.parent.mkdir(exist_ok=True)
        with open(LEDGER, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "salla_product_id", "hs_product_id", "name", "price",
                            "catalog_approval_status", "result"])
            w.writerows(rows)
        print(f"ledger: {LEDGER} (+{len(rows)})")
    if not args.apply:
        print("DRY RUN -- rerun with --apply to write.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
