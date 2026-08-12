#!/usr/bin/env python3
"""Create the client-approved legacy catalog records, then release the orders
they were holding.

Context: tens of thousands of Salla orders are held because their products
were DELETED from Salla. Those line items arrive with product = null, so the
id-based catalog gate can never pass them; only the SKU survives as identity.
The v2.5 gate resolves such items against "legacy records" -- products whose
hs_sku carries the LGCY- prefix and whose salla_product_id is deliberately
EMPTY, so nothing id-based can ever collide with a product the store creates
in Salla later.

This reads the approval sheet the client signed off and creates exactly those
records. It is deliberately narrow:

  * idempotent -- a SKU already present (bare or namespaced) is skipped, so
    re-running after a partial failure is safe
  * dry-run by default; --apply is required to write
  * every creation is appended to mirror/legacy_created.csv
  * SINGLE-PRODUCT SKUs ARE NEVER DECOMPOSED. C18C37 tokenises as C18 + C37
    but the client says it is one product; creating it as a bundle would put
    the wrong line items on ~790 orders. The sheet's `type` column is the
    authority, not the tokenizer.

Usage:
    python3 tools/approve_legacy.py --csv approvals.csv            # dry run
    python3 tools/approve_legacy.py --csv approvals.csv --apply
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")
LEDGER = Path("mirror/legacy_created.csv")


def rows_from(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sku = (row.get("original_salla_sku")
                   or row.get("original_zid_sku") or "").strip()
            if not sku:
                continue
            yield {
                "sku": sku,
                "hs_sku": (row.get("proposed_sku") or f"LGCY-{sku}").strip(),
                "name": (row.get("product_name") or "").strip(),
                "type": (row.get("type") or "").strip(),
                "price": (row.get("last_known_price_sar") or "").strip(),
                "est": (row.get("held_orders_released_est") or "").strip(),
                "approval": (row.get("approval (Yes/No)") or "").strip().lower(),
                "notes": (row.get("client_notes") or "").strip(),
            }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--require-approval", action="store_true",
                    help="only create rows whose approval column says yes")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="approve_legacy.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, token, live=args.apply)

    rows = list(rows_from(args.csv))
    log.info("sheet has %d row(s); mode=%s", len(rows),
             "APPLY" if args.apply else "DRY RUN")

    created = skipped = failed = 0
    est_release = 0
    LEDGER.parent.mkdir(exist_ok=True)
    new_ledger = not LEDGER.exists()
    fh = open(LEDGER, "a", newline="", encoding="utf-8") if args.apply else None
    w = csv.writer(fh) if fh else None
    if w and new_ledger:
        w.writerow(["ts", "item_sku", "hs_sku", "hubspot_id", "name", "price",
                    "type", "est_orders_released"])

    for r in rows:
        if args.require_approval and r["approval"] not in ("yes", "y"):
            log.info("SKIP %-18s not approved on the sheet", r["hs_sku"])
            continue
        # idempotency: either form already present means nothing to do
        if hs.gate_search_product_by_sku([r["sku"], r["hs_sku"]]) > 0:
            log.info("SKIP %-18s already in HubSpot", r["hs_sku"])
            skipped += 1
            continue
        props = {
            "name": r["name"] or r["hs_sku"],
            "hs_sku": r["hs_sku"],
            "catalog_approval_status": "approved",
            "description": (
                "Legacy catalog record: this product was deleted from Salla, "
                "so its historical orders carry a SKU but no product id. "
                f"Original SKU {r['sku']}. Client-approved "
                f"{datetime.now().strftime('%Y-%m-%d')}. Carries no "
                "salla_product_id, so it cannot collide with any product "
                "created in Salla later."),
        }
        if r["price"]:
            try:
                props["price"] = str(float(r["price"]))
            except ValueError:
                pass
        try:
            est_release += int(float(r["est"] or 0))
        except ValueError:
            pass

        if not args.apply:
            log.info("WOULD CREATE %-18s %-26s %s",
                     r["hs_sku"], (r["type"] or "?")[:24], r["name"][:44])
            created += 1
            continue
        hid = hs.create_product(props, f"legacy {r['hs_sku']}")
        if hid:
            created += 1
            log.info("CREATED %-18s -> %s  %s", r["hs_sku"], hid,
                     r["name"][:40])
            w.writerow([now_str(), r["sku"], r["hs_sku"], hid, r["name"],
                        props.get("price", ""), r["type"], r["est"]])
        else:
            failed += 1
            log.error("FAILED  %s", r["hs_sku"])
    if fh:
        fh.close()

    print(f"\n{'CREATED' if args.apply else 'WOULD CREATE'}: {created}"
          f"   skipped(existing): {skipped}   failed: {failed}")
    print(f"estimated held orders these release: ~{est_release:,}")
    if not args.apply:
        print("\nDRY RUN -- rerun with --apply to write to HubSpot.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
