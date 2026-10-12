#!/usr/bin/env python3
"""Re-stamp product_class (and optionally clear warranty_months) on line items
already created from a product whose class was wrong at the time.

HubSpot copies a product's product_class and warranty_months onto a line item
only when the line is created, so fixing the product record
(tools/set_product_class.py) fixes FUTURE lines only. Lines created while the
product was wrong keep the wrong value until re-stamped, and the warranty
sweep and cross-sell signal pass read the line, not the product.

Selection is by the line's own salla_product_id plus its CURRENT wrong value
(blank, or a named wrong class), so a re-run only touches lines still wrong
and the job converges; an interrupted run simply resumes. Every line's old
values are ledgered in mirror/line_class_restamp.csv before the batch that
changes it is sent, so the whole job can be reversed from the ledger.

    python3 tools/restamp_line_class.py --config config.live.json \
        --salla-product-id 1982906533 --to device --from-blank            # dry run
    ... --to accessory --from-blank --clear-warranty --apply
    ... --salla-product-id 2113977878 --to consumable --from-class bundle --apply
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backfill import Config, HubSpot, now_str

LEDGER = Path("mirror/line_class_restamp.csv")
STOP_FILE = Path("STOP.restamp")


def collect(hs, sid, from_blank, from_class):
    f = [{"propertyName": "salla_product_id", "operator": "EQ", "value": str(sid)}]
    if from_blank:
        f.append({"propertyName": "product_class", "operator": "NOT_HAS_PROPERTY"})
    else:
        f.append({"propertyName": "product_class", "operator": "EQ", "value": from_class})
    out, after = [], None
    while True:
        body = {"filterGroups": [{"filters": f}],
                "properties": ["product_class", "warranty_months", "hs_sku"],
                "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
                "limit": 200}
        if after:
            body["after"] = after
        data = hs.search("/crm/v3/objects/line_items/search", body, "restamp collect")
        if after is None and int(data.get("total") or 0) >= 10000:
            sys.exit(f"{data['total']} lines match: over the 10k search ceiling, "
                     "slice by createdate before running")
        for r in data.get("results") or []:
            p = r.get("properties") or {}
            out.append((r["id"], p.get("product_class"), p.get("warranty_months"), p.get("hs_sku")))
        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after:
            return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--salla-product-id", required=True)
    ap.add_argument("--to", required=True, choices=["device", "consumable", "accessory", "bundle", "other"])
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--from-blank", action="store_true")
    g.add_argument("--from-class")
    ap.add_argument("--clear-warranty", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, tok, live=args.apply)

    lines = collect(hs, args.salla_product_id, args.from_blank, args.from_class)
    props = {"product_class": args.to}
    if args.clear_warranty:
        props["warranty_months"] = ""
    print(f"{len(lines)} line(s) of salla product {args.salla_product_id} "
          f"({'blank' if args.from_blank else args.from_class}) -> {props}")
    if not args.apply:
        print("sample:", lines[:3])
        print("DRY RUN -- rerun with --apply to write.")
        return

    new = not LEDGER.exists()
    LEDGER.parent.mkdir(exist_ok=True)
    done = failed = 0
    with open(LEDGER, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["ts", "line_item_id", "salla_product_id", "sku",
                        "old_class", "old_warranty_months", "new_values", "result"])
        for i in range(0, len(lines), 100):
            if STOP_FILE.exists():
                print("STOP.restamp present -- halting cleanly"); break
            chunk = lines[i:i + 100]
            ts = now_str()
            # ledger the OLD values before the write, so a crash mid-batch
            # still leaves a reversal recipe
            for lid, oc, ow, sku in chunk:
                w.writerow([ts, lid, args.salla_product_id, sku, oc or "", ow or "",
                            json.dumps(props), "sent"])
            fh.flush()
            status, resp = hs._write(
                "POST", "/crm/v3/objects/line_items/batch/update",
                {"inputs": [{"id": lid, "properties": props} for lid, *_ in chunk]},
                f"restamp x{len(chunk)}")
            if status in (200, 201):
                done += len(chunk)
            else:
                failed += len(chunk)
                print(f"BATCH FAILED {status}: {json.dumps(resp)[:200]}")
            time.sleep(1.0)
            if (i // 100) % 10 == 0:
                print(f"  {done}/{len(lines)} re-stamped")
    print(f"done: {done} re-stamped, {failed} failed; ledger {LEDGER}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
