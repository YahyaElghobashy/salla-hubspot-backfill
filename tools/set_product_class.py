#!/usr/bin/env python3
"""Set product_class / warranty_months on named HubSpot product records.

Why classes matter: a line item copies its product's product_class only at
the moment it is created, and two downstream systems key on it: the
warranty engine creates a warranty only for class 'device', and the
cross-sell signal layer flags has_purchased_device / has_purchased_consumable
from it. A selling product with no class therefore silently produces no
warranties and no buyer flags for every order created while it is blank.

The 2026-10-12 case: the relisted Multi Styler (Salla 1982906533, HubSpot
434196269276) had been approved on 22 Sep with no class, so its 8,281 orders
since then created no warranty; six new attachments carried a 24-month term
they should not have; CH12 shampoo was classed 'bundle'. This tool fixes the
product records; tools/restamp_line_class.py fixes lines already created.

Each change is written to mirror/product_class_changes.csv with the OLD
values first, so any change is reversed with one PATCH from the ledger.
Dry run by default.

    python3 tools/set_product_class.py --config config.live.json           # dry run
    python3 tools/set_product_class.py --config config.live.json --apply
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backfill import Config, HubSpot, now_str

LEDGER = Path("mirror/product_class_changes.csv")

# (hubspot product id, salla product id, label, product_class, warranty_months)
# warranty_months: a number to set, "" to clear, None to leave untouched.
# Conventions copied from the live catalog: attachments are 'accessory' with
# no term (as CM03/CM04/CM05/Air Diffuser), shampoos and sprays 'consumable'.
CHANGES = [
    ("434196269276", "1982906533", "Multi Styler kit C053_8/C052_8 (device + 8 attachments)", "device", None),
    ("433726829754", "2129093815", "Concentrator attachment CM01/C035", "accessory", ""),
    ("433761161403", "175059489", "Small Ceramic attachment CM06/CM10", "accessory", ""),
    ("433810325741", "1744514520", "MS small brush attachment CM08/CM12", "accessory", ""),
    ("433813958895", "681562003", "Auto-wrap attachment CM02/C036", "accessory", ""),
    ("433817538760", "1197745457", "MS big brush attachment CM09/CM13", "accessory", ""),
    ("433858454766", "2138484285", "Big Ceramic attachment CM07/CM11", "accessory", ""),
    ("419285673149", "2113977878", "CH12 Daily Hydrating Shampoo & Conditioner", "consumable", None),
]


def read(hs, pid):
    status, data = hs._req(
        "GET", f"/crm/v3/objects/products/{pid}?properties=name,salla_product_id,"
        "product_class,warranty_months,catalog_approval_status", what="product read")
    if status != 200:
        raise RuntimeError(f"product {pid}: HTTP {status}")
    return data.get("properties") or {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    cfg = Config.load(args.config)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, tok, live=args.apply)

    rows, failed = [], 0
    for pid, sid, label, cls, wm in CHANGES:
        p = read(hs, pid)
        if str(p.get("salla_product_id")) != sid:
            print(f"REFUSE {pid}: salla_product_id {p.get('salla_product_id')} != expected {sid}")
            failed += 1
            continue
        want = {"product_class": cls}
        if wm is not None:
            want["warranty_months"] = wm
        diff = {k: v for k, v in want.items() if (p.get(k) or "") != (v or "")}
        if not diff:
            print(f"OK     {pid} {label}: already {want}")
            continue
        before = {k: p.get(k) for k in diff}
        print(f"{'PATCH ' if args.apply else 'WOULD '} {pid} {label}: {before} -> {diff}")
        if not args.apply:
            continue
        status, resp = hs._write("PATCH", f"/crm/v3/objects/products/{pid}",
                                 {"properties": diff}, "set product class")
        after = read(hs, pid)
        ok = status in (200, 201) and all((after.get(k) or "") == (v or "") for k, v in diff.items())
        print(f"       readback {'MATCHES' if ok else 'MISMATCH'}: "
              f"{ {k: after.get(k) for k in diff} }")
        if not ok:
            failed += 1
        rows.append([now_str(), pid, sid, label, json.dumps(before, ensure_ascii=False),
                     json.dumps(diff, ensure_ascii=False), "ok" if ok else f"fail {status}"])
    if rows:
        new = not LEDGER.exists()
        LEDGER.parent.mkdir(exist_ok=True)
        with open(LEDGER, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "hs_product_id", "salla_product_id", "label",
                            "old_values", "new_values", "result"])
            w.writerows(rows)
        print(f"ledger: {LEDGER} (+{len(rows)})")
    if not args.apply:
        print("DRY RUN -- rerun with --apply to write.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
