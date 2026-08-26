#!/usr/bin/env python3
"""Build the bundle expansion table from the client's verified workbook.

Zid bundle items import flat (product=None routes every item down the legacy
standalone path), and the warranty engine only creates cover for line items
whose class is 'device'. Flat, a bundle yields ZERO warranties: 220,383 orders
carry a device-bearing bundle, 366,890 devices between them.

The fix is to expand the bundle at NORMALISATION into a parent item plus
component items. This tool builds the table that expansion reads, from the
"What is inside (components)" column the client confirmed row by row.

Only DEVICE-BEARING bundles are expanded (138 of 162). A consumable-only
bundle earns no warranty by expanding, and the ~100k extra line items it would
mint count against portal object limits. That is a deliberate scope decision,
recorded here rather than made silently.

A bundle expands only if EVERY component resolves to an approved product in
the live catalogue; otherwise it stays flat and is listed in the report. Half
an expansion is worse than none: the money would split against components that
then hold.

    python3 tools/build_bundle_expansions.py \
        --xlsx "/path/to/VERIFIED.xlsx" [--snapshot mirror/snapshot/products_full.json]

Writes approvals/bundle_expansions.json:
    { "<bare bundle sku>": {"components": [{"sku","qty"}...], "devices": [...]} }
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import zed_normalize as zn

TOK = re.compile(r"^([A-Za-z0-9.]+)x(\d+)$")


def bare(s):
    s = zn.canon_sku(s)
    return s[5:] if s.startswith("LGCY-") else s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--snapshot", default="mirror/snapshot/products_full.json")
    ap.add_argument("--out", default="approvals/bundle_expansions.json")
    args = ap.parse_args()

    import openpyxl
    wb = openpyxl.load_workbook(args.xlsx, data_only=True)
    ws = wb["Product classification"]
    hdr = [str(c.value or "") for c in ws[1]]
    rows = [dict(zip(hdr, [x if x is not None else "" for x in r]))
            for r in ws.iter_rows(min_row=2, values_only=True)]

    # approved catalogue, indexed by bare SKU
    prods = json.loads(Path(args.snapshot).read_text())
    approved = {}
    for p in prods:
        pr = p["properties"]
        if (pr.get("catalog_approval_status") or "") != "approved":
            continue
        s = zn.canon_sku(pr.get("hs_sku"))
        if s:
            approved[bare(s)] = {"id": p["id"], "hs_sku": pr.get("hs_sku"),
                                 "class": (pr.get("product_class") or "").strip()}

    table, flat_consumable, unresolved, empty = {}, [], [], []
    for r in rows:
        if str(r.get("Product class") or "").strip().lower() != "bundle":
            continue
        sku = bare(r.get("SKU"))
        if not sku or sku in table:
            continue
        comps_raw = str(r.get("What is inside (components)") or "").strip()
        devices = [bare(d) for d in
                   str(r.get("Devices in this item") or "").split("+")
                   if d.strip() and d.strip() != "-"]
        if not devices:
            flat_consumable.append(sku)
            continue                      # consumable-only: stays flat
        if not comps_raw or comps_raw == "-":
            empty.append(sku)
            continue
        comps, bad = [], []
        for t in (x.strip() for x in comps_raw.split("+")):
            m = TOK.match(t)
            if not m:
                bad.append(t)
                continue
            csku = bare(m.group(1))
            if csku not in approved:
                bad.append(f"{csku} (no approved product)")
                continue
            comps.append({"sku": csku, "qty": int(m.group(2)),
                          "is_device": approved[csku]["class"] == "device"})
        if bad:
            unresolved.append((sku, bad))
            continue
        # the devices column and the component classes must AGREE, or the
        # decomposition is wrong somewhere and must not ship
        comp_devices = sorted(c["sku"] for c in comps for _ in range(1)
                              if c["is_device"])
        if sorted(set(devices)) != sorted(set(comp_devices)):
            unresolved.append((sku, [f"device mismatch: sheet says "
                                     f"{sorted(set(devices))}, component "
                                     f"classes say {sorted(set(comp_devices))}"]))
            continue
        table[sku] = {"components": comps,
                      "device_count": sum(c["qty"] for c in comps
                                          if c["is_device"])}

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(table, ensure_ascii=False, indent=1,
                              sort_keys=True))
    print(f"expandable device-bearing bundles : {len(table)}")
    print(f"consumable-only, stay flat        : {len(flat_consumable)}")
    print(f"empty components, stay flat       : {len(empty)}  {empty}")
    print(f"UNRESOLVED, stay flat             : {len(unresolved)}")
    for sku, bad in unresolved:
        print(f"   {sku:<22} {bad}")
    print(f"-> {out}")
    return 1 if unresolved else 0


if __name__ == "__main__":
    sys.exit(main())
