#!/usr/bin/env python3
"""Write Clara's completed classification back to HubSpot.

Without this the whole exercise is a spreadsheet. Nothing in the repo wrote
`product_class` or `warranty_months`: all three product creators build the same
props dict of name/hs_sku/catalog_approval_status/description/price, and the
only product PATCH writes catalog_approval_status alone.

Two design points that are not obvious:

1. This is a SEPARATE PATCH pass, not two extra keys in apply_approvals.
   apply_approvals skips on SKU match before it builds props, so bolting the
   fields on there would classify the records it creates and leave every
   record it skips permanently unreachable -- including the 21 that already
   existed and the 80 that predate this work entirely.

2. A BLANK warranty term CLEARS the value, it does not mean "leave alone".
   An automation stamps warranty_months=24 onto every newly created product,
   including shampoos, and the README promised Clara that whatever she types
   replaces it and blank clears it. Honouring "blank = skip" would quietly let
   an invented 24 stand as her decision on products she deliberately left
   empty.

Refusals, because a warranty written against bad input is worse than no
warranty: a class outside HubSpot's enum, a non-numeric term, or a device with
no term all fail the row and are reported rather than sent.

    python3 warranty_apply.py --sheet-id <id>            # dry run
    python3 warranty_apply.py --sheet-id <id> --live
    python3 warranty_apply.py --csv completed.csv --live
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")

CLASSES = {"device", "consumable", "accessory", "bundle", "other"}
LEDGER = Path("approvals") / "warranty_applied.csv"
TAB = "Product classification"


def from_sheet(sheet_id, token):
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    svc = build("sheets", "v4",
                credentials=Credentials.from_authorized_user_file(str(token)),
                cache_discovery=False)
    vals = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"'{TAB}'!A:L").execute().get("values", [])
    if not vals:
        raise SystemExit("sheet is empty")
    hdr = [h.strip() for h in vals[0]]
    return [dict(zip(hdr, r + [""] * (len(hdr) - len(r)))) for r in vals[1:]]


def from_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def column(row, *names):
    """The confidence column is meant to be deleted before this comes back, so
    positional reads are out; match on header text, tolerantly."""
    for n in names:
        for k, v in row.items():
            if k and k.strip().lower().startswith(n.lower()):
                return (v or "").strip()
    return ""


def plan(rows):
    todo, bad = [], []
    for i, r in enumerate(rows, 2):
        hid = column(r, "HubSpot ID")
        if not hid:
            continue
        cls = column(r, "Product class").lower()
        months = column(r, "Warranty months")
        name = column(r, "Product")
        if cls and cls not in CLASSES:
            bad.append((i, hid, name, f"class {cls!r} is not one of "
                                      f"{sorted(CLASSES)}"))
            continue
        if months:
            try:
                m = int(float(months))
                if m <= 0:
                    raise ValueError
                months = str(m)
            except ValueError:
                bad.append((i, hid, name,
                            f"warranty months {months!r} is not a positive "
                            f"whole number"))
                continue
        if cls == "device" and not months:
            bad.append((i, hid, name,
                        "class is device but no warranty term was given"))
            continue
        props = {"product_class": cls} if cls else {}
        # blank clears: see the module docstring
        props["warranty_months"] = months or ""
        todo.append((hid, name, props))
    return todo, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--sheet-id")
    ap.add_argument("--csv")
    ap.add_argument("--token", default="token.json")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="warranty_apply.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    if args.sheet_id:
        rows = from_sheet(args.sheet_id, args.token)
    elif args.csv:
        rows = from_csv(args.csv)
    else:
        sys.exit("pass --sheet-id or --csv")

    todo, bad = plan(rows)
    log.info("rows read %d, ready to write %d, refused %d",
             len(rows), len(todo), len(bad))
    for i, hid, name, why in bad:
        log.error("  row %d (%s) %s: %s", i, hid, name[:36], why)
    if bad:
        log.error("fix the sheet and re-run; nothing was written")
        return 1

    hs = HubSpot(cfg, token, live=args.live)
    # read current values first so the ledger can undo the change
    before = {}
    ids = [h for h, _, _ in todo]
    for i in range(0, len(ids), 100):
        st, data = hs._req(
            "POST", "/crm/v3/objects/products/batch/read",
            body={"inputs": [{"id": x} for x in ids[i:i + 100]],
                  "properties": ["product_class", "warranty_months", "hs_sku"]},
            what="warranty read")
        if st != 200:
            raise SystemExit(f"batch read HTTP {st}")
        for r in data.get("results", []):
            before[str(r["id"])] = r.get("properties") or {}

    changed = skipped = 0
    if args.live:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        new = not LEDGER.exists()
        lf = open(LEDGER, "a", newline="", encoding="utf-8")
        lw = csv.writer(lf)
        if new:
            lw.writerow(["ts", "hubspot_id", "hs_sku", "was_class",
                         "was_months", "now_class", "now_months"])
    for hid, name, props in todo:
        prev = before.get(str(hid), {})
        same = ((prev.get("product_class") or "") == (props.get("product_class") or "")
                and str(prev.get("warranty_months") or "") == props["warranty_months"])
        if same:
            skipped += 1
            continue
        if not args.live:
            log.info("DRY RUN %s %s: class %r->%r  months %r->%r", hid,
                     name[:30], prev.get("product_class"),
                     props.get("product_class"),
                     prev.get("warranty_months"), props["warranty_months"])
            changed += 1
            continue
        st, _ = hs._req("PATCH", f"/crm/v3/objects/products/{hid}",
                        body={"properties": props}, what=f"warranty {hid}")
        if st in (200, 201):
            changed += 1
            lw.writerow([now_str(), hid, prev.get("hs_sku", ""),
                         prev.get("product_class") or "",
                         prev.get("warranty_months") or "",
                         props.get("product_class", ""),
                         props["warranty_months"]])
            lf.flush()
        else:
            log.error("PATCH %s failed HTTP %s", hid, st)
    if args.live:
        lf.close()
    log.info("%s: %d changed, %d already correct",
             "APPLIED" if args.live else "DRY RUN", changed, skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
