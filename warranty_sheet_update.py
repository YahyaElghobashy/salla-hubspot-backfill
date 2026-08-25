#!/usr/bin/env python3
"""Update an EXISTING classification sheet in place, keeping the owner's work.

Rebuilding the sheet from scratch each time a row is added throws away
whatever the person reviewing it has done: number formats, column widths,
colours, comments, and any answer already filled in. That is a bad trade for
the convenience of a clean write, and it gets worse the further into the
review you are.

So this writes VALUES only, into the existing grid, and never re-creates the
file. Formatting survives because nothing here touches it. New rows inherit
their look by copying the format of an existing data row rather than being
left unstyled.

Two things it will not do silently:
  * it refuses to run if the sheet's column headers do not match the data it
    was handed, because a shifted column would write warranty terms into the
    wrong field
  * it reports any answer already filled in that the new data would change,
    and keeps the human's value

    python3 warranty_sheet_update.py --sheet-id <id> --input rows.json
"""

import argparse
import json
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TAB = "Product classification"
CLASSES = ["device", "consumable", "accessory", "bundle", "other"]

EXPECTED = ["#", "Product", "SKU", "Units sold", "Product class",
            "Devices in this item", "Warranty months",
            "What is inside (components)", "Confidence % (DELETE THIS COLUMN)",
            "Era", "Flags / notes", "HubSpot ID"]


def row_for(i, r):
    return [i, r["name"], r["hs_sku"], r["units_sold"], r["product_class"],
            r.get("devices_inside", ""), r.get("warranty_months", ""),
            r.get("components", ""), r["confidence"],
            "Legacy (Zid)" if r.get("is_legacy") else "Current",
            r.get("note", ""), r["hubspot_id"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--token", default="token.json")
    args = ap.parse_args()

    rows = json.loads(Path(args.input).read_text())
    c = Credentials.from_authorized_user_file(args.token)
    svc = build("sheets", "v4", credentials=c, cache_discovery=False)
    ss = svc.spreadsheets()

    meta = ss.get(spreadsheetId=args.sheet_id).execute()
    tab = next((s for s in meta["sheets"]
                if s["properties"]["title"] == TAB), None)
    if tab is None:
        sys.exit(f"no tab named {TAB!r}")
    sid = tab["properties"]["sheetId"]
    grid = tab["properties"]["gridProperties"]

    cur = ss.values().get(spreadsheetId=args.sheet_id,
                          range=f"'{TAB}'!A:L").execute().get("values", [])
    hdr = [h.strip() for h in (cur[0] if cur else [])]
    if hdr != EXPECTED:
        sys.exit("column headers do not match; refusing to write.\n"
                 f"  sheet: {hdr}\n  expected: {EXPECTED}")

    # keep any answer a human has already given
    col = {h: i for i, h in enumerate(EXPECTED)}
    prev = {}
    for r in cur[1:]:
        r = list(r) + [""] * (len(EXPECTED) - len(r))
        prev[r[col["HubSpot ID"]]] = r
    kept = []
    for r in rows:
        old = prev.get(r["hubspot_id"])
        if not old:
            continue
        for field, key in (("Product class", "product_class"),
                           ("Warranty months", "warranty_months")):
            was = (old[col[field]] or "").strip()
            now = str(r.get(key) or "").strip()
            if was and was != now:
                kept.append((r["hs_sku"], field, now, was))
                r[key] = old[col[field]]
    for sku, f, mine, theirs in kept:
        print(f"  keeping your answer: {sku} {f} = {theirs!r} (not {mine!r})")

    n = len(rows)
    need = n + 1
    reqs = []
    if grid.get("rowCount", 0) < need:
        reqs.append({"appendDimension": {"sheetId": sid, "dimension": "ROWS",
                                         "length": need - grid["rowCount"]}})
    if reqs:
        ss.batchUpdate(spreadsheetId=args.sheet_id,
                       body={"requests": reqs}).execute()

    old_n = max(0, len(cur) - 1)
    if n > old_n and old_n >= 1:
        # new rows inherit whatever styling the owner applied to the existing
        # ones, rather than arriving unformatted
        ss.batchUpdate(spreadsheetId=args.sheet_id, body={"requests": [{
            "copyPaste": {
                "source": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": 2,
                           "startColumnIndex": 0, "endColumnIndex": len(EXPECTED)},
                "destination": {"sheetId": sid, "startRowIndex": old_n + 1,
                                "endRowIndex": n + 1, "startColumnIndex": 0,
                                "endColumnIndex": len(EXPECTED)},
                "pasteType": "PASTE_FORMAT"}}]}).execute()

    ss.values().update(
        spreadsheetId=args.sheet_id, range=f"'{TAB}'!A2",
        valueInputOption="RAW",
        body={"values": [row_for(i, r) for i, r in enumerate(rows, 1)]}).execute()

    if old_n > n:
        ss.values().clear(spreadsheetId=args.sheet_id,
                          range=f"'{TAB}'!A{n+2}:L{old_n+1}").execute()

    # validation has to cover the rows that did not exist before
    ss.batchUpdate(spreadsheetId=args.sheet_id, body={"requests": [
        {"setDataValidation": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": n + 1,
                      "startColumnIndex": 4, "endColumnIndex": 5},
            "rule": {"condition": {"type": "ONE_OF_LIST",
                                   "values": [{"userEnteredValue": v} for v in CLASSES]},
                     "showCustomUi": True, "strict": True}}},
        {"setDataValidation": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": n + 1,
                      "startColumnIndex": 6, "endColumnIndex": 7},
            "rule": {"condition": {"type": "NUMBER_GREATER",
                                   "values": [{"userEnteredValue": "0"}]},
                     "inputMessage": "Months of cover, e.g. 12, 24. Devices only.",
                     "strict": False}}},
        {"setBasicFilter": {"filter": {"range": {
            "sheetId": sid, "startRowIndex": 0, "endRowIndex": n + 1,
            "startColumnIndex": 0, "endColumnIndex": len(EXPECTED)}}}},
    ]}).execute()

    print(f"updated in place: {old_n} -> {n} rows, formatting preserved")
    print(f"https://docs.google.com/spreadsheets/d/{args.sheet_id}/edit")
    return 0


if __name__ == "__main__":
    sys.exit(main())
