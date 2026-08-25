#!/usr/bin/env python3
"""Build the Google Sheet Clara fills in to switch the warranty system on.

The sheet has one job: get a product_class and, for devices, a warranty term
out of a human, with as little friction as possible. Everything here follows
from that.

  * The class column is a real dropdown bound to HubSpot's own enum, so an
    answer cannot be a typo that fails on import.
  * Our proposal is PRE-SELECTED. A sheet of 198 empty dropdowns gets
    abandoned; a sheet that is 90% right and asks for corrections gets done.
  * Confidence sits in its own column, not appended to the value, because
    "device (92%)" is not a valid enum member and would break the import the
    moment someone forgot to strip it. The README asks for that column to be
    deleted before it comes back.
  * Rows are ordered by units sold, so the products that matter get attention
    while attention is still available.
  * Low-confidence rows are highlighted. That is where a human actually adds
    value; the rest is confirmation.

Confidence is the fraction of five independent classifiers that agreed, not a
number we invented. 5/5 is 100, 3/5 is 60.

Usage:
    python3 warranty_sheet.py --input /tmp/classified.json [--share EMAIL]
"""

import argparse
import json
import os
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

CLASSES = ["device", "consumable", "accessory", "bundle", "other"]

HEADERS = ["#", "Product", "SKU", "Units sold", "Product class",
           "Devices in this item", "Warranty months",
           "What is inside (components)", "Confidence % (DELETE THIS COLUMN)",
           "Era", "Flags / notes", "HubSpot ID"]

README_OLD = [
    ["ClaraHair - Product classification for the warranty system"],
    [""],
    ["WHY THIS SHEET EXISTS"],
    ["The warranty system creates a warranty record for every device a customer "
     "buys, starting the day their order is delivered."],
    ["It can only do that if it knows which products ARE devices, and how long "
     "each one is covered for."],
    ["Right now both of those fields are empty on all products, so the system "
     "creates nothing. Filling this sheet in switches it on."],
    [""],
    ["WHAT WE HAVE ALREADY DONE"],
    ["Every row on the 'Product classification' tab already has a suggested "
     "class, pre-selected in the dropdown."],
    ["The 'Confidence %' column shows how sure we are. It is the level of "
     "agreement between five independent reviews of your catalogue."],
    ["Rows highlighted in amber are the ones we were least sure about. If your "
     "time is short, check those first."],
    [""],
    ["WHAT WE NEED YOU TO DO - THREE STEPS"],
    [""],
    ["1. Check the 'Product class' column."],
    ["   Change any row we got wrong using the dropdown. The five options are "
     "the only values the system accepts:"],
    ["      device      - a powered appliance that carries a warranty "
     "(curlers, stylers, dryers, straighteners, heated brushes)"],
    ["      consumable  - runs out (shampoo, conditioner, masks, sprays, "
     "serums, creams, wax)"],
    ["      accessory   - unpowered item (plain brushes, combs, clips, bags, "
     "diffuser attachments)"],
    ["      bundle      - a set containing more than one item"],
    ["      other       - gift vouchers, test records, anything not a real "
     "sellable product"],
    [""],
    ["2. Fill in 'Warranty months' for EVERY row you have marked as 'device'."],
    ["   This is the length of cover in months, as a plain number: 12, 24, 36."],
    ["   A device with no warranty months is skipped by the system and "
     "reported, rather than being given an invented expiry date."],
    ["   Leave it blank for consumables and accessories."],
    [""],
    ["3. Delete the whole 'Confidence %' column before sending this back."],
    ["   It is our working note, not your data, and it must not reach HubSpot."],
    [""],
    ["IMPORTANT - THE 'CURRENTLY IN HUBSPOT' COLUMN"],
    ["Some products already show 24 months in that column. We did NOT put it "
     "there: a HubSpot automation stamps 24 onto every newly created product, "
     "including shampoos and gift sets."],
    ["Treat it as a value to be corrected, not as an answer."],
    ["Whatever you type in 'Warranty months' REPLACES it. If you leave "
     "'Warranty months' blank, we will CLEAR the existing value rather than "
     "let an automated 24 stand as your decision."],
    ["So: every device needs a number from you, and everything that is not a "
     "device should be left blank so the stray 24 gets removed."],
    [""],
    ["A NOTE ON BUNDLES"],
    ["A bundle containing a device still needs that device covered. Where you "
     "mark a row as 'bundle', the 'What is inside' column shows what we "
     "believe it contains."],
    ["Please correct that column if it is wrong, because it decides how many "
     "warranties a customer who buys the set actually gets."],
    [""],
    ["QUESTIONS"],
    ["Anything unclear, leave a comment on the cell and we will pick it up."],
]


README = [
    ["ClaraHair - product classification and warranty terms"],
    [""],
    ["WHAT THIS IS"],
    ["Every product in HubSpot, 199 of them, covering both what you sell today "
     "and the older catalogue recovered from the Zid order history."],
    ["We have already filled in every column. In most cases we are asking you "
     "to CONFIRM rather than to decide."],
    [""],
    ["WHY IT MATTERS"],
    ["The warranty system creates a record for every device a customer buys, "
     "starting the day their order is delivered, so support can see what a "
     "customer owns and whether it is still covered."],
    ["It cannot do any of that until every product says what it is and, for "
     "devices, how long it is covered."],
    ["The same answers release the historical import, which brings six years "
     "of past orders into HubSpot."],
    [""],
    ["WHAT WE NEED FROM YOU - THREE THINGS"],
    [""],
    ["1. CONFIRM THE 24 MONTH DEFAULT."],
    ["   Every device, and every set containing a device, is pre-filled with "
     "24 months."],
    ["   If 24 months is right as a general rule, you do not need to touch the "
     "column at all."],
    ["   Change only the rows where a specific device is covered for a "
     "different length. You can also change any individual term later without "
     "affecting warranties already issued."],
    [""],
    ["2. CHECK THE 'Product class' COLUMN."],
    ["   Correct anything we got wrong using the dropdown. The five values are "
     "the only ones the system accepts:"],
    ["      device      - a powered appliance carrying a warranty"],
    ["      consumable  - runs out (shampoo, spray, serum, cream, mousse)"],
    ["      accessory   - unpowered (plain brushes, combs, clips, bags, "
     "diffuser attachments)"],
    ["      bundle      - a set containing more than one item"],
    ["      other       - gift vouchers, test records"],
    [""],
    ["3. CHECK 'What is inside' FOR THE SETS."],
    ["   For every bundle we have worked out exactly which products it "
     "contains. This decides how many warranties a customer gets: a set with "
     "two devices should produce two."],
    ["   The 'Devices in this item' column shows only the parts that carry "
     "cover. Please correct any set where the contents look wrong."],
    [""],
    ["THEN DELETE THE 'Confidence %' COLUMN AND SEND IT BACK."],
    ["It is our working note, not your data, and it must not reach HubSpot."],
    [""],
    ["HOW TO READ THE SHEET"],
    ["Rows are ordered by how much each product has sold, so the ones that "
     "matter most are at the top."],
    ["Amber rows are the few we were genuinely unsure about. If your time is "
     "short, those are the ones to look at."],
    ["The 'Era' column says whether a product is part of the current catalogue "
     "or was recovered from the older Zid history."],
    [""],
    ["QUESTIONS"],
    ["Leave a comment on any cell and we will pick it up."],
]


def creds(token_path):
    return Credentials.from_authorized_user_file(str(token_path))


def build_sheet(rows, token_path, title, share=None):
    c = creds(token_path)
    sheets = build("sheets", "v4", credentials=c, cache_discovery=False)
    drive = build("drive", "v3", credentials=c, cache_discovery=False)

    ss = sheets.spreadsheets().create(body={
        "properties": {"title": title},
        "sheets": [
            {"properties": {"sheetId": 0, "title": "README",
                            "gridProperties": {"rowCount": len(README) + 5,
                                               "columnCount": 2}}},
            {"properties": {"sheetId": 1, "title": "Product classification",
                            "gridProperties": {"rowCount": len(rows) + 2,
                                               "columnCount": len(HEADERS),
                                               "frozenRowCount": 1}}},
        ],
    }).execute()
    sid = ss["spreadsheetId"]

    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=sid,
        body={"valueInputOption": "RAW", "data": [
            {"range": "README!A1", "values": README},
            {"range": "'Product classification'!A1",
             "values": [HEADERS] + rows},
        ]}).execute()

    n = len(rows)
    reqs = [
        # header
        {"repeatCell": {
            "range": {"sheetId": 1, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": .12, "green": .24, "blue": .35},
                "textFormat": {"bold": True, "fontSize": 10,
                               "foregroundColor": {"red": 1, "green": 1,
                                                   "blue": 1}},
                "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat"}},
        # the class dropdown, bound to HubSpot's enum
        {"setDataValidation": {
            "range": {"sheetId": 1, "startRowIndex": 1, "endRowIndex": n + 1,
                      "startColumnIndex": 4, "endColumnIndex": 5},
            "rule": {"condition": {"type": "ONE_OF_LIST",
                                   "values": [{"userEnteredValue": v}
                                              for v in CLASSES]},
                     "showCustomUi": True, "strict": True}}},
        # warranty months must be a positive number if present
        {"setDataValidation": {
            "range": {"sheetId": 1, "startRowIndex": 1, "endRowIndex": n + 1,
                      "startColumnIndex": 6, "endColumnIndex": 7},
            "rule": {"condition": {"type": "NUMBER_GREATER",
                                   "values": [{"userEnteredValue": "0"}]},
                     "inputMessage": "Months of cover, e.g. 12, 24. Devices "
                                     "only.",
                     "showCustomUi": False, "strict": False}}},
        # amber where the judges disagreed
        {"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": [{"sheetId": 1, "startRowIndex": 1,
                        "endRowIndex": n + 1, "startColumnIndex": 0,
                        "endColumnIndex": len(HEADERS)}],
            "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA",
                              "values": [{"userEnteredValue":
                                          "=AND($I2<>\"\",$I2<80)"}]},
                "format": {"backgroundColor": {"red": 1, "green": .95,
                                               "blue": .8}}}}}},
        # devices needing a term: highlight the empty warranty cell
        {"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": [{"sheetId": 1, "startRowIndex": 1,
                        "endRowIndex": n + 1, "startColumnIndex": 6,
                        "endColumnIndex": 7}],
            "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA",
                              "values": [{"userEnteredValue":
                                          "=AND(OR($E2=\"device\",$F2<>\"\"),$G2=\"\")"}]},
                "format": {"backgroundColor": {"red": 1, "green": .85,
                                               "blue": .85}}}}}},
        {"updateSheetProperties": {
            "properties": {"sheetId": 1,
                           "gridProperties": {"frozenRowCount": 1,
                                              "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,"
                      "gridProperties.frozenColumnCount"}},
        {"setBasicFilter": {"filter": {"range": {
            "sheetId": 1, "startRowIndex": 0, "endRowIndex": n + 1,
            "startColumnIndex": 0, "endColumnIndex": len(HEADERS)}}}},
        {"repeatCell": {
            "range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {"textFormat": {
                "bold": True, "fontSize": 14}}},
            "fields": "userEnteredFormat.textFormat"}},
    ]
    widths = [(0, 40), (1, 300), (2, 140), (3, 90), (4, 120), (5, 190),
              (6, 130), (7, 260), (8, 150), (9, 110), (10, 260), (11, 120)]
    for i, w in widths:
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": 1, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w}, "fields": "pixelSize"}})
    reqs.append({"updateDimensionProperties": {
        "range": {"sheetId": 0, "dimension": "COLUMNS", "startIndex": 0,
                  "endIndex": 1},
        "properties": {"pixelSize": 900}, "fields": "pixelSize"}})

    sheets.spreadsheets().batchUpdate(spreadsheetId=sid,
                                      body={"requests": reqs}).execute()

    if share:
        drive.permissions().create(
            fileId=sid, sendNotificationEmail=False,
            body={"type": "user", "role": "writer",
                  "emailAddress": share}).execute()
    return sid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/tmp/classified.json")
    ap.add_argument("--token", default="token.json")
    ap.add_argument("--title",
                    default="ClaraHair - Product classification & warranty terms")
    ap.add_argument("--share")
    args = ap.parse_args()

    data = json.loads(Path(args.input).read_text())
    rows = []
    for i, r in enumerate(data, 1):
        rows.append([
            i, r["name"], r["hs_sku"], r["units_sold"], r["product_class"],
            r.get("devices_inside", ""), r.get("warranty_months", ""),
            r.get("components", ""), r["confidence"],
            "Legacy (Zid)" if r.get("is_legacy") else "Current",
            r.get("note", ""), r["hubspot_id"],
        ])
    sid = build_sheet(rows, args.token, args.title, args.share)
    url = f"https://docs.google.com/spreadsheets/d/{sid}/edit"
    print(url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
