#!/usr/bin/env python3
"""Relabel the Zid-to-Salla migration copies so they stop double-counting.

The Zid-to-Salla migration copied ~66k tail-end Zid orders into Salla's own
database with fabricated midnight-Riyadh timestamps. 7,186 of those copies
were later created in HubSpot as ordinary Salla-store records (drain waves,
5-25 Aug 2026) while their originals also arrived through the Zid import --
the same physical order exists twice, and any rollup that sums either store
double-counts that revenue.

Selector (verified 2026-09-14, exact): salla_store = "Salla" AND
hs_external_created_date before the 23 Feb 2026 store cut-over. A genuine
Salla-store order cannot predate the store; 7,181 of 7,186 selected records
carry the literal 21:00:00Z (midnight Riyadh) copy signature, and sampled
records fingerprint-match their Zid twins one-for-one (same phone, same
total, same day).

The fix is a RELABEL, not a delete: salla_store becomes "Zid (copy)", which
removes the record from both the "Salla" and the "Zid" rollup filters while
keeping the full record and its associations for audit. Reversible: the
ledger written before any patch records every (order, old, new) triple.

    venv/bin/python3 tools/relabel_zid_copies.py            # dry: select + report
    echo RUN | venv/bin/python3 tools/relabel_zid_copies.py --live

Resumable by construction: a relabeled record leaves the selector, so a
re-run patches only what remains. STOP.relabel halts between batches.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CUTOVER = "2026-02-23"
NEW_VALUE = "Zid (copy)"
LEDGER = Path("mirror/zid_copy_relabel.csv")
STOP_FILE = Path("STOP.relabel")


def api(tok, path, body=None):
    """One call with polite 429 handling: the account budget is shared with
    the live sync and any running sweep, so a throttle means wait, not fail."""
    for attempt in range(6):
        req = urllib.request.Request("https://api.hubapi.com" + path,
            data=json.dumps(body).encode() if body else None,
            headers={"Authorization": "Bearer " + tok,
                     "Content-Type": "application/json"},
            method="POST" if body else "GET")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 5:
                time.sleep(5 * (attempt + 1))
                continue
            raise
    raise RuntimeError("unreachable")


def cutover_ms():
    dt = datetime.fromisoformat(CUTOVER + "T00:00:00+00:00")
    return str(int(dt.timestamp() * 1000))


def select_copies(tok):
    filters = [
        {"propertyName": "salla_store", "operator": "EQ", "value": "Salla"},
        {"propertyName": "hs_external_created_date", "operator": "LT",
         "value": cutover_ms()},
    ]
    rows, midnight, after = [], 0, None
    while True:
        body = {"filterGroups": [{"filters": filters}],
                "properties": ["salla_order_id", "hs_external_created_date"],
                "sorts": [{"propertyName": "hs_object_id",
                           "direction": "ASCENDING"}],
                "limit": 200}
        if after:
            body["after"] = after
        d = api(tok, "/crm/v3/objects/orders/search", body)
        for x in d.get("results") or []:
            p = x.get("properties") or {}
            rows.append((str(p.get("salla_order_id") or ""), x["id"],
                         str(p.get("hs_external_created_date") or "")))
            if str(p.get("hs_external_created_date", "")).endswith("T21:00:00Z"):
                midnight += 1
        after = (d.get("paging") or {}).get("next", {}).get("after")
        time.sleep(1.2)   # leave search budget for the live sync + any sweep
        if not after:
            break
    return rows, midnight


def main():
    ap = argparse.ArgumentParser(description="Relabel Zid migration copies")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    print("selecting pre-cutover Salla-store records ...")
    rows, midnight = select_copies(tok)
    print(f"selected {len(rows)}; {midnight} carry the midnight copy signature")
    outliers = [(s, h, d) for s, h, d in rows if not d.endswith("T21:00:00Z")]
    for sid, hid, d in outliers:
        print(f"  non-midnight outlier (EXCLUDED, review manually): "
              f"salla {sid} hs {hid} date {d}")
    # only records carrying the copy signature are relabeled; a pre-cutover
    # order with a real intraday timestamp is likely a pre-launch test order,
    # not a migration copy, and is left for a human
    rows = [(s, h, d) for s, h, d in rows if d.endswith("T21:00:00Z")]

    if not rows:
        print("nothing to relabel -- selector is clean")
        return
    if not args.live:
        print(f"\nDRY RUN: would relabel {len(rows)} record(s) "
              f"salla_store -> {NEW_VALUE!r}. Re-run with --live.")
        return
    if input(f"LIVE: relabel {len(rows)} orders' salla_store to "
             f"{NEW_VALUE!r}. Type RUN: ").strip() != "RUN":
        sys.exit("Aborted.")

    # ledger BEFORE any write: this is the reversal recipe
    new = not LEDGER.exists()
    LEDGER.parent.mkdir(exist_ok=True)
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "salla_order_id", "hs_order_id",
                        "old_value", "new_value"])
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for sid, hid, _ in rows:
            w.writerow([ts, sid, hid, "Salla", NEW_VALUE])

    patched = 0
    for i in range(0, len(rows), 100):
        if STOP_FILE.exists():
            print(f"STOP.relabel present -- halting at {patched}")
            break
        inputs = [{"id": hid, "properties": {"salla_store": NEW_VALUE}}
                  for _, hid, _ in rows[i:i + 100]]
        api(tok, "/crm/v3/objects/orders/batch/update", {"inputs": inputs})
        patched += len(inputs)
        time.sleep(1.5)
        if patched % 1000 < 100:
            print(f"  {patched}/{len(rows)}")
    print(f"relabeled {patched}/{len(rows)}; ledger: {LEDGER}")


if __name__ == "__main__":
    main()
