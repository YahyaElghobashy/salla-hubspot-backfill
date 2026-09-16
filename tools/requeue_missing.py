#!/usr/bin/env python3
"""Queue NAMED Salla order ids for the live engine to create.

For orders a reconciliation drill-down proved missing from HubSpot: instead
of a month-wide sweep (tens of thousands of scans to create a handful) or a
side-door create (which would bypass the catalog gate), this appends one
'queued' row per id to the Live Queue. The live engine then processes each
through its normal pipeline: gate, contact resolution, line items, ledger.
The engine's created-ledger, dedup search, and duplicate-400 guardrails make
a re-queue of an id that somehow already exists a harmless no-op.

Same mechanism relaunch_held_orders.py uses; this one differs only in where
the ids come from (you name them) and in refusing nothing: the point is
exactly to enqueue orders the queue has never seen.

Usage:
  python3 tools/requeue_missing.py --ids 123,456 ...            # DRY RUN
  python3 tools/requeue_missing.py --ids-file ids.txt --apply
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backfill import Config, GoogleIO, now_str


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--ids", default="", help="comma-separated Salla order ids")
    ap.add_argument("--ids-file", help="file with one Salla order id per line")
    ap.add_argument("--note", default="reconciliation drill-down: missing from HubSpot")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = ap.parse_args()

    ids = [i.strip() for i in args.ids.split(",") if i.strip()]
    if args.ids_file:
        with open(args.ids_file) as f:
            ids += [line.strip() for line in f if line.strip()]
    ids = sorted(set(ids))
    if not ids:
        sys.exit("no ids given")

    cfg = Config.load(args.config)
    print(f"{len(ids)} order(s) to queue -> Live Queue "
          f"({'APPLY' if args.apply else 'DRY RUN'})")
    if not args.apply:
        print("ids:", ", ".join(ids[:30]) + (" ..." if len(ids) > 30 else ""))
        return

    gio = GoogleIO(cfg, enabled=True)
    rows = [[now_str(), oid, "", "requeue", "queued", 0, "reconcile",
             args.note] for oid in ids]
    for k in range(0, len(rows), 500):
        gio.queue_append_rows(cfg.queue_spreadsheet_id, rows[k:k + 500])
    print(f"queued {len(rows)} row(s); the live engine picks them up within "
          f"its normal poll")


if __name__ == "__main__":
    main()
