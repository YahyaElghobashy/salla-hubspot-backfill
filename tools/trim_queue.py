#!/usr/bin/env python3
"""One-off trim of a realtime queue tab (v2.11 capacity relief).

Google caps a workbook at 10,000,000 cells, and the queue workbook holds the
Live, Status and Customer queues. Only the Live Queue was ever trimmed (by the
live engine itself), so the Status Queue alone reached 166k rows x 26 columns.
This tool deletes old FINAL rows from one realtime tab while that tab's
consumer is stopped, archiving every deleted row first.

What it deletes (never anything else):
  Status Queue    done, superseded, error-final, gone   older than --keep-days
  Customer Queue  done, superseded                       older than --keep-days
Queued, blank, deferred, error and held rows are always kept, whatever age.

Order of operations, on --apply:
  1. refuse unless the tab's consumer is stopped (its J1 heartbeat is stale)
  2. take the trim lock (tools that hold row numbers refuse to run)
  3. write the consumer's state start_row=2 BEFORE deleting
  4. archive the rows to mirror/archive/<tab>-<ts>.csv.gz and check the count
  5. re-read and compare ids, then delete bottom-up in grouped ranges
  6. write start_row=2 again, release the lock

Then start the consumer again; it re-reads from row 2 and walks forward.

    python3 tools/trim_queue.py --tab "Status Queue" --keep-days 7            # dry run
    python3 tools/trim_queue.py --tab "Status Queue" --keep-days 7 --apply
"""

import argparse
import collections
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, GoogleIO, setup_logging
from realtime_base import trim_lock, trim_lock_active

log = logging.getLogger("backfill")

STREAMS = {
    "Status Queue": {"name": "status", "service": "salla-status-relay",
                     "deletable": ("done", "superseded", "error-final", "gone")},
    "Customer Queue": {"name": "customers", "service": "salla-customer-sync",
                       "deletable": ("done", "superseded")},
}
HEARTBEAT_QUIET_S = 150      # the consumer writes J1 every ~60 s while running


def workbook_cells(gio, qsid):
    meta = gio.sheets.get(spreadsheetId=qsid, fields="sheets.properties").execute()
    total = 0
    for s in meta.get("sheets", []):
        g = s["properties"].get("gridProperties", {})
        total += g.get("rowCount", 0) * g.get("columnCount", 0)
    return total


def write_state(name, start_row):
    p = Path(f"mirror/{name}_state.json")
    p.parent.mkdir(exist_ok=True)
    p.write_text(json.dumps({"start_row": start_row,
                             "ts": datetime.now().isoformat(timespec="seconds"),
                             "by": "tools/trim_queue.py"}))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--tab", required=True, choices=sorted(STREAMS))
    ap.add_argument("--keep-days", type=int, required=True)
    ap.add_argument("--apply", action="store_true", help="delete (default: dry run)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    if args.keep_days < 3:
        sys.exit("--keep-days below 3 is refused: recent rows are how problems get traced.")

    setup_logging(args.verbose, logfile="trim_queue.log")
    cfg = Config.load(args.config)
    gio = GoogleIO(cfg, enabled=True)
    qsid = cfg.queue_spreadsheet_id
    stream = STREAMS[args.tab]
    cutoff = datetime.now() - timedelta(days=args.keep_days)
    unparseable = [0]

    def keep(r):
        try:
            return datetime.strptime(str(r["received_at"])[:19], "%Y-%m-%d %H:%M:%S") >= cutoff
        except ValueError:
            unparseable[0] += 1
            return True

    rows = gio.queue_read_all(qsid, tab=args.tab)
    states = collections.Counter(r["status"] for r in rows)
    doomed = [r for r in rows if r["status"] in stream["deletable"] and not keep(r)]
    meta = gio.sheets.get(spreadsheetId=qsid, fields="sheets.properties").execute()
    cols = next(s["properties"]["gridProperties"]["columnCount"] for s in meta["sheets"]
                if s["properties"]["title"] == args.tab)
    before = workbook_cells(gio, qsid)
    log.info("%s: %d rows %s; would delete %d (older than %s, states %s); "
             "%d unparseable dates kept; ~%s cells freed; workbook %s cells now",
             args.tab, len(rows), dict(states), len(doomed), cutoff.strftime("%Y-%m-%d %H:%M"),
             "/".join(stream["deletable"]), unparseable[0], f"{len(doomed) * cols:,}", f"{before:,}")
    if not args.apply:
        log.info("DRY RUN -- nothing deleted. Stop %s first, then rerun with --apply.",
                 stream["service"])
        return

    hb = gio.queue_read_heartbeat(qsid, tab=args.tab)
    owner, _, epoch = hb.partition("|")
    try:
        age = time.time() - float(epoch)
    except ValueError:
        age = 1e9
    if age < HEARTBEAT_QUIET_S:
        sys.exit(f"{args.tab} consumer {owner or '?'} wrote its heartbeat {age:.0f}s ago: "
                 f"stop {stream['service']} first.")
    if trim_lock_active():
        sys.exit("another trim holds mirror/trim.lock")

    with trim_lock(args.tab):
        write_state(stream["name"], 2)
        try:
            n = gio.queue_trim(qsid, keep, tab=args.tab, deletable=stream["deletable"])
        finally:
            write_state(stream["name"], 2)
    after = workbook_cells(gio, qsid)
    log.info("DONE %s: deleted %d rows; workbook %s -> %s cells. Start %s again.",
             args.tab, n, f"{before:,}", f"{after:,}", stream["service"])


if __name__ == "__main__":
    main()
