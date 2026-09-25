#!/usr/bin/env python3
"""Set the state of named queue rows, verify-then-write (v2.11).

For the handful of rows a human decides about: e.g. four Status Queue rows
left `queued` since 10 Aug behind the status consumer's cursor, which should
be closed as `superseded` rather than applied six weeks late.

Every row is re-read first; a row is only written when its current state
matches --expect-state and its id and received_at still match what was read
(queue_mark's verify-then-write). Refuses to run while a trim holds the lock.

    python3 tools/queue_set_state.py --tab "Status Queue" --rows 212,223 \\
        --expect-state queued --state superseded --note "stale since 10 Aug"
    ... --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, GoogleIO, now_str, setup_logging
from realtime_base import trim_lock_active

log = logging.getLogger("backfill")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--tab", required=True)
    ap.add_argument("--rows", required=True, help="comma-separated sheet row numbers")
    ap.add_argument("--expect-state", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--note", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    setup_logging(False, logfile="queue_set_state.log")
    if trim_lock_active():
        sys.exit("a queue trim is running (mirror/trim.lock): try again later")
    cfg = Config.load(args.config)
    gio = GoogleIO(cfg, enabled=True)
    qsid = cfg.queue_spreadsheet_id
    wanted = [int(x) for x in args.rows.split(",") if x.strip()]
    done = 0
    for n in wanted:
        rows = gio.queue_read(qsid, start_row=n, tab=args.tab)[:1]
        if not rows or rows[0]["row"] != n:
            log.error("row %d: not found -- skipped", n)
            continue
        r = rows[0]
        if r["status"] != args.expect_state:
            log.error("row %d (%s): state is %r, expected %r -- skipped",
                      n, r["order_id"], r["status"], args.expect_state)
            continue
        log.info("%s row %d id %s received %s: %s -> %s",
                 "SET" if args.apply else "WOULD SET", n, r["order_id"],
                 r["received_at"], r["status"], args.state)
        if args.apply and gio.queue_mark(qsid, n, r["order_id"], args.state, r["attempts"],
                                         f"{args.note} ({now_str()[:10]})", tab=args.tab,
                                         expect_received_at=r["received_at"]):
            done += 1
    log.info("%s: %d of %d row(s)", "APPLIED" if args.apply else "DRY RUN", done, len(wanted))


if __name__ == "__main__":
    main()
