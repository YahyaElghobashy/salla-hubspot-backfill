#!/usr/bin/env python3
"""Replay Order Audit Log arrival rows lost to a Sheets outage (v2.12, one-time).

Between 23 and 25 Sep a Sheets 500/502/timeout failed the arrival append 13
times; the fallback append fired at once into the same outage and 2 rows
never reached the tab. The engine's local mirror (mirror/audit_mirror.csv)
recorded every intended write regardless, so the rows can be rebuilt from it.
GoogleIO.audit_append now backs off and checks before its fallback; this tool
repairs what was lost before that change.

What it does:
  1. refuses while a queue trim holds mirror/trim.lock
  2. reads mirror/audit_mirror.csv for --since..--until (both days included)
  3. reads column A of the audit tab once
  4. an order is a candidate when a mirror event for it has sheet row -1, or
     its id is absent from column A. A candidate already in column A (its
     append landed despite the error) is reported and never appended again
  5. with --apply, appends each missing arrival row through
     GoogleIO.audit_append (same row the engine would have written) and
     mirrors it as a replayed_append event
  6. records every decision in mirror/audit_replay.csv

An arrival with no Drive link and no later update event may come from a dry
run (dry runs mirror their arrivals with row -1 as well), so it is skipped
unless --include-unlinked. A replayed row keeps its arrival columns only:
updates the engine made after a -1 append never reached the sheet, and
update events mirrored before v2.12 carry no order id to rebuild them from.

Run from the app directory:
    python3 tools/audit_replay.py                                     # dry run
    python3 tools/audit_replay.py --since 2026-09-23 --until 2026-09-25 --apply
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import AUDIT_WIDTH, Config, GoogleIO, LocalMirror, now_str, setup_logging
from realtime_base import trim_lock_active

log = logging.getLogger("backfill")

DEFAULT_SINCE = "2026-09-23"
DEFAULT_UNTIL = "2026-09-25"
LEDGER_NAME = "audit_replay.csv"
LEDGER_HEADER = ["ts", "mode", "order_id", "reference", "arrived_at",
                 "mirror_sheet_row", "reason", "action", "sheet_row"]


def _is_rowless(d):
    return str(d.get("sheet_row", "")).strip() == "-1"


def _linked(d):
    """True when the arrival carries a Drive link: only a live run with
    Google enabled uploads the order JSON."""
    return str(d.get("c27", "")).strip().lower().startswith("http")


def collect(mirror_path, since, until):
    """Scan the mirror window. Returns (arrivals, rowless, updated,
    unattributed): the latest arrival event per order id, ids with any -1
    event, ids with any update event, and -1 update events with no id."""
    arrivals, rowless, updated, unattributed = {}, set(), set(), 0
    for d in LocalMirror.read_audit(mirror_path):
        day = str(d.get("ts", ""))[:10]
        if not (since <= day <= until):
            continue
        oid = d["order_id"]
        if not oid:
            if _is_rowless(d):
                unattributed += 1
            continue
        if d.get("event") == "arrived_append":
            arrivals[oid] = d
        elif d.get("event", "").endswith("_update"):
            updated.add(oid)
        if _is_rowless(d):
            rowless.add(oid)
    return arrivals, rowless, updated, unattributed


def _ledger_writer(path):
    new = not path.exists()
    f = open(path, "a", newline="")
    w = csv.writer(f)
    if new:
        w.writerow(LEDGER_HEADER)
    return f, w


def replay(cfg, gio, mirror_dir="mirror", since=DEFAULT_SINCE, until=DEFAULT_UNTIL,
           apply=False, include_unlinked=False):
    """Find and (with apply) append the missing arrival rows. Returns
    {action: count}."""
    mdir = Path(mirror_dir)
    arrivals, rowless, updated, unattributed = collect(
        mdir / "audit_mirror.csv", since, until)
    present = gio.audit_ids()
    log.info("mirror %s..%s: %d arrival(s), %d order(s) with a -1 row, %d "
             "-1 update(s) without an order id; audit tab column A: %d id(s)",
             since, until, len(arrivals), len(rowless), unattributed, len(present))

    mode = "apply" if apply else "dry-run"
    mirror = LocalMirror(mdir) if apply else None
    counts = {}
    f, w = _ledger_writer(mdir / LEDGER_NAME)
    try:
        def record(oid, d, reason, action, sheet_row=""):
            counts[action] = counts.get(action, 0) + 1
            w.writerow([now_str(), mode, oid, (d or {}).get("c1", ""),
                        (d or {}).get("ts", ""), (d or {}).get("sheet_row", ""),
                        reason, action, sheet_row])
            f.flush()

        for oid in sorted(rowless - set(arrivals)):
            log.warning("order %s: -1 update in range but no arrival event in "
                        "range; widen --since to replay it", oid)
            record(oid, None, "row -1", "no_arrival_in_range")

        candidates = [d for oid, d in arrivals.items()
                      if oid in rowless or oid not in present]
        candidates.sort(key=lambda d: (d.get("ts", ""), d["order_id"]))
        for d in candidates:
            oid = d["order_id"]
            reason = ", ".join(r for r, hit in (("row -1", oid in rowless),
                                                ("not in column A", oid not in present))
                               if hit)
            if oid in present:
                log.info("order %s: already in the audit tab at row %d (%s)",
                         oid, present[oid], reason)
                record(oid, d, reason, "present", present[oid])
                continue
            if not include_unlinked and not _linked(d) and oid not in updated:
                log.info("order %s: no Drive link and no later update; maybe a "
                         "dry run, skipped (--include-unlinked replays it)", oid)
                record(oid, d, reason, "skipped_unlinked")
                continue
            if not apply:
                log.info("WOULD APPEND order %s ref %s arrived %s (%s)",
                         oid, d.get("c1", ""), d.get("ts", ""), reason)
                record(oid, d, reason, "would_append")
                continue
            values = {i: d[f"c{i}"] for i in range(AUDIT_WIDTH) if d.get(f"c{i}", "") != ""}
            row = gio.audit_append(values)
            if row > 0:
                present[oid] = row
                mirror.audit_event("replayed_append", row, values, order_id=oid)
                log.info("APPENDED order %s at row %d", oid, row)
                record(oid, d, reason, "appended", row)
            else:
                log.error("order %s: append failed; rerun later", oid)
                record(oid, d, reason, "append_failed", -1)
    finally:
        f.close()
    log.info("%s: %s (ledger %s)", "APPLIED" if apply else "DRY RUN",
             ", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "nothing to do",
             mdir / LEDGER_NAME)
    return counts


def _day(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {s!r}")


def main(argv=None, gio=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--since", type=_day, default=DEFAULT_SINCE)
    ap.add_argument("--until", type=_day, default=DEFAULT_UNTIL)
    ap.add_argument("--mirror-dir", default="mirror")
    ap.add_argument("--include-unlinked", action="store_true",
                    help="also replay arrivals with no Drive link and no later update")
    ap.add_argument("--apply", action="store_true", help="append (default: dry run)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    if args.since > args.until:
        sys.exit(f"--since {args.since} is after --until {args.until}")
    if trim_lock_active():
        sys.exit("a queue trim is running (mirror/trim.lock): try again later")
    if not (Path(args.mirror_dir) / "audit_mirror.csv").exists():
        sys.exit(f"no {args.mirror_dir}/audit_mirror.csv here: run from the app directory")

    setup_logging(args.verbose, logfile="audit_replay.log")
    cfg = Config.load(args.config)
    gio = gio or GoogleIO(cfg, enabled=True)
    counts = replay(cfg, gio, args.mirror_dir, args.since, args.until,
                    apply=args.apply, include_unlinked=args.include_unlinked)
    return 1 if counts.get("append_failed") else 0


if __name__ == "__main__":
    sys.exit(main())
