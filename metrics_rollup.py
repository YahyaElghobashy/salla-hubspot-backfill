#!/usr/bin/env python3
"""Daily metrics rollup -- the memory that makes weekly/monthly reports possible.

Why this exists at all:

Order counts are DERIVABLE forever: mirror/created.csv and mirror/errors.csv are
append-only and timestamped, so "how many orders on 2026-03-14" is answerable in
2027. Most other numbers are not. Queue depth, availability, uptime, disk, the
backfill position -- these are point-in-time SAMPLES. live.log rotates.
credit_state.json is overwritten every five minutes. Nobody can answer "what was
average queue depth last month" unless something wrote it down each night.

So this runs once a day (23:58 Riyadh, via systemd timer) and appends one JSON
line per day to mirror/metrics_daily.jsonl. Small, human-readable, append-only.

Deliberately conservative about what it claims:
  * counts come from ledgers, never from logs (logs rotate, ledgers do not)
  * anything it cannot measure is written as null, never as 0 -- a missing
    measurement and a measured zero mean very different things in a report
  * re-running for a date replaces that date's line rather than duplicating it,
    so a retry after a failed timer is safe

Usage:
    python3 metrics_rollup.py                 # roll up today
    python3 metrics_rollup.py --date 2026-07-29
    python3 metrics_rollup.py --backfill-ledger  # rebuild past days (ledger-only
                                                 # fields; samples stay null)
"""

import argparse
import csv
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MIRROR = ROOT / "mirror"
ROLLUP = MIRROR / "metrics_daily.jsonl"
CREATED = MIRROR / "created.csv"
ERRORS = MIRROR / "errors.csv"
CREDIT_STATE = MIRROR / "credit_state.json"
BLOCKERS = MIRROR / "blocker_matrix.csv"
CURSOR = ROOT / "cursor.json"
LIVELOG = ROOT / "live.log"
DRAINLOG = ROOT / "drain.log"

SERVICES = ("salla-live-sync", "salla-credit-watch", "salla-slack-reporter")

log = logging.getLogger("rollup")

# "2026-07-29 20:12:54,429 INFO [main] QUEUE depth=2 oldest_age=17s processed_today=1204 lanes=0/2"
R_QUEUE = re.compile(
    r"^(\d{4}-\d\d-\d\d) (\d\d):(\d\d):\d\d.*QUEUE depth=(\d+) oldest_age=(\d+)s")
R_HELD = re.compile(r"^(\d{4}-\d\d-\d\d).*HELD order")
R_ALERT = re.compile(r"^(\d{4}-\d\d-\d\d) (\d\d:\d\d):\d\d.*(ALERT|RESOLVED): (.{0,80})")
# "DRAIN SUMMARY" block: "created=4124 blocked(still queued)=6434 ..."
R_DRAIN_BLOCKED = re.compile(r"blocked\(still queued\)=(\d+)")


def _ledger_counts(path, date, stage_field=None):
    """Rows in an append-only ledger whose ts falls on `date`.

    Ledgers are the one source that survives log rotation, so every count that
    ends up in a report comes from here rather than from a log line.
    """
    if not path.exists():
        # absent ledger is unmeasurable, not "zero" -- see the module docstring
        return (None, None) if stage_field else None
    total, by_stage = 0, {}
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                ts = (row.get("ts") or "")[:10]
                if ts != date:
                    continue
                total += 1
                if stage_field:
                    k = (row.get(stage_field) or "unknown").strip()
                    by_stage[k] = by_stage.get(k, 0) + 1
    except OSError as e:
        log.warning("ledger %s unreadable: %s", path.name, e)
        return (None, None) if stage_field else None
    return (total, by_stage) if stage_field else total


def _log_day(path, date):
    """Per-day observations parsed from a log.

    Returns None for every field when the log has no lines for `date` -- which
    happens on any day older than the current log file. That null is the honest
    answer and reports render it as "not recorded", never as zero.
    """
    out = {"queue_depth_max": None, "queue_depth_avg": None,
           "oldest_wait_s_max": None, "poll_minutes_ok": None,
           "availability_pct": None, "held_new": None, "alerts": []}
    if not path.exists():
        return out
    depths, waits, minutes, held, alerts = [], [], set(), 0, []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if not line.startswith(date):
                    continue
                m = R_QUEUE.match(line)
                if m:
                    depths.append(int(m.group(4)))
                    waits.append(int(m.group(5)))
                    minutes.add(f"{m.group(2)}:{m.group(3)}")
                    continue
                if R_HELD.match(line):
                    held += 1
                    continue
                a = R_ALERT.match(line)
                if a:
                    alerts.append({"t": a.group(2), "kind": a.group(3),
                                   "detail": a.group(4).strip()})
    except OSError as e:
        log.warning("log %s unreadable: %s", path.name, e)
        return out
    if not depths:
        return out
    out["queue_depth_max"] = max(depths)
    out["queue_depth_avg"] = round(sum(depths) / len(depths), 2)
    out["oldest_wait_s_max"] = max(waits)
    out["held_new"] = held
    out["alerts"] = alerts
    # Availability measured as "minutes in which the intake actually polled",
    # NOT "minutes the service was up". The 2026-07-23 outage had a perfectly
    # healthy systemd unit sitting on a dead relay for 68.9h; uptime would have
    # reported 100% while nothing synced. This metric would have reported ~0%.
    out["poll_minutes_ok"] = len(minutes)
    span = 1440 if datetime.now().strftime("%Y-%m-%d") != date \
        else max(1, datetime.now().hour * 60 + datetime.now().minute)
    out["availability_pct"] = round(100.0 * min(len(minutes), span) / span, 2)
    return out


def _services():
    """Restart counts and uptime per unit. Empty off-VM (no systemd)."""
    out = {}
    if not shutil.which("systemctl"):
        return out
    for unit in SERVICES:
        try:
            res = subprocess.run(
                ["systemctl", "show", unit, "--property",
                 "ActiveState,NRestarts,ActiveEnterTimestamp"],
                capture_output=True, text=True, timeout=5).stdout
            props = dict(l.split("=", 1) for l in res.splitlines() if "=" in l)
            up_s = None
            ts = props.get("ActiveEnterTimestamp", "")
            if ts:
                try:
                    t0 = datetime.strptime(" ".join(ts.split()[1:3]),
                                           "%Y-%m-%d %H:%M:%S")
                    up_s = int((datetime.now() - t0).total_seconds())
                except ValueError:
                    pass
            out[unit] = {"state": props.get("ActiveState", "unknown"),
                         "restarts": int(props.get("NRestarts", "0") or 0),
                         "uptime_s": up_s}
        except Exception:
            out[unit] = {"state": "unknown", "restarts": None, "uptime_s": None}
    return out


def _credits(date):
    """Credit figures for `date` from the watcher's own 62-day daily buckets."""
    out = {"remaining": None, "consumed_total": None, "burned": None,
           "by_engine": None, "plan": None, "next_reset": None}
    if not CREDIT_STATE.exists():
        return out
    try:
        st = json.loads(CREDIT_STATE.read_text())
    except (OSError, json.JSONDecodeError):
        return out
    org = st.get("org") or {}
    out["remaining"] = org.get("remaining")
    out["consumed_total"] = org.get("consumed")
    out["plan"] = org.get("plan")
    out["next_reset"] = (org.get("next_reset") or "")[:10] or None
    day = (st.get("daily") or {}).get(date)
    if day:
        out["burned"] = day.get("total")
        out["by_engine"] = {k: day.get(k, 0)
                            for k in ("backfill", "live", "other")}
    return out


def _held_total():
    """Last known catalog-held total, with the date it was measured.

    Only a drain scan knows this number -- it lives in the Queue Log sheet, not
    locally. Reporting it undated would imply it is current when it may be days
    old, so the measurement date travels with the value.
    """
    if not DRAINLOG.exists():
        return {"value": None, "as_of": None}
    try:
        last_date, last_val = None, None
        with open(DRAINLOG, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = R_DRAIN_BLOCKED.search(line)
                if m:
                    last_val = int(m.group(1))
                    last_date = line[:10]
        return {"value": last_val, "as_of": last_date}
    except OSError:
        return {"value": None, "as_of": None}


def _blockers():
    if not BLOCKERS.exists():
        return None
    try:
        with open(BLOCKERS, newline="", encoding="utf-8", errors="replace") as f:
            return max(0, sum(1 for _ in f) - 1)
    except OSError:
        return None


def _backfill():
    out = {"window": None, "status": None, "page": None, "total_pages": None}
    if not CURSOR.exists():
        return out
    try:
        c = json.loads(CURSOR.read_text())
        out["window"] = (c.get("from_date") or "")[:7] or None
        out["status"] = c.get("status")
        out["page"] = c.get("next_page")
        out["total_pages"] = c.get("total_pages")
    except (OSError, json.JSONDecodeError):
        pass
    return out


def _disk():
    try:
        st = os.statvfs(str(ROOT))
        total = st.f_blocks * st.f_frsize
        free = st.f_bavail * st.f_frsize
        return round(100.0 * (total - free) / total, 1) if total else None
    except OSError:
        return None


def collect(date, ledger_only=False):
    """Build one day's record. Unmeasurable fields stay null, never zero."""
    created = _ledger_counts(CREATED, date)
    err_total, err_by_stage = _ledger_counts(ERRORS, date, stage_field="stage")
    rec = {
        "date": date,
        "orders_created": created,
        "errors_total": err_total,
        # 'partial' is a data-quality flag on an order that already exists, not
        # a sync failure. Conflating the two would make a healthy day look bad.
        "partials_flagged": (err_by_stage.get("partial", 0)
                             if err_by_stage is not None else None),
        "errors_unrecovered": (sum(v for k, v in err_by_stage.items()
                                   if k != "partial")
                               if err_by_stage is not None else None),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if ledger_only:
        rec["source"] = "ledger-only"
        return rec
    rec.update(_log_day(LIVELOG, date))
    rec["credits"] = _credits(date)
    rec["services"] = _services()
    rec["backfill"] = _backfill()
    rec["held"] = _held_total()
    rec["blocker_products"] = _blockers()
    rec["disk_pct"] = _disk()
    rec["source"] = "full"
    return rec


def write(rec):
    """Append, replacing any existing line for the same date (idempotent retry)."""
    MIRROR.mkdir(exist_ok=True)
    rows = []
    if ROLLUP.exists():
        for line in ROLLUP.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                if json.loads(line).get("date") != rec["date"]:
                    rows.append(line)
            except json.JSONDecodeError:
                rows.append(line)          # keep unparseable lines, never drop data
    rows.append(json.dumps(rec, ensure_ascii=False))
    rows.sort(key=lambda l: json.loads(l).get("date", "") if l.startswith("{") else "")
    tmp = ROLLUP.with_suffix(".jsonl.tmp")
    tmp.write_text("\n".join(rows) + "\n", encoding="utf-8")
    tmp.replace(ROLLUP)                    # atomic; a crash never truncates history


def load(start=None, end=None):
    """Read rollup records within [start, end] inclusive (YYYY-MM-DD strings)."""
    if not ROLLUP.exists():
        return []
    out = []
    for line in ROLLUP.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        d = r.get("date", "")
        if (start and d < start) or (end and d > end):
            continue
        out.append(r)
    return sorted(out, key=lambda r: r.get("date", ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--backfill-ledger", action="store_true",
                    help="rebuild every day present in created.csv, ledger fields only")
    ap.add_argument("--print", dest="show", action="store_true",
                    help="print the record instead of writing it")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s [rollup] %(message)s")

    if args.backfill_ledger:
        dates = set()
        if CREATED.exists():
            with open(CREATED, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    d = (row.get("ts") or "")[:10]
                    if len(d) == 10:
                        dates.add(d)
        today = datetime.now().strftime("%Y-%m-%d")
        existing = {r["date"] for r in load() if r.get("source") == "full"}
        n = 0
        for d in sorted(dates):
            if d == today or d in existing:
                continue          # never overwrite a full record with a thin one
            write(collect(d, ledger_only=True))
            n += 1
        log.info("ledger backfill wrote %d day(s)", n)
        return

    date = args.date or datetime.now().strftime("%Y-%m-%d")
    rec = collect(date)
    if args.show:
        print(json.dumps(rec, indent=2, ensure_ascii=False))
        return
    write(rec)
    log.info("rolled up %s: %s orders, availability %s%%, %s errors",
             date, rec.get("orders_created"), rec.get("availability_pct"),
             rec.get("errors_unrecovered"))


if __name__ == "__main__":
    sys.exit(main())
