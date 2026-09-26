#!/usr/bin/env python3
"""Workbook cell capacity (v2.12).

Google caps a workbook at 10,000,000 cells, counted as rowCount x columnCount
of every tab whether the cells hold data or not. The queue workbook holds the
Live, Status and Customer queues; the Status Queue alone once reached 166k
rows x 26 columns (see tools/trim_queue.py). A full workbook refuses new rows,
so the Make capture appends and the engine's audit appends would start to
fail. This measures how close each workbook is, before that happens.

It reads spreadsheet METADATA only (sheets.properties, no cell values): one
spreadsheets.get per workbook through GoogleIO's own Sheets service, so it
uses the engine's OAuth token and its quota-aware retries. No new auth path.

    capacity(gio, cfg) -> [{"workbook", "spreadsheet_id", "cells", "pct",
                            "tabs": [{"tab", "rows", "cols", "cells"}]}]

Workbooks come from Config: queue_spreadsheet_id and spreadsheet_id (the
audit workbook); an empty id is skipped. Tabs are listed largest first. A
metadata read that fails raises: a workbook that could not be measured is
never reported as empty. report_digest imports the function directly:

    from tools.sheet_capacity import capacity

(tools/ is a package; reconcile.py imports tools.stage_resweep the same way.)
report_digest must call capacity() ONLY and never report(alert=True): the
digest shows the numbers, and the alert (with its cooldown state) belongs to
this CLI alone. measure_workbook(gio, sid) measures one workbook and is the
single cell-count implementation; tools/trim_queue.py uses it too.

CLI, read-only. The only write is the optional Slack alert, sent through
notify.send_alert when a workbook is above cfg.capacity_alert_pct:

    python3 tools/sheet_capacity.py                  # print the table
    python3 tools/sheet_capacity.py --alert          # and alert above the threshold

[v2.12] Alert cooldown, state in mirror/capacity_alert.json: at most one
alert per Riyadh calendar day, unless a workbook has since crossed into a
higher 5-point band (80-85, 85-90, ...) than the one it was last alerted at
that day. A workbook that goes over for the first time that day alerts too.
The state is written only after the alert went out. notify.send_alert never
raises: it returns the Slack message ts when the post landed and None when it
did not (Slack off or failing), so only a ts counts as sent, and an alert
that did not go out is tried again on the next run. The CLI loads .env first
(the Slack settings live there), as reconcile.py and gift_refresh.py do.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

log = logging.getLogger("backfill")

CELL_CAP = 10_000_000
#: (label, Config attribute holding the spreadsheet id), in report order
WORKBOOKS = (("Queue workbook", "queue_spreadsheet_id"),
             ("Audit workbook", "spreadsheet_id"))
META_FIELDS = "sheets.properties(title,gridProperties(rowCount,columnCount))"
ALERT_TABS = 5          # largest tabs listed per workbook in the alert thread
ALERT_STATE = Path("mirror/capacity_alert.json")
ALERT_BAND = 5.0        # percentage points; a higher band re-alerts the same day
RIYADH = timezone(timedelta(hours=3))


def _metadata(gio, sid):
    """spreadsheets.get(fields=sheets.properties) for one workbook."""
    sheets = gio.sheets
    if sheets is None:
        raise RuntimeError("Google access is off (GoogleIO enabled=False); "
                           "workbook capacity needs the Sheets metadata")
    request = sheets.get(spreadsheetId=sid, fields=META_FIELDS)
    gexec = getattr(gio, "_gexec", None)
    if gexec is None:                    # a bare Sheets client (tests, ad hoc)
        return request.execute()
    return gexec(request, "capacity meta", gio.sheets_rl)


def measure_workbook(gio, sid, label=""):
    """One workbook: {"workbook", "spreadsheet_id", "cells", "pct", "tabs"}.

    The one implementation of the cell count (capacity() and
    tools/trim_queue.py both use it). Tabs are listed largest first; pct is
    the workbook's share of CELL_CAP, rounded to two decimals."""
    meta = _metadata(gio, sid) or {}
    tabs = []
    for s in meta.get("sheets", []) or []:
        p = s.get("properties", {}) or {}
        g = p.get("gridProperties", {}) or {}   # absent on chart-only sheets
        rows = int(g.get("rowCount", 0) or 0)
        cols = int(g.get("columnCount", 0) or 0)
        tabs.append({"tab": p.get("title", ""), "rows": rows, "cols": cols,
                     "cells": rows * cols})
    tabs.sort(key=lambda t: t["cells"], reverse=True)
    cells = sum(t["cells"] for t in tabs)
    return {"workbook": label, "spreadsheet_id": sid, "cells": cells,
            "pct": round(100.0 * cells / CELL_CAP, 2), "tabs": tabs}


def capacity(gio, cfg):
    """Cells used per tab and per workbook against Google's 10M-cell cap.

    Returns one dict per configured workbook (see the module docstring)."""
    out, seen = [], set()
    for label, attr in WORKBOOKS:
        sid = str(getattr(cfg, attr, "") or "").strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        out.append(measure_workbook(gio, sid, label))
    return out


def over_threshold(results, threshold):
    """Workbooks strictly above `threshold` percent of the cap."""
    return [w for w in results if w["pct"] > float(threshold)]


def alert_message(over, threshold):
    """(subject, body) for notify.send_alert. Client-visible: plain sentences."""
    worst = max(over, key=lambda w: w["pct"])
    if len(over) == 1:
        subject = (f"🟡 The {worst['workbook']} in Google Sheets is "
                   f"{worst['pct']:.1f}% full.")
    else:
        subject = (f"🟡 {len(over)} Google Sheets workbooks are over "
                   f"{float(threshold):g}% full.")
    lines = [f"Google allows {CELL_CAP:,} cells in one workbook. "
             f"This alert fires above {float(threshold):g}%.", ""]
    for w in sorted(over, key=lambda w: w["pct"], reverse=True):
        lines.append(f"{w['workbook']}: {w['cells']:,} cells, {w['pct']:.1f}% full.")
        for t in [t for t in w["tabs"] if t["cells"] > 0][:ALERT_TABS]:
            lines.append(f"  {t['tab']}: {t['rows']:,} rows x {t['cols']} columns"
                         f" = {t['cells']:,} cells")
        lines.append("")
    lines.append("A full workbook takes no new rows, so new events would stop "
                 "landing in it. Trim old finished rows from the largest tab "
                 "to free space.")
    return subject, "\n".join(lines)


def render(results, threshold):
    """Plain-text table: one line per workbook, then its tabs indented."""
    head = f"{'WORKBOOK / TAB':<34}{'ROWS':>10}{'COLS':>7}{'CELLS':>14}{'OF CAP':>9}"
    lines = [head, "-" * len(head)]
    for w in results:
        flag = f"  over {float(threshold):g}%" if w["pct"] > float(threshold) else ""
        lines.append(f"{w['workbook'][:34]:<34}{'':>10}{'':>7}{w['cells']:>14,}"
                     f"{w['pct']:>8.1f}%{flag}")
        for t in w["tabs"]:
            lines.append(f"  {t['tab'][:32]:<32}{t['rows']:>10,}{t['cols']:>7,}"
                         f"{t['cells']:>14,}{100.0 * t['cells'] / CELL_CAP:>8.1f}%")
    lines.append(f"Google cap: {CELL_CAP:,} cells per workbook. "
                 f"Alert threshold: {float(threshold):g}%.")
    return "\n".join(lines)


def _band(pct):
    return int(float(pct) // ALERT_BAND)


def load_alert_state(path=None):
    """The cooldown state; {} when missing or unreadable (then an alert is due)."""
    try:
        d = json.loads(Path(path or ALERT_STATE).read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}


def alert_due(over, state, today):
    """True when this run should alert: no alert yet on `today` (a Riyadh
    'YYYY-MM-DD'), or a workbook over the threshold is in a higher 5-point
    band than the one it was last alerted at today (or was not alerted)."""
    if state.get("day") != today:
        return True
    bands = state.get("bands") if isinstance(state.get("bands"), dict) else {}
    for w in over:
        try:
            last = int(bands[w["spreadsheet_id"]])
        except (KeyError, TypeError, ValueError):
            return True
        if _band(w["pct"]) > last:
            return True
    return False


def save_alert_state(over, state, today, now, path=None):
    """Record the bands just alerted. Same day: keep the highest band seen per
    workbook, so a dip and a return to the same band does not re-alert."""
    path = Path(path or ALERT_STATE)
    same_day = state.get("day") == today and isinstance(state.get("bands"), dict)
    bands = dict(state["bands"]) if same_day else {}
    for w in over:
        try:
            prev = int(bands.get(w["spreadsheet_id"], -1))
        except (TypeError, ValueError):
            prev = -1
        bands[w["spreadsheet_id"]] = max(prev, _band(w["pct"]))
    new = {"day": today, "bands": bands,
           "pct": {w["spreadsheet_id"]: w["pct"] for w in over},
           "sent_at": now.isoformat(timespec="seconds")}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(new, indent=1, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
    except OSError as e:
        log.warning("capacity alert state not saved (%s): %s", path, e)


def report(results, cfg, alert=False, notifier=None, out=None, now=None,
           state_path=None):
    """Print the table; with alert=True send one alert when any workbook is
    above cfg.capacity_alert_pct, subject to the daily cooldown (module
    docstring). Returns the list of workbooks over it. Only the CLI calls
    this with alert=True; report_digest calls capacity() alone."""
    pct = getattr(cfg, "capacity_alert_pct", None)
    threshold = 80.0 if pct is None else float(pct)
    print(render(results, threshold), file=out or sys.stdout)
    over = over_threshold(results, threshold)
    for w in over:
        log.warning("CAPACITY %s at %.1f%% of the %s-cell cap (threshold %g%%)",
                    w["workbook"], w["pct"], f"{CELL_CAP:,}", threshold)
    if not (alert and over):
        return over
    subject, body = alert_message(over, threshold)
    if not getattr(cfg, "alerts_enabled", True):
        log.warning("ALERT (suppressed, alerts_enabled=false): %s", subject)
        return over
    now = (now or datetime.now(RIYADH)).astimezone(RIYADH)
    today = now.date().isoformat()
    state = load_alert_state(state_path)
    if not alert_due(over, state, today):
        log.info("capacity alert already sent today (%s) and no workbook reached "
                 "a higher %g-point band; not repeated", today, ALERT_BAND)
        return over
    try:
        if notifier is None:
            import notify as notifier
        ts = notifier.send_alert(subject, body)
    except Exception as e:
        log.warning("capacity alert failed: %s", e)
        return over
    if not ts:
        # [v2.12] send_alert swallows its own failures and returns None: no
        # Slack message went out, so no cooldown is recorded
        slack_on = getattr(notifier, "slack_enabled", None)
        why = ("Slack is not configured" if callable(slack_on) and not slack_on()
               else "the Slack post failed")
        log.warning("capacity alert not sent (%s); no cooldown recorded, the "
                    "next run tries again", why)
        return over
    save_alert_state(over, state, today, now, state_path)
    return over


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--alert", action="store_true",
                    help="send a Slack alert when a workbook is above "
                         "capacity_alert_pct (default: print only)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    import backfill
    backfill.setup_logging(args.verbose, logfile="sheet_capacity.log")
    # [v2.12] the Slack settings live in .env; a plain shell or a timer has
    # not loaded it, and without it --alert posts nothing
    from queue_drain import load_dotenv
    load_dotenv()
    cfg = backfill.Config.load(args.config)
    gio = backfill.GoogleIO(cfg, enabled=True)
    results = capacity(gio, cfg)
    if not results:
        sys.exit("no workbook configured (queue_spreadsheet_id, spreadsheet_id)")
    report(results, cfg, alert=args.alert)
    return 0


if __name__ == "__main__":
    main()
