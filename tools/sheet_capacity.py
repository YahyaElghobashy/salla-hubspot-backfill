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

CLI, read-only. The only write is the optional Slack alert, sent through
notify.send_alert when a workbook is above cfg.capacity_alert_pct:

    python3 tools/sheet_capacity.py                  # print the table
    python3 tools/sheet_capacity.py --alert          # and alert above the threshold
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

log = logging.getLogger("backfill")

CELL_CAP = 10_000_000
#: (label, Config attribute holding the spreadsheet id), in report order
WORKBOOKS = (("Queue workbook", "queue_spreadsheet_id"),
             ("Audit workbook", "spreadsheet_id"))
META_FIELDS = "sheets.properties(title,gridProperties(rowCount,columnCount))"
ALERT_TABS = 5          # largest tabs listed per workbook in the alert thread


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


def capacity(gio, cfg):
    """Cells used per tab and per workbook against Google's 10M-cell cap.

    Returns one dict per configured workbook (see the module docstring).
    pct is the workbook's share of CELL_CAP, rounded to two decimals."""
    out, seen = [], set()
    for label, attr in WORKBOOKS:
        sid = str(getattr(cfg, attr, "") or "").strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
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
        out.append({"workbook": label, "spreadsheet_id": sid, "cells": cells,
                    "pct": round(100.0 * cells / CELL_CAP, 2), "tabs": tabs})
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
        for t in w["tabs"][:ALERT_TABS]:
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


def report(results, cfg, alert=False, notifier=None, out=None):
    """Print the table; with alert=True send one alert when any workbook is
    above cfg.capacity_alert_pct. Returns the list of workbooks over it."""
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
    try:
        if notifier is None:
            import notify as notifier
        notifier.send_alert(subject, body)
    except Exception as e:
        log.warning("capacity alert failed: %s", e)
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
    cfg = backfill.Config.load(args.config)
    gio = backfill.GoogleIO(cfg, enabled=True)
    results = capacity(gio, cfg)
    if not results:
        sys.exit("no workbook configured (queue_spreadsheet_id, spreadsheet_id)")
    report(results, cfg, alert=args.alert)
    return 0


if __name__ == "__main__":
    main()
