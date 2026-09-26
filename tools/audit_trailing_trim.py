#!/usr/bin/env python3
"""Shrink the Order Audit Log grid to its data plus a margin (v2.12).

Google caps a workbook at 10,000,000 cells and counts EMPTY rows. The audit
tab carries tens of thousands of blank rows below its last filled row, each
31 cells wide. This deletes the blank rows below `last filled row + margin`.
Nothing that reads the tab depends on the grid size: the engine appends with
INSERT_ROWS (it finds the end of the data itself), the Apps Script menus use
getLastRow(), and the engine's own duplicate check walks up from the bottom
of the grid, so fewer blank rows make it more reliable, not less.

Read-only by default. Never touches a filled row: the cut starts below the
margin, and the tab is re-read right before the delete.

    python3 tools/audit_trailing_trim.py --config config.live.json
    python3 tools/audit_trailing_trim.py --config config.live.json --apply
"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, GoogleIO, setup_logging

log = logging.getLogger("backfill")


def grid(gio, sid, tab):
    meta = gio._gexec(gio.sheets.get(spreadsheetId=sid, fields="sheets.properties(sheetId,title,"
                                                                "gridProperties(rowCount,columnCount))"),
                      "audit grid", gio.sheets_rl)
    for s in meta.get("sheets") or []:
        p = s.get("properties") or {}
        if p.get("title") == tab:
            g = p.get("gridProperties") or {}
            return p["sheetId"], int(g.get("rowCount") or 0), int(g.get("columnCount") or 0)
    raise SystemExit(f"tab {tab!r} not found")


def last_filled(gio, sid, tab):
    v = gio._gexec(gio.sheets.values().get(spreadsheetId=sid, range=f"'{tab}'!A:A",
                                            majorDimension="COLUMNS"), "audit column A", gio.sheets_rl)
    col = (v.get("values") or [[]])[0]
    return max((i for i, x in enumerate(col, 1) if str(x).strip()), default=0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--margin", type=int, default=1000, help="blank rows to keep below the data")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="audit_trailing_trim.log")
    cfg = Config.load(args.config)
    gio = GoogleIO(cfg, enabled=True)
    sid, tab = cfg.spreadsheet_id, cfg.audit_tab
    sheet_id, rows, cols = grid(gio, sid, tab)
    last = last_filled(gio, sid, tab)
    keep = last + max(0, args.margin)
    log.info("%s: %d rows x %d cols = %s cells; last filled row %d; keep %d", tab, rows, cols,
             f"{rows * cols:,}", last, keep)
    if rows <= keep:
        log.info("nothing to delete")
        return
    freed = (rows - keep) * cols
    log.info("%s delete rows %d..%d (%d rows, %s cells)", "WOULD" if not args.apply else "WILL",
             keep + 1, rows, rows - keep, f"{freed:,}")
    if not args.apply:
        return
    # re-check right before the cut: the data must not have grown past the margin
    last2 = last_filled(gio, sid, tab)
    if last2 > last + args.margin // 2:
        raise SystemExit(f"data grew from row {last} to {last2} meanwhile; rerun")
    gio._gexec(gio.sheets.batchUpdate(spreadsheetId=sid, body={"requests": [{"deleteDimension": {
        "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": keep, "endIndex": rows}}}]}),
        "audit trailing delete", gio.sheets_rl)
    _, rows2, cols2 = grid(gio, sid, tab)
    log.info("DONE %s: %d -> %d rows (%s cells)", tab, rows, rows2, f"{rows2 * cols2:,}")


if __name__ == "__main__":
    main()
