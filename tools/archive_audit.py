#!/usr/bin/env python3
"""Archive old Order Audit Log rows into a copy of the workbook (v2.15).

The audit workbook sits at 75% of Google's 10,000,000-cell cap and the Order
Audit Log tab is 63% of it on its own (31 columns, every one used), so
trimming empty columns cannot buy much. This moves the oldest rows out:

  1. plan   (default, read-only) reads columns A (order id) and AD (written
            at) of the whole tab, finds the leading block of rows written
            before the cutoff (today minus --keep-days), and records the block
            in mirror/audit_archive.json with sample order ids for later checks.
  2. --copy makes a Drive copy of the whole workbook (the archive, a new id,
            private to the owning account) and checks the block's sample ids
            are at the same rows in the copy.
  3. --apply deletes the block from the original in one deleteDimension. The
            original keeps its id, so the engine, Make and the sheet's script
            need no change.

Why the block and not every old row: rows are appended in time order, so the
old rows are one run at the top. A row with no readable stamp inside that run
is old too (it sits between old rows); the run stops at the first row written
on or after the cutoff, and older rows further down are left alone.

Row numbers shift by the block size on delete. The engine writes back to an
audit row by number while it processes an order, and a drain pass maps order
ids to rows when it starts, so --apply refuses unless live sync and the queue
drain (service and timer) are stopped. Nothing else writes this tab by row
number, except three menu actions of the sheet's own script that save a row
cursor between runs (held approve, fill all, JSON all): nobody may run those
during the window. Old rows are not needed afterwards: the engine's duplicate
check reads only the last cfg.audit_dedup_rows rows, and an order with no audit
row gets a fresh one appended.

Rollback: the archive copy holds every deleted row, unchanged.

    python3 tools/archive_audit.py --config config.live.json                # plan
    python3 tools/archive_audit.py --config config.live.json --copy         # archive
    sudo systemctl stop salla-queue-drain.timer salla-live-sync.service    # then wait
    python3 tools/archive_audit.py --config config.live.json --apply        # delete
    sudo systemctl start salla-live-sync.service salla-queue-drain.timer
"""

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, GoogleIO, col_letter, setup_logging
from realtime_base import trim_lock_active
from tools.sheet_capacity import CELL_CAP, measure_workbook

log = logging.getLogger("backfill")

KEEP_DAYS = 90
PAGE_ROWS = 20000                   # rows per values.batchGet page
STAMP_COL = 29                      # AD: written at (now_str), see process_order
ID_COL = 0                          # A: Salla order id
SAMPLES = 25                        # order ids checked at fixed rows before copy/delete
PLAN_FILE = Path("mirror/audit_archive.json")
PLAN_MAX_AGE_H = 24                 # an older plan must be re-made before --copy/--apply
MUST_BE_STOPPED = ("salla-live-sync.service", "salla-queue-drain.service",
                   "salla-queue-drain.timer")
SHEETS_EPOCH = datetime(1899, 12, 30)
STAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                 "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S")


# ---- pure helpers (tested) --------------------------------------------------

def parse_stamp(v):
    """A column-AD value read UNFORMATTED: a Sheets serial number when the
    cell parsed as a date, the raw text when it did not. None when unreadable."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if v <= 0:
            return None
        return SHEETS_EPOCH + timedelta(days=float(v))
    s = str(v).strip()
    for fmt in STAMP_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def find_block(stamps, cutoff):
    """Length of the leading run of rows written before cutoff.

    stamps: parsed stamps in row order (None = unreadable). The run ends at the
    first stamp on or after the cutoff; trailing unreadable rows are dropped
    from it, so the last row of the block always carries an old stamp."""
    last_old = -1
    for i, ts in enumerate(stamps):
        if ts is None:
            continue
        if ts >= cutoff:
            break
        last_old = i
    return last_old + 1


def sample_positions(k, n=SAMPLES):
    """0-based offsets inside a block of k rows: first, last and evenly spaced."""
    if k <= 0:
        return []
    if k <= n:
        return list(range(k))
    step = (k - 1) / (n - 1)
    return sorted({round(i * step) for i in range(n)})


# ---- Google layer ------------------------------------------------------------

def tab_props(gio, sid, tab):
    meta = gio._gexec(gio.sheets.get(
        spreadsheetId=sid,
        fields="sheets.properties(sheetId,title,gridProperties(rowCount,columnCount))"),
        "archive meta", gio.sheets_rl)
    for s in meta.get("sheets") or []:
        p = s.get("properties") or {}
        if p.get("title") == tab:
            g = p.get("gridProperties") or {}
            return p["sheetId"], int(g.get("rowCount") or 0), int(g.get("columnCount") or 0)
    raise RuntimeError(f"tab {tab!r} not found in {sid}")


def read_columns(gio, sid, tab, last_row):
    """[(order_id, raw_stamp)] for rows 2..last_row, read in pages."""
    a, ad = col_letter(ID_COL), col_letter(STAMP_COL)
    out = []
    start = 2
    while start <= last_row:
        end = min(last_row, start + PAGE_ROWS - 1)
        resp = gio._gexec(gio.sheets.values().batchGet(
            spreadsheetId=sid, ranges=[f"'{tab}'!{a}{start}:{a}{end}",
                                       f"'{tab}'!{ad}{start}:{ad}{end}"],
            valueRenderOption="UNFORMATTED_VALUE", majorDimension="COLUMNS"),
            f"archive read {start}", gio.sheets_rl)
        cols = [((vr.get("values") or [[]])[0]) for vr in resp.get("valueRanges", [])]
        ids, raw = (cols + [[], []])[:2]
        n = end - start + 1
        ids = list(ids) + [""] * (n - len(ids))
        raw = list(raw) + [""] * (n - len(raw))
        out.extend(zip((str(x) for x in ids), raw))
        log.info("read rows %d-%d", start, end)
        start = end + 1
    return out


def ids_at(gio, sid, tab, rows):
    """Order ids (column A) at the given 1-based sheet rows."""
    a = col_letter(ID_COL)
    resp = gio._gexec(gio.sheets.values().batchGet(
        spreadsheetId=sid, ranges=[f"'{tab}'!{a}{r}" for r in rows],
        valueRenderOption="UNFORMATTED_VALUE"), "archive sample", gio.sheets_rl)
    return [str(((vr.get("values") or [[""]])[0] or [""])[0])
            for vr in resp.get("valueRanges", [])]


# ---- steps -------------------------------------------------------------------

def make_plan(gio, cfg, keep_days):
    sid, tab = cfg.spreadsheet_id, cfg.audit_tab
    sheet_id, row_count, col_count = tab_props(gio, sid, tab)
    rows = read_columns(gio, sid, tab, row_count)
    while rows and not rows[-1][0] and rows[-1][1] in ("", None):
        rows.pop()                                   # trailing empty grid rows
    cutoff = datetime.now() - timedelta(days=keep_days)
    stamps = [parse_stamp(raw) for _, raw in rows]
    k = find_block(stamps, cutoff)
    block = stamps[:k]
    old_later = sum(1 for ts in stamps[k:] if ts is not None and ts < cutoff)
    pos = sample_positions(k)
    wb = measure_workbook(gio, sid)
    plan = {
        "made_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "spreadsheet_id": sid, "tab": tab, "sheet_id": sheet_id,
        "keep_days": keep_days, "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "filled_rows": len(rows), "grid_rows": row_count, "cols": col_count,
        "block_rows": k,
        "block_first_written": min((t for t in block if t), default=None),
        "block_last_written": max((t for t in block if t), default=None),
        "block_unreadable_stamps": sum(1 for t in block if t is None),
        "old_rows_left_below_block": old_later,
        "samples": {str(p + 2): rows[p][0] for p in pos},   # sheet row -> order id
        "next_row_id": rows[k][0] if k < len(rows) else "",
        "cells_freed": k * col_count,
        "workbook_cells": wb["cells"],
        "workbook_pct_after": round(100.0 * (wb["cells"] - k * col_count) / CELL_CAP, 2),
        "workbook_pct_now": wb["pct"],
    }
    for key in ("block_first_written", "block_last_written"):
        if plan[key]:
            plan[key] = plan[key].strftime("%Y-%m-%d %H:%M:%S")
    by_month = {}
    for t in block:
        if t:
            by_month[t.strftime("%Y-%m")] = by_month.get(t.strftime("%Y-%m"), 0) + 1
    plan["block_by_month"] = dict(sorted(by_month.items()))
    return plan


def load_plan(cfg):
    if not PLAN_FILE.exists():
        raise SystemExit("No plan yet: run without --copy/--apply first.")
    plan = json.loads(PLAN_FILE.read_text(encoding="utf-8"))
    made = datetime.strptime(plan["made_at"], "%Y-%m-%d %H:%M:%S")
    if datetime.now() - made > timedelta(hours=PLAN_MAX_AGE_H):
        raise SystemExit(f"Plan is older than {PLAN_MAX_AGE_H} h: run the plan step again.")
    if plan["spreadsheet_id"] != cfg.spreadsheet_id or plan["tab"] != cfg.audit_tab:
        raise SystemExit("Plan was made for a different workbook or tab.")
    if plan["block_rows"] <= 0:
        raise SystemExit("Plan has nothing to archive.")
    return plan


def check_samples(gio, sid, plan, where):
    rows = sorted(int(r) for r in plan["samples"])
    got = ids_at(gio, sid, plan["tab"], rows)
    bad = [(r, plan["samples"][str(r)], g) for r, g in zip(rows, got)
           if g != plan["samples"][str(r)]]
    if bad:
        for r, want, g in bad[:5]:
            log.error("%s row %d: expected %s, found %s", where, r, want, g)
        raise SystemExit(f"{where}: {len(bad)} of {len(rows)} sample rows moved. Stop.")
    log.info("%s: %d sample rows match", where, len(rows))


def do_copy(gio, cfg, plan):
    check_samples(gio, cfg.spreadsheet_id, plan, "original")
    name = (f"ClaraHair Order Audit Archive (rows written before "
            f"{plan['cutoff'][:10]})")
    meta = gio._gexec(gio.drive.files().copy(
        fileId=cfg.spreadsheet_id, body={"name": name},
        fields="id,name,webViewLink", supportsAllDrives=True),
        "archive copy", gio.sheets_rl)
    log.info("archive created: %s (%s)", meta.get("name"), meta.get("id"))
    check_samples(gio, meta["id"], plan, "archive")
    plan["archive"] = {"id": meta["id"], "name": meta.get("name"),
                       "url": meta.get("webViewLink"),
                       "made_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    return plan


def stopped_units():
    """Units that must be stopped but are not (systemctl is-active)."""
    running = []
    for unit in MUST_BE_STOPPED:
        r = subprocess.run(["systemctl", "is-active", unit],
                           capture_output=True, text=True)
        if r.stdout.strip() in ("active", "activating", "reloading"):
            running.append(unit)
    return running


def do_apply(gio, cfg, plan):
    if not plan.get("archive"):
        raise SystemExit("No archive copy recorded: run --copy first.")
    running = stopped_units()
    if running:
        raise SystemExit("Still running, stop these first: " + ", ".join(running))
    if trim_lock_active():
        raise SystemExit("Another trim holds the trim lock. Stop.")
    check_samples(gio, cfg.spreadsheet_id, plan, "original")
    check_samples(gio, plan["archive"]["id"], plan, "archive")
    k = plan["block_rows"]
    _, before_rows, _ = tab_props(gio, cfg.spreadsheet_id, plan["tab"])
    gio._gexec(gio.sheets.batchUpdate(spreadsheetId=cfg.spreadsheet_id, body={
        "requests": [{"deleteDimension": {"range": {
            "sheetId": plan["sheet_id"], "dimension": "ROWS",
            "startIndex": 1, "endIndex": 1 + k}}}]}),
        "archive delete", gio.sheets_rl)
    _, after_rows, _ = tab_props(gio, cfg.spreadsheet_id, plan["tab"])
    first_now = ids_at(gio, cfg.spreadsheet_id, plan["tab"], [2])[0]
    ok = (before_rows - after_rows == k) and first_now == plan["next_row_id"]
    wb = measure_workbook(gio, cfg.spreadsheet_id)
    plan["applied"] = {"at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       "rows_deleted": before_rows - after_rows,
                       "row2_order_id": first_now, "verified": ok,
                       "workbook_pct": wb["pct"]}
    if not ok:
        log.error("Check after delete failed: rows %d -> %d, row 2 is %s (expected %s)",
                  before_rows, after_rows, first_now, plan["next_row_id"])
    return plan


def print_plan(plan):
    print()
    print("ORDER AUDIT LOG ARCHIVE PLAN")
    print("-" * 60)
    print(f"Keep rows written on or after {plan['cutoff'][:10]} ({plan['keep_days']} days)")
    print(f"Filled rows now           {plan['filled_rows']:>12,}")
    print(f"Rows to archive (block)   {plan['block_rows']:>12,}  "
          f"rows 2-{plan['block_rows'] + 1:,}")
    print(f"  written                 {plan['block_first_written']} to {plan['block_last_written']}")
    print(f"  with no readable stamp  {plan['block_unreadable_stamps']:>12,}")
    for month, n in plan["block_by_month"].items():
        print(f"    {month}              {n:>12,}")
    print(f"Old rows left below block {plan['old_rows_left_below_block']:>12,}")
    print(f"Cells freed               {plan['cells_freed']:>12,}")
    print(f"Workbook                  {plan['workbook_pct_now']}% -> {plan['workbook_pct_after']}%")
    if plan.get("archive"):
        print(f"Archive                   {plan['archive']['name']}")
        print(f"                          {plan['archive']['url']}")
    if plan.get("applied"):
        a = plan["applied"]
        print(f"Deleted                   {a['rows_deleted']:,} rows, verified={a['verified']}, "
              f"workbook now {a['workbook_pct']}%")
    print("-" * 60)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--keep-days", type=int, default=KEEP_DAYS)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--copy", action="store_true", help="make and check the archive copy")
    g.add_argument("--apply", action="store_true", help="delete the archived block")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="archive_audit.log")
    cfg = Config.load(args.config)
    gio = GoogleIO(cfg, enabled=True)
    if args.copy:
        plan = do_copy(gio, cfg, load_plan(cfg))
    elif args.apply:
        plan = do_apply(gio, cfg, load_plan(cfg))
    else:
        plan = make_plan(gio, cfg, args.keep_days)
    PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)
    PLAN_FILE.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
    print_plan(plan)
    if args.apply and not plan["applied"]["verified"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
