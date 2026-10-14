"""v2.15 Order Audit Log archive (tools/archive_audit.py).

  * parse_stamp reads a Sheets serial number or the engine's own text stamp,
    and returns None for blanks and junk.
  * find_block takes only the leading run of old rows: it stops at the first
    row written on or after the cutoff, and never ends on an unreadable stamp.
  * The plan, copy and delete steps run against a fake workbook: the plan
    counts the block and records sample ids, the copy checks them in the
    archive, and the delete removes exactly the block and checks row 2.
  * --apply refuses while live sync or the drain still runs, without an
    archive, and when a sample row has moved since the plan.

Offline, against fakes. Run: python3 -m unittest test_archive_audit -v
"""
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from tools import archive_audit as aa


def serial(dt):
    return (dt - aa.SHEETS_EPOCH).total_seconds() / 86400.0


class FakeRequest:
    def __init__(self, fn):
        self.fn = fn

    def execute(self):
        return self.fn()


class FakeBook:
    """One workbook: {tab: list of rows (lists)} with a grid row count."""

    def __init__(self, rows, tab="Order Audit Log", cols=31):
        self.tab, self.cols = tab, cols
        self.rows = [list(r) for r in rows]          # data rows (sheet row 2..)

    def cell(self, sheet_row, col):
        i = sheet_row - 2
        if 0 <= i < len(self.rows):
            r = self.rows[i]
            return r[col] if col < len(r) else ""
        return ""


class FakeSheets:
    def __init__(self, books):
        self.books = books                            # sid -> FakeBook
        self.deletes = []

    def get(self, spreadsheetId, fields=None):
        b = self.books[spreadsheetId]
        return FakeRequest(lambda: {"sheets": [{"properties": {
            "sheetId": 7, "title": b.tab,
            "gridProperties": {"rowCount": len(b.rows) + 1 + 5,
                               "columnCount": b.cols}}}]})

    def values(self):
        return self

    def batchGet(self, spreadsheetId, ranges, valueRenderOption=None,
                 majorDimension="ROWS"):
        b = self.books[spreadsheetId]

        def run():
            out = []
            for rng in ranges:
                _, ref = rng.split("!")
                parts = ref.split(":")
                col = 0 if parts[0].startswith("A") and not parts[0].startswith("AD") else 29
                r0 = int("".join(ch for ch in parts[0] if ch.isdigit()))
                r1 = int("".join(ch for ch in parts[-1] if ch.isdigit()))
                vals = [b.cell(r, col) for r in range(r0, r1 + 1)]
                if majorDimension == "COLUMNS":
                    out.append({"values": [vals]})
                else:
                    out.append({"values": [[v] for v in vals]})
            return {"valueRanges": out}
        return FakeRequest(run)

    def batchUpdate(self, spreadsheetId, body):
        b = self.books[spreadsheetId]

        def run():
            for req in body["requests"]:
                r = req["deleteDimension"]["range"]
                self.deletes.append((spreadsheetId, r["startIndex"], r["endIndex"]))
                del b.rows[r["startIndex"] - 1:r["endIndex"] - 1]
            return {}
        return FakeRequest(run)


class FakeDrive:
    def __init__(self, sheets):
        self.sheets = sheets

    def files(self):
        return self

    def copy(self, fileId, body, fields=None, supportsAllDrives=None):
        def run():
            src = self.sheets.books[fileId]
            self.sheets.books["ARCH"] = FakeBook(src.rows, src.tab, src.cols)
            return {"id": "ARCH", "name": body["name"], "webViewLink": "https://x/ARCH"}
        return FakeRequest(run)


class FakeGio:
    def __init__(self, books):
        self.sheets = FakeSheets(books)
        self.drive = FakeDrive(self.sheets)
        self.sheets_rl = None

    def _gexec(self, request, what, limiter):
        return request.execute()


def row(oid, written):
    r = [""] * 31
    r[0] = oid
    r[29] = written
    return r


class Cfg:
    spreadsheet_id = "LIVE"
    audit_tab = "Order Audit Log"


class Helpers(unittest.TestCase):
    def test_parse_stamp(self):
        dt = datetime(2026, 7, 1, 12, 30)
        self.assertEqual(aa.parse_stamp(serial(dt)).replace(microsecond=0), dt)
        self.assertEqual(aa.parse_stamp("2026-07-01 12:30:00"), dt)
        for junk in ("", None, "Drive upload failed", 0, -3, True):
            self.assertIsNone(aa.parse_stamp(junk), junk)

    def test_block_stops_at_first_new_row(self):
        cut = datetime(2026, 7, 16)
        old, new = datetime(2026, 7, 1), datetime(2026, 8, 1)
        self.assertEqual(aa.find_block([old, None, old, new, old], cut), 3)
        self.assertEqual(aa.find_block([old, old, None, None, new], cut), 2)
        self.assertEqual(aa.find_block([new, old], cut), 0)
        self.assertEqual(aa.find_block([None, None], cut), 0)
        self.assertEqual(aa.find_block([], cut), 0)

    def test_samples_cover_first_and_last(self):
        p = aa.sample_positions(1000)
        self.assertEqual((p[0], p[-1]), (0, 999))
        self.assertLessEqual(len(p), aa.SAMPLES)
        self.assertEqual(aa.sample_positions(3), [0, 1, 2])
        self.assertEqual(aa.sample_positions(0), [])


class Flow(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        now = datetime.now()
        old = [row(f"O{i}", serial(now - timedelta(days=200 - i))) for i in range(50)]
        new = [row(f"N{i}", (now - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S"))
               for i in range(20)]
        self.book = FakeBook(old + new)
        self.gio = FakeGio({"LIVE": self.book})
        self.cap = mock.patch.object(aa, "measure_workbook",
                                     return_value={"cells": 5_000_000, "pct": 50.0})
        self.cap.start()

    def tearDown(self):
        self.cap.stop()
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def plan(self):
        p = aa.make_plan(self.gio, Cfg, 90)
        aa.PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)
        aa.PLAN_FILE.write_text(json.dumps(p))
        return p

    def test_plan_copy_apply(self):
        p = self.plan()
        self.assertEqual(p["block_rows"], 50)
        self.assertEqual(p["next_row_id"], "N0")
        self.assertEqual(p["cells_freed"], 50 * 31)
        p = aa.do_copy(self.gio, Cfg, aa.load_plan(Cfg))
        self.assertEqual(p["archive"]["id"], "ARCH")
        aa.PLAN_FILE.write_text(json.dumps(p))
        with mock.patch.object(aa, "stopped_units", return_value=[]), \
                mock.patch.object(aa, "trim_lock_active", return_value=False):
            p = aa.do_apply(self.gio, Cfg, aa.load_plan(Cfg))
        self.assertTrue(p["applied"]["verified"])
        self.assertEqual(self.book.rows[0][0], "N0")
        self.assertEqual(len(self.book.rows), 20)
        self.assertEqual(len(self.gio.sheets.books["ARCH"].rows), 70)

    def test_apply_refuses_while_services_run(self):
        p = self.plan()
        p = aa.do_copy(self.gio, Cfg, aa.load_plan(Cfg))
        aa.PLAN_FILE.write_text(json.dumps(p))
        with mock.patch.object(aa, "stopped_units",
                               return_value=["salla-live-sync.service"]):
            with self.assertRaises(SystemExit):
                aa.do_apply(self.gio, Cfg, aa.load_plan(Cfg))
        self.assertEqual(len(self.book.rows), 70)

    def test_apply_refuses_without_archive(self):
        self.plan()
        with mock.patch.object(aa, "stopped_units", return_value=[]):
            with self.assertRaises(SystemExit):
                aa.do_apply(self.gio, Cfg, aa.load_plan(Cfg))
        self.assertEqual(len(self.book.rows), 70)

    def test_apply_refuses_when_rows_moved(self):
        p = self.plan()
        p = aa.do_copy(self.gio, Cfg, aa.load_plan(Cfg))
        aa.PLAN_FILE.write_text(json.dumps(p))
        del self.book.rows[0]                         # someone deleted a row since
        with mock.patch.object(aa, "stopped_units", return_value=[]), \
                mock.patch.object(aa, "trim_lock_active", return_value=False):
            with self.assertRaises(SystemExit):
                aa.do_apply(self.gio, Cfg, aa.load_plan(Cfg))
        self.assertEqual(len(self.book.rows), 69)
        self.assertEqual(self.gio.sheets.deletes, [])

    def test_stale_plan_refused(self):
        p = self.plan()
        p["made_at"] = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
        aa.PLAN_FILE.write_text(json.dumps(p))
        with self.assertRaises(SystemExit):
            aa.load_plan(Cfg)


if __name__ == "__main__":
    unittest.main()
