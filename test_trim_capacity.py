"""v2.12 trim ordering and workbook capacity.

  * live.LiveEngine._maybe_trim resets the cursor state to row 2 BEFORE the
    delete and again in `finally`, holds the trim lock for the delete, and a
    failed delete still leaves the state at row 2 (mirrors realtime_base).
  * tools.sheet_capacity.capacity sums rowCount x columnCount per tab and per
    workbook against the 10M-cell cap, through GoogleIO's Sheets service; the
    CLI alerts only above cfg.capacity_alert_pct.

Offline, against fakes. Run: python3 -m unittest test_trim_capacity -v
"""
import io
import json
import sys
import threading
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import backfill
from backfill import Config
import live
import realtime_base
from tools.sheet_capacity import capacity
from tools import sheet_capacity as sc

from test_realtime_ext import _cfg, _chdir_tmp


# -- (a) live trim ordering --------------------------------------------------

class TrimGIO:
    """Records what the cursor state and the trim lock looked like at the
    moment of the delete."""

    def __init__(self, events, deleted=3, fail=None):
        self.events, self.deleted, self.fail = events, deleted, fail
        self.calls = []

    def queue_trim(self, qsid, keep, tab=None, **kw):
        state = json.loads(live.STATE_FILE.read_text())["start_row"]
        self.events.append(("delete", state, realtime_base.trim_lock_active()))
        self.calls.append((qsid, tab, kw))
        if self.fail:
            raise self.fail
        return self.deleted


def trim_engine(test, gio_kw=None, live_mode=True, **cfg_kw):
    _chdir_tmp(test)
    events = []
    e = object.__new__(live.LiveEngine)
    e.cfg = _cfg(live_trim_hour=datetime.now().hour, live_trim_days=7, **cfg_kw)
    e.live = live_mode
    e.qsid = "QS"
    e._last_trim_day = None
    e._start_row = 900
    e.gio = TrimGIO(events, **(gio_kw or {}))
    real_save = live.LiveEngine._save_state

    def save():
        real_save(e)
        events.append(("save", e._start_row))
    e._save_state = save
    live.STATE_FILE.write_text(json.dumps({"start_row": 900}))
    return e, events


class TestLiveTrimOrder(unittest.TestCase):
    def test_state_reset_before_delete_and_after(self):
        e, events = trim_engine(self)
        e._maybe_trim([{"status": "done"}])
        self.assertEqual(events, [("save", 2),              # before the delete
                                  ("delete", 2, True),      # state on disk = 2, lock held
                                  ("save", 2)])             # finally
        self.assertEqual(json.loads(live.STATE_FILE.read_text())["start_row"], 2)
        self.assertFalse(realtime_base.trim_lock_active())  # lock released
        self.assertEqual(e.gio.calls[0][0], "QS")

    def test_state_reset_when_delete_raises(self):
        e, events = trim_engine(self, gio_kw={"fail": RuntimeError("quota")})
        with self.assertLogs("backfill", level="ERROR") as logs:
            e._maybe_trim([])                                 # does not raise
        self.assertIn("TRIM failed", logs.output[0])
        self.assertEqual(events, [("save", 2), ("delete", 2, True), ("save", 2)])
        self.assertEqual(e._start_row, 2)
        self.assertEqual(json.loads(live.STATE_FILE.read_text())["start_row"], 2)
        self.assertFalse(realtime_base.trim_lock_active())
        e._maybe_trim([])                                     # not retried today
        self.assertEqual(len(e.gio.calls), 1)

    def test_runs_once_a_day(self):
        e, events = trim_engine(self)
        e._maybe_trim([])
        e._maybe_trim([])
        self.assertEqual(len(e.gio.calls), 1)

    def test_queued_rows_postpone_the_trim(self):
        e, events = trim_engine(self)
        e._maybe_trim([{"status": "done"}, {"status": "queued"}])
        self.assertEqual(events, [])
        self.assertEqual(e._start_row, 900)                   # cursor untouched
        self.assertIsNone(e._last_trim_day)                   # retried next poll

    def test_dry_run_never_trims(self):
        e, events = trim_engine(self, live_mode=False)
        e._maybe_trim([])
        self.assertEqual(events, [])

    def test_other_hour_does_nothing(self):
        e, events = trim_engine(self)
        e.cfg.live_trim_hour = (datetime.now().hour + 12) % 24
        e._maybe_trim([])
        self.assertEqual(events, [])


# -- (b) workbook capacity ---------------------------------------------------

def tab(title, rows, cols):
    return {"properties": {"title": title,
                           "gridProperties": {"rowCount": rows, "columnCount": cols}}}


QUEUE_META = {"sheets": [tab("Live Queue", 20000, 26), tab("Status Queue", 166000, 26),
                         tab("Customer Queue", 60000, 26),
                         {"properties": {"title": "Chart"}}]}      # no grid
AUDIT_META = {"sheets": [tab("Order Audit Log", 40000, 31), tab("Queue Log", 1000, 14)]}


class FakeRequest:
    def __init__(self, service, sid, fields):
        self.service, self.sid, self.fields = service, sid, fields

    def execute(self):
        self.service.executed.append(self.sid)
        if self.sid in self.service.fail:
            raise RuntimeError("403 caller has no access")
        return self.service.meta[self.sid]


class FakeSheets:
    """The spreadsheets() resource: get() builds a request, execute() runs it."""

    def __init__(self, meta, fail=()):
        self.meta, self.fail = meta, set(fail)
        self.gets, self.executed = [], []

    def get(self, spreadsheetId, fields=None):
        self.gets.append((spreadsheetId, fields))
        return FakeRequest(self, spreadsheetId, fields)


def bare_gio(meta=None, **kw):
    return types.SimpleNamespace(sheets=FakeSheets(
        meta or {"QS": QUEUE_META, "AUDIT": AUDIT_META}, **kw))


def google_io(sheets):
    """A real GoogleIO whose per-thread Sheets service is the fake, so the
    call goes through GoogleIO.sheets and GoogleIO._gexec."""
    g = object.__new__(backfill.GoogleIO)
    g.cfg, g.enabled = _cfg(), True
    g._tl = threading.local()
    g._tl.sheets, g._tl.drive = sheets, None
    g.sheets_rl = types.SimpleNamespace(wait=mock.Mock(), on_success=mock.Mock(),
                                        on_throttle=mock.Mock())
    return g


def fake_googleapiclient():
    errors = types.ModuleType("googleapiclient.errors")
    errors.HttpError = type("HttpError", (Exception,), {})
    pkg = types.ModuleType("googleapiclient")
    pkg.errors = errors
    return {"googleapiclient": pkg, "googleapiclient.errors": errors}


class TestCapacity(unittest.TestCase):
    def test_sums_per_tab_and_workbook(self):
        gio = bare_gio()
        out = capacity(gio, _cfg())
        self.assertEqual([w["workbook"] for w in out], ["Queue workbook", "Audit workbook"])
        q, a = out
        self.assertEqual(q["spreadsheet_id"], "QS")
        self.assertEqual(q["cells"], (20000 + 166000 + 60000) * 26)
        self.assertEqual(q["pct"], round(100.0 * 6396000 / 10_000_000, 2))
        self.assertEqual(q["tabs"][0], {"tab": "Status Queue", "rows": 166000, "cols": 26,
                                        "cells": 4316000})            # largest first
        self.assertEqual(q["tabs"][-1]["cells"], 0)                    # chart sheet
        self.assertEqual(a["cells"], 40000 * 31 + 1000 * 14)
        self.assertEqual(set(out[0]), {"workbook", "spreadsheet_id", "cells", "pct", "tabs"})
        self.assertIn("gridProperties", gio.sheets.gets[0][1])        # metadata only

    def test_goes_through_googleio_service_and_gexec(self):
        sheets = FakeSheets({"QS": QUEUE_META, "AUDIT": AUDIT_META})
        g = google_io(sheets)
        with mock.patch.dict(sys.modules, fake_googleapiclient()):
            out = capacity(g, _cfg())
        self.assertEqual(sheets.executed, ["QS", "AUDIT"])
        self.assertEqual(g.sheets_rl.wait.call_count, 2)              # paced by the engine limiter
        self.assertEqual(out[0]["cells"], 6396000)

    def test_skips_empty_and_duplicate_ids(self):
        gio = bare_gio()
        self.assertEqual([w["workbook"] for w in capacity(gio, _cfg(spreadsheet_id=""))],
                         ["Queue workbook"])
        self.assertEqual(len(capacity(gio, _cfg(spreadsheet_id="QS"))), 1)
        self.assertEqual(capacity(gio, _cfg(queue_spreadsheet_id="", spreadsheet_id="")), [])

    def test_unreadable_workbook_raises_not_zero(self):
        with self.assertRaises(RuntimeError):
            capacity(bare_gio(fail=["AUDIT"]), _cfg())
        with self.assertRaises(RuntimeError):
            capacity(types.SimpleNamespace(sheets=None), _cfg())      # Google disabled


def result(pct_cells, name="Queue workbook"):
    return {"workbook": name, "spreadsheet_id": "QS", "cells": pct_cells,
            "pct": round(100.0 * pct_cells / 10_000_000, 2),
            "tabs": [{"tab": "Status Queue", "rows": 300000, "cols": 26,
                      "cells": 7800000}]}


class TestCapacityReport(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(sc, "log")
        self.log = patcher.start()
        self.addCleanup(patcher.stop)

    def run_report(self, results, alert=True, **cfg_kw):
        notifier = mock.Mock()
        out = io.StringIO()
        over = sc.report(results, _cfg(**cfg_kw), alert=alert, notifier=notifier, out=out)
        return over, notifier, out.getvalue()

    def test_default_threshold_is_80(self):
        self.assertEqual(Config().capacity_alert_pct, 80.0)

    def test_alert_above_threshold(self):
        over, notifier, table = self.run_report([result(8_500_000), result(1_000_000, "Audit workbook")])
        self.assertEqual([w["workbook"] for w in over], ["Queue workbook"])
        notifier.send_alert.assert_called_once()
        subject, body = notifier.send_alert.call_args[0]
        self.assertIn("Queue workbook", subject)
        self.assertIn("85.0% full", subject)
        self.assertIn("Status Queue: 300,000 rows x 26 columns", body)
        for text in (subject, body):
            self.assertNotIn("—", text)                          # no em dash
            self.assertNotIn("–", text)
        self.assertIn("Queue workbook", table)
        self.assertIn("over 80%", table)

    def test_no_alert_at_or_below_threshold_or_without_flag(self):
        _, n1, _ = self.run_report([result(8_000_000)])               # exactly 80%
        _, n2, _ = self.run_report([result(9_000_000)], alert=False)
        _, n3, _ = self.run_report([result(9_000_000)], alerts_enabled=False)
        for n in (n1, n2, n3):
            n.send_alert.assert_not_called()

    def test_threshold_from_config(self):
        _, notifier, _ = self.run_report([result(6_000_000)], capacity_alert_pct=50.0)
        notifier.send_alert.assert_called_once()

    def test_two_workbooks_over(self):
        _, notifier, _ = self.run_report([result(8_500_000), result(9_100_000, "Audit workbook")])
        subject, body = notifier.send_alert.call_args[0]
        self.assertIn("2 Google Sheets workbooks are over 80% full", subject)
        self.assertLess(body.index("Audit workbook"), body.index("Queue workbook"))

    def test_alert_failure_does_not_raise(self):
        notifier = mock.Mock()
        notifier.send_alert.side_effect = RuntimeError("slack down")
        sc.report([result(9_000_000)], _cfg(), alert=True, notifier=notifier, out=io.StringIO())
        self.assertIn("capacity alert failed", self.log.warning.call_args[0][0])

    def test_cli_main(self):
        _chdir_tmp(self)
        cfg = _cfg()
        gio = bare_gio({"QS": {"sheets": [tab("Status Queue", 330000, 26)]},
                        "AUDIT": AUDIT_META})
        out = io.StringIO()
        import notify
        with mock.patch.object(backfill, "setup_logging"), \
                mock.patch.object(backfill.Config, "load", return_value=cfg), \
                mock.patch.object(backfill, "GoogleIO", return_value=gio) as gcls, \
                mock.patch.object(notify, "send_alert") as send, \
                mock.patch("sys.stdout", out):
            self.assertEqual(sc.main(["--alert"]), 0)
        gcls.assert_called_once_with(cfg, enabled=True)
        send.assert_called_once()
        self.assertIn("Status Queue", out.getvalue())


if __name__ == "__main__":
    unittest.main()
