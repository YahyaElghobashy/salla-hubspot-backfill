"""v2.12 trim ordering and workbook capacity.

  * live.LiveEngine._maybe_trim resets the cursor state to row 2 BEFORE the
    delete and again in `finally`, holds the trim lock for the delete, and a
    failed delete still leaves the state at row 2 (mirrors realtime_base).
  * tools.sheet_capacity.capacity sums rowCount x columnCount per tab and per
    workbook against the 10M-cell cap, through GoogleIO's Sheets service; the
    CLI alerts only above cfg.capacity_alert_pct, at most once per Riyadh day
    unless a workbook reaches a higher 5-point band, lists only tabs that
    hold cells, and loads .env before reading the config.
  * realtime_base.trim_lock is owner-aware: "<pid> <tab> <ts>", a live holder
    is refused (TrimLockHeld) and every caller skips instead of overwriting,
    a dead holder is taken over with a WARNING, exit unlinks only its own
    lock. tools/trim_queue.py counts cells through tools.sheet_capacity.

Offline, against fakes. Run: python3 -m unittest test_trim_capacity -v
"""
import io
import json
import os
import re
import subprocess
import sys
import threading
import time
import types
import unittest
from datetime import datetime, timezone
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
        _chdir_tmp(self)                    # the cooldown state lives in mirror/
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

    def test_cli_loads_dotenv_before_config(self):
        _chdir_tmp(self)
        Path(".env").write_text("CAPACITY_DOTENV_PROBE=loaded\n")
        seen = {}

        def load(path):
            seen["probe"] = os.environ.get("CAPACITY_DOTENV_PROBE")
            return _cfg()
        gio = bare_gio()
        with mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch.object(backfill, "setup_logging"), \
                mock.patch.object(backfill.Config, "load", side_effect=load), \
                mock.patch.object(backfill, "GoogleIO", return_value=gio), \
                mock.patch("sys.stdout", io.StringIO()):
            os.environ.pop("CAPACITY_DOTENV_PROBE", None)
            self.assertEqual(sc.main([]), 0)
        self.assertEqual(seen["probe"], "loaded")


# -- (c) capacity alert cooldown and body ------------------------------------

def wb(pct, sid="QS", name="Queue workbook", tabs=None):
    cells = int(round(pct * 100_000))
    return {"workbook": name, "spreadsheet_id": sid, "cells": cells,
            "pct": round(100.0 * cells / 10_000_000, 2),
            "tabs": tabs if tabs is not None else
            [{"tab": "Status Queue", "rows": cells // 26, "cols": 26, "cells": cells}]}


def riyadh(day, hour, minute=0):
    return datetime(2026, 9, day, hour, minute, tzinfo=sc.RIYADH)


class TestCapacityCooldown(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        self.notifier = mock.Mock()

    def send(self, results, now, notifier=None, **cfg_kw):
        n = notifier or self.notifier
        before = n.send_alert.call_count
        with self.assertLogs("backfill", level="INFO") as self.logs:
            sc.report(results, _cfg(**cfg_kw), alert=True, notifier=n,
                      out=io.StringIO(), now=now)
        return n.send_alert.call_count - before

    def state(self):
        return json.loads(sc.ALERT_STATE.read_text())

    def test_first_alert_writes_state(self):
        self.assertEqual(self.send([wb(85.2)], riyadh(26, 9)), 1)
        st = self.state()
        self.assertEqual(st["day"], "2026-09-26")
        self.assertEqual(st["bands"], {"QS": 17})
        self.assertEqual(st["pct"], {"QS": 85.2})

    def test_same_day_same_band_is_not_repeated(self):
        self.send([wb(85.2)], riyadh(26, 9))
        self.assertEqual(self.send([wb(86.9)], riyadh(26, 15)), 0)
        self.assertTrue(any("already sent today" in m for m in self.logs.output))
        self.assertEqual(self.notifier.send_alert.call_count, 1)

    def test_higher_band_same_day_alerts_again(self):
        self.send([wb(84.9)], riyadh(26, 9))                  # band 80-85
        self.assertEqual(self.send([wb(85.1)], riyadh(26, 10)), 1)   # band 85-90
        self.assertEqual(self.state()["bands"], {"QS": 17})
        self.assertEqual(self.send([wb(90.0)], riyadh(26, 11)), 1)   # band 90-95
        self.assertEqual(self.state()["bands"], {"QS": 18})

    def test_dip_and_return_to_same_band_is_not_repeated(self):
        self.send([wb(91.0)], riyadh(26, 9))                  # band 18 alerted
        self.assertEqual(self.send([wb(84.0)], riyadh(26, 10)), 0)   # trimmed a bit
        self.assertEqual(self.send([wb(92.0)], riyadh(26, 11)), 0)   # back in band 18
        self.assertEqual(self.state()["bands"], {"QS": 18})

    def test_new_riyadh_day_alerts_again(self):
        utc = timezone.utc
        self.send([wb(85.0)], datetime(2026, 9, 26, 20, 59, tzinfo=utc))   # 23:59 Riyadh
        self.assertEqual(self.state()["day"], "2026-09-26")
        self.assertEqual(self.send([wb(85.0)], datetime(2026, 9, 26, 21, 1, tzinfo=utc)), 1)
        self.assertEqual(self.state()["day"], "2026-09-27")          # 00:01 Riyadh

    def test_second_workbook_over_same_day_alerts(self):
        self.send([wb(85.0)], riyadh(26, 9))
        both = [wb(85.5), wb(81.0, sid="AUDIT", name="Audit workbook")]
        self.assertEqual(self.send(both, riyadh(26, 10)), 1)
        self.assertEqual(self.state()["bands"], {"QS": 17, "AUDIT": 16})

    def test_failed_send_is_retried_next_run(self):
        broken = mock.Mock()
        broken.send_alert.side_effect = RuntimeError("slack down")
        self.send([wb(85.0)], riyadh(26, 9), notifier=broken)
        self.assertFalse(sc.ALERT_STATE.exists())
        self.assertEqual(self.send([wb(85.0)], riyadh(26, 10)), 1)
        self.assertTrue(sc.ALERT_STATE.exists())

    def test_suppressed_alert_writes_no_state(self):
        self.send([wb(85.0)], riyadh(26, 9), alerts_enabled=False)
        self.notifier.send_alert.assert_not_called()
        self.assertFalse(sc.ALERT_STATE.exists())

    def test_unreadable_state_alerts(self):
        sc.ALERT_STATE.write_text("{not json")
        self.assertEqual(self.send([wb(85.0)], riyadh(26, 9)), 1)
        self.assertEqual(self.state()["bands"], {"QS": 17})

    def test_below_threshold_leaves_state_alone(self):
        self.send([wb(85.0)], riyadh(26, 9))
        before = sc.ALERT_STATE.read_text()
        sc.report([wb(50.0)], _cfg(), alert=True, notifier=self.notifier,
                  out=io.StringIO(), now=riyadh(27, 9))
        self.assertEqual(self.notifier.send_alert.call_count, 1)
        self.assertEqual(sc.ALERT_STATE.read_text(), before)


class TestAlertBody(unittest.TestCase):
    def test_zero_cell_tabs_are_not_listed(self):
        tabs = [{"tab": "Status Queue", "rows": 330000, "cols": 26, "cells": 8580000},
                {"tab": "Live Queue", "rows": 1000, "cols": 26, "cells": 26000},
                {"tab": "Chart", "rows": 0, "cols": 0, "cells": 0},
                {"tab": "Empty", "rows": 0, "cols": 26, "cells": 0}]
        _, body = sc.alert_message([wb(86.06, tabs=tabs)], 80.0)
        self.assertIn("Status Queue: 330,000 rows", body)
        self.assertIn("Live Queue: 1,000 rows", body)
        self.assertNotIn("Chart", body)
        self.assertNotIn("Empty", body)
        self.assertNotIn("= 0 cells", body)

    def test_alert_lists_at_most_five_nonzero_tabs(self):
        tabs = [{"tab": f"T{i}", "rows": 0, "cols": 0, "cells": 0} for i in range(3)]
        tabs = [{"tab": f"Big{i}", "rows": 100, "cols": 10, "cells": 1000} for i in range(7)] + tabs
        _, body = sc.alert_message([wb(90.0, tabs=tabs)], 80.0)
        self.assertEqual(sum(1 for line in body.splitlines() if line.startswith("  ")), 5)


# -- (d) owner-aware trim lock -----------------------------------------------

LOCK_LINE = re.compile(r"^(\d+) (.+) (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)$")


def live_pid(test):
    """A real process that stays alive for the test."""
    p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    test.addCleanup(p.wait)
    test.addCleanup(p.kill)
    return p.pid


def dead_pid():
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid


def hold(pid, tab="Status Queue", age_s=0, raw=None):
    lock = realtime_base.TRIM_LOCK
    lock.write_text(raw if raw is not None else f"{pid} {tab} 2026-09-26T04:00:00\n")
    if age_s:
        t = time.time() - age_s
        os.utime(lock, (t, t))


class TestTrimLock(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        self.lock = realtime_base.TRIM_LOCK

    def test_writes_pid_tab_ts_and_releases(self):
        with realtime_base.trim_lock("Status Queue"):
            m = LOCK_LINE.match(self.lock.read_text().strip())
            self.assertIsNotNone(m)
            self.assertEqual((int(m.group(1)), m.group(2)), (os.getpid(), "Status Queue"))
            h = realtime_base.trim_lock_holder()
            self.assertEqual((h["pid"], h["tab"]), (os.getpid(), "Status Queue"))
            self.assertTrue(realtime_base.trim_lock_active())
        self.assertFalse(self.lock.exists())
        self.assertFalse(realtime_base.trim_lock_active())

    def test_live_holder_is_refused_not_overwritten(self):
        pid = live_pid(self)
        hold(pid)
        before = self.lock.read_text()
        with self.assertRaises(realtime_base.TrimLockHeld) as cm:
            with realtime_base.trim_lock("Customer Queue"):
                self.fail("entered a lock held by a live process")
        self.assertIsInstance(cm.exception, RuntimeError)
        self.assertIn(f"pid {pid}", str(cm.exception))
        self.assertEqual(self.lock.read_text(), before)           # untouched
        self.assertTrue(realtime_base.trim_lock_active())

    def test_own_pid_holder_is_refused_too(self):
        with realtime_base.trim_lock("Status Queue"):
            with self.assertRaises(realtime_base.TrimLockHeld):
                with realtime_base.trim_lock("Live Queue"):
                    pass
            self.assertTrue(self.lock.exists())                   # inner refusal kept it
        self.assertFalse(self.lock.exists())

    def test_dead_holder_is_taken_over_with_warning(self):
        hold(dead_pid())
        self.assertFalse(realtime_base.trim_lock_active())
        with self.assertLogs("backfill", level="WARNING") as logs:
            with realtime_base.trim_lock("Live Queue"):
                self.assertEqual(realtime_base.trim_lock_holder()["pid"], os.getpid())
        self.assertIn("stale lock", logs.output[0])
        self.assertFalse(self.lock.exists())

    def test_active_while_live_holder_exists_however_old(self):
        pid = live_pid(self)
        hold(pid, age_s=5 * 3600)                                 # a long trim
        with mock.patch.object(realtime_base, "_pid_started_at",
                               return_value=time.time() - 6 * 3600):
            self.assertTrue(realtime_base.trim_lock_active())
            with self.assertRaises(realtime_base.TrimLockHeld):
                with realtime_base.trim_lock("Live Queue"):
                    pass

    def test_reused_pid_is_not_a_holder(self):
        pid = live_pid(self)
        hold(pid, age_s=3600)                                     # written before a reboot
        with mock.patch.object(realtime_base, "_pid_started_at",
                               return_value=time.time()):         # pid started later
            self.assertFalse(realtime_base.trim_lock_active())
            with self.assertLogs("backfill", level="WARNING"):
                with realtime_base.trim_lock("Live Queue"):
                    pass

    def test_no_lock_file(self):
        self.assertIsNone(realtime_base.trim_lock_holder())
        self.assertFalse(realtime_base.trim_lock_active())

    def test_v211_json_lock_is_understood(self):
        pid = live_pid(self)
        hold(pid, raw=json.dumps({"tab": "Status Queue", "pid": pid,
                                  "ts": "2026-09-26T04:00:00"}))
        h = realtime_base.trim_lock_holder()
        self.assertEqual((h["pid"], h["tab"]), (pid, "Status Queue"))
        self.assertTrue(realtime_base.trim_lock_active())
        hold(None, raw=json.dumps({"tab": "Status Queue", "pid": dead_pid()}))
        self.assertFalse(realtime_base.trim_lock_active())

    def test_unreadable_lock_falls_back_to_age(self):
        hold(None, raw="")                                        # read mid-write
        self.assertTrue(realtime_base.trim_lock_active())
        with self.assertRaises(realtime_base.TrimLockHeld):
            with realtime_base.trim_lock("Live Queue"):
                pass
        hold(None, raw="garbage", age_s=3 * 3600)
        self.assertFalse(realtime_base.trim_lock_active())
        with self.assertLogs("backfill", level="WARNING"):
            with realtime_base.trim_lock("Live Queue"):
                pass

    def test_exit_leaves_a_lock_that_is_not_ours(self):
        other = live_pid(self)
        with self.assertLogs("backfill", level="WARNING") as logs:
            with realtime_base.trim_lock("Status Queue"):
                hold(other, tab="Customer Queue")                  # taken meanwhile
        self.assertTrue(self.lock.exists())
        self.assertEqual(realtime_base.trim_lock_holder()["pid"], other)
        self.assertIn("not by us", logs.output[-1])

    def test_exit_when_lock_already_gone(self):
        with realtime_base.trim_lock("Status Queue"):
            self.lock.unlink()
        self.assertFalse(self.lock.exists())

    def test_proc_start_time_is_before_now_or_unknown(self):
        started = realtime_base._pid_started_at(os.getpid())
        self.assertTrue(started is None or started <= time.time() + 2)

    def test_proc_start_time_parse(self):
        """The Linux path (the VM), whatever this machine is: field 22 of
        /proc/<pid>/stat in clock ticks after /proc/stat btime."""
        files = {"/proc/123/stat": "123 (python3 a) b) S 1 123 123 0 -1 4194560 100 0 0 0 "
                                   "1 2 0 0 20 0 1 0 5000 12345 67 0\n",
                 "/proc/stat": "cpu  1 2 3\nbtime 1790000000\nprocesses 9\n"}

        def read_text(path, *a, **k):
            try:
                return files[str(path)]
            except KeyError:
                raise FileNotFoundError(str(path))
        with mock.patch.object(Path, "read_text", autospec=True, side_effect=read_text), \
                mock.patch.object(realtime_base.os, "sysconf", return_value=100):
            self.assertEqual(realtime_base._pid_started_at(123), 1790000050.0)
            self.assertIsNone(realtime_base._pid_started_at(456))


class TestTrimCallersSkipAHeldLock(unittest.TestCase):
    def test_live_engine_skips_and_keeps_the_lock(self):
        e, events = trim_engine(self)
        pid = live_pid(self)
        hold(pid, tab="Customer Queue")
        with self.assertLogs("backfill", level="WARNING") as logs:
            e._maybe_trim([])                                     # does not raise
        self.assertIn("TRIM skipped today", logs.output[0])
        self.assertEqual(e.gio.calls, [])                         # nothing deleted
        self.assertEqual(events, [("save", 2), ("save", 2)])
        self.assertEqual(realtime_base.trim_lock_holder()["pid"], pid)
        e._maybe_trim([])                                         # one try per day
        self.assertEqual(e.gio.calls, [])

    def test_realtime_consumer_skips_and_keeps_the_lock(self):
        _chdir_tmp(self)
        import customer_sync
        from test_capture_hardening import ListGIO
        from test_realtime_ext import FakeHS
        gio = ListGIO([])
        hour = (datetime.now().hour - 1) % 24                     # customers offset +1
        s = customer_sync.CustomerSync(_cfg(realtime_trim_enabled=True,
                                            realtime_trim_hour=hour,
                                            customer_trim_days=30),
                                       FakeHS(), gio, live=True)
        s._start_row = 900
        pid = live_pid(self)
        hold(pid, tab="Live Queue")
        with self.assertLogs("backfill", level="WARNING") as logs:
            s._maybe_trim()
        self.assertTrue(any("trim skipped today" in m for m in logs.output))
        self.assertEqual(gio.trims, [])
        self.assertEqual(json.loads(Path("mirror/customers_state.json").read_text())["start_row"], 2)
        self.assertEqual(realtime_base.trim_lock_holder()["pid"], pid)


class FakeQueueGIO:
    """What tools/trim_queue.py reads and deletes through."""

    def __init__(self, meta, rows):
        self.sheets = FakeSheets({"QS": meta})
        self.rows, self.trimmed = rows, []

    def queue_read_all(self, qsid, tab=None):
        return [dict(r) for r in self.rows]

    def queue_read_heartbeat(self, qsid, tab=None):
        return "status-vm|0"                                      # long quiet

    def queue_trim(self, qsid, keep, tab=None, deletable=None):
        self.trimmed.append((tab, realtime_base.trim_lock_active()))
        return 2


class TestTrimQueueTool(unittest.TestCase):
    def run_tool(self, *extra, active=None):
        _chdir_tmp(self)
        from tools import trim_queue as tq
        rows = [{"status": "done", "received_at": "2026-01-01 00:00:00"},
                {"status": "done", "received_at": "2026-01-02 00:00:00"},
                {"status": "queued", "received_at": "2026-01-01 00:00:00"}]
        gio = FakeQueueGIO(QUEUE_META, rows)
        argv = ["trim_queue.py", "--tab", "Status Queue", "--keep-days", "7", *extra]
        patches = [mock.patch.object(tq, "setup_logging"),
                   mock.patch.object(tq.Config, "load", return_value=_cfg()),
                   mock.patch.object(tq, "GoogleIO", return_value=gio),
                   mock.patch.object(sys, "argv", argv)]
        if active is not None:
            patches.append(mock.patch.object(tq, "trim_lock_active", return_value=active))
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        return tq, gio

    def test_dry_run_counts_cells_through_sheet_capacity(self):
        tq, gio = self.run_tool()
        with mock.patch.object(tq, "measure_workbook",
                               wraps=sc.measure_workbook) as mw, \
                self.assertLogs("backfill", level="INFO") as logs:
            tq.main()
        mw.assert_called_once_with(gio, "QS", "Queue workbook")
        self.assertIn("would delete 2", logs.output[0])
        self.assertIn("~52 cells freed", logs.output[0])          # 2 rows x 26 columns
        self.assertIn("workbook 6,396,000 cells now", logs.output[0])
        self.assertEqual(gio.trimmed, [])

    def test_apply_holds_the_lock_and_reports_after(self):
        tq, gio = self.run_tool("--apply")
        with self.assertLogs("backfill", level="INFO") as logs:
            tq.main()
        self.assertEqual(gio.trimmed, [("Status Queue", True)])
        self.assertFalse(realtime_base.TRIM_LOCK.exists())
        self.assertIn("workbook 6,396,000 -> 6,396,000 cells", logs.output[-1])
        self.assertEqual(json.loads(Path("mirror/status_state.json").read_text())["start_row"], 2)

    def test_apply_refuses_a_lock_taken_after_the_check(self):
        tq, gio = self.run_tool("--apply", active=False)
        pid = live_pid(self)
        hold(pid, tab="Live Queue")
        with self.assertRaises(SystemExit) as cm:
            tq.main()
        self.assertIn(f"held by pid {pid}", str(cm.exception.code))
        self.assertEqual(gio.trimmed, [])
        self.assertFalse(Path("mirror/status_state.json").exists())
        self.assertEqual(realtime_base.trim_lock_holder()["pid"], pid)

    def test_apply_refuses_when_lock_active(self):
        tq, gio = self.run_tool("--apply")
        hold(live_pid(self))
        with self.assertRaises(SystemExit):
            tq.main()
        self.assertEqual(gio.trimmed, [])



if __name__ == "__main__":
    unittest.main()
