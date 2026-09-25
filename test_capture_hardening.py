"""v2.11 capture hardening: payload salvage and lookup, payload-safe marks,
the held-row cursor fix, the retry cap, merge safety, and the queue trim.

Offline, against fakes. Run: python3 -m unittest test_capture_hardening -v
"""
import gzip
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import backfill
from backfill import Config, RelayError
import customer_payload as cp

from test_realtime_ext import FakeGIO, FakeHS, _cfg, _chdir_tmp

KEYS = cp.TEMPLATE_KEYS


def template(**over):
    """The capture's text template exactly as Make pastes it today."""
    vals = {"id": "501", "first_name": "Ali", "last_name": "S", "mobile": "500000001",
            "mobile_code": "+966", "email": "a@store.fake", "city": "Riyadh",
            "gender": "male", "lang": "ar", "birthday": "1990-04-02 00:00:00.000000",
            "location": "Olaya 12", "is_notifications_enabled": "true"}
    vals.update(over)
    return "{" + ",".join(f'"{k}":"{vals[k]}"' for k in KEYS) + "}"


class TestSalvage(unittest.TestCase):
    def test_quotes_in_address(self):
        out = cp.salvage_template(template(location='Bldg "Nakheel" 12'), cid="501",
                                  phone="+966500000001")
        self.assertEqual(out["location"], 'Bldg "Nakheel" 12')
        self.assertEqual(out["is_notifications_enabled"], "true")
        self.assertEqual(out["first_name"], "Ali")

    def test_line_break_and_backslash(self):
        out = cp.salvage_template(template(location="Line one\nLine two",
                                           last_name="A\\B"), cid="501")
        self.assertEqual(out["location"], "Line one, Line two")
        self.assertEqual(out["last_name"], "A\\B")

    def test_bare_values(self):
        text = template().replace('"id":"501"', '"id":501').replace(
            '"is_notifications_enabled":"true"', '"is_notifications_enabled":false')
        out = cp.salvage_template(text, cid="501")
        self.assertEqual(out["id"], "501")
        self.assertIs(out["is_notifications_enabled"], False)

    def test_refuses_wrong_row(self):
        self.assertIsNone(cp.salvage_template(template(), cid="999"))
        self.assertIsNone(cp.salvage_template(template(), cid="501", phone="+966599999999"))

    def test_refuses_non_template(self):
        self.assertIsNone(cp.salvage_template("created contact 873881231575", cid="501"))
        self.assertIsNone(cp.salvage_template('{"id":"501","first_name":"A"}', cid="501"))

    def test_from_api_shape(self):
        rec = {"id": 42, "first_name": "N\nA", "mobile": 555, "mobile_code": "+966",
               "birthday": {"date": "1988-02-03 00:00:00.000000", "timezone": "Asia/Riyadh"},
               "is_notifications_enabled": False, "location": "a\r\nb"}
        out = cp.from_api(rec)
        self.assertEqual(out["id"], "42")
        self.assertEqual(out["first_name"], "N A")
        self.assertEqual(out["birthday"], "1988-02-03 00:00:00.000000")
        self.assertEqual(out["is_notifications_enabled"], "false")
        self.assertEqual(out["location"], "a, b")
        self.assertEqual(cp.phone_of(out), "+966555")
        self.assertEqual(cp.from_api({"id": 1})["is_notifications_enabled"], "")


class TestBirthDate(unittest.TestCase):
    def test_shapes(self):
        import customer_sync as csy
        self.assertEqual(csy.birth_date("1990-04-02 00:00:00.000000"), "1990-04-02")
        self.assertEqual(csy.birth_date({"date": "1990-04-02 00:00:00"}), "1990-04-02")
        self.assertEqual(csy.birth_date('{"date":"1990-04-02"}'), "1990-04-02")
        self.assertEqual(csy.birth_date("0000-00-00"), "")
        self.assertEqual(csy.birth_date(f"{datetime.now().year + 1}-01-01"), "")
        self.assertEqual(csy.birth_date(None), "")


class FakeRelay:
    def __init__(self, reply=None, exc=None):
        self.reply, self.exc, self.paths = reply, exc, []

    def get_path_once(self, path, timeout=20.0):
        self.paths.append(path)
        if self.exc:
            raise self.exc
        return self.reply


def crow(cid="501", note=None, source="webhook", attempts=0, row=3):
    return {"row": row, "order_id": str(cid), "reference_id": "+966500000001",
            "event": "customer.created", "status": "queued", "attempts": attempts,
            "source": source, "received_at": "2026-09-26 10:00:00",
            "note": template() if note is None else note}


class TestCustomerSyncV211(unittest.TestCase):
    def mk(self, relay=None, live=True, **cfg_kw):
        _chdir_tmp(self)
        import customer_sync
        self.cs = customer_sync
        hs = FakeHS()
        s = customer_sync.CustomerSync(_cfg(**cfg_kw), hs, FakeGIO(), live=live, relay=relay)
        return s, hs

    def test_salvaged_payload_creates_with_consent(self):
        s, hs = self.mk()
        state, note = s.handle_row(crow(note=template(location='Bldg "N" 1')))
        self.assertEqual(state, "done")
        self.assertTrue(note.endswith("(salvaged)"))
        props = hs.writes[-1][2]["properties"]
        self.assertEqual(props["customer_location"], 'Bldg "N" 1')
        self.assertEqual(props["salla_consent_status"], "true")
        self.assertEqual(props["date_of_birth"], "1990-04-02")

    def test_lost_payload_is_looked_up(self):
        api = {"status": 200, "data": {"id": 501, "first_name": "Sara", "last_name": "K",
                                       "mobile": 500000001, "mobile_code": "+966",
                                       "is_notifications_enabled": True}}
        relay = FakeRelay(reply=api)
        s, hs = self.mk(relay=relay)
        state, note = s.handle_row(crow(note="create failed HTTP 500"))
        self.assertEqual(state, "done")
        self.assertTrue(note.endswith("(looked up)"))
        self.assertIn("fields[]=is_notifications_enabled", relay.paths[0])
        props = hs.writes[-1][2]["properties"]
        self.assertEqual(props["firstname"], "Sara")
        self.assertEqual(props["salla_consent_status"], "true")

    def test_lookup_failure_is_retryable_error(self):
        s, hs = self.mk(relay=FakeRelay(exc=RelayError("timeout")))
        state, note = s.handle_row(crow(note="garbage"))
        self.assertEqual(state, "error")
        self.assertIn("lookup failed", note)
        self.assertEqual(hs.writes, [])
        s2, _ = self.mk(relay=FakeRelay(reply={"status": 404, "data": None}))
        self.assertEqual(s2.handle_row(crow(note="garbage"))[0], "error")

    def test_retry_cap_parks_as_held_and_alerts(self):
        s, _ = self.mk(realtime_max_attempts=12)
        with mock.patch("notify.send_alert", create=True) as al:
            state, note = s._settle(crow(attempts=11), "error", "create failed HTTP 500")
        self.assertEqual(state, "held")
        self.assertIn("gave up after 12 attempts", note)
        self.assertTrue(al.called)
        self.assertEqual(s._settle(crow(attempts=3), "error", "x"), ("error", "x"))

    def test_sweep_rows_never_merge(self):
        s, hs = self.mk()
        hs.contacts = [{"id": "C_new", "properties": {}}, {"id": "C_old", "properties": {}}]
        with mock.patch("notify.send_alert", create=True) as al:
            state, _ = s.handle_row(crow(source="sweep"))
        self.assertEqual(state, "done")
        self.assertEqual(hs.merges, [])
        self.assertTrue(al.called)
        self.assertEqual(hs.writes[-1][1], "/crm/v3/objects/contacts/C_new")

    def test_dry_run_sends_no_alert(self):
        s, _ = self.mk(live=False)
        with mock.patch("notify.send_alert", create=True) as al:
            state, _ = s.handle_row(crow(note="garbage"))
        self.assertEqual(state, "held")
        self.assertFalse(al.called)


class ListGIO(FakeGIO):
    def __init__(self, rows):
        super().__init__()
        self.rows = rows
        self.trims = []

    def queue_read(self, qsid, start_row=2, tab=None):
        return [r for r in self.rows if r["row"] >= start_row]

    def queue_trim(self, qsid, keep, tab=None, deletable=None, **kw):
        self.trims.append((tab, deletable))
        return 0


class TestConsumerLoop(unittest.TestCase):
    def mk(self, rows, **cfg_kw):
        _chdir_tmp(self)
        import customer_sync
        gio = ListGIO(rows)
        s = customer_sync.CustomerSync(_cfg(**cfg_kw), FakeHS(), gio, live=True)
        s.cfg.live_poll_s = 0
        return s, gio

    def test_cursor_walks_past_held_and_marks_keep_payload(self):
        rows = [dict(crow(row=2), status="held"), dict(crow(row=3, cid="502",
                note=template(id="502")), status="done"),
                crow(row=4, cid="503", note=template(id="503"))]
        s, gio = self.mk(rows)
        s._last_rewalk_day = datetime.now().date()          # no re-walk here
        s.run(once=True)
        self.assertEqual(s._start_row, 4)                  # passed held + done
        self.assertEqual(gio.marks[-1][:2], (4, "done"))
        self.assertEqual(gio.mark_kw[-1].get("clear_col"), "I")
        self.assertEqual(gio.mark_kw[-1].get("expect_received_at"), "2026-09-26 10:00:00")

    def test_error_keeps_payload_in_h(self):
        s, gio = self.mk([crow(row=2)])
        s._last_rewalk_day = datetime.now().date()
        s.hs._write = lambda *a, **k: (500, {"message": "boom"})
        s.run(once=True)
        self.assertEqual(gio.marks[-1][1], "error")
        self.assertIn("boom", gio.marks[-1][2])
        self.assertEqual(gio.mark_kw[-1].get("note_col"), "I")

    def test_daily_trim_resets_cursor_and_runs_once(self):
        hour = (datetime.now().hour - 1) % 24               # customers offset +1
        s, gio = self.mk([], realtime_trim_enabled=True, realtime_trim_hour=hour,
                         customer_trim_days=30)
        s._start_row = 900
        s._maybe_trim()
        self.assertEqual(gio.trims, [("Customer Queue", ("done", "superseded"))])
        self.assertEqual(json.loads(Path("mirror/customers_state.json").read_text())["start_row"], 2)
        s._maybe_trim()
        self.assertEqual(len(gio.trims), 1)                # once a day

    def test_trim_off_by_default(self):
        s, gio = self.mk([])
        s._maybe_trim()
        self.assertEqual(gio.trims, [])


class FakeSheetsIO(backfill.GoogleIO):
    """GoogleIO with the Google client replaced by recorders."""

    def __init__(self, rows, tab="Status Queue"):
        self.cfg = Config()
        self.cfg.live_queue_tab = "Live Queue"
        self.sheets_rl = None
        self.rows, self.tab_ = rows, tab
        self.calls = []
        self._sh = mock.MagicMock()
        vals = self._sh.values.return_value
        vals.get.side_effect = lambda **kw: ("get", kw)
        vals.update.side_effect = lambda **kw: ("update", kw)
        vals.batchUpdate.side_effect = lambda **kw: ("values.batchUpdate", kw)
        self._sh.get.side_effect = lambda **kw: ("meta", kw)
        self._sh.batchUpdate.side_effect = lambda **kw: ("batchUpdate", kw)

    @property
    def sheets(self):
        return self._sh

    def queue_read_all(self, qsid, tab=None, chunk=20000):
        return [dict(r) for r in self.rows]

    def _gexec(self, request, what, limiter):
        self.calls.append(request)
        kind, kw = request
        if kind == "meta":
            return {"sheets": [{"properties": {"sheetId": 7, "title": self.tab_}}]}
        if kind == "get":
            return {"values": [["2026-09-26 10:00:00", "501"]]}
        return {}


def qrow(n, status, day="2026-09-01 10:00:00", oid=None):
    return {"row": n, "received_at": day, "order_id": oid or f"o{n}", "reference_id": "",
            "event": "e", "status": status, "attempts": 0, "source": "webhook", "note": ""}


class TestQueueTrimAndMark(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_trim_groups_ranges_bottom_up_and_archives(self):
        rows = [qrow(2, "done"), qrow(3, "superseded"), qrow(4, "held"), qrow(5, "error-final"),
                qrow(6, "queued"), qrow(7, "done", day="2026-09-26 09:00:00")]
        io = FakeSheetsIO(rows)
        cutoff = datetime(2026, 9, 20)
        keep = lambda r: datetime.strptime(r["received_at"], "%Y-%m-%d %H:%M:%S") >= cutoff
        n = io.queue_trim("QS", keep, tab="Status Queue",
                          deletable=("done", "superseded", "error-final"),
                          archive_dir=self.tmp.name)
        self.assertEqual(n, 3)
        deletes = [c for c in io.calls if c[0] == "batchUpdate"]
        ranges = [(q["deleteDimension"]["range"]["startIndex"], q["deleteDimension"]["range"]["endIndex"])
                  for q in deletes[0][1]["body"]["requests"]]
        self.assertEqual(ranges, [(4, 5), (1, 3)])        # row 5, then rows 2-3
        archive = list(Path(self.tmp.name).glob("status-queue-*.csv.gz"))
        with gzip.open(archive[0], "rt") as f:
            self.assertEqual(sum(1 for _ in f) - 1, 3)

    def test_trim_dry_run_and_moved_rows(self):
        io = FakeSheetsIO([qrow(2, "done")])
        self.assertEqual(io.queue_trim("QS", lambda r: False, tab="Status Queue",
                                       dry_run=True, archive_dir=self.tmp.name), 1)
        self.assertFalse([c for c in io.calls if c[0] == "batchUpdate"])
        moving = FakeSheetsIO([qrow(2, "done")])
        reads = iter([[qrow(2, "done")], [qrow(2, "done", oid="other")]])
        moving.queue_read_all = lambda qsid, tab=None, chunk=20000: next(reads)
        self.assertEqual(moving.queue_trim("QS", lambda r: False, tab="Status Queue",
                                           archive_dir=self.tmp.name), 0)
        self.assertFalse([c for c in moving.calls if c[0] == "batchUpdate"])

    def test_mark_note_to_i_keeps_h(self):
        io = FakeSheetsIO([])
        self.assertTrue(io.queue_mark("QS", 9, "501", "error", 2, "boom", tab="Customer Queue",
                                      note_col="I", expect_received_at="2026-09-26 10:00:00"))
        write = io.calls[-1]
        self.assertEqual(write[0], "values.batchUpdate")
        ranges = [d["range"] for d in write[1]["body"]["data"]]
        self.assertEqual(ranges, ["'Customer Queue'!E9:F9", "'Customer Queue'!I9"])

    def test_mark_final_clears_i_and_refuses_moved_row(self):
        io = FakeSheetsIO([])
        io.queue_mark("QS", 9, "501", "done", 1, "created contact 1", tab="Customer Queue",
                      clear_col="I")
        self.assertEqual(io.calls[-1][1]["range"], "'Customer Queue'!E9:I9")
        self.assertEqual(io.calls[-1][1]["body"]["values"][0][-1], "")
        self.assertFalse(io.queue_mark("QS", 9, "501", "done", 1, "x", tab="Customer Queue",
                                       expect_received_at="2026-01-01 00:00:00"))


class TestStatusExhausted(unittest.TestCase):
    def test_exhausted_status_row_goes_to_exceptions_tab(self):
        _chdir_tmp(self)
        import status_relay
        r = status_relay.StatusRelay(_cfg(realtime_max_attempts=3), FakeHS(), FakeGIO(), live=True)
        row = {"row": 5, "order_id": "101", "reference_id": "", "status": "error",
               "event": "status:delivered@2026-08-10T10:00:00", "attempts": 2,
               "source": "webhook", "note": "", "received_at": "x"}
        state, note = r._settle(row, "error", "PATCH failed HTTP 500")
        self.assertEqual(state, "error-final")
        self.assertEqual(r.gio.appends[0][0], "Delivery Status Exceptions")


if __name__ == "__main__":
    unittest.main()
