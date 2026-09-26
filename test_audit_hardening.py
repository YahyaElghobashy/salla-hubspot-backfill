"""v2.12 audit hardening: the arrival append's fallback backs off and checks
column A before writing a second row, the local mirror ties update events to
their order id, and tools/audit_replay.py rebuilds rows a Sheets outage lost.
Offline, against an in-memory Sheets service.

Run: python3 -m unittest test_audit_hardening -v
"""
import csv
import json
import logging
import re
import threading
import unittest
from pathlib import Path
from unittest import mock

import backfill
from backfill import AUDIT_WIDTH, LocalMirror
from test_order_selfheal import ORDER, FakeEngine
from test_realtime_ext import _cfg, _chdir_tmp
from tools import audit_replay

TAB = "Order Audit Log"


class FakeReq:
    def __init__(self, fn):
        self.fn = fn

    def execute(self):
        return self.fn()


class FakeValues:
    def __init__(self, svc):
        self.svc = svc

    def append(self, spreadsheetId, body, **kw):
        def run():
            mode = self.svc.append_script.pop(0) if self.svc.append_script else "ok"
            if mode == "fail":
                raise TimeoutError("The read operation timed out")
            self.svc.rows.append(list(body["values"][0]))
            n = len(self.svc.rows)
            if mode == "land":  # written server-side, error on the way back
                raise RuntimeError("<HttpError 502 when requesting append>")
            return {"updates": {"updatedRange": f"'{TAB}'!A{n}:AE{n}"}}
        return FakeReq(run)

    def get(self, spreadsheetId, **kw):
        rng = kw["range"]   # the API's keyword; the builtin stays usable below

        def run():
            self.svc.reads.append(rng)
            if self.svc.read_fail:
                self.svc.read_fail -= 1
                raise RuntimeError("<HttpError 500 when reading>")
            m = re.search(r"!A(\d+):A(\d*)$", rng)
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else len(self.svc.rows)
            vals = []
            for n in range(start, min(end, len(self.svc.rows)) + 1):
                v = str(self.svc.rows[n - 1][0]) if self.svc.rows[n - 1] else ""
                # UNFORMATTED_VALUE hands numeric ids back as numbers
                vals.append([int(v)] if v.isdigit() else ([v] if v else []))
            while vals and not vals[-1]:
                vals.pop()
            return {"values": vals} if vals else {}
        return FakeReq(run)


class FakeSheets:
    """One audit tab in memory: rows[0] is the header (sheet row 1). The grid
    keeps blank_tail empty rows below the data, like a real tab."""

    def __init__(self, ids=(), blank_tail=0):
        self.rows = [["Salla Order ID"]] + [[str(i)] + [""] * (AUDIT_WIDTH - 1) for i in ids]
        self.blank_tail = blank_tail
        self.append_script = []   # per append: "ok" | "fail" | "land"
        self.read_fail = 0
        self.reads = []
        self.meta_calls = 0

    def values(self):
        return FakeValues(self)

    def get(self, spreadsheetId, fields):
        def run():
            self.meta_calls += 1
            return {"sheets": [
                {"properties": {"title": "Queue Log",
                                "gridProperties": {"rowCount": 5}}},
                {"properties": {"title": TAB, "gridProperties": {
                    "rowCount": len(self.rows) + self.blank_tail}}}]}
        return FakeReq(run)

    def col_a(self):
        return [r[0] for r in self.rows[1:]]


class FakeGIO(backfill.GoogleIO):
    """The real GoogleIO with the Google plumbing swapped for FakeSheets."""

    def __init__(self, cfg, svc):
        self._svc = svc
        super().__init__(cfg, enabled=True)
        self.whats = []

    def _auth(self):
        pass

    @property
    def sheets(self):
        return self._svc

    def _gexec(self, request, what, limiter):
        self.whats.append(what)
        return request.execute()


def _quiet(test):
    """Keep the engine's expected error lines out of the test output; assertLogs
    still sees them."""
    h, lg = logging.NullHandler(), logging.getLogger("backfill")
    lg.addHandler(h)
    test.addCleanup(lg.removeHandler, h)


def _ledger(path="mirror/audit_fallback.csv"):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _arrival(oid, link="https://drive.google.com/file/d/x/view"):
    return {0: oid, 1: f"R{oid}", 11: "Order Arrived", 27: link,
            29: "2026-09-24 10:00:00", 30: "No"}


class TestAuditAppendFallback(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        _quiet(self)
        self.cfg = _cfg()
        self.sleeps = []
        p = mock.patch("backfill.time.sleep", side_effect=self.sleeps.append)
        p.start()
        self.addCleanup(p.stop)

    def gio(self, ids=range(1000, 1030), blank_tail=0):
        self.svc = FakeSheets(ids, blank_tail)
        return FakeGIO(self.cfg, self.svc)

    def test_clean_append_has_no_fallback(self):
        g = self.gio()
        self.assertEqual(g.audit_append(_arrival("5001")), 32)
        self.assertEqual(self.sleeps, [])
        self.assertEqual(self.svc.reads, [])
        self.assertFalse(Path("mirror/audit_fallback.csv").exists())

    def test_disabled_returns_minus_one(self):
        g = self.gio()
        g.enabled = False
        self.assertEqual(g.audit_append(_arrival("5001")), -1)
        self.assertEqual(g.whats, [])

    def test_append_that_landed_despite_error_is_reused(self):
        g = self.gio()
        self.svc.append_script = ["land"]
        with self.assertLogs("backfill", "WARNING") as logs:
            row = g.audit_append(_arrival("5001"))
        self.assertEqual(row, 32)
        self.assertEqual(self.svc.col_a().count("5001"), 1)   # no twin row
        self.assertEqual(self.sleeps, [3.0])                  # waited before checking
        self.assertIn("landed at row 32", "\n".join(logs.output))
        led = _ledger()
        self.assertEqual([(r["salla_order_id"], r["outcome"], r["sheet_row"]) for r in led],
                         [("5001", "found", "32")])
        self.assertEqual(g.audit_fallback_counts, {"found": 1})

    def test_fallback_row_after_backoff_when_first_did_not_land(self):
        g = self.gio()
        self.svc.append_script = ["fail", "ok"]
        row = g.audit_append(_arrival("5001"))
        self.assertEqual(row, 32)
        self.assertEqual(self.sleeps, [3.0])
        self.assertEqual(self.svc.rows[-1][27], backfill.GoogleIO.AUDIT_FALLBACK_TEXT)
        self.assertEqual(self.svc.col_a().count("5001"), 1)
        self.assertEqual(_ledger()[0]["outcome"], "fallback")

    def test_second_attempt_rechecks_a_fallback_that_landed(self):
        g = self.gio()
        self.svc.append_script = ["fail", "land", "ok"]
        row = g.audit_append(_arrival("5001"))
        self.assertEqual(row, 32)
        self.assertEqual(self.sleeps, [3.0, 10.0])
        self.assertEqual(self.svc.col_a().count("5001"), 1)
        self.assertEqual(_ledger()[0]["outcome"], "found")

    def test_all_attempts_fail_records_lost(self):
        g = self.gio()
        self.svc.append_script = ["fail", "fail", "fail"]
        with self.assertLogs("backfill", "ERROR") as logs:
            self.assertEqual(g.audit_append(_arrival("5001")), -1)
        self.assertEqual(self.sleeps, [3.0, 10.0])
        self.assertIn("Audit fallback append also failed", "\n".join(logs.output))
        led = _ledger()
        self.assertEqual((led[0]["outcome"], led[0]["sheet_row"], led[0]["attempts"]),
                         ("lost", "-1", "2"))
        self.assertIn("timed out", led[0]["error"])

    def test_tail_read_failure_still_appends(self):
        g = self.gio()
        self.svc.append_script = ["fail", "ok"]
        self.svc.read_fail = 5
        self.assertEqual(g.audit_append(_arrival("5001")), 32)
        self.assertEqual(_ledger()[0]["outcome"], "fallback_unchecked")

    def test_backoff_schedule_is_configurable(self):
        self.cfg.audit_fallback_backoff_s = [1, 2, 4]
        g = self.gio()
        self.svc.append_script = ["fail", "fail", "fail", "fail"]
        self.assertEqual(g.audit_append(_arrival("5001")), -1)
        self.assertEqual(self.sleeps, [1.0, 2.0, 4.0])

    def test_ledger_can_be_disabled(self):
        self.cfg.audit_fallback_ledger = ""
        g = self.gio()
        self.svc.append_script = ["land"]
        self.assertEqual(g.audit_append(_arrival("5001")), 32)
        self.assertFalse(Path("mirror/audit_fallback.csv").exists())

    def test_without_a_hint_the_read_walks_up_past_blank_grid_rows(self):
        g = self.gio(blank_tail=1500)
        self.svc.append_script = ["land"]
        self.assertEqual(g.audit_append(_arrival("5001")), 32)
        self.assertEqual(self.svc.meta_calls, 1)
        # grid ends at row 1532: first page 533..1532 is all blank, then 2..532
        self.assertEqual(self.svc.reads, [f"'{TAB}'!A533:A1532", f"'{TAB}'!A2:A532"])

    def test_with_a_hint_one_open_ended_read_from_above_it(self):
        g = self.gio(ids=range(1000, 1200))           # rows 2..201
        self.assertEqual(g.audit_append(_arrival("5000")), 202)
        self.svc.append_script = ["land"]
        self.assertEqual(g.audit_append(_arrival("5001")), 203)
        self.assertEqual(self.svc.reads, [f"'{TAB}'!A152:A"])
        self.assertEqual(self.svc.meta_calls, 0)

    def test_id_only_above_the_tail_window_is_not_matched(self):
        g = self.gio(ids=range(1000, 1200))
        self.assertEqual(g.audit_append(_arrival("5000")), 202)
        self.svc.rows[1][0] = "5001"                     # same id far up, row 2
        self.svc.append_script = ["fail", "ok"]
        self.assertEqual(g.audit_append(_arrival("5001")), 203)
        self.assertEqual(_ledger()[0]["outcome"], "fallback")

    def test_audit_ids_last_occurrence_wins(self):
        g = self.gio(ids=["11", "12", "11"])
        self.assertEqual(g.audit_ids(), {"11": 4, "12": 3})


class TestMirrorOrderId(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        _quiet(self)

    def rows(self):
        with open("mirror/audit_mirror.csv", newline="") as f:
            return list(csv.reader(f))

    def test_new_header_appends_order_id_at_the_end(self):
        m = LocalMirror("mirror")
        m.audit_event("arrived_append", 7, {0: "5001", 11: "Order Arrived"})
        m.audit_event("processed_update", 7, {11: "Order Approved"}, order_id="5001")
        m.audit_event("queued_update", -1, {11: "Held for Review"})
        hdr, a, p, q = self.rows()
        self.assertEqual(hdr[:3 + AUDIT_WIDTH],
                         ["ts", "event", "sheet_row"] + [f"c{i}" for i in range(AUDIT_WIDTH)])
        self.assertEqual(hdr[-1], "order_id")
        self.assertEqual((a[-1], p[-1], q[-1]), ("5001", "5001", ""))
        # readers that zip header and row (tools/qa_spot_check.py) still work
        d = dict(zip(hdr, p))
        self.assertEqual((d["event"], d["c11"], d["order_id"]),
                         ("processed_update", "Order Approved", "5001"))

    def test_old_header_file_is_kept_and_still_readable(self):
        old_hdr = ["ts", "event", "sheet_row"] + [f"c{i}" for i in range(AUDIT_WIDTH)]
        old_row = ["2026-09-20 10:00:00", "arrived_append", "40", "4001"] + [""] * (AUDIT_WIDTH - 1)
        old_upd = ["2026-09-20 10:00:05", "processed_update", "40", ""] + [""] * (AUDIT_WIDTH - 1)
        with open("mirror/audit_mirror.csv", "w", newline="") as f:
            csv.writer(f).writerows([old_hdr, old_row, old_upd])
        m = LocalMirror("mirror")
        m.audit_event("processed_update", -1, {14: "HS1"}, order_id="5001")
        rows = self.rows()
        self.assertEqual(rows[0], old_hdr)                   # never rewritten
        d = dict(zip(rows[0], rows[-1]))                     # old-style reader
        self.assertEqual((d["sheet_row"], d["c14"]), ("-1", "HS1"))
        got = list(LocalMirror.read_audit("mirror/audit_mirror.csv"))
        self.assertEqual([g["order_id"] for g in got], ["4001", "", "5001"])
        self.assertEqual(got[2]["c14"], "HS1")

    def test_route_held_mirrors_the_order_id(self):
        e = object.__new__(backfill.Engine)
        e.legacy, e.is_live_sync, e.live = None, True, False
        e.stats, e._stats_lock, e._outcome = backfill.Stats(), threading.Lock(), {}
        e.mirror = LocalMirror("mirror")
        e.route_held({"id": 8123, "items": []}, -1, [{"name": "Serum"}])
        ev = [g for g in LocalMirror.read_audit("mirror/audit_mirror.csv")]
        self.assertEqual([(g["event"], g["sheet_row"], g["order_id"]) for g in ev],
                         [("queued_update", "-1", "8123")])

    def test_processed_update_mirrors_the_order_id(self):
        e = FakeEngine([{"101"}, {"101", "102"}])
        e.cfg = backfill.Config()
        e._outcome, e.stats = {}, {}
        e._bump = lambda k: None
        e.created_ledger = mock.Mock()
        e._stamp_signals = mock.Mock()
        e.hs.create_order = mock.Mock(return_value=("H9", True))   # fresh create
        e.hs.patch_order = mock.Mock()
        e.live = False
        e.mirror = LocalMirror("mirror")
        e._finish_create(ORDER, -1, "C1", 1)
        ev = [g for g in LocalMirror.read_audit("mirror/audit_mirror.csv")
              if g["event"] == "processed_update"]
        self.assertEqual([(g["sheet_row"], g["order_id"], g["c14"]) for g in ev],
                         [("-1", "7", "H9")])


def _mirror_row(ts, event, sheet_row, values, order_id=None, width_old=False):
    row = [""] * AUDIT_WIDTH
    for i, v in values.items():
        row[i] = v
    out = [ts, event, str(sheet_row)] + row
    if not width_old:
        out.append(order_id if order_id is not None else values.get(0, ""))
    return out


class TestAuditReplay(unittest.TestCase):
    LINK = "https://drive.google.com/file/d/abc/view"

    def setUp(self):
        _chdir_tmp(self)
        _quiet(self)
        self.cfg = _cfg()
        p = mock.patch("backfill.time.sleep")
        p.start()
        self.addCleanup(p.stop)
        # a pre-v2.12 mirror: old header, update rows carry no order id
        old_hdr = ["ts", "event", "sheet_row"] + [f"c{i}" for i in range(AUDIT_WIDTH)]
        a = lambda oid, link=self.LINK: {0: oid, 1: f"R{oid}", 2: "2026-09-24 09:59:00",
                                         11: "Order Arrived", 27: link, 30: "No"}
        rows = [
            old_hdr,
            # outside the window: never considered
            _mirror_row("2026-09-22 23:59:59", "arrived_append", -1, a("100"), width_old=True),
            # lost in the outage: -1 and absent from column A
            _mirror_row("2026-09-23 08:00:00", "arrived_append", -1, a("201"), width_old=True),
            _mirror_row("2026-09-23 08:00:04", "processed_update", -1, {11: "Order Approved"},
                        width_old=True),
            # -1 in the mirror, but the append landed anyway: present
            _mirror_row("2026-09-24 12:00:00", "arrived_append", -1, a("202"), width_old=True),
            # healthy row, in column A: not a candidate
            _mirror_row("2026-09-24 12:05:00", "arrived_append", 41, a("203"), width_old=True),
            # a real row number, but the id is not in column A
            _mirror_row("2026-09-25 18:00:00", "arrived_append", 57, a("204"), width_old=True),
            # a dry run: -1, no Drive link, no later update
            _mirror_row("2026-09-25 19:00:00", "arrived_append", -1,
                        a("205", link="Drive upload failed"), width_old=True),
            # after the window
            _mirror_row("2026-09-26 00:00:01", "arrived_append", -1, a("206"), width_old=True),
        ]
        with open("mirror/audit_mirror.csv", "w", newline="") as f:
            csv.writer(f).writerows(rows)
        # lines written after the v2.12 deploy carry the order id at the end
        m = LocalMirror("mirror")
        with mock.patch("backfill.now_str", return_value="2026-09-25 20:00:00"):
            m.audit_event("arrived_append", -1, a("207", link="Drive upload failed"))
            m.audit_event("processed_update", -1, {11: "Order Approved"}, order_id="207")
            m.audit_event("queued_update", -1, {11: "Held for Review"}, order_id="208")
        self.svc = FakeSheets(ids=["150", "202", "203"])
        self.gio = FakeGIO(self.cfg, self.svc)

    def ledger(self):
        with open("mirror/audit_replay.csv", newline="") as f:
            return list(csv.DictReader(f))

    def test_dry_run_reports_and_writes_nothing(self):
        counts = audit_replay.replay(self.cfg, self.gio)
        self.assertEqual(counts, {"would_append": 3, "present": 1,
                                  "skipped_unlinked": 1, "no_arrival_in_range": 1})
        self.assertEqual(self.svc.col_a(), ["150", "202", "203"])   # untouched
        led = {r["order_id"]: r for r in self.ledger()}
        self.assertEqual(led["201"]["action"], "would_append")
        self.assertEqual(led["201"]["reason"], "row -1, not in column A")
        self.assertEqual(led["202"]["action"], "present")
        self.assertEqual(led["202"]["sheet_row"], "3")
        self.assertEqual(led["204"]["reason"], "not in column A")
        self.assertEqual(led["205"]["action"], "skipped_unlinked")
        # no Drive link, but a later update tied by order id proves a live run
        self.assertEqual(led["207"]["action"], "would_append")
        self.assertEqual(led["208"]["action"], "no_arrival_in_range")
        self.assertTrue(all(r["mode"] == "dry-run" for r in led.values()))
        self.assertNotIn("100", led)
        self.assertNotIn("206", led)
        self.assertNotIn("203", led)

    def test_apply_appends_missing_arrivals_once(self):
        counts = audit_replay.replay(self.cfg, self.gio, apply=True)
        self.assertEqual(counts["appended"], 3)
        self.assertEqual(self.svc.col_a(), ["150", "202", "203", "201", "204", "207"])
        replayed = self.svc.rows[4]
        self.assertEqual((replayed[1], replayed[11], replayed[27]),
                         ("R201", "Order Arrived", self.LINK))
        led = {r["order_id"]: r for r in self.ledger() if r["mode"] == "apply"}
        self.assertEqual((led["201"]["action"], led["201"]["sheet_row"]), ("appended", "5"))
        ev = [g for g in LocalMirror.read_audit("mirror/audit_mirror.csv")
              if g["event"] == "replayed_append"]
        self.assertEqual([(g["order_id"], g["sheet_row"]) for g in ev],
                         [("201", "5"), ("204", "6"), ("207", "7")])
        # a second run finds them in column A and appends nothing
        again = audit_replay.replay(self.cfg, self.gio, apply=True)
        self.assertNotIn("appended", again)
        self.assertEqual(len(self.svc.col_a()), 6)

    def test_include_unlinked(self):
        counts = audit_replay.replay(self.cfg, self.gio, include_unlinked=True)
        self.assertEqual(counts["would_append"], 4)

    def test_failed_append_is_recorded_and_exit_code_is_one(self):
        Path("config.json").write_text(json.dumps({"spreadsheet_id": "AUDIT"}))
        self.svc.append_script = ["fail"] * 20
        with mock.patch.object(audit_replay, "setup_logging"):
            rc = audit_replay.main(["--config", "config.json", "--apply"], gio=self.gio)
        self.assertEqual(rc, 1)
        acts = [r["action"] for r in self.ledger()]
        self.assertEqual(acts.count("append_failed"), 3)

    def test_date_range_flags(self):
        Path("config.json").write_text(json.dumps({"spreadsheet_id": "AUDIT"}))
        with mock.patch.object(audit_replay, "setup_logging"):
            rc = audit_replay.main(["--config", "config.json", "--since", "2026-09-25",
                                    "--until", "2026-09-26"], gio=self.gio)
        self.assertEqual(rc, 0)
        ids = {r["order_id"] for r in self.ledger()}
        self.assertEqual(ids, {"204", "205", "206", "207", "208"})
        with self.assertRaises(SystemExit):
            audit_replay.main(["--since", "2026-09-26", "--until", "2026-09-23"])

    def test_refuses_while_trim_lock_held(self):
        Path("mirror/trim.lock").write_text("{}")
        with self.assertRaises(SystemExit) as cm:
            audit_replay.main(["--apply"], gio=self.gio)
        self.assertIn("trim", str(cm.exception.code))
        self.assertFalse(Path("mirror/audit_replay.csv").exists())
        self.assertEqual(self.svc.reads, [])


if __name__ == "__main__":
    unittest.main()
