"""report_digest.py (v2.12): the daily digest's realtime lines.

Customer and Status Queue states, customer payload paths from
customer_sync.log (markers checked against the real CustomerSync code),
sweep finds, consent coverage with its one HubSpot count, workbook capacity
through either sheet_capacity location, and audit events that never got a
sheet row. Every line is measured on its own: a failing one logs a WARNING
and only that line is left out.

Offline, against fakes. Run: python3 -m unittest test_digest_lines -v
"""
import csv
import json
import logging
import os
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import backfill
from backfill import RelayError
import report_digest as rd
from test_realtime_ext import FakeGIO as SyncGIO, FakeHS, _cfg, _chdir_tmp

NOW = datetime(2026, 9, 26, 9, 0, 0)
DAY = "2026-09-25"                       # the daily digest's day (yesterday)
REAL_HS_POST = rd._hs_post               # Base patches it; one test calls it


class QueueGIO:
    """queue_read_all per tab; tabs in `fail` raise like a Sheets 503."""

    def __init__(self, tabs=None, fail=()):
        self.tabs, self.fail, self.reads = tabs or {}, set(fail), []

    def queue_read_all(self, qsid, tab=None, chunk=20000):
        self.reads.append((qsid, tab))
        if tab in self.fail:
            raise RuntimeError(f"HttpError 503 reading {tab}")
        return [dict(r) for r in self.tabs.get(tab, [])]


def qrow(n, status, received="2026-09-26 06:00:00", oid=None):
    return {"row": n, "received_at": received, "order_id": oid or str(1000 + n),
            "reference_id": "", "event": "customer.created", "status": status,
            "attempts": 0, "source": "", "note": ""}


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def capacity_module(name, entries=None, exc=None):
    mod = types.ModuleType(name)

    def capacity(gio, cfg):
        if exc:
            raise exc
        return entries

    mod.capacity = capacity
    return mod


ENTRIES = [{"workbook": "queue", "spreadsheet_id": "QS", "cells": 6_100_000,
            "pct": 61.0, "tabs": {}},
           {"workbook": "audit", "spreadsheet_id": "AUDIT", "cells": 7_600_000,
            "pct": 76.0, "tabs": {}}]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(_chdir_tmp(self))
        for p in (mock.patch.object(rd, "ROOT", self.tmp),
                  mock.patch.dict(os.environ),
                  # never HubSpot from a test; tests that need a reply repatch
                  mock.patch.object(rd, "_hs_post", return_value=None),
                  # no real sheet_capacity module (another change adds one)
                  mock.patch.dict(sys.modules, {"sheet_capacity": None,
                                                "tools.sheet_capacity": None})):
            p.start()
            self.addCleanup(p.stop)
        os.environ.pop("HUBSPOT_ACCESS_TOKEN", None)
        os.environ.pop("ENGINE_CONFIG", None)

    def ops(self, cfg=None, gio=None, now=NOW):
        cfg = cfg or _cfg()
        return rd._ops_watch(DAY, DAY, now=now, engine=(cfg, gio or QueueGIO()))


# ----------------------------------------------------------------------------
# 1. Customer Queue and Status Queue
# ----------------------------------------------------------------------------

class QueueLines(Base):
    def test_counts_by_state_and_oldest_queued(self):
        gio = QueueGIO({
            "Customer Queue": [qrow(2, "done"), qrow(3, "queued", "2026-09-26 06:00:00"),
                               qrow(4, "queued", "2026-09-26 08:30:00"), qrow(5, "held"),
                               qrow(6, "error"), qrow(7, "", "2026-09-26 08:59:00"),
                               {"row": 8, "received_at": "", "order_id": "",
                                "status": ""}],
            "Status Queue": [qrow(2, "deferred"), qrow(3, "error"), qrow(4, "done"),
                             qrow(5, "superseded")]})
        ops = self.ops(gio=gio)
        cq, sq = ops["customer_queue"], ops["status_queue"]
        self.assertEqual((cq["queued"], cq["held"], cq["error"]), (3, 1, 1))
        self.assertEqual(cq["oldest_queued_s"], 3 * 3600)
        self.assertEqual(rd._queue_line(cq),
                         "• Customer Queue: 3 queued (oldest 3h), 1 held, 1 error")
        self.assertEqual(rd._queue_line(sq),
                         "• Status Queue: 0 queued, 0 held, 1 error, 1 deferred")
        self.assertEqual(sorted(gio.reads), [("QS", "Customer Queue"), ("QS", "Status Queue")])

    def test_tab_names_come_from_config(self):
        gio = QueueGIO({"Cust Q": [qrow(2, "queued")], "Stat Q": []})
        ops = self.ops(cfg=_cfg(customer_queue_tab="Cust Q", status_queue_tab="Stat Q"),
                       gio=gio)
        self.assertEqual(ops["customer_queue"]["tab"], "Cust Q")
        self.assertEqual(ops["status_queue"]["tab"], "Stat Q")

    def test_sheets_date_serial_and_unreadable_dates(self):
        serial = (datetime(2026, 9, 26, 6, 0) - datetime(1899, 12, 30)).total_seconds() / 86400
        self.assertLess(abs((rd._when(str(serial)) - datetime(2026, 9, 26, 6, 0))
                            .total_seconds()), 1)
        self.assertEqual(rd._when("2026-09-26T06:00:00.000Z"), datetime(2026, 9, 26, 6, 0))
        self.assertIsNone(rd._when("soon"))
        self.assertIsNone(rd._when("12"))
        gio = QueueGIO({"Customer Queue": [qrow(2, "queued", "soon")]})
        cq = self.ops(gio=gio)["customer_queue"]
        self.assertIsNone(cq["oldest_queued_s"])
        self.assertEqual(rd._queue_line(cq),
                         "• Customer Queue: 1 queued (oldest not recorded), 0 held, 0 error")

    def test_one_failing_tab_drops_only_its_line(self):
        gio = QueueGIO({"Customer Queue": [qrow(2, "queued")]}, fail={"Status Queue"})
        with self.assertLogs("digest", "WARNING") as cm:
            ops = self.ops(gio=gio)
        self.assertIsNone(ops["status_queue"])
        self.assertIsNotNone(ops["customer_queue"])
        self.assertTrue(any("Status Queue" in m and "503" in m for m in cm.output))
        lines = rd._ops_lines(ops)
        self.assertTrue(any(ln.startswith("• Customer Queue") for ln in lines))
        self.assertFalse(any("Status Queue" in ln for ln in lines))

    def test_no_engine_skips_queue_and_capacity_but_not_the_rest(self):
        Path("customer_sync.log").write_text(
            "2026-09-25 10:00:00,000 INFO    [MainThread] CUSTOMER created 1 -> contact 2\n")
        with self.assertLogs("digest", "WARNING") as cm:
            ops = rd._ops_watch(DAY, DAY, now=NOW)       # no config.json here
        self.assertTrue(any("engine config" in m for m in cm.output))
        self.assertIsNone(ops["customer_queue"])
        self.assertIsNone(ops["status_queue"])
        self.assertIsNone(ops["capacity"])
        self.assertEqual(ops["customer_paths"]["json"], 1)


# ----------------------------------------------------------------------------
# 2. Customer payload paths (customer_sync.log)
# ----------------------------------------------------------------------------

LOG = """\
2026-09-24 23:59:58,001 INFO    [MainThread] CUSTOMER payload salvaged 900
2026-09-24 23:59:59,001 INFO    [MainThread] CUSTOMER created 900 -> contact 5900
2026-09-25 00:00:01,001 INFO    [MainThread] CUSTOMER created 101 -> contact 5101
2026-09-25 01:00:00,000 INFO    [MainThread] CUSTOMER updated 102 -> contact 5102
2026-09-25 02:00:00,000 INFO    [MainThread] CUSTOMER payload salvaged 103
2026-09-25 02:00:00,500 INFO    [MainThread] CUSTOMER created 103 -> contact 5103
2026-09-25 03:00:00,000 INFO    [MainThread] CUSTOMER payload looked up 104
2026-09-25 03:00:01,000 INFO    [MainThread] CUSTOMER updated 104 -> contact 5104
2026-09-25 04:00:00,000 ERROR   [MainThread] customers row 17 id 105: gave up after 12 attempts: payload unreadable; Salla lookup failed: relay GET customers/105: HTTP 502
Traceback (most recent call last):
  File "customer_sync.py", line 1, in <module>
2026-09-25 05:00:00,000 INFO    [MainThread] CUSTOMER payload lookup_failed 106
2026-09-25 06:00:00,000 ERROR   [MainThread] CUSTOMER row 19 (107): payload unreadable and no Salla lookup configured -- held
2026-09-25 07:00:00,000 INFO    [MainThread] CUSTOMER created 101 -> contact 5101
2026-09-25 08:00:00,000 DEBUG   [MainThread] PHASE hubspot search
2026-09-26 00:00:01,000 INFO    [MainThread] CUSTOMER created 108 -> contact 5108
"""


class _FixedTime(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return "2026-09-25 10:00:00,000"


class CustomerPaths(Base):
    EXPECT = {"json": 2, "salvaged": 1, "looked up": 1, "lookup_failed": 2, "held": 1}

    def test_counts_distinct_customers_per_path_for_the_day(self):
        Path("customer_sync.log").write_text(LOG)
        p = self.ops()["customer_paths"]
        self.assertEqual(p, self.EXPECT)
        self.assertEqual(rd._paths_line(p),
                         "• Customer payloads yesterday: 2 JSON, 1 salvaged, "
                         "1 looked up, 2 lookup failed, 1 held")

    def test_small_blocks_and_rotated_log(self):
        lines = LOG.splitlines(keepends=True)
        Path("customer_sync.log.1").write_text("".join(lines[:6]))
        Path("customer_sync.log").write_text("".join(lines[6:]))
        with mock.patch.object(rd, "TAIL_BLOCK", 40):
            self.assertEqual(self.ops()["customer_paths"], self.EXPECT)

    def test_no_log_is_no_line(self):
        self.assertIsNone(self.ops()["customer_paths"])

    def test_markers_match_what_customer_sync_logs(self):
        """Drive the real CustomerSync through each path and parse its log."""
        import customer_sync
        from test_capture_hardening import FakeRelay, crow, template
        lg = logging.getLogger("backfill")
        h = logging.FileHandler("customer_sync.log", encoding="utf-8")
        h.setFormatter(_FixedTime("%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s"))
        old = (lg.level, lg.propagate)
        lg.setLevel(logging.DEBUG)
        lg.propagate = False
        lg.addHandler(h)

        def restore():
            lg.removeHandler(h)
            h.close()
            lg.setLevel(old[0])
            lg.propagate = old[1]
        self.addCleanup(restore)

        s = customer_sync.CustomerSync(_cfg(), FakeHS(), SyncGIO(), live=True)
        api = {"status": 200, "data": {"id": 502, "first_name": "Sara", "mobile": 500000002,
                                       "mobile_code": "+966"}}
        with mock.patch("notify.send_alert", create=True):
            s.handle_row(crow(cid="601", note=json.dumps(
                {"id": "601", "first_name": "J", "mobile": "500000006",
                 "mobile_code": "+966"})))                                  # json
            s.handle_row(crow(cid="501", note=template(location='Bldg "N" 1')))  # salvaged
            s.relay = FakeRelay(reply=api)
            s.handle_row(crow(cid="502", note="garbage"))                    # looked up
            s.relay = FakeRelay(exc=RelayError("timeout"))
            row = crow(cid="503", note="garbage", attempts=11)
            s._settle(row, *s.handle_row(row))                               # gave up
            s.relay = None
            s.handle_row(crow(cid="504", note="garbage"))                    # held
        h.flush()
        self.assertEqual(rd._customer_paths(DAY, DAY),
                         {"json": 1, "salvaged": 1, "looked up": 1,
                          "lookup_failed": 1, "held": 1})


# ----------------------------------------------------------------------------
# 3. Sweep finds and 4. consent coverage
# ----------------------------------------------------------------------------

def write_sweep(tmp, last_ts="2026-09-26 04:43:05"):
    (tmp / "mirror/customer_sweep_state.json").write_text(json.dumps({"swept": {
        "2026-09-23": {"customers": 50, "missing": 0, "queued": 0, "ts": "2026-09-24 04:42:10"},
        "2026-09-24": {"customers": 120, "missing": 2, "queued": 2, "ts": "2026-09-26 04:42:30"},
        "2026-09-25": {"customers": 140, "missing": 1, "queued": 1, "ts": last_ts}}}))
    write_csv(tmp / "mirror/customer_sweep.csv", ["ts", "day", "salla_customer_id"],
              [["2026-09-24 04:42:00", "2026-09-22", "700"],
               ["2026-09-26 04:42:30", "2026-09-24", "801"],
               ["2026-09-26 04:42:30", "2026-09-24", "802"],
               ["2026-09-26 04:43:05", "2026-09-25", "803"]])
    write_csv(tmp / "mirror/consent_filled.csv", ["ts", "contact_id", "salla_consent_status"],
              [["2026-09-25 04:45:00", "c1", "true"],
               ["2026-09-25 10:00:00", "c5", "false"],
               ["2026-09-26 04:44:00", "c2", "true"],
               ["2026-09-26 04:44:00", "c3", "false"],
               ["2026-09-26 04:44:01", "c4", "true"]])


class SweepFinds(Base):
    def test_last_run_queued_and_consent_written(self):
        write_sweep(self.tmp)
        s = self.ops()["sweep"]
        self.assertEqual(s["days"], ["2026-09-24", "2026-09-25"])
        self.assertEqual((s["customers"], s["missing"], s["queued"]), (260, 3, 3))
        self.assertEqual(s["ids"], ["801", "802", "803"])
        self.assertEqual(s["consent"], 3)
        self.assertFalse(s["stale"])
        self.assertEqual(rd._sweep_line(s),
                         "• Customer sweep (last run 26 Sep 04:43): 2 day(s), 260 customers "
                         "checked, 3 missing, 3 queued: 801, 802, 803. Consent filler "
                         "wrote 3 flag(s).")

    def test_stale_run_and_long_id_list(self):
        write_sweep(self.tmp)
        with open(self.tmp / "mirror/customer_sweep.csv", "a", newline="") as f:
            csv.writer(f).writerows([["2026-09-26 04:43:05", "2026-09-25", str(900 + i)]
                                     for i in range(4)])
        s = self.ops(now=NOW + timedelta(days=3))["sweep"]
        self.assertTrue(s["stale"])
        line = rd._sweep_line(s)
        self.assertIn("801, 802, 803, 900, 901 and 2 more.", line)
        self.assertIn("⚠️ no run for 3 days", line)

    def test_never_ran_is_none_and_no_consent_ledger(self):
        self.assertIsNone(self.ops()["sweep"])
        write_sweep(self.tmp)
        (self.tmp / "mirror/consent_filled.csv").unlink()
        s = self.ops()["sweep"]
        self.assertIsNone(s["consent"])
        self.assertNotIn("Consent filler", rd._sweep_line(s))

    def test_corrupt_state_warns_and_omits(self):
        (self.tmp / "mirror/customer_sweep_state.json").write_text("{not json")
        with self.assertLogs("digest", "WARNING") as cm:
            ops = self.ops()
        self.assertIsNone(ops["sweep"])
        self.assertTrue(any("customer sweep" in m for m in cm.output))


class ConsentCoverage(Base):
    def test_flags_in_last_24h_and_hubspot_gap(self):
        write_sweep(self.tmp)
        calls = []

        def post(path, body):
            calls.append((path, body))
            return {"total": 12, "results": []}

        with mock.patch.object(rd, "_hs_post", side_effect=post):
            c = self.ops()["consent"]
        self.assertEqual((c["written"], c["yes"], c["no"], c["gap"]), (4, 2, 2, 12))
        self.assertEqual(len(calls), 1)
        path, body = calls[0]
        self.assertEqual(path, "/crm/v3/objects/contacts/search")
        f = {x["propertyName"]: x for x in body["filterGroups"][0]["filters"]}
        self.assertEqual(f["salla_customer_id"]["operator"], "HAS_PROPERTY")
        self.assertEqual(f["salla_consent_status"]["operator"], "NOT_HAS_PROPERTY")
        self.assertEqual(f["createdate"]["value"],
                         str(int((NOW - timedelta(days=3)).timestamp() * 1000)))
        self.assertEqual(body["limit"], 1)
        self.assertEqual(rd._consent_line(c),
                         "• Consent flags written in the last 24h: 4 (2 opted in, 2 opted "
                         "out). Contacts created in the last 3 days with no flag: 12.")

    def test_window_follows_consent_filler_days(self):
        with mock.patch.object(rd, "_hs_post", return_value={"total": 0}) as post:
            c = self.ops(cfg=_cfg(consent_filler_days=5))["consent"]
        body = post.call_args[0][1]
        self.assertEqual(body["filterGroups"][0]["filters"][2]["value"],
                         str(int((NOW - timedelta(days=5)).timestamp() * 1000)))
        self.assertIsNone(c["written"])
        self.assertEqual(rd._consent_line(c),
                         "• Consent flags written in the last 24h: not recorded. Contacts "
                         "created in the last 5 days with no flag: 0.")

    def test_gap_unmeasured_is_not_recorded_never_zero(self):
        write_sweep(self.tmp)
        c = self.ops()["consent"]                       # no token: _hs_post -> None
        self.assertIsNone(c["gap"])
        self.assertTrue(rd._consent_line(c).endswith("with no flag: not recorded."))
        with mock.patch.object(rd, "_hs_post", side_effect=OSError("HTTP 502")):
            with self.assertLogs("digest", "WARNING") as cm:
                c = self.ops()["consent"]
        self.assertIsNone(c["gap"])
        self.assertEqual(c["written"], 4)
        self.assertTrue(any("consent gap" in m for m in cm.output))

    def test_nothing_measurable_is_no_line(self):
        self.assertIsNone(self.ops()["consent"])

    def test_hs_post_token_in_header_only(self):
        with mock.patch("urllib.request.urlopen") as op:
            self.assertIsNone(REAL_HS_POST("/crm/v3/objects/contacts/search", {}))
            self.assertFalse(op.called)                 # no token: no request at all
            os.environ["HUBSPOT_ACCESS_TOKEN"] = "placeholder-not-a-token"
            op.return_value.read.return_value = b'{"total": 7}'
            self.assertEqual(REAL_HS_POST("/crm/v3/objects/contacts/search",
                                          {"limit": 1})["total"], 7)
        req = op.call_args[0][0]
        self.assertEqual(req.full_url,
                         "https://api.hubapi.com/crm/v3/objects/contacts/search")
        self.assertNotIn("placeholder", req.full_url)
        self.assertEqual(req.get_header("Authorization"), "Bearer placeholder-not-a-token")


# ----------------------------------------------------------------------------
# 5. Workbook capacity
# ----------------------------------------------------------------------------

class Capacity(Base):
    def test_top_level_module_and_format(self):
        with mock.patch.dict(sys.modules, {"sheet_capacity": capacity_module(
                "sheet_capacity", ENTRIES)}):
            cap = self.ops()["capacity"]
        self.assertEqual(rd._capacity_line(cap),
                         "• Workbooks: queue 6.1M of 10M cells (61%), audit 7.6M (76%)")
        self.assertEqual(rd._capacity_breaches({"ops": {"capacity": cap}}), [])

    def test_tools_module_and_alert_above_threshold(self):
        mod = capacity_module("tools.sheet_capacity", ENTRIES)
        with mock.patch.dict(sys.modules, {"tools.sheet_capacity": mod}):
            cap = self.ops(cfg=_cfg(capacity_alert_pct=70.0))["capacity"]
        self.assertEqual(rd._capacity_line(cap),
                         "• Workbooks: queue 6.1M of 10M cells (61%), audit 7.6M (76%) ⚠️")
        self.assertEqual(rd._capacity_breaches({"ops": {"capacity": cap}}),
                         ["audit workbook at 76% of its cell limit"])

    def test_pct_missing_is_derived_from_cells(self):
        mod = capacity_module("sheet_capacity", [{"workbook": "queue", "cells": 850_000}])
        with mock.patch.dict(sys.modules, {"sheet_capacity": mod}):
            cap = self.ops()["capacity"]
        self.assertEqual(rd._capacity_line(cap), "• Workbooks: queue 850k of 10M cells (8%)")

    def test_no_module_is_no_line(self):
        self.assertIsNone(self.ops()["capacity"])

    def test_failing_capacity_warns_and_omits(self):
        mod = capacity_module("sheet_capacity", exc=RuntimeError("quota"))
        with mock.patch.dict(sys.modules, {"sheet_capacity": mod}):
            with self.assertLogs("digest", "WARNING") as cm:
                ops = self.ops()
        self.assertIsNone(ops["capacity"])
        self.assertTrue(any("workbook capacity" in m for m in cm.output))


# ----------------------------------------------------------------------------
# 6. Audit events with no sheet row
# ----------------------------------------------------------------------------

class AuditMisses(Base):
    def write(self):
        m = backfill.LocalMirror(self.tmp / "mirror")
        for ts, ev, row, vals in (
                ("2026-09-25 08:00:00", "arrived_append", -1, {0: "1"}),      # too old
                ("2026-09-25 10:00:00", "arrived_append", -1, {0: "2"}),
                ("2026-09-25 11:00:00", "arrived_append", 1234, {0: "3"}),    # has a row
                ("2026-09-26 01:00:00", "processed_update", -1, {0: "4"}),
                ("2026-09-26 02:00:00", "arrived_append", -1,
                 {0: "5", 10: 'Serum, "Gold"\nEdition'}),
                ("2026-09-26 09:30:00", "arrived_append", -1, {0: "6"})):     # after now
            with mock.patch.object(backfill, "now_str", return_value=ts):
                m.audit_event(ev, row, vals)

    def test_counts_sheet_row_minus_one_in_last_24h(self):
        self.write()
        a = self.ops()["audit_misses"]
        self.assertEqual(a, {"total": 3, "by_event": {"arrived_append": 2,
                                                      "processed_update": 1}})
        self.assertEqual(rd._audit_line(a),
                         "• Audit sheet: 3 event(s) in the last 24h got no sheet row "
                         "(arrivals 2, processing updates 1). The local mirror has them.")

    def test_small_blocks_give_the_same_answer(self):
        self.write()
        with mock.patch.object(rd, "TAIL_BLOCK", 64):
            self.assertEqual(self.ops()["audit_misses"]["total"], 3)

    def test_quiet_sheet_and_missing_mirror(self):
        self.assertIsNone(self.ops()["audit_misses"])
        backfill.LocalMirror(self.tmp / "mirror")
        a = self.ops()["audit_misses"]
        self.assertEqual(a["total"], 0)
        self.assertIsNone(rd._audit_line(a))


class TailSince(Base):
    def test_backwards_read_returns_every_line_from_since(self):
        p = self.tmp / "big.log"
        start = datetime(2026, 9, 23)
        rows = [f"{(start + timedelta(minutes=37 * i)):%Y-%m-%d %H:%M:%S},000 INFO x{i}"
                for i in range(300)]
        p.write_text("header\n" + "\n".join(rows) + "\n")
        want = [r for r in rows if r[:19] >= "2026-09-25 00:00:00"]
        for block in (17, 256, 1 << 20):
            got = [ln for ln in rd._tail_since(p, "2026-09-25", block=block)
                   if ln[:4].isdigit() and ln[:10] >= "2026-09-25"]
            self.assertEqual(got, want, block)
        self.assertEqual(rd._tail_since(p, "2020-01-01", block=64)[0], "header")


# ----------------------------------------------------------------------------
# rendering: placement, isolation, weekly silence, plain text
# ----------------------------------------------------------------------------

def digest(ops, period="daily"):
    return {"period": period, "label": "Friday 25 September", "start": DAY, "end": DAY,
            "days_recorded": 1, "days_expected": 1, "created": 10, "prev_created": 9,
            "daily_series": [], "errors_unrecovered": 0, "partials": 0,
            "availability": 100.0, "queue_depth_max": None, "wait_max_s": None,
            "credits_burned": None, "credits_daily": [], "live_created": None,
            "alerts": [], "restarts": 0, "latest": {}, "now": {},
            "live_held": None, "gift_watch": None, "reconcile": None,
            "backfill_stalled": False, "ops": ops}


class Render(Base):
    def full_ops(self, alert_pct=80.0):
        write_sweep(self.tmp)
        Path("customer_sync.log").write_text(LOG)
        gio = QueueGIO({"Customer Queue": [qrow(2, "queued"), qrow(3, "held")],
                        "Status Queue": [qrow(2, "error")]})
        with mock.patch.dict(sys.modules, {"sheet_capacity": capacity_module(
                "sheet_capacity", ENTRIES)}), \
                mock.patch.object(rd, "_hs_post", return_value={"total": 12}):
            return self.ops(cfg=_cfg(capacity_alert_pct=alert_pct), gio=gio)

    def render(self, a):
        with mock.patch.object(rd, "_stuck_in_review", return_value=None):
            return rd.render(a)

    def test_headline_and_thread_placement(self):
        head, replies = self.render(digest(self.full_ops()))
        self.assertIn("• Workbooks: queue 6.1M of 10M cells (61%), audit 7.6M (76%)", head)
        self.assertIn("Healthy.", head)
        block = [r for r in replies if r.startswith("*Customers and realtime queues*")]
        self.assertEqual(len(block), 1)
        lines = block[0].split("\n")[1:]
        self.assertEqual([ln.split(":")[0] for ln in lines],
                         ["• Customer Queue", "• Status Queue", "• Customer payloads yesterday",
                          "• Customer sweep (last run 26 Sep 04", "• Consent flags written in the last 24h"])
        v212 = "\n".join(lines + [rd._capacity_line(self.full_ops()["capacity"])])
        self.assertNotIn("—", v212)                     # no em dashes in new text

    def test_capacity_breach_reaches_the_verdict(self):
        head, _ = self.render(digest(self.full_ops(alert_pct=70.0)))
        self.assertIn("Needs a look: audit workbook at 76% of its cell limit.", head)
        self.assertIn("audit 7.6M (76%) ⚠️", head)

    def test_a_line_that_cannot_render_is_dropped_alone(self):
        ops = self.full_ops()
        ops["sweep"] = {"ts": "not a datetime"}
        ops["capacity"] = {"entries": [{"workbook": "queue"}], "alert_pct": 80.0}
        with self.assertLogs("digest", "WARNING") as cm:
            head, replies = self.render(digest(ops))
        self.assertTrue(any("customer sweep" in m for m in cm.output))
        self.assertTrue(any("workbook capacity" in m for m in cm.output))
        self.assertNotIn("Workbooks", head)
        block = [r for r in replies if r.startswith("*Customers and realtime queues*")][0]
        self.assertNotIn("Customer sweep", block)
        self.assertIn("• Customer Queue: 1 queued", block)
        self.assertIn("• Consent flags written", block)

    def test_weekly_and_empty_ops_add_nothing(self):
        for ops in (None, {}):
            head, replies = self.render(digest(ops, period="weekly"))
            self.assertNotIn("Workbooks", head)
            self.assertFalse(any("realtime queues" in r for r in replies))


if __name__ == "__main__":
    unittest.main(verbosity=1)
