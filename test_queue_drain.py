#!/usr/bin/env python3
"""Offline test suite for queue_drain.py (no network, no sheets).

    ./venv/bin/python3 -m unittest test_queue_drain -v

Covers: .env parsing, claim/dedupe rules, the cached catalog gate, pre-existing
resolution, drain_one dispatch, buffered audit updates (incl. hold-reason
injection + contiguous-run grouping), Queue Log verify-then-write marking, and
stress passes (20k-row claim path, 5k-mark batching, threaded gate cache).
"""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import backfill
import queue_drain
from backfill import Config
from queue_drain import (QC_ATTEMPTS, QC_NOTES, QC_OID, QC_PROCESSED_AT,
                         QC_RESULT, QC_STATUS, DrainGoogleIO,
                         QueueDrainEngine, load_dotenv)


# ----------------------------------------------------------------------------
# fakes
# ----------------------------------------------------------------------------

class FakeGio(DrainGoogleIO):
    """DrainGoogleIO with the Google plumbing replaced by recorders."""

    def __init__(self, cfg):
        self._svc = MagicMock()
        super().__init__(cfg, enabled=True)
        self.exec_calls = []          # (what, body-ish)
        self.batch_get_ids = {}       # range -> value returned by batchGet

    def _auth(self):  # no google libs / credentials in tests
        pass

    def _services(self):
        pass

    @property
    def sheets(self):  # never build a real service
        return self._svc

    @property
    def drive(self):
        return self._svc

    def _gexec(self, request, what, limiter):
        self.exec_calls.append(what)
        if what.startswith("qlog verify batch"):
            # emulate batchGet: echo back the expected ids unless overridden
            ranges = self._last_ranges
            return {"valueRanges": [
                {"values": [[self.batch_get_ids.get(r, self._expected.get(r, ""))]]}
                for r in ranges]}
        return {}

    # capture the ranges/expectations qlog_mark_batch will verify
    def prime_verify(self, marks):
        self._last_ranges = [f"'{self.cfg.queue_tab}'!B{m[0]}" for m in marks]
        self._expected = {f"'{self.cfg.queue_tab}'!B{m[0]}": str(m[1])
                          for m in marks}


class RecordingGio(FakeGio):
    """Also records batchUpdate bodies for structural assertions."""

    def __init__(self, cfg):
        super().__init__(cfg)
        self.updates = []

    def _gexec(self, request, what, limiter):
        if what.startswith("qlog verify batch"):
            return super()._gexec(request, what, limiter)
        self.exec_calls.append(what)
        return {}


def make_cfg(tmp):
    # Config is a dataclass with ~19 required fields; fill them with inert
    # placeholders so tests construct one without a config file on disk.
    from dataclasses import MISSING, fields as _dc_fields
    _kw = {}
    for _f in _dc_fields(Config):
        if _f.default is MISSING and _f.default_factory is MISSING:
            _t = str(_f.type)
            _kw[_f.name] = 1 if ("int" in _t or "float" in _t) else "x"
    cfg = Config(**_kw)
    cfg.spreadsheet_id = "SS"
    cfg.audit_tab = "Order Audit Log"
    cfg.queue_tab = "Queue Log"
    cfg.archive_dir = str(Path(tmp) / "archive")
    cfg.record_url_base = "https://example/rec"
    cfg.workers = 2
    cfg.live_yield_enabled = False
    cfg.relay_batch_size = 12
    cfg.live_max_attempts = 3
    return cfg


def make_engine(tmp, live=False, cfg=None, gio_cls=FakeGio):
    cfg = cfg or make_cfg(tmp)
    gio = gio_cls(cfg)
    hs = MagicMock()
    relay = MagicMock()
    relay._gap = SimpleNamespace(rate=1.0)
    mirror = MagicMock()
    mirror.dir = Path(tmp) / "mirror"
    mirror.dir.mkdir(parents=True, exist_ok=True)
    eng = QueueDrainEngine(cfg, relay, hs, gio, mirror, live=live)
    return eng, hs, relay, gio


def order(oid, items):
    """items: list of (product_id, product_type)"""
    return {"id": oid, "reference_id": f"R{oid}",
            "payment_method": "mada",
            "date": {"date": "2026-05-01 10:00:00"},
            "customer": {"mobile": "5551", "mobile_code": "+966",
                         "full_name": "T", "email": "t@x.co", "id": 9,
                         "created_at": {"date": "2020-01-01"}},
            "amounts": {"total": {"amount": 100},
                        "shipping_cost": {"currency": "SAR"}},
            "items": [{"id": f"i{n}", "name": f"item-{pid}",
                       "product_type": pt, "product": {"id": pid}}
                      for n, (pid, pt) in enumerate(items)]}


# ----------------------------------------------------------------------------
# tests
# ----------------------------------------------------------------------------

class TestDotenv(unittest.TestCase):
    def test_shell_format(self):
        tmp = tempfile.mkdtemp()
        p = Path(tmp) / ".env"
        p.write_text('# c\nexport A="v1"\nB=\'v2\'\nexport C=v3\nbroken\n')
        for k in ("A", "B", "C"):
            os.environ.pop(k, None)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            load_dotenv()
        finally:
            os.chdir(cwd)
        self.assertEqual(os.environ.get("A"), "v1")
        self.assertEqual(os.environ.get("B"), "v2")
        self.assertEqual(os.environ.get("C"), "v3")
        shutil.rmtree(tmp)

    def test_never_overrides(self):
        tmp = tempfile.mkdtemp()
        (Path(tmp) / ".env").write_text("export A=file\n")
        os.environ["A"] = "env"
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            load_dotenv()
        finally:
            os.chdir(cwd)
        self.assertEqual(os.environ["A"], "env")
        shutil.rmtree(tmp)


class TestClaimDedupe(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eng, *_ = make_engine(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def row(self, n, oid, status, attempts=0):
        return {"row": n, "order_id": oid, "status": status,
                "attempts": attempts, "reason": "", "items": "",
                "notes": "", "older": "", "reference": ""}

    def test_claimable_statuses(self):
        rows = [self.row(2, "1", "queued"),
                self.row(3, "2", "processed"),
                self.row(4, "3", "processing"),
                self.row(5, "4", "error", 1),
                self.row(6, "5", "error", 3),     # at cap -> not claimable
                self.row(7, "6", "duplicate"),
                self.row(8, "7", "gone"),
                self.row(9, "", "queued"),        # no id
                self.row(10, "8", "")]            # blank status = queued-ish
        got = [r["order_id"] for r in self.eng.claimable(rows)]
        self.assertEqual(got, ["1", "3", "4", "8"])

    def test_split_primaries(self):
        rows = [self.row(2, "A", "queued"), self.row(3, "B", "queued"),
                self.row(4, "A", "queued"), self.row(5, "A", "error")]
        prim, twins = QueueDrainEngine.split_primaries(rows)
        self.assertEqual([r["order_id"] for r in prim], ["A", "B"])
        self.assertEqual([(r["order_id"], p) for r, p in twins],
                         [("A", 2), ("A", 2)])


class TestGateCache(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eng, self.hs, *_ = make_engine(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def prime(self, table):
        """table: pid -> (p, te, ta)"""
        self.hs.gate_search_product_approved.side_effect = \
            lambda pid: table[str(pid)][0]
        self.hs.gate_search_template.side_effect = \
            lambda pid, elig: table[str(pid)][1 if elig else 2]

    def test_rules(self):
        self.prime({"10": (0, 0, 0),   # unknown -> blocked
                    "11": (0, 0, 1),   # inactive bundle -> blocked
                    "12": (1, 0, 0),   # approved product -> pass
                    "13": (0, 1, 1)})  # active bundle -> pass
        o = order("1", [("10", "product"), ("11", "product"),
                        ("12", "product"), ("13", "product"),
                        ("99", "group_products")])  # native bundle skipped
        unv = self.eng.gate_cached(o)
        self.assertEqual([u["pid"] for u in unv], ["10", "11"])
        self.assertIn("activate bundle", unv[1]["why"])
        self.assertIn("missing or unapproved", unv[0]["why"])

    def test_cache_dedupes_searches(self):
        self.prime({"10": (0, 0, 0)})
        o = order("1", [("10", "product")] * 5)
        self.eng.gate_cached(o)
        self.eng.gate_cached(order("2", [("10", "product")]))
        self.assertEqual(self.hs.gate_search_product_approved.call_count, 1)
        self.assertEqual(self.hs.gate_search_template.call_count, 2)  # te+ta

    def test_threaded_cache_bounded(self):
        table = {str(p): (0, 0, 0) for p in range(10)}
        self.prime(table)
        orders = [order(str(i), [(str(i % 10), "product")]) for i in range(400)]
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(self.eng.gate_cached, orders))
        # worst case a few duplicate misses per pid across racing threads,
        # never anywhere near 400 searches
        self.assertLessEqual(
            self.hs.gate_search_product_approved.call_count, 40)
        self.assertEqual(len(self.eng._gate_cache), 10)


class TestResolvePreexisting(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eng, self.hs, *_ = make_engine(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_ledger_hit(self):
        self.eng.created_ledger.add("55", "HS9")
        st, note = self.eng.resolve_preexisting("55", 3)
        self.assertEqual(st, "Processed")
        self.assertIn("HS9", note)
        self.hs.find_order_by_salla_id.assert_not_called()

    def test_not_found_goes_to_create(self):
        self.hs.find_order_by_salla_id.return_value = None
        self.assertIsNone(self.eng.resolve_preexisting("1", 2))

    def test_verified_complete(self):
        self.hs.find_order_by_salla_id.return_value = "HS7"
        self.hs.order_line_item_count.return_value = 4
        st, note = self.eng.resolve_preexisting("2", 3)
        self.assertEqual(st, "Processed")
        self.assertEqual(self.eng.created_ledger.get("2"), "HS7")

    def test_partial_flagged(self):
        self.hs.find_order_by_salla_id.return_value = "HS7"
        self.hs.order_line_item_count.return_value = 1
        st, note = self.eng.resolve_preexisting("3", 3)
        self.assertEqual(st, "Error")
        self.assertIn("partial", note)

    def test_li_verify_unavailable(self):
        self.hs.find_order_by_salla_id.return_value = "HS7"
        self.hs.order_line_item_count.return_value = -1
        st, note = self.eng.resolve_preexisting("4", 1)
        self.assertEqual(st, "Error")
        self.assertIn("retry", note)


class TestDrainOne(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_blocked_stays_queued_with_note(self):
        eng, hs, *_ = make_engine(self.tmp, live=True)
        hs.find_order_by_salla_id.return_value = None
        hs.gate_search_product_approved.return_value = 0
        hs.gate_search_template.return_value = 0
        row = {"row": 2, "order_id": "1", "attempts": 0}
        st, note, d = eng.drain_one(row, order("1", [("10", "product")]))
        self.assertEqual(st, "Queued")
        self.assertIn("blocked: item-10", note)
        self.assertEqual(d, 1)
        self.assertIn("10", eng._blocked)

    def test_ready_in_verify_mode(self):
        eng, hs, *_ = make_engine(self.tmp, live=False)
        hs.find_order_by_salla_id.return_value = None
        hs.gate_search_product_approved.return_value = 1
        hs.gate_search_template.return_value = 0
        st, note, d = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0},
                                    order("1", [("10", "product")]))
        self.assertEqual(st, "Ready")

    def test_live_create_uses_existing_audit_row(self):
        eng, hs, relay, gio = make_engine(self.tmp, live=True)
        hs.find_order_by_salla_id.return_value = None
        hs.gate_search_product_approved.return_value = 1
        hs.gate_search_template.return_value = 0
        eng._audit_rows = {"1": 777}
        seen = {}

        def fake_route_create(o, audit_row):
            seen["audit_row"] = audit_row
            eng._outcome[str(o["id"])] = ("created", "HS42")
        eng.route_create = fake_route_create
        st, note, d = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0},
                                    order("1", [("10", "product")]))
        self.assertEqual(st, "Processed")
        self.assertIn("HS42", note)
        self.assertEqual(seen["audit_row"], 777)
        # archive written
        self.assertTrue(list((Path(self.tmp) / "archive").glob("order_RID*")))

    def test_live_create_failure_marks_error(self):
        eng, hs, *_ = make_engine(self.tmp, live=True)
        hs.find_order_by_salla_id.return_value = None
        hs.gate_search_product_approved.return_value = 1
        hs.gate_search_template.return_value = 0
        eng._audit_rows = {"1": 5}
        eng.route_create = lambda o, r: None  # no outcome set = failure
        st, note, d = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0},
                                    order("1", [("10", "product")]))
        self.assertEqual(st, "Error")


class TestBufferedAudit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.cfg = make_cfg(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_hold_reason_injected_and_runs_grouped(self):
        gio = FakeGio(self.cfg)
        captured = []
        orig = gio._gexec

        def spy(request, what, limiter):
            captured.append(what)
            return orig(request, what, limiter)
        gio._gexec = spy
        gio.audit_update(100, {11: "Order Approved", 13: "TRUE", 14: "9"},
                         "processed")
        gio.flush_audit()
        self.assertTrue(any(w.startswith("audit flush") for w in captured))
        # buffer injected col 12 = "" -> cols 11..14 must be ONE contiguous run
        body = gio._svc.values().batchUpdate.call_args.kwargs["body"]
        ranges = [d["range"] for d in body["data"]]
        self.assertEqual(len(ranges), 1)
        self.assertIn("!L100:O100", ranges[0])  # L=idx11 .. O=idx14
        self.assertEqual(body["data"][0]["values"], [["Order Approved", "",
                                                      "TRUE", "9"]])

    def test_merge_and_auto_flush(self):
        gio = FakeGio(self.cfg)
        flushes = []
        gio.flush_audit_orig = gio.flush_audit
        for i in range(gio.AUDIT_FLUSH_N):
            gio.audit_update(i + 2, {25: "t"}, "x")
        # threshold reached -> buffer emptied
        self.assertEqual(len(gio._abuf), 0)

    def test_disabled_noop(self):
        gio = FakeGio(self.cfg)
        gio.enabled = False
        gio.audit_update(5, {11: "Order Approved"}, "x")
        self.assertEqual(len(gio._abuf), 0)


class TestQlogMark(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.cfg = make_cfg(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_verify_then_write_refuses_moved_rows(self):
        gio = FakeGio(self.cfg)
        marks = [(2, "111", {QC_STATUS: "Processed"}),
                 (3, "222", {QC_STATUS: "Processed"})]
        gio.prime_verify(marks)
        # row 3 was sorted away: sheet now shows a different order id
        gio.batch_get_ids[f"'{self.cfg.queue_tab}'!B3"] = "999"
        written = gio.qlog_mark_batch(marks)
        self.assertEqual(written, 1)
        body = gio._svc.values().batchUpdate.call_args.kwargs["body"]
        self.assertTrue(all("2:" in d["range"] or d["range"].endswith("2")
                            for d in body["data"]))

    def test_sparse_runs(self):
        gio = FakeGio(self.cfg)
        marks = [(2, "1", {QC_STATUS: "Processed", QC_PROCESSED_AT: "t",
                           QC_RESULT: "r", QC_ATTEMPTS: 1, QC_NOTES: ""})]
        gio.prime_verify(marks)
        gio.qlog_mark_batch(marks)
        body = gio._svc.values().batchUpdate.call_args.kwargs["body"]
        ranges = sorted(d["range"] for d in body["data"])
        # cols 6 (G) alone, then 8..11 (I..L) contiguous
        self.assertEqual(len(ranges), 2)
        self.assertTrue(ranges[0].endswith("!G2:G2"))
        self.assertTrue(ranges[1].endswith("!I2:L2"))


class TestStress(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_20k_claim_and_dedupe_fast(self):
        eng, *_ = make_engine(self.tmp)
        rows = [{"row": i + 2, "order_id": str(i % 12000), "status": "queued",
                 "attempts": 0, "reason": "", "items": "", "notes": "",
                 "older": "", "reference": ""} for i in range(20000)]
        t0 = time.monotonic()
        prim, twins = QueueDrainEngine.split_primaries(eng.claimable(rows))
        dt = time.monotonic() - t0
        self.assertEqual(len(prim), 12000)
        self.assertEqual(len(twins), 8000)
        self.assertLess(dt, 2.0, f"claim+dedupe took {dt:.2f}s")

    def test_5k_marks_batch_in_chunks(self):
        gio = FakeGio(make_cfg(self.tmp))
        marks = [(i + 2, str(i), {QC_STATUS: "Processed"}) for i in range(5000)]
        # prime per chunk: FakeGio primes once; emulate by making batchGet echo
        gio.prime_verify(marks)
        orig = gio._gexec

        def chunk_aware(request, what, limiter):
            if what.startswith("qlog verify batch"):
                # figure out which chunk by tracking calls
                n = sum(1 for c in gio.exec_calls
                        if c.startswith("qlog verify batch"))
                chunk = marks[n * 80:(n + 1) * 80]
                gio._last_ranges = [f"'{gio.cfg.queue_tab}'!B{m[0]}"
                                    for m in chunk]
                gio._expected = {f"'{gio.cfg.queue_tab}'!B{m[0]}": str(m[1])
                                 for m in chunk}
            return orig(request, what, limiter)
        gio._gexec = chunk_aware
        written = gio.qlog_mark_batch(marks)
        self.assertEqual(written, 5000)
        verifies = sum(1 for c in gio.exec_calls
                       if c.startswith("qlog verify batch"))
        updates = sum(1 for c in gio.exec_calls if c.startswith("qlog mark"))
        self.assertEqual(verifies, 63)   # ceil(5000/80)
        self.assertEqual(updates, 63)

    def test_end_to_end_verify_mode_1k_orders(self):
        """1000 fake queued orders through run_drain in verify mode with
        8 lanes: no writes, correct ready/blocked split, bounded searches."""
        cfg = make_cfg(self.tmp)
        cfg.workers = 8
        eng, hs, relay, gio = make_engine(self.tmp, live=False, cfg=cfg)
        # 6 products: 3 blocked, 3 ready
        table = {"1": (0, 0, 0), "2": (0, 0, 1), "3": (0, 0, 0),
                 "4": (1, 0, 0), "5": (0, 1, 1), "6": (1, 0, 0)}
        hs.gate_search_product_approved.side_effect = \
            lambda pid: table[str(pid)][0]
        hs.gate_search_template.side_effect = \
            lambda pid, elig: table[str(pid)][1 if elig else 2]
        hs.find_order_by_salla_id.return_value = None
        orders_db = {str(i): order(str(i), [(str(i % 6 + 1), "product")])
                     for i in range(1000)}
        relay.fetch_orders.side_effect = \
            lambda ids: {i: orders_db[i] for i in ids}
        rows = [{"row": i + 2, "order_id": str(i), "status": "queued",
                 "attempts": 0, "reason": "", "items": "", "notes": "",
                 "older": "", "reference": ""} for i in range(1000)]
        gio.qlog_read = lambda: rows
        t0 = time.monotonic()
        eng.run_drain()
        dt = time.monotonic() - t0
        # products 1,2,3 block; 4,5,6 pass
        expected_blocked = sum(1 for i in range(1000) if (i % 6 + 1) <= 3)
        self.assertEqual(eng.stats.held, expected_blocked)  # 501
        self.assertEqual(eng.stats.created, 0)          # verify mode
        self.assertEqual(eng.stats.errors, 0)
        self.assertEqual(len(eng._gate_cache), 6)
        # verify mode must not write to sheets
        self.assertFalse(any(c.startswith(("qlog mark", "audit flush"))
                             for c in gio.exec_calls))
        self.assertLess(dt, 30, f"1k verify took {dt:.1f}s")
        # blocker matrix written
        self.assertTrue(queue_drain.MATRIX_FILE.exists())


if __name__ == "__main__":
    unittest.main()
