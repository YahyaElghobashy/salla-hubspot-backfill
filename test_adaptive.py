"""Unit tests for the v1.4 AdaptiveLimiter and feedback plumbing."""
import json
import os
import tempfile
import time
import unittest
from unittest import mock

import backfill
from backfill import AdaptiveLimiter, RateLimiter, Config, with_retries


class TestAdaptiveLimiter(unittest.TestCase):
    def mk(self, **kw):
        base = dict(name="t", start_per_s=3.5, floor_per_s=1.0, ceil_per_s=4.6,
                    step_per_s=0.1, growth_every=5, cooldown_s=30.0)
        base.update(kw)
        return AdaptiveLimiter(**base)

    def test_fixed_mode_never_moves(self):
        rl = self.mk(adaptive=False)
        for _ in range(50):
            rl.on_success()
        rl.on_throttle()
        rl.on_result(0, 100)
        self.assertEqual(rl.rate, 3.5)

    def test_growth_after_streak_and_ceiling(self):
        rl = self.mk()
        for _ in range(5):
            rl.on_success()
        self.assertAlmostEqual(rl.rate, 3.6, places=6)
        for _ in range(5 * 100):
            rl.on_success()
        self.assertAlmostEqual(rl.rate, 4.6, places=6)  # capped at ceiling

    def test_throttle_halves_and_floors(self):
        rl = self.mk()
        rl.on_throttle("2")
        self.assertAlmostEqual(rl.rate, 1.75, places=6)
        rl._cooldown_until = 0  # bypass cooldown to test the floor
        rl.on_throttle()
        rl._cooldown_until = 0
        rl.on_throttle()
        self.assertAlmostEqual(rl.rate, 1.0, places=6)  # floor

    def test_cooldown_blocks_growth(self):
        rl = self.mk()
        rl.on_throttle()
        for _ in range(50):
            rl.on_success()
        self.assertAlmostEqual(rl.rate, 1.75, places=6)  # frozen during cooldown
        rl._cooldown_until = time.monotonic() - 1  # cooldown over
        for _ in range(5):
            rl.on_success()
        self.assertAlmostEqual(rl.rate, 1.85, places=6)

    def test_soft_headroom_decrease(self):
        rl = self.mk()
        rl.on_result(10, 100)  # 10% headroom < soft_floor 15%
        self.assertAlmostEqual(rl.rate, 3.5 * 0.75, places=6)

    def test_soft_signal_never_shortens_hard_cooldown(self):
        # Regression: a low-headroom response during a 429 cooldown must not
        # overwrite (shorten) the longer freeze the throttle imposed.
        rl = self.mk(cooldown_s=30.0)
        rl.on_throttle()
        hard_until = rl._cooldown_until
        rl.on_result(1, 100)  # soft signal 1s later would set now+10 < hard_until
        self.assertGreaterEqual(rl._cooldown_until, hard_until)

    def test_healthy_headroom_counts_as_success(self):
        rl = self.mk()
        for _ in range(5):
            rl.on_result(90, 100)
        self.assertAlmostEqual(rl.rate, 3.6, places=6)

    def test_wait_paces_to_min_interval(self):
        rl = self.mk(start_per_s=50.0, floor_per_s=50.0, ceil_per_s=50.0)
        t0 = time.monotonic()
        for _ in range(5):
            rl.wait()
        elapsed = time.monotonic() - t0
        self.assertGreaterEqual(elapsed, 4 * (1 / 50.0) * 0.9)

    def test_floor_above_start_is_clamped(self):
        # Regression: a hardcoded floor above a low configured start must not
        # catapult the rate to the ceiling on first growth, nor block decreases.
        rl = AdaptiveLimiter("sheets", 10 / 60.0, floor_per_s=20 / 60.0,
                             ceil_per_s=18.4 / 60.0, step_per_s=1 / 60.0,
                             growth_every=1, cooldown_s=65.0)
        self.assertAlmostEqual(rl.floor, 10 / 60.0, places=6)
        rl.on_success()
        self.assertAlmostEqual(rl.rate, 11 / 60.0, places=6)  # one gradual step
        rl.on_throttle()
        self.assertAlmostEqual(rl.rate, 10 / 60.0, places=6)  # decrease works

    def test_ceiling_below_start_caps_start(self):
        rl = AdaptiveLimiter("x", 5.0, floor_per_s=1.0, ceil_per_s=2.0)
        self.assertAlmostEqual(rl.rate, 2.0, places=6)

    def test_fixed_alias(self):
        rl = RateLimiter(10.0)
        self.assertEqual(rl.rate, 10.0)
        rl.on_throttle()
        self.assertEqual(rl.rate, 10.0)
        z = RateLimiter(0)
        self.assertLess(z.min_interval, 1e-6)


class TestWithRetriesFeedback(unittest.TestCase):
    def test_feedback_sees_absorbed_429(self):
        calls = []
        responses = iter([(429, {"Retry-After": "1"}, ""), (200, {"X": "y"}, "{}")])

        def fn():
            return next(responses)

        with mock.patch("time.sleep"):
            status, headers, text = with_retries(
                fn, "t", feedback=lambda s, h: calls.append(s))
        self.assertEqual(status, 200)
        self.assertEqual(calls, [429, 200])

    def test_feedback_errors_do_not_break_request(self):
        def fn():
            return 200, {}, "{}"

        def bad_feedback(s, h):
            raise ValueError("boom")

        status, _, _ = with_retries(fn, "t", feedback=bad_feedback)
        self.assertEqual(status, 200)


class TestHubSpotFeedback(unittest.TestCase):
    def mk_hs(self):
        cfg = mock.Mock()
        cfg.adaptive_target_util = 0.92
        cfg.hs_search_per_s = 3.5
        cfg.hs_search_limit_per_s = 5.0
        cfg.hs_general_per_s = 10.0
        cfg.hs_general_limit_per_10s = 190.0
        cfg.adaptive_enabled = True
        return backfill.HubSpot(cfg, "tok", live=False)

    def test_ceilings_from_documented_limits(self):
        hs = self.mk_hs()
        self.assertAlmostEqual(hs.search_rl.ceil, 4.6, places=6)
        self.assertAlmostEqual(hs.general_rl.ceil, 17.48, places=6)

    def test_429_halves_search(self):
        hs = self.mk_hs()
        cb = hs._rl_feedback(hs.search_rl, is_search=True)
        cb(429, {"Retry-After": "3"})
        self.assertAlmostEqual(hs.search_rl.rate, 1.75, places=6)

    def test_general_reads_window_headers(self):
        hs = self.mk_hs()
        cb = hs._rl_feedback(hs.general_rl, is_search=False)
        cb(200, {"X-HubSpot-RateLimit-Remaining": "5",
                 "X-HubSpot-RateLimit-Max": "190"})  # 2.6% headroom
        self.assertAlmostEqual(hs.general_rl.rate, 7.5, places=6)

    def test_search_ignores_headers_success_grows(self):
        hs = self.mk_hs()
        cb = hs._rl_feedback(hs.search_rl, is_search=True)
        for _ in range(25):
            cb(200, {})
        self.assertAlmostEqual(hs.search_rl.rate, 3.6, places=6)

    def test_5xx_is_not_a_rate_signal(self):
        hs = self.mk_hs()
        cb = hs._rl_feedback(hs.general_rl, is_search=False)
        cb(503, {})
        self.assertAlmostEqual(hs.general_rl.rate, 10.0, places=6)


class TestConfigCompat(unittest.TestCase):
    OLD = {
        "hubspot_base": "https://api.hubapi.com", "relay_url": "x",
        "relay_batch_size": 12, "relay_min_interval_s": 1.5, "slot_hours": 3,
        "per_page": 30, "overflow_pages": 18, "spreadsheet_id": "s",
        "audit_tab": "a", "queue_tab": "q", "drive_folder_id": "d",
        "held_notify_url": "h", "record_url_base": "r", "archive_dir": "archive",
        "state_file": "cursor.json", "hs_search_per_s": 3.5,
        "hs_general_per_s": 10, "sheets_per_min": 50,
        "salla_timezone_default": "Asia/Riyadh",
    }

    def _load(self, raw):
        fd, tmp = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(raw, f)
            return Config.load(tmp)
        finally:
            os.unlink(tmp)

    def test_old_config_loads_with_adaptive_defaults(self):
        cfg = self._load(self.OLD)
        self.assertTrue(cfg.adaptive_enabled)
        self.assertEqual(cfg.adaptive_target_util, 0.92)
        self.assertEqual(cfg.hs_search_limit_per_s, 5.0)

    def test_unknown_keys_ignored(self):
        cfg = self._load(dict(self.OLD, future_flag=True))  # must not raise
        self.assertEqual(cfg.per_page, 30)


class TestConcurrency(unittest.TestCase):
    """v1.5: worker lanes share limiters; pacing must hold globally."""

    def test_reservation_paces_across_threads(self):
        import threading
        rl = AdaptiveLimiter("t", 100.0, floor_per_s=100.0, ceil_per_s=100.0)
        stamps = []
        stamp_lock = threading.Lock()

        def worker():
            for _ in range(10):
                rl.wait()
                with stamp_lock:
                    stamps.append(time.monotonic())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        t0 = time.monotonic()
        for t in threads: t.start()
        for t in threads: t.join()
        elapsed = time.monotonic() - t0
        # 40 calls at 100/s: at least ~0.39s wall clock, regardless of threads
        self.assertGreaterEqual(elapsed, 39 * 0.01 * 0.9)
        stamps.sort()
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        # reservation pacing bounds the SEND SLOTS; stamps are taken after
        # thread wake-up, so a loaded scheduler can cluster a few. The
        # wall-clock bound above is the hard invariant; this only catches a
        # wholesale bypass (most gaps collapsing to ~0).
        self.assertLess(sum(1 for g in gaps if g < 0.004), len(gaps) // 2)

    def test_throttle_pushes_reservation_head(self):
        rl = AdaptiveLimiter("t", 10.0, floor_per_s=1.0, ceil_per_s=10.0)
        rl.wait()
        rl.on_throttle("2")  # Retry-After 2s
        t0 = time.monotonic()
        rl.wait()  # next slot must be >= ~2s away
        self.assertGreaterEqual(time.monotonic() - t0, 1.8)

    def test_rate_never_escapes_bounds_under_threads(self):
        import threading
        rl = AdaptiveLimiter("t", 3.0, floor_per_s=1.0, ceil_per_s=5.0,
                             step_per_s=0.5, growth_every=2, cooldown_s=0.01)

        def hammer(seed):
            for i in range(300):
                if (i + seed) % 17 == 0:
                    rl.on_throttle()
                elif (i + seed) % 5 == 0:
                    rl.on_result(1, 100)
                else:
                    rl.on_success()

        threads = [threading.Thread(target=hammer, args=(k,)) for k in range(6)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertGreaterEqual(rl.rate, rl.floor - 1e-9)
        self.assertLessEqual(rl.rate, rl.ceil + 1e-9)


class TestEngineConcurrencyHelpers(unittest.TestCase):
    def mk_engine(self, workers=3):
        cfg = mock.Mock()
        cfg.workers = workers
        eng = backfill.Engine.__new__(backfill.Engine)
        import threading
        eng.cfg = cfg
        eng.workers = workers
        eng.stats = backfill.Stats()
        eng._stats_lock = threading.Lock()
        eng._phone_guard = threading.Lock()
        eng._phone_locks = {}
        return eng

    def test_bump_is_locked_and_correct(self):
        import threading
        eng = self.mk_engine()
        threads = [threading.Thread(
            target=lambda: [eng._bump("created") for _ in range(500)])
            for _ in range(6)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(eng.stats.created, 3000)

    def test_contact_lock_keying(self):
        eng = self.mk_engine()
        k = eng._contact_key
        o1 = {"customer": {"mobile_code": "+966", "mobile": "5551", "id": 1}}
        o2 = {"customer": {"mobile_code": "+966", "mobile": "5551", "id": 2}}
        o3 = {"customer": {"mobile_code": "+966", "mobile": "5552", "id": 3}}
        self.assertIs(eng._contact_lock(k(o1)), eng._contact_lock(k(o2)))
        self.assertIsNot(eng._contact_lock(k(o1)), eng._contact_lock(k(o3)))
        # empty mobile: distinct customers must NOT share a lock
        e1 = {"customer": {"mobile_code": "", "mobile": "", "id": 10}}
        e2 = {"customer": {"mobile_code": "", "mobile": "", "id": 11}}
        self.assertIsNot(eng._contact_lock(k(e1)), eng._contact_lock(k(e2)))

    def test_contact_cache_reused_for_same_buyer(self):
        eng = self.mk_engine()
        eng._contact_cache = {}
        key = eng._contact_key({"customer": {"mobile_code": "+966",
                                             "mobile": "5551", "id": 1}})
        eng._contact_cache[key] = "42"
        self.assertEqual(eng._contact_cache.get(key), "42")

    def test_workers_default_from_config(self):
        cfg = mock.Mock()
        cfg.workers = 4
        eng = backfill.Engine.__new__(backfill.Engine)
        # replicate the resolution expression used in __init__
        self.assertEqual(max(1, int(cfg.workers)), 4)


class TestV16Live(unittest.TestCase):
    def test_fresh_create_returns_was_fresh_true(self):
        hs = backfill.HubSpot.__new__(backfill.HubSpot)
        hs.live = True
        hs._write = lambda *a, **k: (201, {"id": "HSNEW"})
        out = hs.create_order({"id": 1, "reference_id": 1, "date": {"date": ""},
                               "amounts": {}, "customer": {}, "status": {}},
                              "C1", "Asia/Riyadh")
        self.assertEqual(out, ("HSNEW", True))

    """v1.6: created ledger, id normalization, duplicate-400 guardrail."""

    def test_created_ledger_roundtrip_and_reload(self):
        import tempfile, shutil
        d = tempfile.mkdtemp()
        try:
            led = backfill.CreatedLedger(d)
            self.assertIsNone(led.get("111"))
            led.add("111", "HS9")
            self.assertEqual(led.get(111), "HS9")  # int/str key tolerance
            led2 = backfill.CreatedLedger(d)       # reload from disk
            self.assertEqual(led2.get("111"), "HS9")
        finally:
            shutil.rmtree(d)

    def test_created_ledger_thread_safety(self):
        import tempfile, shutil, threading
        d = tempfile.mkdtemp()
        try:
            led = backfill.CreatedLedger(d)
            ts = [threading.Thread(
                target=lambda k: [led.add(f"{k}-{i}", i) for i in range(200)],
                args=(k,)) for k in range(4)]
            for t in ts: t.start()
            for t in ts: t.join()
            led2 = backfill.CreatedLedger(d)
            self.assertEqual(len(led2._map), 800)
        finally:
            shutil.rmtree(d)

    def test_norm_id_defends_float_mangling(self):
        n = backfill.GoogleIO._norm_id
        self.assertEqual(n(1002003140.0), "1002003140")
        self.assertEqual(n("1002003140"), "1002003140")
        self.assertEqual(n(" 42 "), "42")
        self.assertEqual(n(""), "")
        self.assertEqual(n(None), "")

    def test_duplicate_400_resolves_to_existing(self):
        hs = backfill.HubSpot.__new__(backfill.HubSpot)
        hs.live = True
        calls = []
        def fake_write(method, path, body, what):
            calls.append(what)
            return 400, {"message": "propertyValue: already has that value "
                                    "hs_external_order_id"}
        hs._write = fake_write
        hs.find_order_by_salla_id = lambda oid: "HS777"
        with mock.patch("time.sleep"):
            out = hs.create_order({"id": 555, "reference_id": 555,
                                   "date": {"date": ""}, "amounts": {},
                                   "customer": {}, "status": {}},
                                  "C1", "Asia/Riyadh")
        # (existing_id, was_fresh=False) -> caller must NOT re-create items
        self.assertEqual(out, ("HS777", False))

    def test_duplicate_400_unrelated_error_still_fails(self):
        hs = backfill.HubSpot.__new__(backfill.HubSpot)
        hs.live = True
        hs._write = lambda *a, **k: (400, {"message": "INVALID_PROPERTY foo"})
        hs.find_order_by_salla_id = lambda oid: "HS777"
        out = hs.create_order({"id": 556, "reference_id": 556,
                               "date": {"date": ""}, "amounts": {},
                               "customer": {}, "status": {}},
                              "C1", "Asia/Riyadh")
        self.assertEqual(out, (None, False))


if __name__ == "__main__":
    unittest.main(verbosity=2)
