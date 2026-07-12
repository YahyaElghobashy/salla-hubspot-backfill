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


if __name__ == "__main__":
    unittest.main(verbosity=2)
