#!/usr/bin/env python3
"""v2.14 report truthfulness: what the 11 and 12 Oct 2026 reports got wrong.

- The weekly report said "Healthy" while 1,649 orders were held, because
  only the daily report read the live held count.
- The thread's "Catalog holds: 120" was the drain's per-pass cap, quoted as
  the backlog while 1,700+ orders were held.
- "Backfill: page 15 of 29, unchanged" flagged a stall every day for two
  months for a backfill that was deliberately parked (STOP file).
- "Split today: backfill 3,874" named the shared fetch relay "backfill".
"""
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import report_digest as rd
from test_digest_lines import digest

HELD = {"value": 1649, "as_of": "live", "oldest_days": 6, "top": "Multi Styler (1640)"}


def render(a):
    with mock.patch.object(rd, "_stuck_in_review", return_value=None):
        return rd.render(a)


class Verdict(unittest.TestCase):
    def test_weekly_verdict_sees_held_orders(self):
        a = digest(None, period="weekly")
        a["live_held"] = HELD
        head, _ = render(a)
        self.assertNotIn("Healthy.", head)
        self.assertIn("1649", head.replace(",", ""))

    def test_aggregate_reads_live_held_for_every_period(self):
        import inspect
        src = inspect.getsource(rd.aggregate) if hasattr(rd, "aggregate") else open(rd.__file__).read()
        self.assertIn('"live_held": _live_held(),', src)


class CatalogHolds(unittest.TestCase):
    def test_thread_quotes_the_live_total_not_the_pass_cap(self):
        a = digest(None)
        a["live_held"] = HELD
        a["now"] = {"held": {"value": 120, "as_of": "2026-10-12"}}
        _, replies = render(a)
        block = [r for r in replies if r.startswith("*Catalog holds*")]
        self.assertEqual(len(block), 1)
        self.assertIn("1,649 orders held right now", block[0])
        self.assertNotIn("120 orders held", block[0])

    def test_without_live_count_the_pass_figure_is_labelled_a_sample(self):
        a = digest(None)
        a["now"] = {"held": {"value": 120, "as_of": "2026-10-12"}}
        _, replies = render(a)
        block = [r for r in replies if r.startswith("*Catalog holds*")][0]
        self.assertIn("per-pass sample", block)


UNREAD = {"value": None, "as_of": None, "oldest_days": None, "top": ""}   # _live_held on a failed read


class HeldCountUnreadable(unittest.TestCase):
    """v2.14 review: with the live count unreadable the headline quoted the
    drain's per-pass cap (120) as the held total and the verdict said
    Healthy while about 1,700 orders were held."""

    def test_headline_never_quotes_the_pass_cap_as_the_total(self):
        for period in ("daily", "weekly"):
            for lh in (UNREAD, None):
                a = digest(None, period=period)
                a["live_held"] = lh
                a["now"] = {"held": {"value": 120, "as_of": "2026-10-11"}}
                head, replies = render(a)
                with self.subTest(period=period, live_held=lh):
                    self.assertNotIn("Healthy.", head)
                    self.assertIn("Needs a look: held count not recorded.", head)
                    self.assertIn("• Held for catalog: held count not recorded (the last release "
                                  "pass checked 120, not the total)", head)
                    self.assertNotIn("Held for catalog: 120", head)
                    self.assertNotIn("—", head)
                    block = [r for r in replies if r.startswith("*Catalog holds*")][0]
                    self.assertIn("per-pass sample", block)

    def test_failed_read_without_a_pass_figure_still_says_not_recorded(self):
        a = digest(None)
        a["live_held"] = UNREAD
        head, _ = render(a)
        self.assertIn("• Held for catalog: held count not recorded\n", head)
        self.assertIn("Needs a look: held count not recorded.", head)

    def test_no_held_figure_at_all_adds_nothing(self):
        head, _ = render(digest(None))          # live_held None, no drain figure
        self.assertIn("Healthy.", head)
        self.assertNotIn("Held for catalog", head)


class LiveHeldCount(unittest.TestCase):
    """_live_held counts what the Sunday certificate counts (reconcile.held_index)."""

    def _serial(self, days_ago):
        return str((datetime.now() - datetime(1899, 12, 30)).days - days_ago)

    def test_counts_both_queues_once_each_without_zid_collisions(self):
        import reconcile
        from backfill import Config
        from test_reconcile import _sheets
        live = [{"order_id": "1", "status": "held", "received_at": self._serial(10),
                 "note": "catalog gate: Dryer, Brush -- in review queue"},
                {"order_id": "2", "status": "held", "received_at": self._serial(30),
                 "note": "zid collision: salla_order_id 2 is held by Zid order HS 9; re-key it"},
                {"order_id": "3", "status": "done", "received_at": self._serial(40), "note": "HS 7"}]
        qlog = [{"order_id": "1", "status": "queued", "queued_at": self._serial(12), "items": "Dryer"},
                {"order_id": "4", "status": "queued", "queued_at": self._serial(20), "items": "Dryer"},
                {"order_id": "5", "status": "processed", "queued_at": self._serial(50), "items": "Iron"}]
        cfg = Config()
        with mock.patch("backfill.Config.load", return_value=cfg), _sheets(live, qlog):
            lh = rd._live_held()
            certificate = reconcile.held_index(cfg)
        self.assertEqual(lh["value"], 2)
        self.assertEqual(lh["value"], len(certificate))
        self.assertEqual(lh["oldest_days"], 20)
        self.assertEqual(lh["top"], "Dryer (2), Brush (1)")
        self.assertEqual(lh["as_of"], "live")

    def test_failed_read_is_marked_not_recorded(self):
        with mock.patch("backfill.Config.load", side_effect=RuntimeError("no config")), \
                self.assertLogs("digest", "WARNING"):
            lh = rd._live_held()
        self.assertIsNotNone(lh)
        self.assertIsNone(lh["value"])

    def test_thread_names_both_queues(self):
        a = digest(None)
        a["live_held"] = HELD
        _, replies = render(a)
        block = [r for r in replies if r.startswith("*Catalog holds*")][0]
        self.assertIn("Live Queue and the Queue Log", block)


class Backfill(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_parked_detected_from_stop_file(self):
        root = Path(self.tmp.name)
        (root / "STOP").write_text("")
        with mock.patch.object(rd, "ROOT", root):
            self.assertRegex(rd._backfill_parked(), r"^\d{4}-\d{2}-\d{2}$")

    def test_not_parked_without_stop(self):
        with mock.patch.object(rd, "ROOT", Path(self.tmp.name)):
            self.assertEqual(rd._backfill_parked(), "")

    def test_parked_renders_as_parked_not_stalled(self):
        a = digest(None)
        a["backfill_parked"] = "2026-08-09"
        a["backfill_stalled"] = False
        a["now"] = {"backfill": {"window": "2025-12", "page": 15, "total_pages": 29, "status": "overflow"}}
        head, replies = render(a)
        self.assertIn("parked since 2026-08-09", head)
        self.assertNotIn("page 15 of 29", head)
        self.assertNotIn("backfill has not advanced", head)


class Credits(unittest.TestCase):
    def test_relay_is_not_called_backfill(self):
        a = digest(None)
        a["credits_burned"] = 29549
        a["now"] = {"credits": {"remaining": 743549, "by_engine": {"live": 333, "backfill": 3874, "other": 2546},
                                "plan": 1000000}}
        _, replies = render(a)
        block = [r for r in replies if r.startswith("*Make credits*")][0]
        self.assertIn("fetch relay 3,874", block)
        self.assertNotIn("backfill 3,874", block)


if __name__ == "__main__":
    unittest.main()
