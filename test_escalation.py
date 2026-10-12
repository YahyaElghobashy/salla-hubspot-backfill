#!/usr/bin/env python3
"""v2.14 alert truthfulness and escalation.

The Oct 2026 hold was detected within 7 hours and posted 26 times, but the
alerts told people to approve an already-approved product and nobody acted.
These tests pin the corrected wording and the escalation that tags named
owners once a hold passes a threshold, at most once per day.
"""
import unittest
from unittest import mock

from test_realtime_ext import _chdir_tmp, _cfg, FakeHS, FakeGIO


def relay(**kw):
    import status_relay
    return status_relay.StatusRelay(_cfg(**kw), FakeHS(), FakeGIO(), live=True)


class Escalation(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_off_by_default(self):
        r = relay()
        with mock.patch("notify.issue_open") as io:
            r._escalate_held(5000, "Multi Styler (1760)")
        io.assert_not_called()

    def test_below_threshold_silent(self):
        r = relay(held_escalate_count=100, escalation_mentions=["U1"])
        with mock.patch("notify.issue_open") as io:
            r._escalate_held(99, "")
        io.assert_not_called()

    def test_over_threshold_tags_owners_once_per_day_key(self):
        r = relay(held_escalate_count=100, escalation_mentions=["U0A1F5MPR2B", "U0AQ36NEFDX"])
        with mock.patch("notify.issue_open", return_value=True) as io, \
             mock.patch("notify.email_enabled", return_value=False):
            r._escalate_held(1760, "Multi Styler (1760)")
        key, subject, body = io.call_args[0][:3]
        self.assertTrue(key.startswith("held-escalation:"))
        self.assertIn("<@U0A1F5MPR2B>", subject)
        self.assertIn("<@U0AQ36NEFDX>", subject)
        self.assertIn("1760", subject)
        self.assertIn("Multi Styler", body)

    def test_email_sent_only_when_configured_and_thread_new(self):
        r = relay(held_escalate_count=100)
        with mock.patch("notify.issue_open", return_value=True), \
             mock.patch("notify.email_enabled", return_value=True), \
             mock.patch("notify.send_email") as se:
            r._escalate_held(150, "")
        se.assert_called_once()
        with mock.patch("notify.issue_open", return_value=False), \
             mock.patch("notify.email_enabled", return_value=True), \
             mock.patch("notify.send_email") as se2:
            r._escalate_held(150, "")   # same day: thread already open
        se2.assert_not_called()

    def test_dry_run_never_escalates(self):
        import status_relay
        r = status_relay.StatusRelay(_cfg(held_escalate_count=1), FakeHS(), FakeGIO(), live=False)
        with mock.patch("notify.issue_open") as io:
            r._escalate_held(10, "")
        io.assert_not_called()


class Wording(unittest.TestCase):
    def test_backlog_thread_no_longer_says_approve_each_product(self):
        import inspect, status_relay
        code = inspect.getsource(status_relay.StatusRelay._giveup_alert)
        self.assertNotIn("Approve each product listed above", code)
        self.assertIn("do not approve", code)

    def test_stray_held_text_tells_the_real_cause(self):
        import backfill
        from test_queue_drain import chdir_tmp, make_cfg, make_engine, order
        tmp = chdir_tmp(self)
        eng, hs, relay_, gio = make_engine(tmp, cfg=make_cfg(tmp))
        hs.search.return_value = {"results": [{"id": "454482395354"}]}
        o = order("1", [("1982906533", "product")])
        u = {"name": "Multi Styler", "sku": "C053_8", "pid": "1982906533", "why": "bundle_inactive"}
        with mock.patch("slack_text.SheetIndex.row_link", return_value=("Bundles tab", False)):
            subject, body = eng.held_text(o, u)
        self.assertIn("empty bundle record", subject)
        self.assertIn("Do NOT approve", body)
        self.assertIn("bundle", body)

    def test_real_bundle_build_keeps_push_instructions(self):
        from test_queue_drain import chdir_tmp, make_cfg, make_engine, order
        tmp = chdir_tmp(self)
        eng, hs, relay_, gio = make_engine(tmp, cfg=make_cfg(tmp))
        hs.search.return_value = {"results": [{"id": "1"}]}
        o = order("1", [("1448268221", "product")])
        u = {"name": "Styler set", "sku": "C053CH11CH16", "pid": "1448268221", "why": "bundle_inactive"}
        with mock.patch("slack_text.SheetIndex.row_link", return_value=("row 2", True)):
            subject, body = eng.held_text(o, u)
        self.assertIn("not active yet", subject)
        self.assertIn("Push to HubSpot", body)


if __name__ == "__main__":
    unittest.main()
