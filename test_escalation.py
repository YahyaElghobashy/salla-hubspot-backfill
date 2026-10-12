#!/usr/bin/env python3
"""v2.14 alert truthfulness and escalation.

The Oct 2026 hold was detected within 7 hours and posted 26 times, but the
alerts told people to approve an already-approved product and nobody acted.
These tests pin the corrected wording and the escalation that tags named
owners once a hold passes a threshold, at most once per day.
"""
import time
import unittest
from unittest import mock

from test_realtime_ext import _chdir_tmp, _cfg, _row, FakeHS, FakeGIO, FakeSlack, IndexedGIO

EM_DASH = "\u2014"


def relay(**kw):
    import status_relay
    return status_relay.StatusRelay(_cfg(**kw), FakeHS(), FakeGIO(), live=True)


def held_relay(n_held=150, **kw):
    """A classifying relay whose Live Queue lists n_held catalog-held orders
    (ids 1..n_held), all blocked by Multi Styler."""
    import status_relay
    rows = [{"order_id": str(i), "status": "held",
             "note": "catalog gate: Multi Styler -- in review queue"} for i in range(1, n_held + 1)]
    kw.setdefault("live_min_reference", 280000000)
    kw.setdefault("live_queue_tab", "Live Queue")
    return status_relay.StatusRelay(_cfg(**kw), FakeHS(), IndexedGIO(rows), live=True)


class Channels:
    """FakeSlack plus a recorded email channel, over the real notify registry."""

    def __init__(self, email=True):
        self.slack, self.emails, self.email = FakeSlack(), [], email

    def _send(self, subject, body, dry_run=False):
        self.emails.append((subject, body))
        return True

    def patch(self, post=None):
        import notify
        return mock.patch.multiple(notify, slack_enabled=lambda: True, slack_channels=lambda: ["C1"],
                                   post_slack=post or self.slack.post, send_email=self._send,
                                   email_enabled=lambda: self.email, add_reaction=lambda *a, **k: True)

    def escalations(self):
        return [h for h, _ in self.slack.sent if "orders are on hold and not in HubSpot" in h]

    def escalation_emails(self):
        return [e for e in self.emails if "orders are on hold and not in HubSpot" in e[0]]


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
        key, subject, body = io.call_args_list[0][0][:3]
        self.assertTrue(key.startswith("held-escalation:"))
        self.assertIn("<@U0A1F5MPR2B>", subject)
        self.assertIn("<@U0AQ36NEFDX>", subject)
        self.assertIn("1760", subject)
        self.assertIn("Multi Styler", body)

    def test_email_sent_only_when_configured_and_thread_new(self):
        r = relay(held_escalate_count=100, escalation_mentions=["U0A1F5MPR2B"])
        with mock.patch("notify.issue_open", return_value=True) as io, \
             mock.patch("notify.email_enabled", return_value=True), \
             mock.patch("notify.send_email") as se:
            r._escalate_held(150, "")
        se.assert_called_once()
        self.assertNotIn("<@", se.call_args[0][0])          # no Slack mention markup in email
        self.assertIn("150 orders", se.call_args[0][0])
        self.assertIs(io.call_args_list[0].kwargs.get("email"), False)   # issue_open does not email too
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

class EscalationThroughGiveups(unittest.TestCase):
    """[v2.14.1] The escalation fires from the paths production really takes:
    a give-up for an order already in its product's thread, and the
    periodic pass, with exactly one plain-subject email per day's thread."""

    def setUp(self):
        _chdir_tmp(self)

    def _product_thread(self, ch, orders):
        import notify
        with ch.patch():
            notify.issue_open("held:1982906533", "Multi Styler holds orders", "detail",
                              meta={"orders": [str(o) for o in orders]}, email=False)

    def test_giveup_in_product_thread_escalates_once_and_emails_once(self):
        r = held_relay(150, held_escalate_count=100, escalation_mentions=["U0A1F5MPR2B"])
        ch = Channels()
        self._product_thread(ch, range(1, 151))
        with ch.patch():
            for oid in ("5", "6", "7"):                     # three give-ups, all threaded
                state, _ = r.handle_row(_row(oid=oid, ref="284000005", attempts=5))
                self.assertEqual(state, "error-final")
        esc = ch.escalations()
        self.assertEqual(len(esc), 1, ch.slack.sent)
        self.assertIn("<@U0A1F5MPR2B>", esc[0])
        self.assertIn("150 orders", esc[0])
        emails = ch.escalation_emails()
        self.assertEqual(len(emails), 1, ch.emails)
        self.assertNotIn("<@", emails[0][0])
        self.assertNotIn(EM_DASH, emails[0][0] + emails[0][1])
        # the threaded orders joined their product thread, so no backlog thread
        self.assertFalse(any("on hold for catalog approval" in h for h, _ in ch.slack.sent))

    def test_periodic_escalates_without_any_giveup(self):
        r = held_relay(150, held_escalate_count=100)
        ch = Channels(email=False)
        with ch.patch():
            r.periodic()
            r._periodic_at = 0
            r.periodic()                                    # same day: no second thread
        self.assertEqual(len(ch.escalations()), 1)

    def test_periodic_quiet_when_escalation_off_or_below_threshold(self):
        for kw in ({}, {"held_escalate_count": 500}):
            r = held_relay(150, **kw)
            ch = Channels()
            with ch.patch():
                r.periodic()
            self.assertEqual(ch.slack.sent, [], kw)
            self.assertEqual(ch.emails, [], kw)

    def test_slack_outage_still_emails_once_and_retry_does_not_repeat_it(self):
        import notify
        r = held_relay(150, held_escalate_count=100, escalation_mentions=["U1"])
        ch = Channels()
        with ch.patch(post=lambda *a, **k: None):           # Slack refuses every post
            r._escalate_held(150, "Multi Styler (150)")
            r._escalate_held(151, "Multi Styler (151)")
        self.assertEqual(len(ch.escalation_emails()), 1)
        self.assertFalse(notify.issues_open("held-escalation:"))   # stored failed, not open
        # Slack is back and the retry window has passed: the thread posts, no new email
        with notify._issues() as d:
            for k, rec in d.items():
                if k.startswith("held-escalation:"):
                    rec["retry_at"] = time.time() - 1
        with ch.patch():
            r._escalate_held(152, "Multi Styler (152)")
        self.assertEqual(len(ch.escalations()), 1)
        self.assertEqual(len(ch.escalation_emails()), 1)

    def test_not_reopened_from_a_live_queue_read_older_than_the_all_clear(self):
        import notify
        r = held_relay(150, held_escalate_count=100)
        ch = Channels(email=False)
        with ch.patch():
            r.periodic()
            for key in notify.issues_open("held-escalation:"):  # the live settle's all-clear
                notify.issue_resolve(key, "Resolved: the hold has cleared.")
            r._periodic_at = 0
            r.periodic()                    # index cached from before the all-clear
            self.assertEqual(len(ch.escalations()), 1)
            r.gio.live_rows = []            # the next real read sees the hold gone
            r._live_index_at = r._periodic_at = 0
            r.periodic()
        self.assertEqual(len(ch.escalations()), 1)


class GiveupRobustness(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_registry_fault_in_held_branch_still_settles_the_row(self):
        r = held_relay(3)
        ch = Channels(email=False)
        with ch.patch(), mock.patch("notify.issues_open", side_effect=OSError("lock unreadable")), \
             mock.patch("notify.issue_update", side_effect=OSError("disk full")):
            state, _ = r.handle_row(_row(oid="2", ref="284000002", attempts=5))
            self.assertEqual(state, "error-final")
            state, _ = r.handle_row(_row(oid="3", ref="284000003", attempts=5))   # backlog update path
            self.assertEqual(state, "error-final")
        # it fell through to the backlog thread
        self.assertTrue(any("on hold for catalog approval" in h for h, _ in ch.slack.sent))


class UnclassifiedGiveups(unittest.TestCase):
    """[v2.14.1] With live_min_reference unset nothing can be classified, so
    give-ups share one neutral thread instead of a red thread per order."""

    def setUp(self):
        _chdir_tmp(self)

    def test_many_giveups_share_one_neutral_thread(self):
        import notify
        r = relay(live_min_reference=0)
        ch = Channels(email=False)
        with ch.patch():
            for i in range(60):
                state, _ = r.handle_row(_row(oid=str(1000 + i), ref=str(284000000 + i), attempts=5))
                self.assertEqual(state, "error-final")
        self.assertEqual(len(ch.slack.sent), 1, [h for h, _ in ch.slack.sent][:3])
        head, body = ch.slack.sent[0]
        self.assertIn("not in HubSpot", head)
        self.assertNotIn("\U0001F534", head)
        self.assertNotIn("not on hold for the catalog", body)
        self.assertNotIn(EM_DASH, head + body)
        self.assertEqual(ch.slack.replies, [])              # news throttled to every 6 h
        m = notify.issue_get("status-unclassified")["meta"]
        self.assertEqual(len(m["orders"]), 60)
        self.assertEqual(m["count"], 60)
        self.assertFalse(notify.issues_open("status-missing:"))

    def test_news_after_six_hours_counts_orders_once(self):
        import notify
        r = relay(live_min_reference=0)
        ch = Channels(email=False)
        with ch.patch():
            r.handle_row(_row(oid="1", ref="284000001", attempts=5))
            r.handle_row(_row(oid="1", ref="284000001", attempts=5))     # a second update, same order
            with notify._issues() as d:
                d["status-unclassified"]["last_update"] = time.time() - 7 * 3600
            r.handle_row(_row(oid="2", ref="284000002", attempts=5))
        self.assertEqual(len(ch.slack.replies), 1)
        self.assertIn("2 orders like this", ch.slack.replies[0][1])

    def test_closed_after_a_quiet_day_then_a_new_one_opens(self):
        import notify
        r = relay(live_min_reference=0)
        ch = Channels(email=False)
        with ch.patch():
            r.handle_row(_row(oid="1", ref="284000001", attempts=5))
            r.periodic()                                     # not quiet yet
            self.assertTrue(notify.issue_is_open("status-unclassified"))
            with notify._issues() as d:
                d["status-unclassified"]["meta"]["last"] = time.time() - 25 * 3600
            r._periodic_at = 0
            r.periodic()
            self.assertFalse(notify.issue_is_open("status-unclassified"))
            r.handle_row(_row(oid="9", ref="284000009", attempts=5))
        self.assertEqual(len(ch.slack.sent), 2)
        self.assertTrue(any("Closed" in t for _, t, _ in ch.slack.replies))
        self.assertEqual(notify.issue_get("status-unclassified")["meta"]["orders"], ["9"])

    def test_recent_kind_keeps_its_own_red_thread(self):
        r = held_relay(0)
        ch = Channels(email=False)
        with ch.patch():
            r.handle_row(_row(oid="909", ref="284123456", attempts=5))
            r.handle_row(_row(oid="910", ref="284123457", attempts=5))
        self.assertEqual(len(ch.slack.sent), 2)
        self.assertTrue(all("\U0001F534" in h for h, _ in ch.slack.sent))


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
        u = {"name": "Multi Styler", "sku": "C053_8", "pid": "1982906533", "why": "bundle_inactive",
             "approved": True}
        with mock.patch("slack_text.SheetIndex.row_link", return_value=("Bundles tab", False)), \
             mock.patch.object(backfill.Engine, "stray_verdict", return_value=True):
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
