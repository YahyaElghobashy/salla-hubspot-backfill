#!/usr/bin/env python3
"""v2.14 alert truthfulness and escalation.

The Oct 2026 hold was detected within 7 hours and posted 26 times, but the
alerts told people to approve an already-approved product and nobody acted.
These tests pin the corrected wording and the escalation that tags named
owners once a hold passes a threshold, at most once per day.
"""
import json
import time
import types
import unittest
from contextlib import ExitStack
from datetime import datetime
from unittest import mock

from test_realtime_ext import _chdir_tmp, _cfg, _row, FakeHS, FakeGIO, FakeSlack, IndexedGIO

EM_DASH = "\u2014"
HELD_NOTE = "catalog gate: Multi Styler -- in review queue"


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
        return [e for e in self.emails if "orders are on hold and not in HubSpot" in e[0]
                and not e[0].startswith("Resolved")]

    def resolved_emails(self):
        return [e for e in self.emails if e[0].startswith("Resolved")
                and "orders are on hold and not in HubSpot" in e[0]]


class Clock:
    """[v2.14.1] One fake clock for time.time() and the relay's day key."""

    def __init__(self, start=datetime(2026, 10, 12, 9, 0)):
        self.t = start.timestamp()

    def time(self):
        return self.t

    def day(self):
        return datetime.fromtimestamp(self.t).strftime("%Y-%m-%d")

    def patch(self):
        import notify
        import status_relay
        clock = self

        class Now(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromtimestamp(clock.t, tz)

        stack = ExitStack()
        stack.enter_context(mock.patch("time.time", self.time))
        stack.enter_context(mock.patch.object(status_relay, "datetime", Now))
        # this clock sits near the real one, so a Slack breaker another test
        # module left open would skip these posts: start it closed
        stack.enter_context(mock.patch.dict(notify._breaker, {"until": 0.0}))
        return stack


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
            self.assertEqual(ch.escalations(), [])          # [v2.14.1] a caller never posts a failed key
            r.periodic()                                    # the relay leaves hold keys to the settle
            self.assertEqual(ch.escalations(), [])
            notify.retry_failed()                           # the live settle's retry, after its hold check
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


class NewHoldSameDay(unittest.TestCase):
    """[v2.14.1] review round 3: once the day's escalation closed, a NEW hold
    that passed the threshold later that day stayed silent until midnight.
    It reopens when the live settle's own count (drain-closed rows left out)
    confirms the hold; rows that only still read "held" never reopen it."""

    def setUp(self):
        _chdir_tmp(self)

    def closed_today(self, ch, r):
        import notify
        with ch.patch():
            r._escalate_held(150, "Multi Styler (150)")
            for key in notify.issues_open("held-escalation:"):
                notify.issue_resolve(key, "Resolved: the hold has cleared.")

    def stamp(self, count, age_s=0):
        import live
        live.stamp_settle_held(count)
        if age_s:
            d = json.loads(live.SETTLE_HELD.read_text())
            d["t"] -= age_s
            live.SETTLE_HELD.write_text(json.dumps(d))

    def test_reopens_when_the_settle_counts_a_new_hold(self):
        r = held_relay(150, held_escalate_count=100)
        ch = Channels(email=False)
        self.closed_today(ch, r)
        self.stamp(140)
        with ch.patch():
            r._escalate_held(150, "Multi Styler (150)")
        self.assertEqual(len(ch.escalations()), 2)

    def test_stays_closed_when_the_settle_count_is_low_or_stale(self):
        r = held_relay(150, held_escalate_count=100)
        ch = Channels(email=False)
        self.closed_today(ch, r)
        for count, age in ((20, 0), (140, 3 * 3600)):
            self.stamp(count, age)
            with ch.patch():
                r._escalate_held(150, "Multi Styler (150)")
        self.assertEqual(len(ch.escalations()), 1)


class EscalationNotReopened(unittest.TestCase):
    """[v2.14.1] N4: the relay counts every "held" Live Queue row, including
    orders the catalog drain has finished with (deleted in Salla, out of
    attempts), which nothing marks terminal. The live settle leaves those out
    and closes the day's escalation, and the relay reopened it on its next
    fresh read: a red thread tagging the owners, and an email, every hour
    with no hold in progress. A day's escalation that closed stays closed
    that day."""

    def setUp(self):
        _chdir_tmp(self)

    @staticmethod
    def all_clear():
        import notify
        for key in notify.issues_open("held-escalation:"):      # the live settle's all-clear
            notify.issue_resolve(key, "✅ Resolved: the hold has cleared.")

    def test_closed_today_is_not_reopened_by_a_fresh_read_or_a_giveup(self):
        r = held_relay(150, held_escalate_count=100, escalation_mentions=["U1"])
        ch, clock = Channels(), Clock()
        with ch.patch(), clock.patch():
            r.periodic()
            self.all_clear()
            for i in range(3):                       # fresh reads, still 150 "held" rows
                clock.t += 600
                r.periodic()
                r.handle_row(_row(oid=str(5 + i), ref=str(284000005 + i), attempts=5))   # held give-ups
        self.assertEqual(len(ch.escalations()), 1, [h for h, _ in ch.slack.sent])
        self.assertEqual(len(ch.escalation_emails()), 1)

    def test_a_resolve_reply_still_waiting_to_post_counts_as_closed(self):
        import notify
        r = held_relay(150, held_escalate_count=100)
        ch, clock = Channels(email=False), Clock()
        refuse_replies = lambda text, **k: None if text.startswith("✅") else ch.slack.post(text, **k)
        with clock.patch():
            with ch.patch():
                r.periodic()
            key = f"held-escalation:{clock.day()}"
            with ch.patch(post=refuse_replies):
                self.all_clear()
            self.assertEqual(notify.issue_get(key)["state"], "resolving")
            clock.t += 600
            with ch.patch():
                r.periodic()                        # posts the waiting reply; opens nothing
            self.assertEqual(notify.issue_get(key)["state"], "closed")
            clock.t += 600
            with ch.patch():
                r.periodic()
        self.assertEqual(len(ch.escalations()), 1)
        self.assertEqual([t for _, t, _ in ch.slack.replies], ["✅ Resolved: the hold has cleared."])

    def test_a_hold_still_there_tomorrow_escalates_on_tomorrows_key(self):
        import notify
        r = held_relay(150, held_escalate_count=100)
        ch, clock = Channels(email=False), Clock(datetime(2026, 10, 12, 22, 0))
        with ch.patch(), clock.patch():
            r.periodic()
            self.all_clear()
            for _ in range(18):                     # 22:00 to 01:00, every 10 minutes
                clock.t += 600
                r.periodic()
        self.assertEqual(len(ch.escalations()), 2)
        self.assertEqual(notify.issue_get("held-escalation:2026-10-12")["state"], "closed")
        self.assertTrue(notify.issue_is_open("held-escalation:2026-10-13"))


class RelayAndSettleOverHours(unittest.TestCase):
    """[v2.14.1] N4 end to end: the relay's periodic pass every 10 minutes and
    the live settle every hour, over 18 simulated hours across midnight. 120
    Live Queue rows read "held" for orders the catalog drain has closed (their
    Queue Log rows are gone or out of attempts) and no order is really on
    hold. Round 1 posted a red thread, an escalation email and a "Resolved"
    email every hour; now it is one red thread a day at most."""

    def setUp(self):
        _chdir_tmp(self)

    def run_hours(self, hours, qlog, land=True):
        import backfill
        import live
        import queue_drain
        import status_relay
        rows = [{"row": i + 1, "order_id": str(i), "reference_id": str(284000000 + i), "status": "held",
                 "attempts": 0, "note": HELD_NOTE, "received_at": "2026-10-10 10:00:00"} for i in range(1, 121)]
        relay_ = status_relay.StatusRelay(
            _cfg(held_escalate_count=100, escalation_mentions=["U1"], live_min_reference=280000000,
                 live_queue_tab="Live Queue"), FakeHS(), IndexedGIO(rows), live=True)

        def mark(q, marks):                 # the settle's marks land on the sheet the relay reads
            if not land:
                return 0                    # a write that never reached the sheet
            by_row = {r["row"]: r for r in rows}
            for m in marks:
                if m[0] in by_row:
                    by_row[m[0]].update(status=m[2], attempts=m[3], note=m[4])
            return len(marks)

        eng = object.__new__(live.LiveEngine)
        eng.cfg = backfill.Config()
        eng.live, eng.qsid = True, "Q"
        eng.created_ledger = types.SimpleNamespace(get=lambda oid: None)
        eng.gio = types.SimpleNamespace(queue_read_all=lambda q, tab=None: [dict(r) for r in rows],
                                        queue_mark_batch=mark)
        eng.hs = mock.Mock()
        ch, clock, per_day = Channels(), Clock(), {}
        with ch.patch(), clock.patch(), \
                mock.patch.object(queue_drain.DrainGoogleIO, "qlog_read", lambda gio: list(qlog)), \
                mock.patch("realtime_base.trim_lock_active", return_value=False):
            for step in range(hours * 6):
                before = len(ch.escalations())
                relay_.periodic()
                if step % 6 == 3:                   # the hourly settle
                    eng._next_settle = 0
                    live.LiveEngine._maybe_settle_held(eng)
                per_day[clock.day()] = per_day.get(clock.day(), 0) + len(ch.escalations()) - before
                clock.t += 600
        return ch, per_day, rows

    QLOG = ([{"order_id": str(i), "status": "gone", "attempts": 8} for i in range(1, 81)]
            + [{"order_id": str(i), "status": "error", "attempts": 8} for i in range(81, 121)])

    def test_drain_closed_rows_raise_at_most_one_red_thread_a_day(self):
        import notify
        ch, per_day, rows = self.run_hours(18, self.QLOG)  # 09:00 to 03:00 the next day
        # the settle marked the drain-closed rows terminal, so the relay stops counting them
        self.assertTrue(all(r["status"] == "gone" for r in rows))
        # the settle really ran and gave its all-clear (else this proves nothing)
        self.assertEqual(notify.issue_get("held-escalation:2026-10-12")["state"], "closed")
        self.assertTrue(any("the hold has cleared" in t for _, t, _ in ch.slack.replies))
        self.assertEqual(per_day["2026-10-12"], 1, per_day)
        self.assertTrue(all(n <= 1 for n in per_day.values()), per_day)
        threads = len(ch.escalations())
        self.assertEqual(threads, sum(per_day.values()))
        self.assertEqual(len(ch.escalation_emails()), threads)
        self.assertLessEqual(len(ch.resolved_emails()), threads)
        self.assertLessEqual(len([t for _, t, _ in ch.slack.replies if "the hold has cleared" in t]), threads)
        self.assertTrue(all("<@U1>" in h for h in ch.escalations()))

    def test_rows_still_read_held_reopen_nothing_the_same_day(self):
        """The belt alone: the settle's marks never reach the sheet, so the
        relay counts 120 held rows all day. The day's escalation that the
        settle closed stays closed; the next day gets one thread of its own."""
        import notify
        ch, per_day, rows = self.run_hours(18, self.QLOG, land=False)
        self.assertTrue(all(r["status"] == "held" for r in rows))
        self.assertEqual(notify.issue_get("held-escalation:2026-10-12")["state"], "closed")
        self.assertEqual(per_day.get("2026-10-12"), 1, per_day)
        self.assertEqual(per_day.get("2026-10-13"), 1, per_day)
        self.assertEqual(len(ch.escalation_emails()), 2)
        self.assertEqual(len(ch.resolved_emails()), 2)


class EscalationEmailBackoff(unittest.TestCase):
    """[v2.14.1] N5: while Slack refused the day's escalation, its email was
    tried again on every held give-up and every periodic pass, and each try
    against an unreachable SMTP server can block the single-threaded relay
    for SMTP_TIMEOUT_S. Each attempt is now recorded before the send, and the
    next one waits notify's failed backoff (5, 10, 20 ... minutes)."""

    def setUp(self):
        _chdir_tmp(self)

    def channels(self, send):
        import notify
        return mock.patch.multiple(notify, slack_enabled=lambda: True, slack_channels=lambda: ["C1"],
                                   post_slack=lambda *a, **k: None, email_enabled=lambda: True,
                                   send_email=send, add_reaction=lambda *a, **k: True)

    def test_dead_smtp_is_tried_once_per_retry_window(self):
        import notify
        r = held_relay(150, held_escalate_count=100, escalation_mentions=["U1"])
        tries, clock = [], Clock()

        def dead_smtp(subject, body, dry_run=False):
            tries.append(subject)
            return False

        def giveups(oids):
            for oid in oids:
                state, _ = r.handle_row(_row(oid=str(oid), ref=str(284000000 + oid), attempts=5))
                self.assertEqual(state, "error-final")

        esc = lambda: [s for s in tries if "orders are on hold and not in HubSpot" in s]
        with clock.patch(), self.channels(dead_smtp):
            giveups(range(1, 21))                          # 20 held give-ups in one window
            r.periodic()
            self.assertEqual(len(esc()), 1)
            clock.t += notify.FAILED_RETRY_S
            giveups(range(21, 31))
            self.assertEqual(len(esc()), 2)
            clock.t += notify.FAILED_RETRY_S                # the second wait is twice as long
            giveups([31])
            self.assertEqual(len(esc()), 2)
            clock.t += notify.FAILED_RETRY_S
            giveups([32, 33])
            self.assertEqual(len(esc()), 3)
        self.assertNotIn("<@", esc()[0])

    def test_an_attempt_that_raises_still_waits_its_window(self):
        import notify
        r = held_relay(150, held_escalate_count=100)
        calls, clock = [], Clock()

        def hung(subject, body, dry_run=False):
            calls.append(subject)
            raise OSError("SMTP connect timed out")

        with clock.patch(), self.channels(hung):
            r._escalate_held(150, "")
            r._escalate_held(151, "")
            self.assertEqual(len(calls), 1)
            clock.t += notify.FAILED_RETRY_S
            r._escalate_held(152, "")
        self.assertEqual(len(calls), 2)

    def test_sent_once_then_never_again(self):
        import notify
        r = held_relay(150, held_escalate_count=100)
        sent, clock = [], Clock()
        smtp = {"up": False}

        def send(subject, body, dry_run=False):
            sent.append((subject, smtp["up"]))
            return smtp["up"]

        with clock.patch(), self.channels(send):
            r._escalate_held(150, "")
            clock.t += notify.FAILED_RETRY_S
            smtp["up"] = True
            r._escalate_held(150, "")
            for _ in range(36):                     # 6 more hours of periodic passes, Slack still down
                clock.t += 600
                r._escalate_held(150, "")
        self.assertEqual(sent, [(sent[0][0], False), (sent[0][0], True)])


class PeriodicRetriesFailed(unittest.TestCase):
    """[v2.14.1] #28 remainder: a Slack thread that was refused is posted
    only by notify.retry_failed(), from the main loops, and the relay's
    periodic pass is one of them. A key raised once (a lost live order, a
    Zid collision) still reaches the channel, and a "Resolved" reply that
    was refused still lands in its thread."""

    def setUp(self):
        _chdir_tmp(self)

    def test_a_key_raised_once_posts_from_periodic(self):
        import notify
        r = relay(live_min_reference=280000000)
        ch, clock = Channels(email=False), Clock()
        with clock.patch():
            with ch.patch(post=lambda *a, **k: None):
                state, _ = r.handle_row(_row(oid="909", ref="284123456", attempts=5))
            self.assertEqual(state, "error-final")
            self.assertEqual(notify.issue_get("status-missing:909")["state"], "failed")
            with ch.patch():
                r.periodic()                                # not due yet
                self.assertEqual(ch.slack.sent, [])
                clock.t += notify.FAILED_RETRY_S
                r._periodic_at = 0
                r.periodic()
        self.assertEqual(len(ch.slack.sent), 1)
        self.assertIn("#284123456", ch.slack.sent[0][0])
        self.assertTrue(notify.issue_is_open("status-missing:909"))

    def test_a_refused_resolve_reply_posts_from_periodic(self):
        import notify
        r = relay()
        ch, clock = Channels(email=False), Clock()
        with clock.patch():
            with ch.patch():
                r.handle_row(_row(event="status:weird@2026"))   # unmapped:weird opens
            r.stage_map["weird"] = "st-NEW"
            with ch.patch(post=lambda *a, **k: None):
                r.periodic()                                # the resolve reply is refused
            self.assertEqual(notify.issue_get("unmapped:weird")["state"], "resolving")
            clock.t += 600
            with ch.patch():
                r.periodic()
        self.assertEqual(notify.issue_get("unmapped:weird")["state"], "closed")
        self.assertEqual(len(ch.slack.replies), 1)
        self.assertIn("now moves orders to a HubSpot stage", ch.slack.replies[0][1])


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
