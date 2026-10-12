#!/usr/bin/env python3
"""v2.14.1 issue-thread registry: failed opens and fast-fail posts.

Review findings #7, #24, #28: an open whose Slack post failed on every
channel was still stored "open" with no thread, so every later update and the
resolve posted nothing and the incident never reached the channel. It is now
stored "failed" and the first caller after retry_at posts the thread.

Finding #14: issue posts run inside the live worker lanes, where with_retries
(3 tries, 20 s timeout, backoff) held a lane for about 75 s per channel during
a Slack outage. Issue posts now fail fast; every other caller keeps the old
retries.

Findings #15, #22, #32 need issue_open(email=False) so a caller that sends
its own email does not send it twice.

Everything is offline: Slack, SMTP and the clock are fakes.
"""
import json
import os
import time
import unittest
from unittest import mock

import notify
from test_realtime_ext import FakeSlack, _chdir_tmp


class FlakySlack(FakeSlack):
    """FakeSlack (same strict post signature) whose posts fail for the
    channels in `down` ("*" = every channel)."""

    def __init__(self, down=()):
        super().__init__()
        self.down = set(down)
        self.calls = []

    def post(self, text, blocks=None, thread_ts=None, dry_run=False, channel=None, reply_broadcast=False):
        self.calls.append((channel, thread_ts, text))
        if "*" in self.down or channel in self.down:
            return None
        return super().post(text, blocks, thread_ts, dry_run, channel, reply_broadcast)


class RegistryCase(unittest.TestCase):
    channels = ["C1", "C2"]

    def setUp(self):
        _chdir_tmp(self)
        self.slack = FlakySlack()
        self.email = mock.Mock(return_value=False)
        self.reactions = mock.Mock(return_value=True)
        p = mock.patch.multiple(notify, slack_enabled=lambda: True, slack_channels=lambda: list(self.channels),
                                post_slack=self.slack.post, send_email=self.email,
                                add_reaction=self.reactions)
        p.start()
        self.addCleanup(p.stop)

    def later(self, seconds):
        """The registry clock `seconds` from now."""
        return mock.patch("time.time", return_value=time.time() + seconds)


class FailedOpen(RegistryCase):
    def test_failed_open_is_not_open_and_posts_after_retry_at(self):
        self.slack.down = {"*"}
        t0 = time.time()
        self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail",
                                           meta={"orders": ["A"]}))
        rec = notify.issue_get("held:1")
        self.assertEqual(rec["state"], "failed")
        self.assertNotIn("threads", rec)
        self.assertAlmostEqual(rec["retry_at"], t0 + notify.FAILED_RETRY_S, delta=5)
        self.assertFalse(notify.issue_is_open("held:1"))
        self.assertEqual(notify.issues_open("held:"), {})

        # Slack is back, but the retry is not due yet: meta only, no post
        self.slack.down = set()
        n = len(self.slack.calls)
        self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail",
                                           meta={"orders": ["B"]}))
        self.assertEqual(len(self.slack.calls), n)
        self.assertEqual(notify.issue_get("held:1")["meta"]["orders"], ["A", "B"])

        # the next call after retry_at posts the thread in every channel
        with self.later(notify.FAILED_RETRY_S + 1):
            self.assertTrue(notify.issue_open("held:1", "Product 1 holds orders", "detail",
                                              meta={"orders": ["C"]}))
            self.assertEqual(self.slack.sent, [["Product 1 holds orders", "detail"]] * 2)
            rec = notify.issue_get("held:1")
            self.assertEqual(rec["state"], "open")
            self.assertEqual(set(rec["threads"]), {"C1", "C2"})
            self.assertEqual(rec["meta"]["orders"], ["A", "B", "C"])   # orders that waited are kept
            self.assertNotIn("retry_at", rec)
            self.assertTrue(notify.issue_is_open("held:1"))

            # and the thread is now answered like any other
            self.assertTrue(notify.issue_update("held:1", "news"))
            self.assertTrue(notify.issue_resolve("held:1", "Released"))
        self.assertEqual([r[1] for r in self.slack.replies], ["news", "news", "Released", "Released"])

    def test_update_and_resolve_on_failed_key_return_false(self):
        self.slack.down = {"*"}
        notify.issue_open("make-off:1", "Scenario is off", "detail")
        self.slack.down = set()
        n = len(self.slack.calls)
        self.assertFalse(notify.issue_update("make-off:1", "still off", meta={"orders": ["X"]}))
        self.assertEqual(notify.issue_get("make-off:1")["meta"]["orders"], ["X"])
        self.assertFalse(notify.issue_resolve("make-off:1", "Back on"))
        self.assertEqual(len(self.slack.calls), n)
        self.reactions.assert_not_called()
        # nothing was ever posted, so the resolve closes it and the next
        # occurrence opens a fresh thread at once, without waiting for retry_at
        self.assertEqual(notify.issue_get("make-off:1")["state"], "closed")
        self.assertTrue(notify.issue_open("make-off:1", "Scenario is off again", "detail"))
        self.assertEqual(len(self.slack.sent), 2)

    def test_partial_success_counts_as_open(self):
        self.slack.down = {"C2"}
        self.assertTrue(notify.issue_open("zid:9", "Order 9 collides", "detail"))
        rec = notify.issue_get("zid:9")
        self.assertEqual((rec["state"], set(rec["threads"])), ("open", {"C1"}))
        self.slack.down = set()
        n = len(self.slack.calls)
        self.assertTrue(notify.issue_update("zid:9", "news"))
        self.assertEqual(self.slack.calls[n:], [("C1", rec["threads"]["C1"], "news")])

    def test_slack_not_configured_still_records_open(self):
        with mock.patch.object(notify, "slack_enabled", lambda: False):
            self.assertTrue(notify.issue_open("held:2", "Product 2 holds orders", "detail"))
        self.assertEqual(notify.issue_get("held:2")["state"], "open")
        self.assertEqual(self.slack.calls, [])

    def test_failed_try_emails_once_and_the_retry_does_not_email_again(self):
        self.email.return_value = True
        with mock.patch.object(notify, "email_enabled", lambda: True):
            self.slack.down = {"*"}
            self.assertFalse(notify.issue_open("held:3", "Product 3 holds orders", "detail"))
            self.assertEqual(self.email.call_count, 1)    # email is the only signal while Slack fails
            self.slack.down = set()
            with self.later(notify.FAILED_RETRY_S + 1):
                self.assertTrue(notify.issue_open("held:3", "Product 3 holds orders", "detail"))
        self.assertEqual(self.email.call_count, 1)

    def test_email_false_sends_no_email(self):
        self.assertTrue(notify.issue_open("held-escalation:2026-10-12", "Escalation", "detail", email=False))
        self.email.assert_not_called()
        self.assertTrue(notify.issue_open("held:4", "Product 4 holds orders", "detail"))
        self.email.assert_called_once_with("Product 4 holds orders", "detail")

    def test_failed_records_are_pruned_after_a_month(self):
        old = time.time() - notify.CLOSED_KEEP_S - 60
        notify.ISSUES.write_text(json.dumps({
            "held:old": {"state": "failed", "retry_at": old},
            "held:new": {"state": "failed", "retry_at": time.time() + 60}}))
        notify.issues_open()
        self.assertEqual(set(json.loads(notify.ISSUES.read_text())), {"held:new"})

    def test_resolve_email_subject_has_no_slack_mentions(self):
        """Finding #32's other half: the escalation subject tags owners for
        Slack, and the resolve email reused it with the raw markup."""
        subject = "🔴 <@U0A1F5MPR2B> <@U0AQ36NEFDX> 150 orders are on hold and not in HubSpot."
        self.assertTrue(notify.issue_open("held-escalation:2026-10-12", subject, "detail", email=False))
        self.assertTrue(notify.issue_resolve("held-escalation:2026-10-12", "Resolved: the hold has cleared."))
        self.email.assert_called_once_with("Resolved: 🔴 150 orders are on hold and not in HubSpot.",
                                           "Resolved: the hold has cleared.")
        self.assertIn("<@U0A1F5MPR2B>", self.slack.sent[0][0])   # Slack still tags them


class OpeningClock(RegistryCase):
    def test_slow_open_is_not_taken_over_by_a_second_lane(self):
        """Each post takes 100 s on the fake clock, so the whole open takes
        400 s, past OPENING_TTL_S. A second lane arriving during the second
        channel's post must still see the open in flight (finding #7: it
        used to open a duplicate thread and overwrite the first one's ts)."""
        clock = [time.time()]
        fired, second = [], []
        base = self.slack.post

        def slow(text, blocks=None, thread_ts=None, dry_run=False, channel=None, reply_broadcast=False):
            clock[0] += 100
            if channel == "C2" and thread_ts is None and not fired:
                fired.append(1)
                second.append(notify.issue_open("held:5", "Product 5 holds orders", "detail",
                                                meta={"orders": ["B"]}))
            return base(text, blocks, thread_ts, dry_run, channel, reply_broadcast)

        with mock.patch.object(notify, "post_slack", slow), \
                mock.patch("time.time", side_effect=lambda: clock[0]):
            self.assertTrue(notify.issue_open("held:5", "Product 5 holds orders", "detail",
                                              meta={"orders": ["A"]}))
        self.assertEqual(second, [False])
        self.assertEqual(len(self.slack.sent), 2)        # one thread per channel, no duplicate
        rec = notify.issue_get("held:5")
        self.assertEqual(set(rec["threads"]), {"C1", "C2"})
        self.assertEqual(rec["meta"]["orders"], ["A", "B"])
        self.assertNotIn("t", rec)


class FastFailPosts(unittest.TestCase):
    """The real post_slack against a fake http_request: issue_* posts use a
    short timeout and at most one retry; send_alert keeps 3 tries at 20 s."""

    DOWN = (0, {}, "NETWORK_ERROR: timed out")
    OK = (200, {}, json.dumps({"ok": True, "ts": "1.0001"}))

    def setUp(self):
        _chdir_tmp(self)
        env = mock.patch.dict(os.environ, {"SLACK_BOT_TOKEN": "xoxb-test", "SLACK_CHANNEL_IDS": "C1,C2"})
        env.start()
        self.addCleanup(env.stop)
        for k in ("SMTP_HOST", "SMTP_FROM", "SMTP_TO"):
            os.environ.pop(k, None)
        self.reply = self.DOWN
        self.calls, self.sleeps = [], []

        def http(method, url, headers=None, body=None, timeout=90):
            self.calls.append((url, timeout))
            return self.reply

        for p in (mock.patch.object(notify, "http_request", side_effect=http),
                  mock.patch("time.sleep", side_effect=self.sleeps.append),
                  mock.patch.object(notify, "send_email", return_value=False)):
            p.start()
            self.addCleanup(p.stop)

    def test_issue_open_fails_fast(self):
        self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        # one parent post per channel (no body after a failed parent), each
        # tried twice at most, short timeouts, short pauses, none after the last try
        self.assertEqual(len(self.calls), 4)
        self.assertTrue(all(t <= 10 for _, t in self.calls))
        self.assertEqual(len(self.sleeps), 2)
        self.assertTrue(all(s <= notify.ISSUE_RETRY_PAUSE_S for s in self.sleeps))

    def test_long_retry_after_is_not_waited_for(self):
        self.reply = (429, {"Retry-After": "30"}, "")
        self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        self.assertEqual(len(self.calls), 2)           # one try per channel
        self.assertEqual(self.sleeps, [])

    def test_update_and_resolve_fail_fast(self):
        self.reply = self.OK
        self.assertTrue(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        self.reply, self.calls[:], self.sleeps[:] = self.DOWN, [], []
        self.assertTrue(notify.issue_update("held:1", "news"))
        self.assertTrue(notify.issue_resolve("held:1", "Released"))
        # update: 2 channels x 2 tries; resolve: 2 x 2 tries plus one reaction each
        self.assertEqual(len(self.calls), 10)
        self.assertTrue(all(t <= 10 for _, t in self.calls))
        self.assertEqual(sum(1 for u, _ in self.calls if u == notify.SLACK_REACT), 2)

    def test_other_callers_keep_patient_defaults(self):
        notify.issue_open("held:1", "Product 1 holds orders", "detail")
        self.calls[:] = []
        notify.send_alert("Something broke", "detail")
        # 3 tries per channel at 20 s: the fast mode did not leak out of issue_open
        self.assertEqual(self.calls, [(notify.SLACK_API, 20)] * 6)

    def test_fast_mode_is_restored_after_an_error_and_when_nested(self):
        with self.assertRaises(RuntimeError):
            with notify._fast_posts():
                with notify._fast_posts():
                    pass
                self.assertTrue(notify._posting.fast)   # the inner exit restores, not clears
                raise RuntimeError
        notify.post_slack("plain")
        self.assertEqual(self.calls[0], (notify.SLACK_API, 20))


if __name__ == "__main__":
    unittest.main(verbosity=2)
