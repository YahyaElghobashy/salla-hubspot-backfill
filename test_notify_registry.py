#!/usr/bin/env python3
"""v2.14.1 issue-thread registry: failed opens, retries and fast-fail posts.

Review findings #7, #24, #28: an open whose Slack post failed on every
channel was still stored "open" with no thread, so every later update and the
resolve posted nothing and the incident never reached the channel. It is now
stored "failed" with its subject and body, callers only merge into it, and
retry_failed() (from the main loops) posts it once retry_at passes, backing
off per key. A key raised only once (credits-out) is therefore still posted.

Finding #14: issue posts ran inside the live worker lanes, where with_retries
(3 tries, 20 s timeout, backoff) held a lane for about 75 s per channel during
a Slack outage. Inside notify.fast_posts() (entered by the lane entry points)
posts fail fast; every other caller keeps the patient retries (N6).

N6: a resolve that no channel took stays "resolving" and retry_failed() posts
the reply later, so one Slack 429 no longer loses the "Resolved" answer.
N8: a failed record from a past episode does not carry its old orders into a
new thread, and close_failed() closes one silently for the live settle.
N11: a process-wide breaker stops every issue post for BREAKER_S after a
network-level failure, so each new key no longer pays its own stall.

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


def _reset_breaker(test):
    notify._breaker["until"] = 0.0
    test.addCleanup(notify._breaker.__setitem__, "until", 0.0)


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
        _reset_breaker(self)
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

    def clock(self):
        """A registry clock the test moves by hand: clock[0] is "now"."""
        clock = [time.time()]
        p = mock.patch("time.time", side_effect=lambda: clock[0])
        p.start()
        self.addCleanup(p.stop)
        return clock


class FailedOpen(RegistryCase):
    def test_failed_open_is_not_open_and_retry_failed_posts_it(self):
        self.slack.down = {"*"}
        t0 = time.time()
        self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail",
                                           meta={"orders": ["A"]}))
        rec = notify.issue_get("held:1")
        self.assertEqual(rec["state"], "failed")
        self.assertNotIn("threads", rec)
        self.assertEqual((rec["subject"], rec["body"]), ("Product 1 holds orders", "detail"))
        self.assertAlmostEqual(rec["retry_at"], t0 + notify.FAILED_RETRY_S, delta=5)
        self.assertFalse(notify.issue_is_open("held:1"))
        self.assertEqual(notify.issues_open("held:"), {})

        # Slack is back, but the retry is not due yet: meta only, no post
        self.slack.down = set()
        n = len(self.slack.calls)
        self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail",
                                           meta={"orders": ["B"]}))
        self.assertEqual(notify.retry_failed(), 0)
        self.assertEqual(len(self.slack.calls), n)
        self.assertEqual(notify.issue_get("held:1")["meta"]["orders"], ["A", "B"])

        with self.later(notify.FAILED_RETRY_S + 1):
            # a caller never posts a failed key, even once it is due
            self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail",
                                               meta={"orders": ["C"]}))
            self.assertEqual(len(self.slack.calls), n)
            # the main loop's retry posts the stored thread in every channel
            self.assertEqual(notify.retry_failed(), 1)
            self.assertEqual(self.slack.sent, [["Product 1 holds orders", "detail"]] * 2)
            rec = notify.issue_get("held:1")
            self.assertEqual(rec["state"], "open")
            self.assertEqual(set(rec["threads"]), {"C1", "C2"})
            self.assertEqual(rec["meta"]["orders"], ["A", "B", "C"])   # orders that waited are kept
            for f in ("retry_at", "body", "attempts"):
                self.assertNotIn(f, rec)
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
            self.assertEqual(notify.retry_failed(), 0)
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
                self.assertEqual(notify.retry_failed(), 1)
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
            "held:new": {"state": "failed", "retry_at": time.time() + 60},
            "held:9~1.0": {"state": "resolving", "rid": "1.0", "closed_at": old},
            "held:8~2.0": {"state": "resolving", "rid": "2.0", "closed_at": time.time()}}))
        notify.issues_open()
        self.assertEqual(set(json.loads(notify.ISSUES.read_text())), {"held:new", "held:8~2.0"})

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


class RetryFailed(RegistryCase):
    """#28 remainder: retry_failed() posts what Slack refused, from the main
    loops, with the stored text; nobody has to raise the key again."""

    def test_a_key_raised_once_is_still_posted(self):
        """credits-out is raised once, on the transition. Round 1 left it
        "failed" for good when that one open failed: 24 healthy ticks later
        nothing had been posted (scratchpad v/verify6.py)."""
        clock = self.clock()
        self.slack.down = {"*"}
        self.assertFalse(notify.issue_open("credits-out", "🔴 Make is out of credits", "Add credits."))
        self.slack.down = set()
        posted = 0
        for _ in range(24):                     # two hours of 5-minute ticks, nobody raises it again
            clock[0] += 300
            posted += notify.retry_failed()
        self.assertEqual(posted, 1)
        self.assertEqual(self.slack.sent, [["🔴 Make is out of credits", "Add credits."]] * 2)
        self.assertTrue(notify.issue_is_open("credits-out"))

    def test_backoff_doubles_per_failure_and_caps_at_two_hours(self):
        clock = self.clock()
        self.slack.down = {"*"}
        notify.issue_open("zid:1", "Order 1 collides", "detail")
        gaps = []
        for _ in range(7):
            rec = notify.issue_get("zid:1")
            gaps.append(round(rec["retry_at"] - clock[0]))
            clock[0] = rec["retry_at"]
            self.assertEqual(notify.retry_failed(), 0)
        self.assertEqual(gaps, [300, 600, 1200, 2400, 4800, 7200, 7200])
        self.assertEqual(notify.issue_get("zid:1")["state"], "failed")

    def test_two_services_never_post_the_same_key_twice(self):
        """While one service's retry is posting, a second service's retry and
        a lane's open both see the key in flight."""
        self.slack.down = {"*"}
        notify.issue_open("held:6", "Product 6 holds orders", "detail", meta={"orders": ["A"]})
        self.slack.down = set()
        inside, base = [], self.slack.post

        def post(text, blocks=None, thread_ts=None, dry_run=False, channel=None, reply_broadcast=False):
            if not inside:
                inside.append((notify.retry_failed(), notify.issue_open("held:6", "x", "y", meta={"orders": ["B"]})))
            return base(text, blocks, thread_ts, dry_run, channel, reply_broadcast)

        with self.later(notify.FAILED_RETRY_S + 1), mock.patch.object(notify, "post_slack", post):
            self.assertEqual(notify.retry_failed(), 1)
        self.assertEqual(inside, [(0, False)])
        self.assertEqual(self.slack.sent, [["Product 6 holds orders", "detail"]] * 2)
        self.assertEqual(notify.issue_get("held:6")["meta"]["orders"], ["A", "B"])

    def test_retry_emails_only_when_the_first_try_did_not(self):
        with mock.patch.object(notify, "email_enabled", lambda: True):
            self.slack.down = {"*"}
            notify.issue_open("held:7", "Product 7 holds orders", "detail")     # email fails too
            notify.issue_open("held:8", "Product 8 holds orders", "detail", email=False)
            self.assertEqual(self.email.call_count, 1)
            self.slack.down, self.email.return_value = set(), True
            with self.later(notify.FAILED_RETRY_S + 1):
                self.assertEqual(notify.retry_failed(), 2)
        self.assertEqual([c.args[0] for c in self.email.call_args_list],
                         ["Product 7 holds orders", "Product 7 holds orders"])

    def test_prefix_and_max_n_oldest_first(self):
        clock = self.clock()
        self.slack.down = {"*"}
        for i in range(4):
            notify.issue_open(f"held:{i}", f"Product {i}", "detail")
            clock[0] += 1
        notify.issue_open("credits-out", "Out of credits", "detail")
        self.slack.down = set()
        clock[0] += notify.FAILED_RETRY_S + 1
        self.assertEqual(notify.retry_failed(prefix="held:", max_n=3), 3)
        self.assertEqual([s[0] for s in self.slack.sent], ["Product 0", "Product 0", "Product 1", "Product 1",
                                                           "Product 2", "Product 2"])
        self.assertEqual(notify.issue_get("credits-out")["state"], "failed")
        self.assertEqual(notify.retry_failed(), 2)

    def test_old_registry_records_still_load_and_behave(self):
        """Records written by v2.13 and round 1: an open thread, an open that
        died midway (no body), a failed open with no body, a closed one."""
        old = time.time() - 3600
        notify.ISSUES.write_text(json.dumps({
            "held:open": {"state": "open", "opened_at": old, "last_update": old, "subject": "S0",
                          "threads": {"C1": "p1", "C2": "p2"}, "meta": {"orders": ["A"]}},
            "held:dead": {"state": "opening", "t": old, "subject": "S1", "meta": {"orders": ["B"]}},
            "held:r1": {"state": "failed", "subject": "S2", "emailed": False,
                        "retry_at": time.time() - 60, "meta": {"orders": ["C"]}},
            "held:r1b": {"state": "failed", "subject": "S3", "emailed": False,
                         "retry_at": time.time() + 60, "meta": {}},
            "held:shut": {"state": "closed", "closed_at": old, "subject": "S4"}}))
        self.assertEqual(set(notify.issues_open("held:")), {"held:open"})
        # a caller fills in the body round 1 did not store
        self.assertFalse(notify.issue_open("held:r1b", "S3", "detail 3", meta={"orders": ["D"]}))
        with self.later(120):
            self.assertEqual(notify.retry_failed(), 3)
        self.assertEqual(sorted(self.slack.sent), sorted([["S1", ""], ["S1", ""], ["S2", ""], ["S2", ""],
                                                          ["S3", "detail 3"], ["S3", "detail 3"]]))
        self.assertEqual(notify.issue_get("held:dead")["meta"]["orders"], ["B"])
        self.assertTrue(notify.issue_resolve("held:open", "Released"))
        self.assertEqual(self.slack.replies[-2:], [("p1", "Released", False), ("p2", "Released", False)])
        self.assertTrue(notify.issue_open("held:shut", "S4 again", "detail"))
        # only the record whose caller asked for email is emailed on its retry
        self.assertEqual([c.args[0] for c in self.email.call_args_list], ["S3", "Resolved: S0", "S4 again"])


class ResolveReply(RegistryCase):
    """N6: a resolve that no channel took is kept "resolving" and posted by
    retry_failed(); round 1 marked it closed and the reply was gone."""

    def test_reply_no_channel_took_is_retried(self):
        self.assertTrue(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        threads = notify.issue_get("held:1")["threads"]
        self.slack.down = {"*"}
        self.assertTrue(notify.issue_resolve("held:1", "✅ Released: Product 1 passes now"))
        rec = notify.issue_get("held:1")
        self.assertEqual(rec["state"], "resolving")
        self.assertFalse(notify.issue_is_open("held:1"))
        self.assertEqual(notify.issues_open("held"), {})
        self.reactions.assert_not_called()
        self.slack.down = set()
        self.assertEqual(notify.retry_failed(), 0)           # not due yet
        self.assertEqual(self.slack.replies, [])
        with self.later(notify.FAILED_RETRY_S + 1):
            self.assertEqual(notify.retry_failed(), 0)       # it answers; it does not open
        self.assertEqual(self.slack.replies, [(threads["C1"], "✅ Released: Product 1 passes now", False),
                                              (threads["C2"], "✅ Released: Product 1 passes now", False)])
        self.assertEqual(self.reactions.call_count, 2)
        rec = notify.issue_get("held:1")
        self.assertEqual(rec["state"], "closed")
        for f in ("rid", "reply", "retry_at", "t"):
            self.assertNotIn(f, rec)

    def test_one_channel_taking_the_reply_closes_it(self):
        notify.issue_open("held:2", "Product 2 holds orders", "detail")
        self.slack.down = {"C2"}
        self.assertTrue(notify.issue_resolve("held:2", "Released"))
        self.assertEqual(notify.issue_get("held:2")["state"], "closed")
        self.assertEqual(self.reactions.call_count, 1)

    def test_problem_back_before_the_reply_got_through(self):
        """The pending reply is set aside and still answers the old thread;
        the new occurrence opens its own thread at once."""
        notify.issue_open("held:3", "Product 3 holds orders", "detail", meta={"orders": ["A"]})
        old = notify.issue_get("held:3")["threads"]
        self.slack.down = {"*"}
        notify.issue_resolve("held:3", "Released")
        self.slack.down = set()
        self.assertTrue(notify.issue_open("held:3", "Product 3 holds orders again", "detail",
                                          meta={"orders": ["B"]}))
        rec = notify.issue_get("held:3")
        self.assertEqual((rec["state"], rec["meta"]["orders"]), ("open", ["B"]))
        self.assertNotEqual(rec["threads"], old)
        with self.later(notify.FAILED_RETRY_S + 1):
            notify.retry_failed()
        self.assertEqual(self.slack.replies, [(old["C1"], "Released", False), (old["C2"], "Released", False)])
        self.assertTrue(notify.issue_is_open("held:3"))

    def test_update_no_channel_took_does_not_spend_the_throttle(self):
        notify.issue_open("held:4", "Product 4 holds orders", "detail")
        with self.later(6 * 3600 + 1):
            self.slack.down = {"*"}
            self.assertFalse(notify.issue_update("held:4", "Order 2 is also waiting", every_s=6 * 3600))
            self.slack.down = set()
            self.assertTrue(notify.issue_update("held:4", "Order 3 is also waiting", every_s=6 * 3600))
            self.assertFalse(notify.issue_update("held:4", "Order 4 is also waiting", every_s=6 * 3600))
        self.assertEqual([r[1] for r in self.slack.replies], ["Order 3 is also waiting"] * 2)


class StaleFailed(RegistryCase):
    """N8: a failed open nobody raised again lingered 30 days, and a reopen
    weeks later listed its old orders (scratchpad repro_stale_failed.py)."""

    def test_a_past_episode_starts_fresh(self):
        clock = self.clock()
        self.slack.down = {"*"}
        notify.issue_open("held:P1", "Order #RA waits for Rose Oil", "steps", meta={"orders": ["A"], "refs": ["RA"]})
        self.slack.down = set()
        clock[0] += 14 * 86400
        self.assertFalse(notify.issue_open("held:P1", "Order #RB waits for Rose Oil", "steps",
                                           meta={"orders": ["B"], "refs": ["RB"]}))
        self.assertEqual(self.slack.sent, [])
        rec = notify.issue_get("held:P1")
        self.assertEqual((rec["meta"]["orders"], rec["meta"]["refs"], rec["subject"]),
                         (["B"], ["RB"], "Order #RB waits for Rose Oil"))
        self.assertEqual(notify.retry_failed(), 1)           # due at once
        self.assertEqual(self.slack.sent, [["Order #RB waits for Rose Oil", "steps"]] * 2)

    def test_a_recent_failure_keeps_its_orders(self):
        clock = self.clock()
        self.slack.down = {"*"}
        notify.issue_open("held:P2", "S", "steps", meta={"orders": ["A"]})
        clock[0] += notify.FAILED_RETRY_S + notify.FAILED_STALE_S - 60
        notify.issue_open("held:P2", "S", "steps", meta={"orders": ["B"]})
        self.assertEqual(notify.issue_get("held:P2")["meta"]["orders"], ["A", "B"])

    def test_a_key_raised_through_a_long_outage_keeps_its_orders(self):
        """Retries are not happening (Slack is down, the breaker holds them),
        but orders keep joining: that is one episode, not a past one."""
        clock = self.clock()
        self.slack.down = {"*"}
        notify.issue_open("held:P3", "S", "steps", meta={"orders": ["0"]})
        for i in range(1, 7):                    # three hours, an order every 30 minutes
            clock[0] += 1800
            notify.issue_update("held:P3", "news", meta={"orders": [str(i)]})
        self.assertEqual(notify.issue_get("held:P3")["meta"]["orders"], [str(i) for i in range(7)])

    def test_update_on_a_past_episode_drops_the_old_orders(self):
        clock = self.clock()
        self.slack.down = {"*"}
        notify.issue_open("held:P4", "S", "steps", meta={"orders": ["A"]})
        clock[0] += 3 * 86400
        notify.issue_update("held:P4", "news", meta={"orders": ["B"]})
        self.assertEqual(notify.issue_get("held:P4")["meta"]["orders"], ["B"])

    def test_close_failed_closes_silently(self):
        self.slack.down = {"*"}
        notify.issue_open("held:P5", "S", "steps", meta={"orders": ["A"]})
        self.slack.down = set()
        n = len(self.slack.calls)
        self.assertTrue(notify.close_failed("held:P5", "every order it held is in HubSpot"))
        rec = notify.issue_get("held:P5")
        self.assertEqual((rec["state"], rec["resolution"]), ("closed", "every order it held is in HubSpot"))
        self.assertNotIn("retry_at", rec)
        with self.later(notify.FAILED_RETRY_S + 1):
            self.assertEqual(notify.retry_failed(), 0)
        self.assertEqual(len(self.slack.calls), n)
        self.reactions.assert_not_called()
        self.assertFalse(notify.close_failed("held:P5", "again"))
        self.assertFalse(notify.close_failed("held:none", "x"))
        self.assertTrue(notify.issue_open("held:P5", "S", "steps", meta={"orders": ["B"]}))
        self.assertEqual(notify.issue_get("held:P5")["meta"]["orders"], ["B"])
        self.assertFalse(notify.close_failed("held:P5", "open threads are resolved, not closed"))


class RealPostCase(unittest.TestCase):
    """The real post_slack against a fake http_request."""

    DOWN = (0, {}, "NETWORK_ERROR: timed out")
    OK = (200, {}, json.dumps({"ok": True, "ts": "1.0001"}))

    def setUp(self):
        _chdir_tmp(self)
        _reset_breaker(self)
        env = mock.patch.dict(os.environ, {"SLACK_BOT_TOKEN": "xoxb-test", "SLACK_CHANNEL_IDS": "C1,C2"})
        env.start()
        self.addCleanup(env.stop)
        for k in ("SMTP_HOST", "SMTP_FROM", "SMTP_TO"):
            os.environ.pop(k, None)
        self.reply = self.DOWN
        self.calls, self.sleeps = [], []

        def http(method, url, headers=None, body=None, timeout=90):
            self.calls.append((url, timeout))
            return self.reply.pop(0) if isinstance(self.reply, list) else self.reply

        for p in (mock.patch.object(notify, "http_request", side_effect=http),
                  mock.patch("time.sleep", side_effect=self.sleeps.append),
                  mock.patch.object(notify, "send_email", return_value=False)):
            p.start()
            self.addCleanup(p.stop)


class FastFailPosts(RealPostCase):
    """Inside notify.fast_posts() (the lane entry points) issue posts use a
    short timeout and at most one retry; outside it they keep 3 tries at
    20 s, so a throttle in a main loop no longer drops a reply (N6)."""

    def test_issue_open_fails_fast_inside_fast_posts(self):
        with notify.fast_posts():
            self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        # the first channel's post, tried twice at a short timeout with a short
        # pause and none after the last try; the breaker then spares the second
        self.assertEqual(self.calls, [(notify.SLACK_API, notify.ISSUE_POST_TIMEOUT_S)] * 2)
        self.assertTrue(notify.ISSUE_POST_TIMEOUT_S <= 10)
        self.assertEqual(len(self.sleeps), 1)
        self.assertTrue(all(s <= notify.ISSUE_RETRY_PAUSE_S for s in self.sleeps))

    def test_long_retry_after_is_not_waited_for(self):
        self.reply = (429, {"Retry-After": "30"}, "")
        with notify.fast_posts():
            self.assertFalse(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        self.assertEqual(len(self.calls), 2)           # one try per channel (a 429 trips no breaker)
        self.assertEqual(self.sleeps, [])

    def test_update_and_resolve_fail_fast_inside_fast_posts(self):
        self.reply = self.OK
        self.assertTrue(notify.issue_open("held:1", "Product 1 holds orders", "detail"))
        self.reply, self.calls[:], self.sleeps[:] = (429, {"Retry-After": "1"}, ""), [], []
        with notify.fast_posts():
            self.assertFalse(notify.issue_update("held:1", "news"))
            self.assertTrue(notify.issue_resolve("held:1", "Released"))
        # update: 2 channels x 2 tries; resolve: 2 x 2 tries, no reaction on a reply that failed
        self.assertEqual(self.calls, [(notify.SLACK_API, notify.ISSUE_POST_TIMEOUT_S)] * 8)
        self.assertEqual(notify.issue_get("held:1")["state"], "resolving")

    def test_resolve_outside_fast_posts_waits_out_a_429(self):
        """N6 (scratchpad v/verify7.py): one 429 with Retry-After 5 on the
        resolve post. Round 1 failed fast for every caller and the reply was
        never posted."""
        self.reply = self.OK
        self.assertTrue(notify.issue_open("held:1", "Multi Styler holds orders", "detail"))
        self.reply = [(429, {"Retry-After": "5"}, ""), self.OK, (200, {}, '{"ok": true}'),
                      self.OK, (200, {}, '{"ok": true}')]
        self.calls[:], self.sleeps[:] = [], []
        self.assertTrue(notify.issue_resolve("held:1", "Released: Multi Styler passes now"))
        self.assertIn(5.0, self.sleeps)
        posts = [t for u, t in self.calls if u == notify.SLACK_API]
        self.assertEqual(posts, [20, 20, 20])          # the patient defaults: 3 posts, 20 s each
        self.assertEqual(notify.issue_get("held:1")["state"], "closed")

    def test_other_callers_keep_patient_defaults(self):
        with notify.fast_posts():
            notify.issue_open("held:1", "Product 1 holds orders", "detail")
        self.calls[:] = []
        notify.send_alert("Something broke", "detail")
        # 3 tries per channel at 20 s: the fast mode did not leak out, and the
        # issue breaker the open tripped does not touch plain alerts
        self.assertEqual(self.calls, [(notify.SLACK_API, 20)] * 6)

    def test_fast_mode_is_restored_after_an_error_and_when_nested(self):
        with self.assertRaises(RuntimeError):
            with notify.fast_posts():
                with notify.fast_posts():
                    pass
                self.assertTrue(notify._posting.fast)   # the inner exit restores, not clears
                raise RuntimeError
        notify.post_slack("plain")
        self.assertEqual(self.calls[0], (notify.SLACK_API, 20))

    def test_retry_failed_posts_fast(self):
        with notify.fast_posts():
            notify.issue_open("held:1", "Product 1 holds orders", "detail")
        self.reply, self.calls[:] = (503, {}, ""), []
        notify._breaker["until"] = 0.0
        with mock.patch("time.time", return_value=time.time() + notify.FAILED_RETRY_S + 1):
            self.assertEqual(notify.retry_failed(), 0)
        self.assertEqual(self.calls, [(notify.SLACK_API, notify.ISSUE_POST_TIMEOUT_S)] * 2)


class SlackBreaker(RealPostCase):
    """N11: during a Slack black hole every new key paid its own stall in
    the lanes (scratchpad skeptic_breaker/sim.py: about 34 s per key and
    attempt). A network-level failure now pauses issue posts process-wide."""

    def test_one_stall_then_no_http_for_other_keys(self):
        with notify.fast_posts():
            for i in range(5):
                self.assertFalse(notify.issue_open(f"held:{i}", f"Product {i} holds orders", "detail"))
        self.assertEqual(len(self.calls), 2)          # only the first key's first channel
        for i in range(5):
            rec = notify.issue_get(f"held:{i}")
            self.assertEqual((rec["state"], rec["body"]), ("failed", "detail"))
        self.calls[:] = []
        with mock.patch("time.time", return_value=time.time() + notify.BREAKER_S + 1), notify.fast_posts():
            notify.issue_open("held:9", "Product 9 holds orders", "detail")
        self.assertEqual(len(self.calls), 2)          # the pause is over: Slack is tried again

    def test_5xx_trips_it_429_and_ok_false_do_not(self):
        for reply, trips in (((503, {}, ""), True), ((429, {"Retry-After": "30"}, ""), False),
                             ((200, {}, '{"ok": false, "error": "not_in_channel"}'), False)):
            notify._breaker["until"] = 0.0
            self.reply, self.calls[:] = reply, []
            with notify.fast_posts():
                notify.issue_open(f"k:{reply[0]}", "S", "detail")
            self.assertEqual(notify._breaker_open(), trips, reply)
            # a 5xx: the first channel twice, then the breaker spares the second;
            # otherwise one try per channel
            self.assertEqual(len(self.calls), 2, reply)

    def test_nothing_is_retried_or_updated_while_it_is_open(self):
        self.reply = self.OK
        notify.issue_open("held:1", "Product 1 holds orders", "detail")
        self.reply = self.DOWN
        notify.issue_open("held:2", "Product 2 holds orders", "detail")       # trips it
        self.calls[:] = []
        with mock.patch("time.time", return_value=time.time() + notify.FAILED_RETRY_S + 1):
            notify._breaker["until"] = time.time() + 60
            self.assertEqual(notify.retry_failed(), 0)
            self.assertFalse(notify.issue_update("held:1", "news", every_s=3600))
            self.assertTrue(notify.issue_resolve("held:1", "Released"))       # kept for later
        self.assertEqual(self.calls, [])
        self.assertEqual(notify.issue_get("held:1")["state"], "resolving")

    def test_update_while_open_does_not_spend_the_throttle(self):
        self.reply = self.OK
        notify.issue_open("held:1", "Product 1 holds orders", "detail")
        with mock.patch("time.time", return_value=time.time() + 3601):
            notify._breaker["until"] = time.time() + 60
            self.assertFalse(notify.issue_update("held:1", "news", every_s=3600))
            notify._breaker["until"] = 0.0
            self.calls[:] = []
            self.assertTrue(notify.issue_update("held:1", "news", every_s=3600))
        self.assertEqual(len(self.calls), 2)

    def test_a_clock_jump_does_not_hold_it_shut(self):
        notify._breaker["until"] = time.time() + 10 * 86400
        self.assertFalse(notify._breaker_open())
        notify._breaker["until"] = time.time() + notify.BREAKER_S - 1
        self.assertTrue(notify._breaker_open())


if __name__ == "__main__":
    unittest.main(verbosity=2)


class RoundThree(RealPostCase):
    """[v2.14.1] review round 3 (alerts lens)."""

    def test_patient_issue_post_stays_under_the_opening_ttl(self):
        # Retry-After 90 three times: with_retries honoured all of it and slept
        # after the last try (270 s), so another service's retry_failed() took
        # the open still in flight and posted a second thread
        self.reply = (429, {"Retry-After": "90"}, "")
        os.environ["SLACK_CHANNEL_IDS"] = "C1"
        self.assertFalse(notify.issue_open("status-missing:1", "Order #1 is not in HubSpot", ""))
        self.assertEqual(len(self.calls), notify.ISSUE_PATIENT_TRIES)
        self.assertEqual(self.sleeps, [notify.ISSUE_PATIENT_MAX_WAIT_S] * (notify.ISSUE_PATIENT_TRIES - 1))
        worst = notify.ISSUE_PATIENT_TRIES * 20 + sum(self.sleeps)
        self.assertLess(worst, notify.OPENING_TTL_S)

    def test_plain_alerts_keep_with_retries(self):
        self.reply = (429, {"Retry-After": "90"}, "")
        os.environ["SLACK_CHANNEL_IDS"] = "C1"
        notify.send_alert("Something broke", "")
        self.assertIn(90.0, self.sleeps)

    def test_relay_and_credit_skip_refused_hold_opens_but_retry_pending_replies(self):
        self.reply = self.DOWN
        notify.issue_open("held:P1", "P1 holds orders", "detail")          # refused: stored failed
        notify.issue_open("credits-out", "Make credits ran out", "detail")
        notify._breaker["until"] = 0.0
        self.reply = self.OK
        notify.issue_open("held-escalation:2026-10-13", "120 orders on hold", "detail")
        self.assertEqual(notify.issue_get("held-escalation:2026-10-13")["state"], "open")
        self.reply = self.DOWN
        notify.issue_resolve("held-escalation:2026-10-13", "Resolved")      # reply refused: resolving
        self.assertEqual(notify.issue_get("held-escalation:2026-10-13")["state"], "resolving")
        notify._breaker["until"] = 0.0
        with notify._issues() as d:
            for rec in d.values():
                rec["retry_at"] = 0
                rec.pop("t", None)
        self.reply = self.OK
        notify.retry_failed(skip=notify.HOLD_KEYS)
        self.assertEqual(notify.issue_get("held:P1")["state"], "failed")                   # the settle's
        self.assertEqual(notify.issue_get("credits-out")["state"], "open")
        self.assertEqual(notify.issue_get("held-escalation:2026-10-13")["state"], "closed")  # reply posted


class ResolveExpectOrders(RegistryCase):
    """[v2.14.1] review round 3: an order a lane joined while the settle was
    checking the thread was closed into it with "every order is in HubSpot"."""

    def test_refused_when_an_order_joined_since_the_check(self):
        notify.issue_open("held:P5", "P5 holds orders", "detail", meta={"orders": ["A"]})
        notify.issue_update("held:P5", "", every_s=float("inf"), meta={"orders": ["Z"]})   # the lane
        self.assertFalse(notify.issue_resolve("held:P5", "Released", expect_orders=["A"]))
        self.assertEqual(notify.issue_get("held:P5")["state"], "open")
        self.assertTrue(notify.issue_resolve("held:P5", "Released", expect_orders=["A", "Z"]))
