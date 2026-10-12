#!/usr/bin/env python3
"""v2.14 Make scenario on/off watch: "Salla | Product Updated" was switched
off by Make for 23 days from 30 Aug 2026 and nobody knew."""
import json
import logging
import os
import unittest
from unittest import mock

import credit_watch as cw
from test_make_queue_alerts import cfg, fresh_config_warnings
from test_realtime_ext import _chdir_tmp

WATCH = [{"id": 6281245, "label": "Salla | Product Updated"}]
KEY = "make-off:6281245"
ORG = {"remaining": 90000.0, "consumed": 10000.0, "extra": 0.0, "plan": 100000,
       "last_reset": "2026-10-01", "next_reset": "2026-11-01",
       "auto_purchase_on": False, "auto_purchase_running": False}


class ScenarioWatch(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        fresh_config_warnings(self)
        self.opened, self.resolved = [], []
        p = mock.patch.multiple(
            cw.notify,
            issue_is_open=mock.Mock(return_value=False),
            issue_open=mock.Mock(side_effect=lambda k, s, b, **kw: self.opened.append((k, s, b)) or True),
            issue_resolve=mock.Mock(side_effect=lambda k, t, **kw: self.resolved.append((k, t)) or True))
        p.start()
        self.addCleanup(p.stop)

    def get(self, active):
        return lambda path: {"scenario": {"id": 6281245, "name": "Salla | Product Updated",
                                          "isActive": active}}

    def test_off_opens_a_red_keyed_thread(self):
        w = cw.CreditWatch(cfg(make_watch_scenarios=WATCH))
        w.check_scenarios_active(get=self.get(False))
        self.assertEqual(len(self.opened), 1)
        key, subject, body = self.opened[0]
        self.assertEqual(key, "make-off:6281245")
        self.assertIn("switched itself off", subject)
        self.assertIn("History", body)
        self.assertNotIn("—", subject + body)   # no em dash in client text

    def test_on_resolves(self):
        w = cw.CreditWatch(cfg(make_watch_scenarios=WATCH))
        w.check_scenarios_active(get=self.get(True))
        self.assertEqual(self.opened, [])
        self.assertEqual(self.resolved[0][0], "make-off:6281245")

    def test_nothing_watched_by_default(self):
        w = cw.CreditWatch(cfg())
        calls = []
        w.check_scenarios_active(get=lambda p: calls.append(p) or {})
        self.assertEqual(calls, [])

    def test_read_failure_is_quiet_not_an_alert(self):
        w = cw.CreditWatch(cfg(make_watch_scenarios=[6281245]))

        def boom(path):
            raise RuntimeError("Make 502")
        w.check_scenarios_active(get=boom)
        self.assertEqual(self.opened, [])
        self.assertEqual(self.resolved, [])

    def test_unknown_state_neither_opens_nor_resolves(self):
        w = cw.CreditWatch(cfg(make_watch_scenarios=[6281245]))
        w.check_scenarios_active(get=lambda p: {"scenario": {}})
        self.assertEqual((self.opened, self.resolved), ([], []))

    def test_open_thread_gets_the_short_line_throttled_to_a_day(self):
        # what _alert hands notify once the thread is open: the one-line
        # reminder and the 24 h throttle, never the full alert with every_s=0
        cw.notify.issue_is_open.return_value = True
        with mock.patch.object(cw.notify, "issue_update", return_value=False) as up:
            cw.CreditWatch(cfg(make_watch_scenarios=WATCH)).check_scenarios_active(get=self.get(False))
        self.assertEqual(self.opened, [])
        (key, text), kw = up.call_args
        self.assertEqual(key, KEY)
        self.assertEqual(kw["every_s"], cw.SCENARIO_OFF_REMIND_S)
        self.assertGreaterEqual(cw.SCENARIO_OFF_REMIND_S, 24 * 3600)
        self.assertNotIn("\n", text)
        self.assertNotIn("History", text)
        self.assertFalse(kw.get("broadcast"))


class KeyedAlertPassThrough(unittest.TestCase):
    """_alert without every_s/update keeps the v2.13 behaviour the other
    keyed callers (retry queue, credit thresholds) rely on."""

    def setUp(self):
        _chdir_tmp(self)
        fresh_config_warnings(self)
        p = mock.patch.multiple(cw.notify, issue_is_open=mock.Mock(return_value=True),
                                issue_update=mock.Mock(return_value=True))
        p.start()
        self.addCleanup(p.stop)

    def test_default_is_the_full_alert_unthrottled(self):
        cw.CreditWatch(cfg())._alert("🟠 subject", "body", key=cw.DLQ_KEY)
        cw.notify.issue_update.assert_called_once_with(
            cw.DLQ_KEY, "*🟠 subject*\nbody", every_s=0, broadcast=False)


class ScenarioOffAcrossTicks(unittest.TestCase):
    """The real notify registry in a temp dir, only Slack and email mocked:
    the scenario stays off for many ticks and the thread must not fill up.
    Review findings #8, #17, #25: every 5-minute tick reposted the whole red
    alert (288 replies a day in the channel shared with the client)."""

    def setUp(self):
        _chdir_tmp(self)
        fresh_config_warnings(self)
        env = mock.patch.dict(os.environ, {"SLACK_BOT_TOKEN": "xoxb-test",
                                           "SLACK_CHANNEL_IDS": "C1"})
        env.start()
        self.addCleanup(env.stop)
        self.posts = []
        p = mock.patch.multiple(
            cw.notify,
            post_slack=mock.Mock(side_effect=self._post),
            send_email=mock.Mock(return_value=False),
            add_reaction=mock.Mock(return_value=True),
            http_request=mock.Mock(side_effect=AssertionError("no network in tests")))
        p.start()
        self.addCleanup(p.stop)
        self.active = False
        g = mock.patch.object(cw, "_get", side_effect=lambda path: {
            "scenario": {"id": 6281245, "name": "Salla | Product Updated",
                         "isActive": self.active}})
        g.start()
        self.addCleanup(g.stop)
        self.w = cw.CreditWatch(cfg(make_watch_scenarios=WATCH, make_org_id="1"))
        for name, value in (("read_org", dict(ORG)), ("read_consumptions", {}),
                            ("read_usage", None), ("check_relay_dlq", None)):
            m = mock.patch.object(self.w, name, return_value=value)
            m.start()
            self.addCleanup(m.stop)

    def _post(self, text, *a, **kw):
        self.posts.append({"text": text, "thread_ts": kw.get("thread_ts"),
                           "channel": kw.get("channel"),
                           "broadcast": kw.get("reply_broadcast", False)})
        return f"{len(self.posts)}.0001"

    def _rewind(self, seconds):
        """Age the open thread as if `seconds` had passed since its last post."""
        path = cw.notify.ISSUES
        data = json.loads(path.read_text())
        for field in ("opened_at", "last_update"):
            data[KEY][field] = float(data[KEY][field]) - seconds
        path.write_text(json.dumps(data))

    def test_two_ticks_still_off_post_once(self):
        self.w.tick()
        self.w.tick()
        self.assertEqual(len(self.posts), 2)
        parent, detail = self.posts
        self.assertIsNone(parent["thread_ts"])
        self.assertIn("switched itself off", parent["text"])
        self.assertEqual(detail["thread_ts"], "1.0001")
        self.assertIn("History", detail["text"])
        self.assertTrue(cw.notify_open(KEY))

    def test_a_day_of_ticks_still_off_posts_once(self):
        with self.assertLogs("backfill", level=logging.ERROR) as logs:
            for _ in range(288):            # 24 h at the 300 s interval
                self.w.tick()
        self.assertEqual([p["thread_ts"] for p in self.posts], [None, "1.0001"])
        self.assertEqual(sum("ALERT:" in line for line in logs.output), 1)

    def test_one_short_reminder_a_day_while_still_off(self):
        self.w.tick()
        self._rewind(cw.SCENARIO_OFF_REMIND_S + 60)
        for _ in range(20):
            self.w.tick()
        self.assertEqual(len(self.posts), 3)
        reminder = self.posts[2]
        self.assertEqual(reminder["thread_ts"], "1.0001")    # in the thread
        self.assertFalse(reminder["broadcast"])
        self.assertNotIn("\n", reminder["text"])             # one line
        self.assertLess(len(reminder["text"]), 300)
        self.assertIn('"Salla | Product Updated" has been switched off for at least 24 hours',
                      reminder["text"])
        self.assertIn("still not reaching HubSpot", reminder["text"])
        self.assertNotIn("—", reminder["text"])              # no em dash
        self.assertNotIn("History", reminder["text"])        # not the full alert

    def test_back_on_resolves_then_off_again_opens_a_new_thread(self):
        self.w.tick()
        self.active = True
        self.w.tick()
        self.w.tick()
        self.assertEqual(len(self.posts), 3)
        self.assertEqual(self.posts[2]["thread_ts"], "1.0001")
        self.assertIn("switched on again", self.posts[2]["text"])
        self.assertFalse(cw.notify_open(KEY))
        self.active = False
        self.w.tick()
        self.w.tick()
        self.assertEqual(len(self.posts), 5)
        self.assertIsNone(self.posts[3]["thread_ts"])        # a new parent
        self.assertEqual(self.posts[4]["thread_ts"], "4.0001")


if __name__ == "__main__":
    unittest.main()
