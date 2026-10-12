#!/usr/bin/env python3
"""v2.14 Make scenario on/off watch: "Salla | Product Updated" was switched
off by Make for 23 days from 30 Aug 2026 and nobody knew."""
import unittest
from unittest import mock

import credit_watch as cw
from test_make_queue_alerts import cfg, fresh_config_warnings
from test_realtime_ext import _chdir_tmp

WATCH = [{"id": 6281245, "label": "Salla | Product Updated"}]


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


if __name__ == "__main__":
    unittest.main()
