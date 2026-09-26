#!/usr/bin/env python3
"""Make retry-queue watch (v2.12): credit_watch.check_relay_dlq and the
reconcile certificate's "make queue" section. Offline, against a fake _get.

Under test: the six-scenario default and the Config override, the age gate
that leaves young items to Make's own retry, paging at pg[limit]=100 up to
500 items, WARNING logs that name the scenario and never the token, the
per-scenario alert lines with their replay notes, the unchanged state keys
plus relay_dlq_by_scenario, the 12h cooldown, and the certificate section
(including "not recorded" never blocking repairs).

Run: python3 -m unittest test_make_queue_alerts -v
"""
import json
import os
import re
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import credit_watch as cw
import reconcile
from reconcile import Finding, insane, render
from test_realtime_ext import _cfg, _chdir_tmp

NOW = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)
TOKEN = "tok-DO-NOT-LEAK-123"


def item(age_min, resolved=False, i=0):
    ts = (NOW - timedelta(minutes=age_min)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {"id": f"dlq{i}", "created": ts, "resolved": resolved,
            "reason": "x"}


class FakeMake:
    """Programmable GET /dlqs, keyed by scenario id. A scenario mapped to an
    Exception raises it; otherwise its item list is paged like Make does."""

    def __init__(self, queues):
        self.queues = queues
        self.paths = []

    def __call__(self, path):
        self.paths.append(path)
        sid = re.search(r"scenarioId=(\d+)", path).group(1)
        limit = int(re.search(r"pg\[limit\]=(\d+)", path).group(1))
        offset = int(re.search(r"pg\[offset\]=(\d+)", path).group(1))
        q = self.queues.get(sid, [])
        if isinstance(q, Exception):
            raise q
        return {"dlqs": q[offset:offset + limit], "pg": {"limit": limit}}


def cfg(**kw):
    base = dict(make_team_id="777", make_intake_scenario_id="6568689",
                make_backfill_scenario_id="1111111", alerts_enabled=True)
    base.update(kw)
    return _cfg(**base)


class WatchList(unittest.TestCase):
    def test_default_is_both_relays_plus_four(self):
        w = cw.dlq_watch_list(cfg())
        self.assertEqual([x["id"] for x in w],
                         ["6568689", "1111111", "6892982", "6893541",
                          "5563154", "5780791"])
        self.assertEqual([x["label"] for x in w],
                         ["live intake", "backfill relay", "customer capture",
                          "status capture", "customer updated",
                          "abandoned cart"])
        notes = {x["label"]: x["replay_note"] for x in w}
        self.assertIn("engine", notes["live intake"])
        self.assertIn("queue sheet", notes["customer capture"])
        self.assertIn("Check the contact first", notes["customer updated"])
        self.assertIn("Check the contact first", notes["abandoned cart"])

    def test_unset_relay_ids_are_skipped_and_dupes_watched_once(self):
        w = cw.dlq_watch_list(cfg(make_intake_scenario_id="",
                                  make_backfill_scenario_id="6892982"))
        ids = [x["id"] for x in w]
        self.assertEqual(ids, ["6892982", "6893541", "5563154", "5780791"])
        self.assertEqual(w[0]["label"], "backfill relay")   # first wins

    def test_config_list_overrides(self):
        w = cw.dlq_watch_list(cfg(make_dlq_watch=[
            {"id": 42, "label": "orders", "replay_note": "Safe."},
            {"id": "43"}]))
        self.assertEqual(w[0], {"id": "42", "label": "orders",
                                "replay_note": "Safe."})
        self.assertEqual(w[1]["label"], "scenario 43")
        self.assertEqual(w[1]["replay_note"], cw.REPLAY_UNKNOWN)

    def test_label_to_id_mapping_accepted(self):
        w = cw.dlq_watch_list(cfg(make_dlq_watch={"orders": "42"}))
        self.assertEqual([(x["id"], x["label"]) for x in w], [("42", "orders")])

    def test_config_fields_declared(self):
        c = cw.Config()
        self.assertIsNone(c.make_dlq_watch)
        self.assertEqual(c.dlq_min_age_minutes, 45)


class Scan(unittest.TestCase):
    def test_age_gate_and_resolved(self):
        fake = FakeMake({"6568689": [item(10), item(44), item(46),
                                     item(600), item(900, resolved=True)]})
        rows = cw.scan_dlq(cfg(), get=fake, now=NOW)
        live = rows[0]
        self.assertEqual(live["count"], 2)            # 46 min + 10 h
        self.assertEqual(live["oldest_age_min"], 600)
        self.assertEqual(rows[1]["count"], 0)

    def test_min_age_is_configurable(self):
        fake = FakeMake({"6568689": [item(10), item(46)]})
        rows = cw.scan_dlq(cfg(dlq_min_age_minutes=5), get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 2)

    def test_unparseable_timestamp_counts(self):
        fake = FakeMake({"6568689": [{"created": "", "resolved": False}]})
        rows = cw.scan_dlq(cfg(), get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 1)
        self.assertIsNone(rows[0]["oldest_age_min"])

    def test_pages_until_short_page(self):
        q = [item(60, i=i) for i in range(230)]
        fake = FakeMake({"6568689": q})
        rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689"}]),
                           get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 230)
        self.assertFalse(rows[0]["capped"])
        self.assertEqual(len(fake.paths), 3)
        self.assertTrue(all("pg[limit]=100" in p for p in fake.paths))
        self.assertEqual([re.search(r"pg\[offset\]=(\d+)", p).group(1)
                          for p in fake.paths], ["0", "100", "200"])

    def test_exact_full_page_asks_once_more(self):
        fake = FakeMake({"6568689": [item(60, i=i) for i in range(100)]})
        rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689"}]),
                           get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 100)
        self.assertEqual(len(fake.paths), 2)

    def test_caps_at_500(self):
        fake = FakeMake({"6568689": [item(60, i=i) for i in range(900)]})
        rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689"}]),
                           get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 500)
        self.assertTrue(rows[0]["capped"])
        self.assertEqual(len(fake.paths), 5)
        self.assertIn("500+ stuck", cw.dlq_line(rows[0]))

    def test_failed_read_is_none_and_warns_without_token(self):
        fake = FakeMake({"5563154": RuntimeError(f"HTTP 401 for {TOKEN}")})
        with mock.patch.dict(os.environ, {"MAKE_API_TOKEN": TOKEN}):
            with self.assertLogs("backfill", level="WARNING") as cm:
                rows = cw.scan_dlq(cfg(), get=fake, now=NOW)
        row = next(r for r in rows if r["id"] == "5563154")
        self.assertIsNone(row["count"])
        out = "\n".join(cm.output)
        self.assertIn("customer updated", out)
        self.assertNotIn(TOKEN, out)
        self.assertNotIn(TOKEN, row["error"])
        self.assertTrue(all(r["count"] == 0 for r in rows if r is not row))

    def test_token_never_in_url(self):
        fake = FakeMake({})
        with mock.patch.dict(os.environ, {"MAKE_API_TOKEN": TOKEN}):
            cw.scan_dlq(cfg(), get=fake, now=NOW)
        self.assertEqual(len(fake.paths), 6)
        self.assertFalse(any(TOKEN in p for p in fake.paths))

    def test_default_get_is_module_get(self):
        fake = FakeMake({})
        with mock.patch.object(cw, "_get", fake):
            cw.scan_dlq(cfg(), now=NOW)
        self.assertEqual(len(fake.paths), 6)


class Alert(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        self.sent = []
        p = mock.patch.object(cw.notify, "send_alert",
                              side_effect=lambda s, b, **k: self.sent.append((s, b)))
        p.start()
        self.addCleanup(p.stop)

    def _watch(self, **kw):
        return cw.CreditWatch(cfg(**kw))

    def test_one_line_per_stuck_scenario_with_note_and_link(self):
        w = self._watch()
        fake = FakeMake({"6568689": [item(120), item(3000)],
                         "5563154": [item(60)],
                         "6892982": [item(5)]})          # young: Make's to retry
        w.check_relay_dlq(get=fake, now=NOW)
        self.assertEqual(len(self.sent), 1)
        subject, body = self.sent[0]
        self.assertIn("3 failed Make runs", subject)
        lines = [l for l in body.splitlines() if l.startswith("• ")]
        self.assertEqual(len(lines), 2)
        self.assertIn("live intake: 2 stuck, oldest 2 days old", lines[0])
        self.assertIn(cw.REPLAY_ORDER, lines[0])
        self.assertIn("https://eu1.make.com/777/scenarios/6568689", lines[0])
        self.assertIn("customer updated: 1 stuck, oldest 60 min old", lines[1])
        self.assertIn("Check the contact first", lines[1])
        self.assertNotIn("customer capture", body)
        for text in (subject, body):
            self.assertNotIn("—", text)     # no em dash in client text
            self.assertNotIn("–", text)

    def test_singular_subject(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"5780791": [item(90)]}), now=NOW)
        self.assertIn("1 failed Make run is stuck", self.sent[0][0])

    def test_state_keys(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"6568689": [item(120)],
                                        "5563154": RuntimeError("503")}),
                          now=NOW)
        self.assertEqual(w.state["relay_dlq"], 1)
        self.assertIn("dlq_alerted_at", w.state)
        by = w.state["relay_dlq_by_scenario"]
        self.assertEqual(set(by), {"6568689", "1111111", "6892982", "6893541",
                                   "5563154", "5780791"})
        self.assertEqual(by["6568689"]["count"], 1)
        self.assertEqual(by["6568689"]["label"], "live intake")
        self.assertIsNone(by["5563154"]["count"])       # never a fake 0
        self.assertEqual(by["6892982"]["count"], 0)
        self.assertIn("Not checked this time: customer updated.",
                      self.sent[0][1])
        w._save()
        saved = json.loads(Path("mirror/credit_state.json").read_text())
        self.assertEqual(saved["relay_dlq_by_scenario"]["6568689"]["count"], 1)

    def test_cooldown_12h(self):
        w = self._watch()
        fake = FakeMake({"6568689": [item(120)]})
        w.check_relay_dlq(get=fake, now=NOW)
        w.check_relay_dlq(get=fake, now=NOW)
        self.assertEqual(len(self.sent), 1)
        w.state["dlq_alerted_at"] = w.state["dlq_alerted_at"] - 12 * 3600 - 1
        w.check_relay_dlq(get=fake, now=NOW)
        self.assertEqual(len(self.sent), 2)

    def test_clear_rearms_but_failed_read_does_not(self):
        w = self._watch()
        w.state["dlq_alerted_at"] = 1.0
        w.check_relay_dlq(get=FakeMake({"6568689": RuntimeError("x")}),
                          now=NOW)
        self.assertIn("dlq_alerted_at", w.state)
        self.assertEqual(w.state["relay_dlq"], 0)
        w.check_relay_dlq(get=FakeMake({}), now=NOW)
        self.assertNotIn("dlq_alerted_at", w.state)
        self.assertEqual(self.sent, [])

    def test_young_items_never_alert(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"6568689": [item(30), item(44)]}),
                          now=NOW)
        self.assertEqual(self.sent, [])
        self.assertEqual(w.state["relay_dlq"], 0)

    def test_tick_logs_watch_failure_at_warning(self):
        w = self._watch()
        with mock.patch.object(w, "check_relay_dlq",
                               side_effect=RuntimeError("boom")), \
                mock.patch.object(w, "read_org", return_value={
                    "remaining": 90000.0, "consumed": 0.0, "extra": 0.0,
                    "plan": 100000, "last_reset": "", "next_reset": "",
                    "auto_purchase_on": False,
                    "auto_purchase_running": False}), \
                mock.patch.object(w, "read_consumptions", return_value={}), \
                mock.patch.object(w, "read_usage", return_value={}), \
                mock.patch.object(w, "read_scenario_names", return_value={}):
            with self.assertLogs("backfill", level="WARNING") as cm:
                w.tick()
        self.assertTrue(any("retry-queue watch skipped" in l
                            for l in cm.output))


class Certificate(unittest.TestCase):
    def test_section_lists_every_watched_scenario(self):
        fake = FakeMake({"6568689": [item(120)], "5780791": [item(20)]})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertEqual(f.phase, "make queue")
        self.assertTrue(f.measured)
        self.assertFalse(f.ok)
        self.assertEqual(len(f.detail), 6)
        self.assertIn("live intake: 1 stuck", f.detail[0])
        self.assertIn(cw.REPLAY_ORDER, f.detail[0])
        self.assertIn("abandoned cart: clear", f.detail[5])   # 20 min: young
        self.assertIn("older than 45 min", f.summary)
        self.assertEqual(f.data["total"], 1)
        self.assertEqual(f.data["by_scenario"]["6568689"]["count"], 1)

    def test_clear_is_green(self):
        f = reconcile.phase_make_queue(cfg(), get=FakeMake({}), now=NOW)
        self.assertTrue(f.measured and f.ok)
        self.assertIn("in 6 scenario(s)", f.summary)

    def test_partial_read_is_a_finding_not_zero(self):
        fake = FakeMake({"6893541": RuntimeError("503")})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertTrue(f.measured)
        self.assertFalse(f.ok)
        self.assertIn("1 scenario(s) not checked", f.summary)
        self.assertTrue(any("status capture: not checked" in d
                            for d in f.detail))

    def test_all_reads_failing_is_not_recorded(self):
        fake = FakeMake({s: RuntimeError("no token") for s in
                         ("6568689", "1111111", "6892982", "6893541",
                          "5563154", "5780791")})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertFalse(f.measured)
        head, body = render([f], cfg())
        self.assertIn("NOT RECORDED", body)
        self.assertNotIn(": 0", body)

    def test_unrecorded_make_queue_never_suppresses_repairs(self):
        fs = [Finding("counts", True, True, "ok", data={"gaps": [],
                                                        "missing_total": 0}),
              Finding("make queue", False, False, "not recorded")]
        self.assertIsNone(insane(fs, cfg()))
        fs.append(Finding("pipeline", False, False, "not recorded"))
        self.assertIsNotNone(insane(fs, cfg()))

    def test_section_renders_in_certificate(self):
        fake = FakeMake({"5563154": [item(200)]})
        fs = [Finding("counts", True, True, "all match"),
              reconcile.phase_make_queue(cfg(), get=fake, now=NOW)]
        head, body = render(fs, cfg())
        self.assertIn("make queue", head)
        self.assertIn("*make queue*", body)
        self.assertIn("customer updated: 1 stuck, oldest 3 hours old", body)
        self.assertIn("Check the contact first", body)

    def test_finding_survives_manifest_round_trip(self):
        import dataclasses
        f = reconcile.phase_make_queue(cfg(), get=FakeMake({}), now=NOW)
        again = Finding(**json.loads(json.dumps(dataclasses.asdict(f))))
        self.assertEqual(again.summary, f.summary)


if __name__ == "__main__":
    unittest.main()
