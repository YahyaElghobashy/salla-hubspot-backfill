#!/usr/bin/env python3
"""Make retry-queue watch (v2.12): credit_watch.check_relay_dlq and the
reconcile certificate's "make queue" section. Offline, against a fake _get
(and, for the token test, a patched urllib.request.urlopen).

Under test: the six-scenario default and the Config override (malformed
values tolerated with one WARNING), status=unresolved newest first plus the
client-side filter that leaves scheduled / in-progress items to Make's own
backoff (1, 10, 10, 30, 30, 180, 180 min, about 7.4 h), the age floor,
paging at pg[limit]=100 up to 500 items with ids de-duplicated and an
offset-ignoring API caught, a capped scan with nothing unresolved reading
"not measured" and never "clear", scenarios that do not store incomplete
executions shown as not watchable and left out of every count, WARNING logs
that name the scenario and never the token, the token only ever in the
Authorization header, per-scenario alert lines with their replay notes, state
keys (relay_dlq kept, never a fake 0, when nothing could be measured), read
failures logged once per state change with one alert after 6h under the 12h
cooldown, and the certificate section (including "not recorded" never
blocking repairs and a malformed config never killing the certificate).

Run: python3 -m unittest test_make_queue_alerts -v
"""
import itertools
import json
import os
import re
import time
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
WATCHABLE = ("6568689", "1111111", "5563154", "5780791")
ALL_IDS = ("6568689", "1111111", "6892982", "6893541", "5563154", "5780791")
_ids = itertools.count()


def item(age_min, resolved=False, i=None, status=None):
    ts = (NOW - timedelta(minutes=age_min)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    x = {"id": f"dlq{next(_ids) if i is None else i}", "created": ts,
         "resolved": resolved, "reason": "x"}
    if status is not None:
        x["status"] = status
    return x


class FakeMake:
    """Programmable GET /dlqs, keyed by scenario id. A scenario mapped to an
    Exception raises it; otherwise its item list is paged like Make does.
    Ignores status= and sort, like an API that does not know them."""

    def __init__(self, queues):
        self.queues = queues
        self.paths = []

    def _page(self, q, offset, limit):
        return q[offset:offset + limit]

    def __call__(self, path):
        self.paths.append(path)
        sid = re.search(r"scenarioId=(\d+)", path).group(1)
        limit = int(re.search(r"pg\[limit\]=(\d+)", path).group(1))
        offset = int(re.search(r"pg\[offset\]=(\d+)", path).group(1))
        q = self.queues.get(sid, [])
        if isinstance(q, Exception):
            raise q
        return {"dlqs": self._page(q, offset, limit), "pg": {"limit": limit}}


class FakeMakeNoOffset(FakeMake):
    """An API that ignores pg[offset]: every call returns page one."""

    def _page(self, q, offset, limit):
        return q[:limit]


class FakeMakeShifting(FakeMake):
    """New items arrive between calls, so each page starts 10 items early
    and repeats the tail of the page before it."""

    def _page(self, q, offset, limit):
        start = max(0, offset - 10)
        return q[start:start + limit]


def cfg(**kw):
    base = dict(make_team_id="777", make_intake_scenario_id="6568689",
                make_backfill_scenario_id="1111111", alerts_enabled=True)
    base.update(kw)
    return _cfg(**base)


def fresh_config_warnings(test):
    p = mock.patch.object(cw, "_CONFIG_WARNED", set())
    p.start()
    test.addCleanup(p.stop)


def offsets(fake):
    return [re.search(r"pg\[offset\]=(\d+)", p).group(1) for p in fake.paths]


class WatchList(unittest.TestCase):
    def setUp(self):
        fresh_config_warnings(self)

    def test_default_is_both_relays_plus_four(self):
        w = cw.dlq_watch_list(cfg())
        self.assertEqual([x["id"] for x in w], list(ALL_IDS))
        self.assertEqual([x["label"] for x in w],
                         ["live intake", "backfill relay", "customer capture",
                          "status capture", "customer updated",
                          "abandoned cart"])
        notes = {x["label"]: x["replay_note"] for x in w}
        self.assertIn("engine", notes["live intake"])
        self.assertEqual(notes["backfill relay"], cw.REPLAY_FETCH)
        self.assertIn("Dismiss it.", notes["backfill relay"])
        self.assertIn("queue sheet", notes["customer capture"])
        self.assertIn("Check the contact first", notes["customer updated"])
        self.assertIn("Check the contact first", notes["abandoned cart"])
        # the two captures do not store incomplete executions today
        self.assertEqual([x["stores_incomplete"] for x in w],
                         [True, True, False, False, True, True])

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
                                "replay_note": "Safe.",
                                "stores_incomplete": True})
        self.assertEqual(w[1]["label"], "scenario 43")
        self.assertEqual(w[1]["replay_note"], cw.REPLAY_UNKNOWN)

    def test_stores_incomplete_from_config(self):
        w = cw.dlq_watch_list(cfg(make_dlq_watch=[
            {"id": "42", "stores_incomplete": False},
            {"id": "43", "stores_incomplete": "false"},
            {"id": "44", "stores_incomplete": "true"},
            {"id": "45", "stores_incomplete": None},
            {"id": "46"}]))
        self.assertEqual([x["stores_incomplete"] for x in w],
                         [False, False, True, True, True])

    def test_label_to_id_mapping_accepted(self):
        w = cw.dlq_watch_list(cfg(make_dlq_watch={"orders": "42"}))
        self.assertEqual([(x["id"], x["label"]) for x in w], [("42", "orders")])

    def test_non_iterable_watch_warns_and_uses_defaults(self):
        for bad in (42, 4.2, True, "6568689"):
            with self.subTest(bad=bad):
                with self.assertLogs("backfill", level="WARNING") as cm:
                    w = cw.dlq_watch_list(cfg(make_dlq_watch=bad))
                self.assertEqual([x["id"] for x in w], list(ALL_IDS))
                self.assertIn("make_dlq_watch is a", "\n".join(cm.output))

    def test_non_numeric_id_is_skipped_with_warning(self):
        with self.assertLogs("backfill", level="WARNING") as cm:
            w = cw.dlq_watch_list(cfg(make_dlq_watch=[
                {"id": "abc"}, {"id": "42"}, {"id": {"x": 1}}]))
        self.assertEqual([x["id"] for x in w], ["42"])
        self.assertIn("'abc' is not a number", "\n".join(cm.output))

    def test_list_with_no_usable_id_warns_and_uses_defaults(self):
        with self.assertLogs("backfill", level="WARNING") as cm:
            w = cw.dlq_watch_list(cfg(make_dlq_watch=[{"id": "abc"},
                                                      {"id": ""}]))
        self.assertEqual([x["id"] for x in w], list(ALL_IDS))
        self.assertIn("no usable scenario id", "\n".join(cm.output))

    def test_config_warning_is_logged_once_per_process(self):
        with self.assertLogs("backfill", level="WARNING"):
            cw.dlq_watch_list(cfg(make_dlq_watch=42))
        with self.assertNoLogs("backfill", level="WARNING"):
            cw.dlq_watch_list(cfg(make_dlq_watch=42))

    def test_min_age_tolerates_bad_values(self):
        self.assertEqual(cw.dlq_min_age(cfg()), 45)
        self.assertEqual(cw.dlq_min_age(cfg(dlq_min_age_minutes=None)), 45)
        self.assertEqual(cw.dlq_min_age(cfg(dlq_min_age_minutes="60")), 60)
        self.assertEqual(cw.dlq_min_age(cfg(dlq_min_age_minutes=-5)), 0)
        for bad in ("soon", [45], float("nan"), False):
            with self.subTest(bad=bad):
                with self.assertLogs("backfill", level="WARNING") as cm:
                    self.assertEqual(
                        cw.dlq_min_age(cfg(dlq_min_age_minutes=bad)), 45)
                self.assertIn("dlq_min_age_minutes", "\n".join(cm.output))

    def test_config_fields_declared(self):
        c = cw.Config()
        self.assertIsNone(c.make_dlq_watch)
        self.assertEqual(c.dlq_min_age_minutes, 45)

    def test_example_config_carries_both_keys(self):
        here = Path(__file__).parent
        ex = json.loads((here / "config.example.json").read_text())
        self.assertIsNone(ex["make_dlq_watch"])
        self.assertEqual(ex["dlq_min_age_minutes"], 45)


class Scan(unittest.TestCase):
    def setUp(self):
        fresh_config_warnings(self)

    def test_asks_for_unresolved_newest_first(self):
        fake = FakeMake({})
        cw.scan_dlq(cfg(), get=fake, now=NOW)
        self.assertEqual(len(fake.paths), 4)
        for p in fake.paths:
            self.assertIn("status=unresolved", p)
            self.assertIn("pg[sortBy]=created", p)
            self.assertIn("pg[sortDir]=desc", p)
            self.assertIn("pg[limit]=100", p)

    def test_age_gate_and_resolved(self):
        fake = FakeMake({"6568689": [item(10), item(44), item(46),
                                     item(600), item(900, resolved=True)]})
        rows = cw.scan_dlq(cfg(), get=fake, now=NOW)
        live = rows[0]
        self.assertEqual(live["count"], 2)            # 46 min + 10 h
        self.assertEqual(live["oldest_age_min"], 600)
        self.assertEqual(rows[1]["count"], 0)

    def test_items_make_is_still_retrying_never_count(self):
        # the server may ignore status=unresolved; the client filter holds
        fake = FakeMake({"6568689": [
            item(300, status="scheduled"), item(300, status="inprogress"),
            item(300, status="in_progress"), item(300, status="resolved"),
            item(300, status="unresolved"), item(300)]})
        rows = cw.scan_dlq(cfg(), get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 2)

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
        self.assertEqual(offsets(fake), ["0", "100", "200"])

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
        self.assertIn("500+ stuck, oldest read", cw.dlq_line(rows[0]))

    def test_capped_with_nothing_unresolved_is_not_measured(self):
        for label, q in (
                ("all resolved",
                 [item(60, resolved=True, i=i) for i in range(900)]),
                ("all younger than the floor",
                 [item(5, i=i) for i in range(900)])):
            with self.subTest(label):
                fake = FakeMake({"6568689": q})
                rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689",
                                                        "label": "live"}]),
                                   get=fake, now=NOW)
                row = rows[0]
                self.assertIsNone(row["count"])       # never a fake 0
                self.assertTrue(row["capped"])
                self.assertEqual(row["records"], 500)
                self.assertEqual(row["error"], "")
                self.assertEqual(cw.dlq_line(row),
                                 "live: not measured (500+ records, none "
                                 "unresolved on the pages read)")
                self.assertFalse(cw.dlq_state(rows)["6568689"]["measured"])

    def test_duplicate_ids_across_pages_count_once(self):
        q = [item(60, i=i) for i in range(150)]
        fake = FakeMakeShifting({"6568689": q})
        rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689"}]),
                           get=fake, now=NOW)
        self.assertEqual(rows[0]["count"], 150)       # not 160
        self.assertEqual(rows[0]["records"], 150)
        self.assertFalse(rows[0]["capped"])

    def test_offset_ignoring_api_stops_paging_and_is_a_floor(self):
        q = [item(60, i=i) for i in range(250)]
        fake = FakeMakeNoOffset({"6568689": q})
        with self.assertLogs("backfill", level="INFO") as cm:
            rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689"}]),
                               get=fake, now=NOW)
        self.assertEqual(len(fake.paths), 2)          # not 5 copies of page 1
        self.assertEqual(offsets(fake), ["0", "100"])
        self.assertEqual(rows[0]["count"], 100)
        self.assertTrue(rows[0]["capped"])
        self.assertIn("100+ stuck", cw.dlq_line(rows[0]))
        self.assertIn("added no new items", "\n".join(cm.output))

    def test_offset_ignoring_api_with_nothing_unresolved_is_not_measured(self):
        q = [item(60, resolved=True, i=i) for i in range(250)]
        fake = FakeMakeNoOffset({"6568689": q})
        rows = cw.scan_dlq(cfg(make_dlq_watch=[{"id": "6568689",
                                                "label": "live"}]),
                           get=fake, now=NOW)
        self.assertIsNone(rows[0]["count"])
        self.assertEqual(cw.dlq_line(rows[0]),
                         "live: not measured (100+ records, none unresolved "
                         "on the pages read)")

    def test_unwatchable_scenarios_are_not_read(self):
        fake = FakeMake({"6892982": [item(600)], "6893541": [item(600)]})
        rows = cw.scan_dlq(cfg(), get=fake, now=NOW)
        self.assertFalse(any("6892982" in p or "6893541" in p
                             for p in fake.paths))
        row = next(r for r in rows if r["id"] == "6892982")
        self.assertIsNone(row["count"])
        self.assertEqual(cw.dlq_line(row),
                         "customer capture: not watchable (incomplete "
                         "executions are not stored)")
        self.assertFalse(cw.dlq_state(rows)["6892982"]["watchable"])

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
        self.assertTrue(all(r["count"] == 0 for r in rows
                            if r is not row and r["stores_incomplete"]))

    def test_quiet_scan_leaves_failure_logging_to_the_caller(self):
        fake = FakeMake({"5563154": RuntimeError("503")})
        with self.assertNoLogs("backfill", level="WARNING"):
            rows = cw.scan_dlq(cfg(), get=fake, now=NOW, quiet=True)
        self.assertIn("503", next(r for r in rows
                                  if r["id"] == "5563154")["error"])

    def test_token_never_in_url(self):
        fake = FakeMake({})
        with mock.patch.dict(os.environ, {"MAKE_API_TOKEN": TOKEN}):
            cw.scan_dlq(cfg(), get=fake, now=NOW)
        self.assertEqual(len(fake.paths), 4)
        self.assertFalse(any(TOKEN in p for p in fake.paths))

    def test_default_get_is_module_get(self):
        fake = FakeMake({})
        with mock.patch.object(cw, "_get", fake):
            cw.scan_dlq(cfg(), now=NOW)
        self.assertEqual(len(fake.paths), 4)

    def test_token_only_in_authorization_header(self):
        seen = []

        def fake_urlopen(req, timeout=None):
            seen.append(req)
            resp = mock.MagicMock()
            resp.__enter__.return_value.read.return_value = b'{"dlqs": []}'
            return resp

        with mock.patch.dict(os.environ, {"MAKE_API_TOKEN": TOKEN}), \
                mock.patch("urllib.request.urlopen",
                           side_effect=fake_urlopen):
            rows = cw.scan_dlq(cfg(), now=NOW)
        self.assertEqual(len(seen), 4)
        self.assertTrue(all(r["count"] == 0 for r in rows
                            if r["stores_incomplete"]))
        for req in seen:
            self.assertTrue(req.full_url.startswith(cw.MAKE_API + "/dlqs?"))
            self.assertNotIn(TOKEN, req.full_url)
            self.assertEqual(req.get_header("Authorization"), f"Token {TOKEN}")
            others = {k: v for k, v in req.header_items()
                      if k.lower() != "authorization"}
            self.assertFalse(any(TOKEN in str(v) for v in others.values()))
            self.assertIsNone(req.data)


class Alert(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        fresh_config_warnings(self)
        self.sent = []
        p = mock.patch.object(cw.notify, "send_alert",
                              side_effect=lambda s, b, **k: self.sent.append((s, b)))
        p.start()
        self.addCleanup(p.stop)

    def _watch(self, **kw):
        return cw.CreditWatch(cfg(**kw))

    def assertClientText(self, *texts):
        for text in texts:
            self.assertNotIn("—", text)     # no em dash in client text
            self.assertNotIn("–", text)

    def test_one_line_per_stuck_scenario_with_note_and_link(self):
        w = self._watch()
        fake = FakeMake({"6568689": [item(120), item(3000)],
                         "5563154": [item(60)],
                         "5780791": [item(5)]})          # young: the floor
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
        self.assertNotIn("abandoned cart", body)
        self.assertNotIn("customer capture", body)
        self.assertClientText(subject, body)

    def test_backfill_relay_says_dismiss(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"1111111": [item(600)]}), now=NOW)
        self.assertIn("backfill relay: 1 stuck", self.sent[0][1])
        self.assertIn("Replaying does nothing useful: the engine already "
                      "retried the fetch itself. Dismiss it.", self.sent[0][1])

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
        self.assertEqual(set(by), set(ALL_IDS))
        self.assertEqual(by["6568689"]["count"], 1)
        self.assertEqual(by["6568689"]["label"], "live intake")
        self.assertIsNone(by["5563154"]["count"])       # never a fake 0
        self.assertEqual(by["5780791"]["count"], 0)
        self.assertIsNone(by["6892982"]["count"])       # not watchable
        self.assertFalse(by["6892982"]["watchable"])
        self.assertEqual(w.state["dlq_read_fail"]["5563154"]["ticks"], 1)
        self.assertIn("Not checked this time: customer updated.",
                      self.sent[0][1])
        w._save()
        saved = json.loads(Path("mirror/credit_state.json").read_text())
        self.assertEqual(saved["relay_dlq_by_scenario"]["6568689"]["count"], 1)

    def test_unwatchable_never_counts(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"6892982": [item(600)],
                                        "6893541": [item(600)]}), now=NOW)
        self.assertEqual(self.sent, [])
        self.assertEqual(w.state["relay_dlq"], 0)

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
        # the other three watchable queues were read: their sum is real
        self.assertEqual(w.state["relay_dlq"], 0)
        w.check_relay_dlq(get=FakeMake({}), now=NOW)
        self.assertNotIn("dlq_alerted_at", w.state)
        self.assertEqual(self.sent, [])

    def test_every_read_failing_keeps_the_last_total(self):
        down = FakeMake({s: RuntimeError("503") for s in WATCHABLE})
        w = self._watch()
        w.check_relay_dlq(get=down, now=NOW)
        self.assertIn("relay_dlq", w.state)
        self.assertIsNone(w.state["relay_dlq"])         # none known: None
        w.state["relay_dlq"] = 3
        w.check_relay_dlq(get=down, now=NOW)
        self.assertEqual(w.state["relay_dlq"], 3)       # kept, never 0
        self.assertEqual(self.sent, [])

    def test_capped_zero_neither_alerts_nor_rearms(self):
        w = self._watch()
        w.state["dlq_alerted_at"] = 1.0
        q = [item(60, resolved=True, i=i) for i in range(900)]
        w.check_relay_dlq(get=FakeMake({"6568689": q}), now=NOW)
        self.assertEqual(self.sent, [])
        self.assertIn("dlq_alerted_at", w.state)
        self.assertIsNone(w.state["relay_dlq_by_scenario"]["6568689"]["count"])
        self.assertNotIn("6568689", w.state["dlq_read_fail"])  # not a failure

    def test_young_items_never_alert(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"6568689": [item(30), item(44)]}),
                          now=NOW)
        self.assertEqual(self.sent, [])
        self.assertEqual(w.state["relay_dlq"], 0)

    def test_items_make_is_still_retrying_never_alert(self):
        w = self._watch()
        w.check_relay_dlq(get=FakeMake({"6568689": [
            item(200, status="scheduled"), item(400, status="inprogress")]}),
            now=NOW)
        self.assertEqual(self.sent, [])
        self.assertEqual(w.state["relay_dlq"], 0)

    def test_read_failure_warns_once_per_state_change(self):
        w = self._watch()
        down = FakeMake({"5563154": RuntimeError("503")})
        with self.assertLogs("backfill", level="WARNING") as cm:
            w.check_relay_dlq(get=down, now=NOW)
        self.assertEqual(len([l for l in cm.output
                              if "customer updated" in l]), 1)
        with self.assertNoLogs("backfill", level="WARNING"):
            w.check_relay_dlq(get=down, now=NOW)
            w.check_relay_dlq(get=down, now=NOW)
        self.assertEqual(w.state["dlq_read_fail"]["5563154"]["ticks"], 3)
        with self.assertLogs("backfill", level="WARNING") as cm:
            w.check_relay_dlq(get=FakeMake({}), now=NOW)
        self.assertIn("customer updated can be read again", cm.output[0])
        self.assertEqual(w.state["dlq_read_fail"], {})
        with self.assertNoLogs("backfill", level="WARNING"):
            w.check_relay_dlq(get=FakeMake({}), now=NOW)

    def test_six_hours_unreadable_alerts_once_under_cooldown(self):
        w = self._watch()
        down = FakeMake({"5563154": RuntimeError(f"HTTP 403 {TOKEN}")})
        with mock.patch.dict(os.environ, {"MAKE_API_TOKEN": TOKEN}):
            w.check_relay_dlq(get=down, now=NOW)
            self.assertEqual(self.sent, [])
            # five hours in: still quiet
            w.state["dlq_read_fail"]["5563154"]["since"] = time.time() - 5 * 3600
            w.check_relay_dlq(get=down, now=NOW)
            self.assertEqual(self.sent, [])
            w.state["dlq_read_fail"]["5563154"]["since"] = (
                time.time() - 6 * 3600 - 1)
            w.check_relay_dlq(get=down, now=NOW)
            self.assertEqual(len(self.sent), 1)
            w.check_relay_dlq(get=down, now=NOW)        # 12h cooldown
            self.assertEqual(len(self.sent), 1)
        subject, body = self.sent[0]
        self.assertIn("A Make retry queue has not been readable for 6 hours",
                      subject)
        self.assertIn("customer updated: 3 checks in a row failed", body)
        self.assertIn("https://eu1.make.com/777/scenarios/5563154", body)
        self.assertNotIn(TOKEN, subject + body)
        self.assertClientText(subject, body)
        # still failing, cooldown over: it says so again
        w.state["dlq_alerted_at"] -= 12 * 3600 + 1
        w.check_relay_dlq(get=down, now=NOW)
        self.assertEqual(len(self.sent), 2)

    def test_long_failure_rides_along_with_a_stuck_alert(self):
        w = self._watch()
        w.state["dlq_read_fail"] = {"5563154": {
            "label": "customer updated", "ticks": 80,
            "since": time.time() - 7 * 3600}}
        w.check_relay_dlq(get=FakeMake({"6568689": [item(120)],
                                        "5563154": RuntimeError("503")}),
                          now=NOW)
        self.assertEqual(len(self.sent), 1)
        subject, body = self.sent[0]
        self.assertIn("1 failed Make run is stuck", subject)
        self.assertIn("Not readable for 6 hours or more.", body)
        self.assertIn("customer updated: 81 checks in a row failed", body)
        self.assertNotIn("Not checked this time", body)
        self.assertClientText(subject, body)

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


class BadList(list):
    """A make_dlq_watch that looks like a list and blows up when read."""

    def __iter__(self):
        raise ValueError("unreadable watch list")


class Certificate(unittest.TestCase):
    def setUp(self):
        fresh_config_warnings(self)

    def test_section_lists_every_watched_scenario(self):
        fake = FakeMake({"6568689": [item(120)], "5780791": [item(20)]})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertEqual(f.phase, "make queue")
        self.assertTrue(f.measured)
        self.assertFalse(f.ok)
        self.assertEqual(len(f.detail), 6)
        self.assertIn("live intake: 1 stuck", f.detail[0])
        self.assertIn(cw.REPLAY_ORDER, f.detail[0])
        self.assertIn("customer capture: not watchable", f.detail[2])
        self.assertIn("abandoned cart: clear", f.detail[5])   # 20 min: young
        self.assertIn("older than 45 min", f.summary)
        self.assertIn("in 1 of 4 scenario(s)", f.summary)
        self.assertEqual(f.data["total"], 1)
        self.assertEqual(f.data["by_scenario"]["6568689"]["count"], 1)

    def test_clear_is_green(self):
        f = reconcile.phase_make_queue(cfg(), get=FakeMake({}), now=NOW)
        self.assertTrue(f.measured and f.ok)
        self.assertIn("in 4 scenario(s)", f.summary)
        self.assertIn("2 not watchable", f.summary)

    def test_unwatchable_left_out_of_ok_and_counts(self):
        fake = FakeMake({"6892982": [item(600)], "6893541": [item(600)]})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertTrue(f.measured and f.ok)
        self.assertEqual(f.data["total"], 0)
        self.assertTrue(any("status capture: not watchable (incomplete "
                            "executions are not stored)" in d
                            for d in f.detail))

    def test_partial_read_is_a_finding_not_zero(self):
        fake = FakeMake({"5563154": RuntimeError("503")})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertTrue(f.measured)
        self.assertFalse(f.ok)
        self.assertIn("1 scenario(s) not checked", f.summary)
        self.assertTrue(any("customer updated: not checked" in d
                            for d in f.detail))

    def test_all_reads_failing_is_not_recorded(self):
        fake = FakeMake({s: RuntimeError("no token") for s in ALL_IDS})
        f = reconcile.phase_make_queue(cfg(), get=fake, now=NOW)
        self.assertFalse(f.measured)
        self.assertIn("Make API unreadable", f.summary)
        head, body = render([f], cfg())
        self.assertIn("NOT RECORDED", body)
        self.assertNotIn(": 0", body)

    def test_capped_zero_is_not_measured_never_clear(self):
        q = [item(60, resolved=True, i=i) for i in range(900)]
        # the only watched queue: the section is not measured at all
        one = cfg(make_dlq_watch=[{"id": "6568689", "label": "live intake"}])
        f = reconcile.phase_make_queue(one, get=FakeMake({"6568689": q}),
                                       now=NOW)
        self.assertFalse(f.measured)
        self.assertFalse(f.ok)
        self.assertIn("not recorded (not measured)", f.summary)
        self.assertEqual(f.detail, ["live intake: not measured (500+ records, "
                                    "none unresolved on the pages read)"])
        self.assertNotIn("clear", " ".join(f.detail))
        head, body = render([f], cfg())
        self.assertNotIn("✅", head)
        # one of four: the section is measured but not ok
        f = reconcile.phase_make_queue(cfg(), get=FakeMake({"6568689": q}),
                                       now=NOW)
        self.assertTrue(f.measured)
        self.assertFalse(f.ok)
        self.assertIn("1 not measured", f.summary)
        self.assertIn("live intake: not measured", f.detail[0])

    def test_malformed_config_values_are_tolerated(self):
        c = cfg(make_dlq_watch=42, dlq_min_age_minutes="soon")
        with self.assertLogs("backfill", level="WARNING") as cm:
            f = reconcile.phase_make_queue(c, get=FakeMake({}), now=NOW)
        self.assertTrue(f.measured and f.ok)
        self.assertIn("older than 45 min", f.summary)
        out = "\n".join(cm.output)
        self.assertIn("make_dlq_watch", out)
        self.assertIn("dlq_min_age_minutes", out)

    def test_error_inside_the_section_is_not_recorded_not_a_crash(self):
        c = cfg(make_dlq_watch=BadList([{"id": "42"}]))
        with self.assertLogs("backfill", level="WARNING") as cm:
            f = reconcile.phase_make_queue(c, get=FakeMake({}), now=NOW)
        self.assertEqual(f.phase, "make queue")
        self.assertFalse(f.measured)
        self.assertIn("not recorded (config or read error)", f.summary)
        self.assertIn("make queue section not recorded", "\n".join(cm.output))
        counts = Finding("counts", True, True, "ok",
                         data={"gaps": [], "missing_total": 0})
        self.assertIsNone(insane([counts, f], cfg()))
        head, body = render([counts, f], cfg())
        self.assertIn("NOT RECORDED", body)

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
