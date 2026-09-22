"""Unit tests for the v2.7 realtime extensions (status relay, customer sync).

Everything runs offline against fakes. The behaviors under test are the ones
the E2E plan depends on: stage-map completeness, no stage regression on
out-of-order replays, the deferred retry ladder, superseded settlement, the
customer 0/1/2+ routes, the merge kill-switch, ledger idempotency across a
process restart, and the backfill's yield-glob picking up the new signals.
"""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import backfill
from backfill import Config


def _chdir_tmp(test):
    tmp = tempfile.TemporaryDirectory()
    test.addCleanup(tmp.cleanup)
    old = os.getcwd()
    os.chdir(tmp.name)
    test.addCleanup(os.chdir, old)
    Path("mirror").mkdir()
    return tmp.name


class FakeHS:
    """Just enough of HubSpot for both consumers."""

    def __init__(self):
        self.orders = {}          # salla_order_id -> {hs id, stage}
        self.contacts = []        # list of {id, properties}
        self.patches = []         # (hs_id, props)
        self.writes = []          # (method, path, body)
        self.merges = []

    # status relay path
    def search(self, path, body, what):
        if "orders/search" in path:
            oid = body["filterGroups"][0]["filters"][0]["value"]
            o = self.orders.get(str(oid))
            if not o:
                return {"total": 0, "results": []}
            return {"total": 1, "results": [{
                "id": o["id"],
                "properties": {"hs_object_id": o["id"],
                               "hs_pipeline_stage": o["stage"]}}]}
        if "contacts/search" in path:
            return {"total": len(self.contacts), "results": self.contacts[:5]}
        return {"total": 0, "results": []}

    def update_order(self, hs_id, props, what):
        self.patches.append((str(hs_id), dict(props)))
        for o in self.orders.values():
            if str(o["id"]) == str(hs_id):
                o["stage"] = props.get("hs_pipeline_stage", o["stage"])
        return 200, {}

    def _write(self, method, path, body, what):
        self.writes.append((method, path, body))
        if method == "POST" and path.endswith("/contacts"):
            new = {"id": str(9000 + len(self.contacts))}
            return 201, new
        if path.endswith("/merge"):
            self.merges.append(body)
            return 200, {}
        return 200, {"id": "patched"}


class FakeGIO:
    def __init__(self):
        self.marks = []
        self.appends = []

    def queue_read_heartbeat(self, qsid, tab=None):
        return ""

    def queue_write_heartbeat(self, qsid, instance_id, tab=None):
        pass

    def queue_read(self, qsid, start_row=2, tab=None):
        return []

    def queue_mark(self, qsid, row, expect, status, attempts, note, tab=None):
        self.marks.append((row, status, note))
        return True

    def queue_append_rows(self, qsid, rows, tab=None):
        self.appends.append((tab, rows))


def _cfg(**kw):
    c = Config()
    c.status_stage_map = {"delivered": "st-DEL", "shipped": "st-SHP",
                          "delivering": "st-ING", "completed": "st-CMP",
                          "canceled": "st-CAN", "restored": "st-RST",
                          "restoring": "st-RST", "deleted": "st-CAN"}
    c.queue_spreadsheet_id = "QS"
    c.spreadsheet_id = "AUDIT"
    for k, v in kw.items():
        setattr(c, k, v)
    return c


def _row(oid="101", event="status:delivered@2026-08-10T10:00:00",
         attempts=0, note="", ref=""):
    return {"row": 5, "order_id": str(oid), "reference_id": ref,
            "event": event, "status": "queued", "attempts": attempts,
            "source": "webhook", "note": note, "received_at": "x"}


class TestStatusRelay(unittest.TestCase):
    def mk(self, hs=None, **cfg_kw):
        _chdir_tmp(self)
        import status_relay
        self.status_relay = status_relay
        hs = hs or FakeHS()
        r = status_relay.StatusRelay(_cfg(**cfg_kw), hs, FakeGIO(), live=True)
        return r, hs

    def test_all_eight_slugs_map(self):
        r, _ = self.mk()
        for slug in ("delivered", "shipped", "delivering", "completed",
                     "canceled", "restored", "restoring", "deleted"):
            self.assertIn(slug, r.stage_map, slug)

    def test_applies_stage_and_ledgers(self):
        r, hs = self.mk()
        hs.orders["101"] = {"id": "H1", "stage": "st-OLD"}
        state, note = r.handle_row(_row())
        self.assertEqual(state, "done")
        self.assertEqual(hs.patches, [("H1", {"hs_pipeline_stage": "st-DEL"})])
        self.assertFalse(r.ledger.newer_than_applied("101", "2026-08-09T00:00:00"))

    def test_out_of_order_event_never_regresses(self):
        r, hs = self.mk()
        hs.orders["101"] = {"id": "H1", "stage": "st-OLD"}
        r.handle_row(_row(event="status:delivered@2026-08-10T10:00:00"))
        state, note = r.handle_row(
            _row(event="status:shipped@2026-08-09T09:00:00"))
        self.assertEqual(state, "superseded")
        self.assertEqual(len(hs.patches), 1)  # no second write

    def test_same_stage_settles_superseded(self):
        r, hs = self.mk()
        hs.orders["101"] = {"id": "H1", "stage": "st-DEL"}
        state, _ = r.handle_row(_row())
        self.assertEqual(state, "superseded")
        self.assertEqual(hs.patches, [])

    def test_missing_order_defers_with_ladder(self):
        r, hs = self.mk()
        t0 = time.time()
        state, note = r.handle_row(_row(attempts=0))
        self.assertEqual(state, "deferred")
        nb = float(note.split("nb=")[1].split()[0])
        self.assertAlmostEqual(nb - t0, 30, delta=5)
        state, note = r.handle_row(_row(attempts=2))
        nb = float(note.split("nb=")[1].split()[0])
        self.assertAlmostEqual(nb - t0, 600, delta=5)

    def test_ladder_exhaustion_goes_final_and_logs_exception(self):
        r, hs = self.mk()
        with mock.patch.object(r, "_alert"):
            state, _ = r.handle_row(_row(attempts=5))
        self.assertEqual(state, "error-final")
        self.assertEqual(r.gio.appends[0][0], "Delivery Status Exceptions")

    def test_unmapped_slug_is_final_exception(self):
        r, hs = self.mk()
        hs.orders["101"] = {"id": "H1", "stage": "st-OLD"}
        with mock.patch.object(r, "_alert") as al:
            state, _ = r.handle_row(_row(event="status:weird@2026"))
        self.assertEqual(state, "error-final")
        self.assertTrue(al.called)
        self.assertEqual(hs.patches, [])

    def test_ledger_survives_restart(self):
        r, hs = self.mk()
        hs.orders["101"] = {"id": "H1", "stage": "st-OLD"}
        r.handle_row(_row())
        r2 = self.status_relay.StatusRelay(_cfg(), hs, FakeGIO(), live=True)
        self.assertFalse(r2.ledger.newer_than_applied(
            "101", "2026-08-01T00:00:00"))


def _cust_row(cid="777", payload=None, phone="9665550001"):
    return {"row": 3, "order_id": str(cid), "reference_id": phone,
            "event": "customer.created", "status": "queued", "attempts": 0,
            "source": "webhook", "received_at": "x",
            "note": json.dumps(payload or {
                "id": cid, "first_name": "Nora", "last_name": "K",
                "mobile": "5550001", "mobile_code": "966", "city": "Jeddah",
                "gender": "female", "lang": "ar", "email": "x@store.fake"})}


class TestCustomerSync(unittest.TestCase):
    def mk(self, hs=None, **cfg_kw):
        _chdir_tmp(self)
        import customer_sync
        self.customer_sync = customer_sync
        hs = hs or FakeHS()
        s = customer_sync.CustomerSync(_cfg(**cfg_kw), hs, FakeGIO(),
                                       live=True)
        return s, hs

    def test_zero_hits_creates_lead(self):
        s, hs = self.mk()
        state, note = s.handle_row(_cust_row())
        self.assertEqual(state, "done")
        method, path, body = hs.writes[0]
        self.assertEqual((method, path), ("POST", "/crm/v3/objects/contacts"))
        self.assertEqual(body["properties"]["lifecyclestage"], "lead")
        self.assertEqual(body["properties"]["incorrect_email"], "x@store.fake")
        self.assertEqual(body["properties"]["phone"], "9665550001")

    def test_consent_flag_lands_on_create_and_update(self):
        # [v2.10] is_notifications_enabled -> salla_consent_status, both paths
        s, hs = self.mk()
        on = {"id": "778", "first_name": "N", "last_name": "K", "mobile": "5550002",
              "mobile_code": "966", "is_notifications_enabled": True}
        s.handle_row(_cust_row(cid="778", payload=on, phone="9665550002"))
        self.assertEqual(hs.writes[-1][2]["properties"]["salla_consent_status"], "true")

        hs.contacts = [{"id": "C7", "properties": {}}]
        off = {"id": "779", "first_name": "N", "last_name": "K", "mobile": "5550003",
               "mobile_code": "966", "is_notifications_enabled": False}
        s.handle_row(_cust_row(cid="779", payload=off, phone="9665550003"))
        method, path, body = hs.writes[-1]
        self.assertEqual((method, path), ("PATCH", "/crm/v3/objects/contacts/C7"))
        self.assertEqual(body["properties"]["salla_consent_status"], "false")

    def test_missing_consent_flag_leaves_property_alone(self):
        s, hs = self.mk()
        s.handle_row(_cust_row())            # default payload has no flag
        self.assertNotIn("salla_consent_status", hs.writes[-1][2]["properties"])

    def test_one_hit_updates(self):
        s, hs = self.mk()
        hs.contacts = [{"id": "C1", "properties": {"firstname": "N"}}]
        state, note = s.handle_row(_cust_row())
        self.assertEqual(state, "done")
        self.assertIn("updated contact C1", note)
        self.assertEqual(hs.merges, [])

    def test_two_hits_auto_merges_most_recent_primary(self):
        s, hs = self.mk()
        hs.contacts = [{"id": "C_new", "properties": {}},
                       {"id": "C_old", "properties": {}}]
        with mock.patch("notify.send_alert", create=True):
            state, _ = s.handle_row(_cust_row())
        self.assertEqual(state, "done")
        self.assertEqual(len(hs.merges), 1)
        self.assertEqual(hs.merges[0]["primaryObjectId"], "C_new")
        self.assertEqual(hs.merges[0]["objectIdToMerge"], "C_old")
        self.assertTrue(Path("mirror/contact_merges.csv").exists())

    def test_merge_kill_switch(self):
        s, hs = self.mk(customer_auto_merge=False)
        hs.contacts = [{"id": "C_new", "properties": {}},
                       {"id": "C_old", "properties": {}}]
        with mock.patch("notify.send_alert", create=True):
            state, _ = s.handle_row(_cust_row())
        self.assertEqual(state, "done")
        self.assertEqual(hs.merges, [])   # updated most-recent, no merge

    def test_repeat_event_short_circuits_after_restart(self):
        s, hs = self.mk()
        s.handle_row(_cust_row())
        s2 = self.customer_sync.CustomerSync(_cfg(), hs, FakeGIO(), live=True)
        state, note = s2.handle_row(_cust_row())
        self.assertEqual(state, "superseded")
        self.assertEqual(len([w for w in hs.writes
                              if w[0] == "POST" and w[1].endswith("contacts")]),
                         1)


class TestConsentStatus(unittest.TestCase):
    """[v2.10] One helper feeds both contact writers."""

    def test_flag_rendering(self):
        cs = backfill.consent_status
        self.assertEqual(cs({"is_notifications_enabled": True}), "true")
        self.assertEqual(cs({"is_notifications_enabled": False}), "false")
        self.assertEqual(cs({"is_notifications_enabled": "true"}), "true")
        self.assertEqual(cs({"is_notifications_enabled": 0}), "false")
        self.assertIsNone(cs({"is_notifications_enabled": None}))
        self.assertIsNone(cs({}))
        self.assertIsNone(cs(None))

    def test_order_path_contact_carries_flag_only_when_present(self):
        hs = backfill.HubSpot(_cfg(), "tok", live=False)
        seen = []
        hs._write = lambda m, p, body, what: (seen.append(body), (201, {"id": "C9"}))[1]
        base = {"first_name": "A", "mobile": "5", "mobile_code": "966",
                "urls": {"admin": "u"}}
        hs.create_contact({"customer": {**base, "id": 1,
                                        "is_notifications_enabled": True}})
        self.assertEqual(seen[-1]["properties"]["salla_consent_status"], "true")
        hs.create_contact({"customer": {**base, "id": 2}})
        self.assertNotIn("salla_consent_status", seen[-1]["properties"])
        hs.create_contact({"customer": None})       # the null-customer order
        self.assertNotIn("salla_consent_status", seen[-1]["properties"])


class TestTwinCollapse(unittest.TestCase):
    """Regression: a second row for the same ORDER is a different status
    event and must be applied, not collapsed. A second row for the same
    CUSTOMER is a replayed webhook and must be collapsed."""

    def test_status_relay_does_not_collapse(self):
        import status_relay
        self.assertFalse(status_relay.StatusRelay.collapse_twins)

    def test_customer_sync_collapses(self):
        import customer_sync
        self.assertTrue(customer_sync.CustomerSync.collapse_twins)

    def test_two_status_events_one_order_both_apply(self):
        _chdir_tmp(self)
        import status_relay
        hs = FakeHS()
        hs.orders["101"] = {"id": "H1", "stage": "st-OLD"}
        r = status_relay.StatusRelay(_cfg(), hs, FakeGIO(), live=True)
        s1, _ = r.handle_row(_row(event="status:shipped@2026-08-10T10:00:00"))
        s2, _ = r.handle_row(_row(event="status:delivered@2026-08-10T11:00:00"))
        self.assertEqual((s1, s2), ("done", "done"))
        self.assertEqual([p[1]["hs_pipeline_stage"] for p in hs.patches],
                         ["st-SHP", "st-DEL"])


class TestYieldGlob(unittest.TestCase):
    def test_backfill_yields_to_any_realtime_signal(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        mdir = Path(tmp.name)
        eng = backfill.Engine.__new__(backfill.Engine)
        eng.cfg = _cfg()
        eng.mirror = mock.Mock(dir=str(mdir))
        eng.hs = mock.Mock()
        eng._yielding = None
        # only the STATUS signal is active; live orders idle
        (mdir / "live_active.json").write_text(json.dumps(
            {"active": False, "ts": time.time()}))
        (mdir / "live_active_status.json").write_text(json.dumps(
            {"active": True, "ts": time.time()}))
        backfill.Engine._yield_to_live(eng)
        self.assertTrue(eng._yielding)
        eng.hs.search_rl.set_ceiling.assert_called()  # yielded

    def test_stale_signals_reclaim(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        mdir = Path(tmp.name)
        eng = backfill.Engine.__new__(backfill.Engine)
        eng.cfg = _cfg()
        eng.mirror = mock.Mock(dir=str(mdir))
        eng.hs = mock.Mock()
        eng._yielding = None
        (mdir / "live_active_customers.json").write_text(json.dumps(
            {"active": True, "ts": time.time() - 300}))   # stale
        backfill.Engine._yield_to_live(eng)
        self.assertFalse(eng._yielding)


# ---------------------------------------------------------------------------
# v2.8: classification-aware give-up alerting
# ---------------------------------------------------------------------------

class IndexedGIO(FakeGIO):
    """FakeGIO whose Live Queue tab read returns configurable rows."""

    def __init__(self, live_rows=None):
        super().__init__()
        self.live_rows = live_rows or []

    def queue_read(self, qsid, start_row=2, tab=None):
        if tab == "Live Queue":
            return self.live_rows
        return []


class TestStatusClassification(unittest.TestCase):
    def mk(self, live_rows=None, **cfg_kw):
        _chdir_tmp(self)
        import status_relay
        self.status_relay = status_relay
        hs = FakeHS()
        cfg_kw.setdefault("live_min_reference", 280000000)
        cfg_kw.setdefault("live_queue_tab", "Live Queue")
        r = status_relay.StatusRelay(_cfg(**cfg_kw), hs,
                                     IndexedGIO(live_rows), live=True)
        return r, hs

    def _patch_alerts(self):
        import notify
        sent = []
        return sent, mock.patch.object(
            notify, "send_alert",
            side_effect=lambda s, b, **k: sent.append((s, b)))

    def test_held_order_gets_held_story_not_legacy(self):
        rows = [{"order_id": "101", "status": "held",
                 "note": "catalog gate: Airbrush Combo -- in review queue"},
                {"order_id": "202", "status": "held",
                 "note": "catalog gate: Airbrush Combo -- in review queue"}]
        r, _ = self.mk(rows)
        sent, patcher = self._patch_alerts()
        with patcher:
            state, _ = r.handle_row(_row(oid="101", ref="284000001",
                                         attempts=5))
        self.assertEqual(state, "error-final")
        self.assertEqual(len(sent), 1)
        subj, body = sent[0]
        self.assertIn("held for catalog", subj)
        self.assertIn("2 order(s)", subj)
        self.assertIn("Airbrush Combo (2)", body)
        self.assertIn("docs.google.com", body)
        self.assertIn("catalog-held", r.gio.appends[0][1][0][5])

    def test_live_era_missing_is_red_and_specific(self):
        r, _ = self.mk([])
        sent, patcher = self._patch_alerts()
        with patcher:
            state, _ = r.handle_row(_row(oid="909", ref="284123456",
                                         attempts=5))
        self.assertEqual(state, "error-final")
        self.assertEqual(len(sent), 1)
        self.assertIn("\U0001F534", sent[0][0])
        self.assertIn("909", sent[0][0])
        self.assertIn("NOT catalog-hold", sent[0][1])

    def test_engine_seen_but_absent_is_red_even_with_old_reference(self):
        rows = [{"order_id": "777", "status": "done", "note": "HS 123"}]
        r, _ = self.mk(rows)
        sent, patcher = self._patch_alerts()
        with patcher:
            r.handle_row(_row(oid="777", ref="100000000", attempts=5))
        self.assertEqual(len(sent), 1)
        self.assertIn("\U0001F534", sent[0][0])

    def test_backfill_era_goes_to_digest_not_alert(self):
        # pin the digest hour past the wall clock: the flush fires when
        # now.hour >= status_digest_hour, so any run at or after the default
        # 18:00 flushed per-row and made the buffer assertion below flaky.
        # now.hour + 1 is never reached within the test (and 24 at 23:xx is
        # simply an hour that never arrives).
        import datetime as _dt
        r, _ = self.mk([], status_digest_hour=_dt.datetime.now().hour + 1)
        sent, patcher = self._patch_alerts()
        with patcher:
            for i in range(4):
                r.handle_row(_row(oid=str(500 + i), ref="250000000",
                                  attempts=5))
            self.assertEqual(sent, [])
            self.assertEqual(len(r._digest), 4)
            r._flush_digest(force=True)
        self.assertEqual(len(sent), 1)
        self.assertIn("4 status event(s)", sent[0][0])
        self.assertEqual(r._digest, [])

    def test_zero_min_reference_preserves_legacy_alert(self):
        r, _ = self.mk([], live_min_reference=0)
        sent, patcher = self._patch_alerts()
        with patcher:
            r.handle_row(_row(oid="101", ref="284000001", attempts=5))
        self.assertEqual(len(sent), 1)
        self.assertIn("Status events arriving", sent[0][0])

    def test_held_backlog_cooldown_suppresses_second_alert(self):
        rows = [{"order_id": "101", "status": "held", "note": "catalog gate"},
                {"order_id": "102", "status": "held", "note": "catalog gate"}]
        r, _ = self.mk(rows)
        sent, patcher = self._patch_alerts()
        with patcher:
            r.handle_row(_row(oid="101", ref="284000001", attempts=5))
            r.handle_row(_row(oid="102", ref="284000002", attempts=5))
        self.assertEqual(len(sent), 1)

    def test_held_outcome_carries_names(self):
        _chdir_tmp(self)
        eng = backfill.Engine.__new__(backfill.Engine)
        eng.cfg = _cfg()
        eng.live = False
        eng.is_live_sync = True
        eng.legacy = None
        eng.mirror = mock.Mock()
        eng.gio = mock.Mock()
        eng.stats = backfill.Stats()
        eng._stats_lock = __import__("threading").Lock()
        eng._outcome = {}
        order = {"id": 55, "reference_id": 284999999, "items": []}
        eng.route_held(order, 7, [{"id": 1, "name": "Airbrush Combo"},
                                  {"id": 2, "name": "Volume Foam"}])
        outcome, ref = eng._outcome["55"]
        self.assertEqual(outcome, "held")
        self.assertIn("Airbrush Combo", ref)
        self.assertIn("Volume Foam", ref)


class TestStatusStress(unittest.TestCase):
    def test_five_hundred_giveups_bounded_alerts_and_time(self):
        _chdir_tmp(self)
        import status_relay
        import notify
        rows = [{"order_id": str(9000 + i), "status": "held",
                 "note": "catalog gate: X -- in review queue"}
                for i in range(50)]
        cfg = _cfg(live_min_reference=280000000, live_queue_tab="Live Queue")
        r = status_relay.StatusRelay(cfg, FakeHS(), IndexedGIO(rows),
                                     live=True)
        sent = []
        t0 = time.time()
        with mock.patch.object(notify, "send_alert",
                               side_effect=lambda s, b, **k: sent.append(s)):
            for i in range(400):
                r.handle_row(_row(oid=str(20000 + i), ref="250000000",
                                  attempts=5))
            for i in range(50):
                r.handle_row(_row(oid=str(9000 + i), ref="284000000",
                                  attempts=5))
            for i in range(50):
                r.handle_row(_row(oid=str(70000 + i),
                                  ref=str(284100000 + i), attempts=5))
            r._flush_digest(force=True)
        took = time.time() - t0
        self.assertLess(took, 10, f"stress took {took:.1f}s")
        self.assertEqual(len(sent), 52, sent[:5])
        self.assertEqual(len([s for s in sent if "\U0001F534" in s]), 50)
        self.assertEqual(len([s for s in sent if "held for catalog" in s]), 1)
        self.assertEqual(len([s for s in sent if "backfill" in s]), 1)
        self.assertEqual(sum(len(a[1]) for a in r.gio.appends
                             if a[0] == "Delivery Status Exceptions"), 500)


if __name__ == "__main__":
    unittest.main()
