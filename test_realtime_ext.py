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


if __name__ == "__main__":
    unittest.main()
