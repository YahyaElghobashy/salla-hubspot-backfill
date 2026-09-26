"""v2.12 Zid collision guard: the Zid import stored Zid order numbers in the
unique salla_order_id, so a Salla order with the same number must never be
matched to, verified against, topped up onto or status-updated on the Zid
record. Offline, against fakes.

Run: python3 -m unittest test_zid_collision -v
"""
import json
import types
import unittest
from pathlib import Path
from unittest import mock

import backfill
import live
import queue_drain
import status_relay
from test_realtime_ext import _chdir_tmp

CONFLICT = {"status": "error", "message": (
    "Cannot set PropertyValueCoordinates{portalId=1, objectTypeId=ObjectTypeId{"
    "legacyObjectType=ORDER}, propertyName=salla_order_id, value=4938528} on "
    "1378000000001. 1337741441227 already has that value.")}
ORDER = {"id": 4938528, "items": [{"id": 346417258}], "amounts": {}, "customer": {},
         "date": {"date": "2026-09-21 03:00:26"}}


def hs_with(results):
    hs = object.__new__(backfill.HubSpot)
    hs.live = True
    hs.cfg = backfill.Config()
    hs.search = mock.Mock(return_value={"results": results})
    return hs


ZID = {"id": "1337741441227", "properties": {"salla_store": "Zid", "hs_source_store": "Zid"}}
SALLA = {"id": "1378000000009", "properties": {"salla_store": "Salla", "hs_source_store": "Salla"}}
# a genuine Salla order that zed_create_missing relabelled on 2026-09-16
MERGED = {"id": "1284409404634", "properties": {"salla_store": "Zid", "hs_source_store": "Salla"}}


class TestLookups(unittest.TestCase):
    def test_zid_holder_is_reported_not_returned(self):
        hs = hs_with([ZID])
        self.assertEqual(hs.orders_by_salla_id("4938528"), (None, "1337741441227"))
        self.assertIsNone(hs.find_order_by_salla_id("4938528"))
        self.assertFalse(hs.dedup_order_exists("4938528"))
        body = hs.search.call_args[0][1]
        self.assertIn("salla_store", body["properties"])

    def test_salla_order_still_found(self):
        hs = hs_with([SALLA])
        self.assertEqual(hs.find_order_by_salla_id("7"), "1378000000009")
        self.assertTrue(hs.dedup_order_exists("7"))
        legacy = hs_with([{"id": "55", "properties": {}}])   # pre-engine Make order
        self.assertEqual(legacy.find_order_by_salla_id("8"), "55")
        copy = hs_with([{"id": "56", "properties": {"salla_store": "Zid (copy)"}}])
        self.assertEqual(copy.find_order_by_salla_id("9"), "56")
        merged = hs_with([MERGED])      # relabelled Salla order: still the Salla order
        self.assertEqual(merged.orders_by_salla_id("57671183"), ("1284409404634", None))
        self.assertIn("hs_source_store", merged.search.call_args[0][1]["properties"])

    def test_zid_item_keys_make_the_order_unsafe(self):
        hs = hs_with([])

        def req(method, path, body=None, is_search=False, what=""):
            if "associations" in path:
                return 200, {"results": [{"toObjectId": 1}]}
            return 200, {"results": [{"properties": {"salla_order_item_id": "Z4938528-1"}}]}
        hs._req = req
        self.assertIsNone(hs.order_item_keys("1337741441227"))


class TestCreate(unittest.TestCase):
    def test_conflict_with_zid_holder_raises(self):
        hs = hs_with([])
        hs._write = lambda m, p, b, w: (400, CONFLICT)
        hs._req = lambda method, path, body=None, is_search=False, what="": (
            200, {"properties": {"salla_order_id": "4938528", "salla_store": "Zid",
                                 "hs_source_store": "Zid"}})
        with mock.patch("time.sleep"), self.assertRaises(backfill.ZidCollision) as cm:
            hs.create_order(ORDER, None, "Asia/Riyadh")
        self.assertEqual(cm.exception.zid_hs_id, "1337741441227")

    def test_search_fallback_finding_only_zid_raises(self):
        hs = hs_with([ZID])
        hs._write = lambda m, p, b, w: (400, {"message": "DUPLICATE"})
        with mock.patch("time.sleep"), self.assertRaises(backfill.ZidCollision):
            hs.create_order(ORDER, None, "Asia/Riyadh")

    def test_salla_holder_still_resolves(self):
        hs = hs_with([])
        hs._write = lambda m, p, b, w: (400, CONFLICT)
        hs._req = lambda method, path, body=None, is_search=False, what="": (
            200, {"properties": {"salla_order_id": "4938528", "salla_store": "Salla"}})
        with mock.patch("time.sleep"):
            self.assertEqual(hs.create_order(ORDER, None, "Asia/Riyadh"),
                             ("1337741441227", False))


class FakeEngine(backfill.Engine):
    def __init__(self, live_mode=True):
        _ = live_mode
        self.cfg = backfill.Config()
        self.cfg.alerts_enabled = True
        self.hs = types.SimpleNamespace(live=live_mode)
        self.mirror = types.SimpleNamespace(dir=Path("mirror"), error=mock.Mock())
        self._outcome, self.stats = {}, {}
        self._bump = lambda k: None


class TestPark(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_finish_create_parks_and_alerts_once(self):
        e = FakeEngine()
        e.hs.create_order = mock.Mock(side_effect=backfill.ZidCollision("4938528", "Z1"))
        e.top_up_items = mock.Mock()
        with mock.patch("notify.send_alert", create=True) as alert:
            e._finish_create(ORDER, -1, None, 1)
            e2 = FakeEngine()
            e2.hs.create_order = e.hs.create_order
            e2._finish_create(ORDER, -1, None, 1)
        state, note = e._outcome["4938528"]
        self.assertEqual(state, "held")
        self.assertTrue(note.startswith("zid collision"))
        self.assertEqual(alert.call_count, 1)                 # once across processes
        e.top_up_items.assert_not_called()
        seen = json.loads(Path("mirror/zid_collisions.json").read_text())
        self.assertEqual(seen["4938528"]["zid_hs_id"], "Z1")
        e.mirror.error.assert_called_once()

    def test_dry_run_writes_no_worklist_and_no_alert(self):
        e = FakeEngine(live_mode=False)
        with mock.patch("notify.send_alert", create=True) as alert:
            e.zid_collision("1", "Z1")
        alert.assert_not_called()
        self.assertFalse(Path("mirror/zid_collisions.json").exists())


class TestLivePath(unittest.TestCase):
    def test_zid_holder_is_parked_not_topped_up(self):
        _chdir_tmp(self)
        f = types.SimpleNamespace()
        f.created_ledger = types.SimpleNamespace(get=lambda oid: None, add=mock.Mock())
        f.hs = types.SimpleNamespace(orders_by_salla_id=lambda oid: (None, "1337741441227"),
                                     order_line_item_count=mock.Mock())
        f.top_up_items = mock.Mock()
        f.zid_collision = lambda oid, z: f"zid collision: {oid} {z}"
        res = live.LiveEngine._resolve_preexisting(f, {"order_id": "4938528"})
        self.assertEqual(res[0], "held")
        self.assertTrue(res[1].startswith("zid collision"))
        f.top_up_items.assert_not_called()
        f.hs.order_line_item_count.assert_not_called()
        f.created_ledger.add.assert_not_called()


class TestDrainPath(unittest.TestCase):
    def test_collision_spends_the_attempt_budget(self):
        f = types.SimpleNamespace(cfg=backfill.Config())
        f.created_ledger = types.SimpleNamespace(get=lambda oid: None)
        f.hs = types.SimpleNamespace(orders_by_salla_id=lambda oid: (None, "Z1"))
        f.zid_collision = lambda oid, z: "zid collision: x"
        f._bump = lambda k: None
        res = queue_drain.QueueDrainEngine.resolve_preexisting(f, "4938528", 1)
        self.assertEqual(res, ("Error", "zid collision: x", f.cfg.live_max_attempts))


class TestStatusRelay(unittest.TestCase):
    def test_zid_match_is_ignored(self):
        f = types.SimpleNamespace(hs=hs_with([ZID]))
        self.assertIsNone(status_relay.StatusRelay._find_order(f, "4938528", "287459205"))
        f = types.SimpleNamespace(hs=hs_with([ZID, SALLA]))
        self.assertEqual(status_relay.StatusRelay._find_order(f, "4938528", "4938528")["id"],
                         "1378000000009")
        f = types.SimpleNamespace(hs=hs_with([MERGED]))
        self.assertEqual(status_relay.StatusRelay._find_order(f, "57671183", "244866476")["id"],
                         "1284409404634")


if __name__ == "__main__":
    unittest.main()
