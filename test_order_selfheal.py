"""v2.11 partial-order self-heal: the duplicate guardrail reads the holder id
from HubSpot's 400, unique-key conflicts reuse the existing record, the keyed
top-up, the recovered-create path, per-thread growth signals, and the live
verify path. Offline, against fakes.

Run: python3 -m unittest test_order_selfheal -v
"""
import threading
import types
import unittest
from unittest import mock

import backfill
import live

CONFLICT_LI = {"status": "error", "message": (
    "Cannot set PropertyValueCoordinates{portalId=1, objectTypeId=ObjectTypeId{"
    "legacyObjectType=LINE_ITEM}, propertyName=salla_order_item_id, value=840737206} "
    "on 487000000001. 487168139499 already has that value.")}
CONFLICT_ORDER = {"status": "error", "message": (
    "Cannot set PropertyValueCoordinates{portalId=1, objectTypeId=ObjectTypeId{"
    "legacyObjectType=ORDER}, propertyName=salla_order_id, value=465318964} on "
    "1374378254574. 1374395867335 already has that value.")}


def bare_hs():
    hs = object.__new__(backfill.HubSpot)
    hs.live = True
    return hs


class TestConflicts(unittest.TestCase):
    def test_holder_id_only_for_that_property(self):
        f = backfill.HubSpot.existing_id_from_conflict
        self.assertEqual(f(CONFLICT_LI, "salla_order_item_id"), "487168139499")
        self.assertIsNone(f(CONFLICT_LI, "salla_order_id"))
        self.assertIsNone(f({"message": "boom"}, "salla_order_item_id"))

    def test_line_item_create_reuses_existing(self):
        hs = bare_hs()
        hs._write = lambda m, p, b, w: (400, CONFLICT_LI)
        self.assertEqual(hs.create_line_item({"salla_order_item_id": "840737206"}, "LI"),
                         "487168139499")
        hs._write = lambda m, p, b, w: (500, {"message": "server"})
        self.assertIsNone(hs.create_line_item({}, "LI"))

    def test_duplicate_order_resolves_from_body_when_confirmed(self):
        hs = bare_hs()
        hs.cfg = backfill.Config()
        hs._write = lambda m, p, b, w: (400, CONFLICT_ORDER)
        hs._req = lambda method, path, body=None, is_search=False, what="": (
            200, {"properties": {"salla_order_id": "465318964", "salla_store": "Salla"}})
        hs.orders_by_salla_id = mock.Mock(return_value=("SEARCHED", None))
        order = {"id": 465318964, "items": [], "date": {"date": "2026-09-23 13:08:00"},
                 "amounts": {}, "customer": {}}
        with mock.patch("time.sleep"):
            self.assertEqual(hs.create_order(order, None, "Asia/Riyadh"),
                             ("1374395867335", False))
        hs.orders_by_salla_id.assert_not_called()
        hs._req = lambda method, path, body=None, is_search=False, what="": (
            200, {"properties": {"salla_order_id": "someone else"}})
        with mock.patch("time.sleep"):
            self.assertEqual(hs.create_order(order, None, "Asia/Riyadh"),
                             ("SEARCHED", False))


class FakeEngine(backfill.Engine):
    """Engine without __init__: only what top_up_items touches."""

    def __init__(self, keys_seq, unverified=()):
        self.hs = types.SimpleNamespace(order_item_keys=mock.Mock(side_effect=keys_seq))
        self.unverified = list(unverified)
        self.processed = []
        self.mirror = mock.Mock()

    def gate_unverified_items(self, order):
        return self.unverified

    def process_item(self, order, order_id, item):
        self.processed.append(str(item["id"]))


ORDER = {"id": 7, "items": [{"id": 101}, {"id": 102}]}


class TestTopUp(unittest.TestCase):
    def test_states(self):
        self.assertEqual(FakeEngine([None]).top_up_items(ORDER, "H"), ("unsafe", 0))
        e = FakeEngine([{"101", "102_C1"}])
        self.assertEqual(e.top_up_items(ORDER, "H"), ("complete", 0))
        e = FakeEngine([{"101"}], unverified=[{"id": 102}])
        self.assertEqual(e.top_up_items(ORDER, "H"), ("held", 0))
        self.assertEqual(e.processed, [])
        e = FakeEngine([{"101"}, {"101", "102"}])
        self.assertEqual(e.top_up_items(ORDER, "H"), ("complete", 1))
        self.assertEqual(e.processed, ["102"])
        e = FakeEngine([set(), {"101"}])
        self.assertEqual(e.top_up_items(ORDER, "H"), ("partial", 2))

    def test_recovered_create_ledgers_and_marks_created(self):
        e = FakeEngine([{"101"}, {"101", "102"}])
        e.cfg = backfill.Config()
        e._outcome, e.stats = {}, {}
        e._bump = lambda k: None
        e.created_ledger = mock.Mock()
        e._stamp_signals = mock.Mock()
        e.hs.create_order = mock.Mock(return_value=("H9", False))
        e.hs.patch_order = mock.Mock()
        e._finish_create(ORDER, -1, "C1", 1)
        e.created_ledger.add.assert_called_once_with("7", "H9")
        self.assertEqual(e._outcome["7"], ("created", "H9"))
        e.hs.patch_order.assert_called_once()


class TestSignalsPerThread(unittest.TestCase):
    def test_lanes_do_not_share_signals(self):
        e = object.__new__(backfill.Engine)
        e._order_signals = {"device"}
        seen = {}

        def lane():
            e._order_signals = set()
            e._order_signals.add("consumable")
            seen["lane"] = set(e._order_signals)

        t = threading.Thread(target=lane)
        t.start(); t.join()
        self.assertEqual(e._order_signals, {"device"})
        self.assertEqual(seen["lane"], {"consumable"})


class TestLiveVerifyPath(unittest.TestCase):
    def fake(self, li, topup):
        f = types.SimpleNamespace()
        f.created_ledger = types.SimpleNamespace(get=lambda oid: None, add=mock.Mock())
        f.hs = types.SimpleNamespace(orders_by_salla_id=lambda oid: ("H1", None),
                                     order_line_item_count=mock.Mock(return_value=li))
        f.relay = types.SimpleNamespace(fetch_orders=lambda ids: {"7": ORDER})
        f.top_up_items = mock.Mock(return_value=topup)
        f._topups_left = 3
        f.mirror = mock.Mock()
        f._bump = lambda k: None
        return f

    def test_topped_up_order_is_done(self):
        f = self.fake(li=1, topup=("complete", 1))
        res = live.LiveEngine._resolve_preexisting(f, {"order_id": "7"})
        self.assertEqual(res[0], "done")
        f.created_ledger.add.assert_called_once_with("7", "H1")
        self.assertEqual(f._topups_left, 2)

    def test_unsafe_falls_back_to_count(self):
        f = self.fake(li=2, topup=("unsafe", 0))
        self.assertEqual(live.LiveEngine._resolve_preexisting(f, {"order_id": "7"})[0], "done")

    def test_still_short_is_partial(self):
        f = self.fake(li=5, topup=("partial", 1))
        self.assertEqual(live.LiveEngine._resolve_preexisting(f, {"order_id": "7"})[0], "error")

    def test_over_the_cap_uses_count_only(self):
        f = self.fake(li=0, topup=("complete", 2))
        f._topups_left = 0
        self.assertEqual(live.LiveEngine._resolve_preexisting(f, {"order_id": "7"})[0], "error")
        f.top_up_items.assert_not_called()


if __name__ == "__main__":
    unittest.main()
