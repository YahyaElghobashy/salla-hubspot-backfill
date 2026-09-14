#!/usr/bin/env python3
"""gift_props: the mapping and every way a payload can try to break it.

The GOOD fixture is synthetic. It mirrors the exact shape of a buy_as_gift
order as returned by the production relay (trimmed to the fields the helper
reads), but every value — ids, names, phones, message, confirmation token —
is invented. Fixtures in this repo are synthetic by policy; never paste a
real payload here.
"""
import copy, json, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill import gift_props, GIFT_TEXT_LIMIT

GOOD = {
    "id": 900000001, "type": "gift", "source": "buy_as_gift",
    "source_details": {"type": "buy_as_gift"},
    "address_incomplete": False,
    "urls": {"gift_confirmation": "https://clarahair.com/en/gifts/TESTTOKEN0TESTTOKEN0TESTTOKEN000"},
    "gift": {"text": "هدية بسيطة وكل عام وأنتم بخير ❤️",
             "image": "https://cdn.salla.sa/example/card.jpg",
             "deliver_at": None, "expiry_date": "2026-10-31 13:40:14"},
    "receiver": {"name": "نور ❤️", "email": "", "phone": "+966500000001", "notify": False},
    "customer": {"first_name": "Saleh", "mobile": "512345678"},
}

NORMAL = {
    "id": 1, "type": "order", "source": "store",
    "source_details": {"type": "store"},
    "urls": {"gift_confirmation": None},
    "gift": None, "receiver": None,
    "customer": {"first_name": "Sara"},
}


class GoodPayload(unittest.TestCase):
    def setUp(self):
        self.p = gift_props(copy.deepcopy(GOOD))

    def test_flag_and_receiver(self):
        self.assertEqual(self.p["is_gift_order"], "true")
        self.assertEqual(self.p["gift_receiver_name"], "نور ❤️")
        self.assertEqual(self.p["gift_receiver_phone"], "+966500000001")

    def test_message_card_and_links(self):
        self.assertIn("هدية", self.p["gift_message"])
        self.assertTrue(self.p["gift_card_image_url"].startswith("https://cdn.salla.sa/"))
        self.assertIn("/gifts/", self.p["gift_confirmation_url"])

    def test_dates_and_booleans(self):
        self.assertEqual(self.p["gift_confirmation_expiry"], "2026-10-31")
        self.assertNotIn("gift_deliver_at", self.p)          # null -> omitted
        self.assertEqual(self.p["gift_receiver_salla_notified"], "false")
        self.assertEqual(self.p["gift_address_incomplete"], "false")

    def test_empty_email_omitted(self):
        self.assertNotIn("gift_receiver_email", self.p)


class NormalOrders(unittest.TestCase):
    def test_normal_order_yields_nothing(self):
        self.assertEqual(gift_props(copy.deepcopy(NORMAL)), {})

    def test_normal_order_with_absent_keys(self):
        self.assertEqual(gift_props({"id": 2, "customer": {}}), {})

    def test_null_customer_regression_shape(self):
        # the engine has already met "customer": null in production
        self.assertEqual(gift_props({"id": 3, "customer": None}), {})


class DetectionEdges(unittest.TestCase):
    def test_type_alone_is_enough(self):
        self.assertEqual(gift_props({"id": 4, "type": "gift"})["is_gift_order"], "true")

    def test_source_alone_is_enough(self):
        self.assertEqual(gift_props({"id": 5, "source": "buy_as_gift"})["is_gift_order"], "true")

    def test_receiver_alone_is_enough(self):
        p = gift_props({"id": 6, "receiver": {"name": "x", "phone": "", "notify": True}})
        self.assertEqual(p["is_gift_order"], "true")
        self.assertEqual(p["gift_receiver_salla_notified"], "true")

    def test_case_and_whitespace(self):
        self.assertTrue(gift_props({"id": 7, "type": " GIFT "}))


class HostileShapes(unittest.TestCase):
    """Every field the wrong type, none of it may raise or leak garbage."""

    def test_gift_is_a_string(self):
        p = gift_props({"id": 8, "type": "gift", "gift": "surprise!", "receiver": []})
        self.assertEqual(p["is_gift_order"], "true")
        self.assertNotIn("gift_message", p)

    def test_receiver_is_a_list(self):
        p = gift_props({"id": 9, "type": "gift", "receiver": ["a", "b"]})
        self.assertNotIn("gift_receiver_name", p)

    def test_phone_variants(self):
        def ph(v):
            return gift_props({"id": 10, "type": "gift",
                               "receiver": {"phone": v}}).get("gift_receiver_phone")
        self.assertEqual(ph("+966 50 000-0001"), "+966500000001")
        self.assertEqual(ph("00500000001"), "+500000001")
        self.assertEqual(ph("966500000001"), "+966500000001")
        self.assertIsNone(ph("call me maybe"))
        self.assertIsNone(ph("+9665abc"))
        self.assertIsNone(ph("123"))            # too short to be real
        self.assertIsNone(ph(None))

    def test_message_truncated_not_rejected(self):
        p = gift_props({"id": 11, "type": "gift", "gift": {"text": "ه" * 99999}})
        self.assertEqual(len(p["gift_message"]), GIFT_TEXT_LIMIT)

    def test_malformed_dates_omitted(self):
        for bad in ("soon", "31-10-2026", 20261031, {"date": "x"}, ""):
            p = gift_props({"id": 12, "type": "gift", "gift": {"expiry_date": bad}})
            self.assertNotIn("gift_confirmation_expiry", p, bad)

    def test_totally_broken_order_returns_empty(self):
        class Evil:
            def get(self, *a, **k): raise RuntimeError("boom")
        self.assertEqual(gift_props(Evil()), {})

    def test_none_order(self):
        self.assertEqual(gift_props({}), {})


class UntrimmedRelayFixture(unittest.TestCase):
    """An untrimmed relay-shaped payload, when a synthetic one is checked in.

    fixtures/gift_order_relay.json must be SYNTHETIC (same policy as GOOD):
    full relay field surface, invented values, receiver phone +966500000001.
    """
    FIX = Path(__file__).resolve().parent / "fixtures" / "gift_order_relay.json"

    def test_full_payload(self):
        if not self.FIX.exists():
            self.skipTest("no synthetic relay fixture checked in")
        p = gift_props(json.loads(self.FIX.read_text()))
        self.assertEqual(p["is_gift_order"], "true")
        self.assertEqual(p["gift_receiver_phone"], "+966500000001")
        self.assertEqual(p["gift_confirmation_expiry"], "2026-10-31")


if __name__ == "__main__":
    unittest.main(verbosity=1)
