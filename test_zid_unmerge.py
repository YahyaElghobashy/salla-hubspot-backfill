"""tools/zid_unmerge.py (v2.12): the Zid order is rebuilt through the engine's
own property builder without any write, labelled and keyed as an imported
order. Offline.

Run: python3 -m unittest test_zid_unmerge -v
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent / "tools"))

import backfill
import zid_unmerge as zu

ZID_ORDER = {"id": 41410387, "reference_id": 41410387, "payment_method": "cod",
             "date": {"date": "2025-02-03 10:00:00.000000", "timezone": "Asia/Riyadh"},
             "status": {"slug": "delivered", "name": "Delivered"},
             "customer": {"first_name": "Sahar", "last_name": "K", "mobile": "500000001", "mobile_code": "+966"},
             "amounts": {"total": {"amount": 260}, "sub_total": {"amount": 226}, "tax": {"amount": {"amount": 34}},
                         "shipping_cost": {"amount": 0, "currency": "SAR"}, "discounts": []},
             "items": [{"id": 1}]}


class TestZidProps(unittest.TestCase):
    def test_captured_without_writing_and_relabelled(self):
        hs = object.__new__(backfill.HubSpot)
        hs.live = True
        hs.cfg = backfill.Config()
        hs._write = mock.Mock(side_effect=AssertionError("must not write"))
        with mock.patch.dict(backfill.STATUS_STAGE_MAP, {"delivered": "D"}, clear=False):
            p = zu.zid_props(hs, ZID_ORDER, "Asia/Riyadh")
        hs._write.assert_not_called()
        self.assertEqual(p["salla_order_id"], "Z41410387")
        self.assertEqual(p["salla_order_reference"], "Z41410387")
        self.assertEqual(p["hs_external_order_id"], "Z41410387")
        self.assertEqual((p["salla_store"], p["hs_source_store"]), ("Zid", "Zid"))
        self.assertIn("| Zid |", p["hs_order_name"])
        self.assertEqual(p["delivery_date"], "2025-02-03")
        self.assertEqual(p["last_salla_sync_status"], "delivered")
        self.assertEqual(p["hs_total_price"], "260")


class TestCorpusRepair(unittest.TestCase):
    def test_total_rebuilt_or_refused(self):
        o = {"amounts": {"total": {"amount": "SAR"}, "sub_total": {"amount": "300"},
                         "shipping_cost": {"amount": "25", "currency": "مجفف"}}}
        self.assertTrue(zu.repair_corpus_row(o))
        self.assertEqual(o["amounts"]["total"]["amount"], 325.0)
        self.assertEqual(o["amounts"]["shipping_cost"]["currency"], "SAR")
        bad = {"amounts": {"total": {"amount": "SAR"}, "sub_total": {"amount": "x"}, "shipping_cost": {}}}
        self.assertFalse(zu.repair_corpus_row(bad))


if __name__ == "__main__":
    unittest.main()
