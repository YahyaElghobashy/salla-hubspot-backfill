"""v2.15 bundle component backfill (tools/backfill_bundle_components.py).

  * parse_instance_key reads "<order>::<item>::<template>" and rejects anything else.
  * components_for swaps the current dryer for the retired one only on orders
    placed before the relist cutoff, and leaves every other component alone.
  * line_props writes the engine's component line: price 0, the engine's
    unique salla_order_item_id "<item>_<code>", quantity = bundle qty x
    component qty, and hs_product_id in the create payload.

Offline. Run: python3 -m unittest test_bundle_component_backfill -v
"""
import unittest

from tools import backfill_bundle_components as bb

COMPS = [{"salla_id": "1982906533", "qty": 1}, {"salla_id": "1724277051", "qty": "1"},
         {"salla_id": "1001864450", "qty": 1}]


class Helpers(unittest.TestCase):
    def test_parse_instance_key(self):
        self.assertEqual(bb.parse_instance_key("101::202::1448268221"), ("101", "202", "1448268221"))
        for bad in ("", None, "101::202", "101::::1448268221", "a::b::c::d"):
            self.assertIsNone(bb.parse_instance_key(bad), bad)

    def test_pre_relist_orders_get_the_retired_dryer(self):
        before = bb.PRE_RELIST_CUTOFF_MS - 1
        got = [c["salla_id"] for c in bb.components_for(before, COMPS)]
        self.assertEqual(got, ["1268166868", "1724277051", "1001864450"])

    def test_post_relist_and_undated_orders_keep_the_current_set(self):
        for ms in (bb.PRE_RELIST_CUTOFF_MS, bb.PRE_RELIST_CUTOFF_MS + 1):
            got = [c["salla_id"] for c in bb.components_for(ms, COMPS)]
            self.assertEqual(got, ["1982906533", "1724277051", "1001864450"])
        # no date: never guess the retired product
        self.assertEqual(bb.components_for(0, COMPS)[0]["salla_id"], "1982906533")

    def test_line_props_match_the_engine(self):
        p = bb.line_props("555", "999", "1448268221", "Dryer set", "2",
                          {"salla_id": "1982906533", "qty": "1"},
                          {"id": "434196269276", "name": "Multi Styler", "hs_sku": "C053_8"}, "SAR")
        self.assertEqual(p["price"], "0")
        self.assertEqual(p["quantity"], "2")
        self.assertEqual(p["salla_order_item_id"], "555_1982906533")
        self.assertEqual(p["hs_product_id"], "434196269276")
        self.assertEqual(p["sale_context"], "bundle_component")
        self.assertTrue(p["is_bundle_component"])
        self.assertEqual(p["hs_line_item_currency_code"], "SAR")
        self.assertEqual(p["bundle_template_key"], "1448268221")


if __name__ == "__main__":
    unittest.main()
