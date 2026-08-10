"""Unit tests for the Zid normaliser.

These cover the failure modes that would corrupt the client's CRM silently:
a Zid GUID reaching a Salla id field, a column-swapped row writing hs_sku="1",
a barcode being mistaken for a bundle, a naive timestamp shifting every order
by three hours, and an unmapped status defaulting instead of stopping.
"""
import unittest

import zed_normalize as zn


LEGACY_HDR = ["id", "order_status", "source", "customer_note", "customer_name",
              "customer_email", "customer_mobile", "payment_method",
              "payment_status", "shipping_method", "shipping_short_address",
              "shipping_address", "shipping_city",
              "shipping_company_tracking_id", "googlemaps_location",
              "coupon_code", "coupon_name", "sub_totals", "vat", "shipping",
              "cod", "discount", "total", "currency", "product name", "sku",
              "quantity", "order_products_cost", "unit_price",
              "transaction_reference", "added_at (Asia/Riyadh)",
              "last_update_at (Asia/Riyadh)", "pos_inventory_location",
              "pos_cashier_user_name", "split_payment_method_1_name",
              "split_payment_method_1_total", "split_payment_method_2_name",
              "split_payment_method_2_total"]
LEGACY_IX = {h: i for i, h in enumerate(LEGACY_HDR)}


def legacy_row(**kw):
    r = [None] * len(LEGACY_HDR)
    base = {"id": 2790027, "order_status": "تم التوصيل",
            "customer_name": "Shahad AlMedlej",
            "customer_email": "s@example.com", "customer_mobile": "966555121671",
            "payment_method": "بطاقة إئتمانية", "shipping_city": "الرياض",
            "sub_totals": 100, "vat": 15, "shipping": 20, "total": 135,
            "currency": "SAR", "product name": "Multi Styler", "sku": "C18",
            "quantity": 2, "unit_price": 50,
            "added_at (Asia/Riyadh)": "2020-06-29 07:03 PM"}
    base.update(kw)
    for k, v in base.items():
        r[LEGACY_IX[k]] = v
    return r


RICH_HDR = ["net_sale_price", "net_additions_price", "gross_additions_price",
            "tax_percentage", "tax_amount", "total_value_without_tax_amount",
            "is_discounted", "product_cost", "order_currency_code",
            "product_id", "product_sku", "product_name", "product_name_ar",
            "product_price", "additions_price", "total_value", "net_price",
            "gross_price", "gross_sale_price", "store_id", "order_id",
            "order_tracking_id", "order_date", "delivered_at", "order_code",
            "order_status_name", "order_status_name_ar", "customer_id",
            "customer_name", "customer_email", "customer_telephone",
            "payment_method_code", "payment_method_name",
            "payment_method_name_ar", "shipping_method_code",
            "shipping_method_name", "shipping_method_name_ar",
            "has_different_consignee", "is_guest_customer", "city_name",
            "city_name_ar", "order_source_code", "order_source_name",
            "order_source_name_ar", "vat_value", "shipping_fees",
            "zid_cod_value", "sub_total_value", "coupon_value",
            "products_discount_value", "coupon_cod_discount_value",
            "shipping_discount_value", "free_shipping_coupon_value",
            "total_before_vat_value", "taxable_amount_value", "product_total",
            "is_taxable", "Quantity"]
RICH_IX = {h: i for i, h in enumerate(RICH_HDR)}


def rich_row(**kw):
    r = [None] * len(RICH_HDR)
    base = {"order_currency_code": "SAR",
            "product_id": "3c1e1c63492d4fda9db97e1ef2fe822a",
            "product_sku": "C18", "product_name": "Multi Styler",
            "product_name_ar": "المجفف متعدد الاستخدام",
            "total_value": 897.25, "net_price": 430.43, "gross_price": 495,
            "order_id": 54363806, "order_tracking_id": "ARS2202938562",
            "order_date": "2025-07-01 00:00:00", "order_code": "YQozPWyIZn",
            "order_status_name": "Canceled", "customer_id": 39428,
            "customer_name": "جواهر الشهري", "customer_email": "a@example.com",
            "customer_telephone": "966503474047",
            "payment_method_name": "Cash on Delivery", "city_name": "Riyadh",
            "vat_value": 117.03, "shipping_fees": 21.74, "sub_total_value": 780,
            "Quantity": 1}
    base.update(kw)
    for k, v in base.items():
        r[RICH_IX[k]] = v
    return r


class TestPhone(unittest.TestCase):
    def test_normalisation_table(self):
        cases = [("966555121671", "+966555121671"),
                 ("0555121671", "+966555121671"),
                 ("555121671", "+966555121671"),
                 ("+966 55 512 1671", "+966555121671"),
                 ("00966555121671", "+966555121671"),
                 ("", ""), (None, ""), ("abc", ""), ("12", "")]
        for raw, want in cases:
            self.assertEqual(zn.zed_phone_key(raw), want, raw)

    def test_split_matches_engine_fields(self):
        self.assertEqual(zn.split_phone("+966555121671"), ("966", "555121671"))


class TestSku(unittest.TestCase):
    def test_kinds(self):
        self.assertEqual(zn.classify_sku("C18")[0], "single")
        self.assertEqual(zn.classify_sku("C18CH11CH10")[0], "composite")
        self.assertEqual(zn.classify_sku("C13CH9CH10C45C46")[0], "composite")
        self.assertEqual(zn.classify_sku("6287032431307")[0], "barcode")
        self.assertEqual(zn.classify_sku("")[0], "empty")

    def test_barcode_is_never_a_bundle(self):
        """A 13-digit barcode must not tokenize into a composite."""
        self.assertFalse(zn.is_composite("6287032431307"))

    def test_composite_requires_known_tokens(self):
        singles = {"C18", "CH11", "CH10"}
        self.assertTrue(zn.is_composite("C18CH11CH10", singles))
        self.assertFalse(zn.is_composite("C18ZZ99", singles))

    def test_tokens(self):
        self.assertEqual(zn.classify_sku("C18CH11CH10")[1],
                         ["C18", "CH11", "CH10"])


class TestColumnSwap(unittest.TestCase):
    def test_repairs_and_recovers_quantity(self):
        sku, name, qty, fixed = zn.repair_column_swap("1", "C18")
        self.assertEqual((sku, qty, fixed), ("C18", "1", True))

    def test_repairs_barcode_variant(self):
        """~10,900 rows in 2025 put a barcode in the name column."""
        sku, name, qty, fixed = zn.repair_column_swap("1", "6287032431307")
        self.assertEqual((sku, qty, fixed), ("6287032431307", "1", True))

    def test_leaves_good_rows_alone(self):
        sku, name, qty, fixed = zn.repair_column_swap("C18", "Multi Styler")
        self.assertEqual((sku, name, fixed), ("C18", "Multi Styler", False))

    def test_swapped_row_yields_real_sku_not_one(self):
        m = zn.LegacyMapper()
        row = legacy_row(sku=1, **{"product name": "C18"})
        o = m.build("2790027", [row], LEGACY_IX, {"C18": "Multi Styler"})
        self.assertEqual(o["items"][0]["sku"], "C18")
        self.assertNotEqual(o["items"][0]["sku"], "1")
        self.assertEqual(o["items"][0]["name"], "Multi Styler")


class TestJunk(unittest.TestCase):
    def test_null_status_and_mobile_is_junk(self):
        self.assertTrue(zn.is_junk_row(None, None))
        self.assertTrue(zn.is_junk_row("", ""))

    def test_either_field_present_is_kept(self):
        self.assertFalse(zn.is_junk_row("تم التوصيل", None))
        self.assertFalse(zn.is_junk_row(None, "966555121671"))


class TestStatus(unittest.TestCase):
    def test_arabic_and_english(self):
        self.assertEqual(zn.status_slug("تم التوصيل")[0], "delivered")
        self.assertEqual(zn.status_slug("تم الإلغاء")[0], "canceled")
        self.assertEqual(zn.status_slug("Delivered")[0], "delivered")
        self.assertEqual(zn.status_slug("In Delivery")[0], "delivering")

    def test_census_statuses_all_map(self):
        """Every distinct status the census found across all 7 files."""
        for v in ("\u062a\u0645 \u0627\u0644\u062a\u0648\u0635\u064a\u0644",
                  "\u062a\u0645 \u0627\u0644\u0625\u0644\u063a\u0627\u0621",
                  "\u062c\u0627\u0631\u064a \u0627\u0644\u062a\u0648\u0635\u064a\u0644",
                  "\u062c\u062f\u064a\u062f", "\u062a\u062c\u0647\u064a\u0632",
                  "\u0645\u0633\u062a\u0631\u062c\u0639",
                  "\u0645\u0633\u062a\u0631\u062c\u0639 \u062c\u0632\u0626\u064a",
                  "\u062c\u0627\u0647\u0632",
                  "Delivered", "Canceled", "In Delivery", "New",
                  "Prepairing", "Ready"):
            slug, _ = zn.status_slug(v)
            self.assertTrue(slug, f"unmapped: {v}")

    def test_unmapped_raises_rather_than_defaulting(self):
        with self.assertRaises(zn.UnmappedStatus):
            zn.status_slug("حالة غير معروفة")

    def test_empty_is_not_an_error(self):
        self.assertEqual(zn.status_slug(None), ("", ""))


class TestTimestamps(unittest.TestCase):
    def test_legacy_12h_clock(self):
        self.assertEqual(zn._dt("2020-06-29 07:03 PM"), "2020-06-29 19:03:00")
        self.assertEqual(zn._dt("2020-06-29 07:03 AM"), "2020-06-29 07:03:00")

    def test_rich_24h(self):
        self.assertEqual(zn._dt("2025-07-01 00:00:00"), "2025-07-01 00:00:00")

    def test_timezone_is_stamped_riyadh(self):
        """A naive pass would shift every hs_external_created_date by 3h."""
        for mapper, row, ix in ((zn.LegacyMapper(), legacy_row(), LEGACY_IX),
                                (zn.RichMapper(), rich_row(), RICH_IX)):
            o = mapper.build("1", [row], ix, {})
            self.assertEqual(o["date"]["timezone"], "Asia/Riyadh")


class TestCanonicalShape(unittest.TestCase):
    def test_product_is_none_on_every_item(self):
        """Forces the engine's legacy-SKU path; a GUID here holds forever."""
        for mapper, row, ix in ((zn.LegacyMapper(), legacy_row(), LEGACY_IX),
                                (zn.RichMapper(), rich_row(), RICH_IX)):
            o = mapper.build("1", [row], ix, {})
            self.assertIsNone(o["items"][0]["product"])

    def test_zid_ids_never_reach_salla_fields(self):
        o = zn.RichMapper().build("54363806", [rich_row()], RICH_IX, {})
        self.assertEqual(o["customer"]["id"], "")
        self.assertEqual(o["_zed"]["zid_customer_id"], "39428")
        self.assertEqual(o["items"][0]["_zed"]["zid_product_id"],
                         "3c1e1c63492d4fda9db97e1ef2fe822a")

    def test_engine_required_paths_exist(self):
        """The exact nested paths backfill.create_order reads."""
        o = zn.LegacyMapper().build("2790027", [legacy_row()], LEGACY_IX, {})
        self.assertTrue(o["date"]["date"])
        self.assertTrue(o["amounts"]["shipping_cost"]["currency"])
        self.assertTrue(o["amounts"]["tax"]["amount"]["amount"])
        self.assertTrue(o["amounts"]["total"]["amount"])
        self.assertIn("mobile_code", o["customer"])
        self.assertIn("mobile", o["customer"])

    def test_item_ids_are_deterministic(self):
        """Re-running the emitter must not mint new line items."""
        rows = [legacy_row(sku="C18"), legacy_row(sku="C26")]
        a = zn.LegacyMapper().build("777", rows, LEGACY_IX, {})
        b = zn.LegacyMapper().build("777", rows, LEGACY_IX, {})
        self.assertEqual([i["id"] for i in a["items"]], ["Z777-1", "Z777-2"])
        self.assertEqual([i["id"] for i in a["items"]],
                         [i["id"] for i in b["items"]])

    def test_multi_row_order_groups_into_one(self):
        rows = [legacy_row(sku="C18"), legacy_row(sku="C26"), legacy_row(sku="C41")]
        o = zn.LegacyMapper().build("2790027", rows, LEGACY_IX, {})
        self.assertEqual(len(o["items"]), 3)
        self.assertEqual(o["id"], "2790027")

    def test_rich_reference_id_is_order_code(self):
        o = zn.RichMapper().build("54363806", [rich_row()], RICH_IX, {})
        self.assertEqual(o["reference_id"], "YQozPWyIZn")

    def test_mapper_autodetect(self):
        self.assertIsInstance(zn.mapper_for(RICH_IX), zn.RichMapper)
        self.assertIsInstance(zn.mapper_for(LEGACY_IX), zn.LegacyMapper)


if __name__ == "__main__":
    unittest.main()
