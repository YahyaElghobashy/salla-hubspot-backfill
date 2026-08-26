"""Unit tests for the Zid normaliser.

These cover the failure modes that would corrupt the client's CRM silently:
a Zid GUID reaching a Salla id field, a column-swapped row writing hs_sku="1",
a barcode being mistaken for a bundle, a naive timestamp shifting every order
by three hours, and an unmapped status defaulting instead of stopping.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import backfill
import zed_normalize as zn
import zed_plan as zp
import zed_snapshot as zs


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

    def test_vocabulary_catches_what_shape_matching_missed(self):
        """1,656 items shape-matching missed: a five-digit run (SKU_SHAPE_RE
        allows three) and a purely alphabetic SKU that is real and approved."""
        vocab = {"CH91011C45C46", "BRUSHES"}
        for displaced in ("CH91011C45C46", "brushes"):
            with self.subTest(displaced):
                self.assertFalse(zn.repair_column_swap("1", displaced)[3],
                                 "shape matching alone should miss this")
                sku, _, qty, fixed = zn.repair_column_swap("1", displaced,
                                                           vocab)
                self.assertTrue(fixed)
                self.assertEqual((sku, qty), (displaced, "1"))

    def test_vocabulary_does_not_swallow_real_product_names(self):
        """A one-word ASCII product name is exactly what a loosened shape rule
        would misread as a SKU; membership keeps it a name."""
        sku, name, _, fixed = zn.repair_column_swap("1", "AirGlow",
                                                    {"C18", "BRUSHES"})
        self.assertFalse(fixed)
        self.assertEqual((sku, name), ("1", "AirGlow"))

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


class TestRowShift(unittest.TestCase):
    """Legacy rows with no coupon_name lose the CELL, sliding every later
    column one left. 56,993 orders (5.9%). The row stays plausible, which is
    what makes it dangerous: total becomes the currency string, sku becomes
    the quantity, and added_at becomes last_update -- so the order is filed
    under the wrong month."""

    def _row(self, shifted):
        r = legacy_row(**{"coupon_code": "-", "sub_totals": 528.85, "vat": 0,
                          "shipping": 48.97, "cod": 0, "discount": 0,
                          "total": 577.81, "currency": "AED",
                          "product name": "المجفف", "sku": "C18CH8",
                          "quantity": 1})
        if not shifted:
            return r
        at = LEGACY_IX["coupon_name"]
        return tuple(list(r[:at]) + list(r[at + 1:]) + [None])

    def test_aligned_rows_are_left_alone(self):
        row = self._row(shifted=False)
        out, shift = zn.repair_row_shift(row, LEGACY_IX)
        self.assertEqual(shift, 0)
        self.assertEqual(out[LEGACY_IX["total"]], 577.81)

    def test_shifted_row_is_realigned(self):
        out, shift = zn.repair_row_shift(self._row(shifted=True), LEGACY_IX)
        self.assertEqual(shift, 1)
        self.assertEqual(out[LEGACY_IX["total"]], 577.81)
        self.assertEqual(out[LEGACY_IX["currency"]], "AED")
        self.assertEqual(out[LEGACY_IX["sku"]], "C18CH8")
        self.assertEqual(out[LEGACY_IX["sub_totals"]], 528.85)

    def test_the_date_is_what_makes_this_urgent(self):
        """A shifted row reads last_update as added_at, so the order lands in
        the wrong monthly file."""
        ix = LEGACY_IX
        dt = "added_at (Asia/Riyadh)"
        row = self._row(shifted=True)
        self.assertNotEqual(row[ix[dt]], self._row(shifted=False)[ix[dt]])
        out, _ = zn.repair_row_shift(row, ix)
        self.assertEqual(out[ix[dt]], self._row(shifted=False)[ix[dt]])

    def test_total_is_never_left_as_a_currency_code(self):
        out, _ = zn.repair_row_shift(self._row(shifted=True), LEGACY_IX)
        self.assertNotIn(str(out[LEGACY_IX["total"]]).upper(), zn.CURRENCIES)


class TestEmitterPlanParsing(unittest.TestCase):
    """The emitter must understand every op the planner records, refuse what
    it does not, and fold the status PATCH into the create."""

    def _plan_lines(self):
        return [
            {"op": "POST", "path": "/crm/v3/objects/orders", "sym": "§1",
             "order_id": "9", "what": "o",
             "body": {"properties": {"salla_order_id": "9"},
                      "associations": []}},
            {"op": "PATCH", "path": "/crm/v3/objects/orders/§1",
             "order_id": "9", "sym": "§2", "what": "s",
             "body": {"properties": {"last_salla_sync_status": "synced"}}},
            {"op": "POST", "path": "/crm/v3/objects/line_items", "sym": "§3",
             "order_id": "9", "what": "li",
             "body": {"properties": {"salla_order_item_id": "Z9-1"}}},
            {"op": "POST",
             "path": "/crm/v4/associations/order/line_items/batch/create",
             "order_id": "9", "sym": "§4", "what": "a",
             "body": {"inputs": [{"from": {"id": "§1"}, "to": {"id": "§3"},
                                  "types": []}]}},
        ]

    def _write_plan(self, tmp, lines):
        import zed_emit
        plans = Path(tmp) / "plans"; norm = Path(tmp) / "norm"
        plans.mkdir(); norm.mkdir()
        pf = plans / "2020-01.plan.jsonl"
        pf.write_text("\n".join(json.dumps(x) for x in lines))
        return zed_emit, plans, norm

    def test_parses_and_folds(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            ze, plans, norm = self._write_plan(tmp, self._plan_lines())
            old_p, old_n = ze.PLANS, ze.NORM
            ze.PLANS, ze.NORM = plans, norm
            try:
                orders = ze.load_plan("2020-01")
            finally:
                ze.PLANS, ze.NORM = old_p, old_n
            o = orders["9"]
            self.assertEqual(o["status"], "synced")     # folded, not sent
            self.assertEqual(len(o["lis"]), 1)
            self.assertEqual(len(o["assoc"]), 1)

    def test_unknown_op_refuses_the_month(self):
        import tempfile
        bad = self._plan_lines() + [{"op": "DELETE", "path": "/x",
                                     "order_id": "9", "sym": "§9",
                                     "what": "?", "body": {}}]
        with tempfile.TemporaryDirectory() as tmp:
            ze, plans, norm = self._write_plan(tmp, bad)
            old_p, old_n = ze.PLANS, ze.NORM
            ze.PLANS, ze.NORM = plans, norm
            try:
                with self.assertRaises(SystemExit):
                    ze.load_plan("2020-01")
            finally:
                ze.PLANS, ze.NORM = old_p, old_n

    def test_stale_plan_refused(self):
        import tempfile, gzip, os, time
        with tempfile.TemporaryDirectory() as tmp:
            ze, plans, norm = self._write_plan(tmp, self._plan_lines())
            nf = norm / "2020-01.jsonl.gz"
            with gzip.open(nf, "wt") as f:
                f.write("{}")
            past = time.time() - 9999
            os.utime(plans / "2020-01.plan.jsonl", (past, past))
            old_p, old_n = ze.PLANS, ze.NORM
            ze.PLANS, ze.NORM = plans, norm
            try:
                with self.assertRaises(SystemExit):
                    ze.load_plan("2020-01")
            finally:
                ze.PLANS, ze.NORM = old_p, old_n


class TestBundleExpansion(unittest.TestCase):
    """Zid items carry product=None, so all of them take the legacy standalone
    route and the engine's bundle machinery never runs. Flat, a device-bearing
    bundle yields ZERO warranties (the warranty engine only covers
    class=device line items): 220,383 orders, 366,890 devices. Expansion at
    normalisation is the fix, and these pin its contract."""

    EXP = {"C1C3": {"components": [
        {"sku": "C1", "qty": 1, "is_device": True},
        {"sku": "C3", "qty": 1, "is_device": True}], "device_count": 2}}

    def _mapper(self):
        m = zn.LegacyMapper()
        m.aliases, m.excluded, m.expansions = {}, frozenset(), self.EXP
        return m

    def test_bundle_becomes_parent_plus_components(self):
        o = self._mapper().build("77", [legacy_row(sku="C1C3")], LEGACY_IX, {})
        self.assertEqual([i["sku"] for i in o["items"]], ["C1C3", "C1", "C3"])
        self.assertEqual([i.get("_zed_sale_context") for i in o["items"]],
                         ["bundle_parent", "bundle_component",
                          "bundle_component"])

    def test_money_counted_once(self):
        """Parent keeps the price; components are 0. Anything else double
        counts revenue."""
        o = self._mapper().build("77", [legacy_row(sku="C1C3")], LEGACY_IX, {})
        parent, c1, c3 = o["items"]
        self.assertNotEqual(str(parent["amounts"]["price_without_tax"]["amount"]), "0")
        for c in (c1, c3):
            self.assertEqual(str(c["amounts"]["price_without_tax"]["amount"]), "0")

    def test_component_ids_extend_the_parent_deterministically(self):
        o = self._mapper().build("77", [legacy_row(sku="C1C3")], LEGACY_IX, {})
        parent = o["items"][0]
        self.assertEqual([i["id"] for i in o["items"][1:]],
                         [f"{parent['id']}.1", f"{parent['id']}.2"])

    def test_quantity_multiplies_into_components(self):
        o = self._mapper().build("77", [legacy_row(sku="C1C3", quantity=2)],
                                 LEGACY_IX, {})
        self.assertEqual([i["quantity"] for i in o["items"][1:]], ["2", "2"])

    def test_non_bundle_items_pass_through_unchanged(self):
        o = self._mapper().build("77", [legacy_row(sku="C2")], LEGACY_IX, {})
        self.assertEqual(len(o["items"]), 1)
        self.assertNotIn("_zed_sale_context", o["items"][0])

    def test_no_table_means_no_expansion(self):
        m = self._mapper(); m.expansions = {}
        o = m.build("77", [legacy_row(sku="C1C3")], LEGACY_IX, {})
        self.assertEqual(len(o["items"]), 1)


class TestZedContextHint(unittest.TestCase):
    """The engine honours the normaliser's hint, and ONLY the hint: a live
    Salla item never carries it, so live props must pass through untouched."""

    def test_absent_hint_is_identity(self):
        props = {"sale_context": "standalone_product",
                 "revenue_attribution_method": "standalone_revenue"}
        out = backfill.zed_context({"sku": "C2"}, props)
        self.assertIs(out, props)      # not even copied

    def test_parent_hint(self):
        out = backfill.zed_context(
            {"_zed_sale_context": "bundle_parent"},
            {"sale_context": "standalone_product",
             "revenue_attribution_method": "standalone_revenue"})
        self.assertEqual(out["sale_context"], "bundle_parent")
        self.assertEqual(out["revenue_attribution_method"],
                         "bundle_parent_revenue")
        self.assertTrue(out["is_bundle_parent"])

    def test_component_hint(self):
        out = backfill.zed_context(
            {"_zed_sale_context": "bundle_component"},
            {"sale_context": "standalone_product",
             "revenue_attribution_method": "standalone_revenue"})
        self.assertEqual(out["sale_context"], "bundle_component")
        self.assertEqual(out["revenue_attribution_method"],
                         "component_quantity_only")
        self.assertNotIn("is_bundle_parent", out)


class TestProductStampedAtCreate(unittest.TestCase):
    """HubSpot copies product_class and warranty_months onto a line item only
    when hs_product_id is in the CREATE call. Stamping it afterwards by PATCH
    associates the product but copies nothing, which is why 294,264 line items
    carry no classification against a tagged catalogue.

    These guard the shape of the payload, which is the thing that actually
    decides whether the warranty system has anything to read."""

    def test_id_goes_into_the_payload(self):
        out = backfill.with_product({"name": "Curler"}, "419285918921")
        self.assertEqual(out["hs_product_id"], "419285918921")
        self.assertEqual(out["name"], "Curler")

    def test_caller_props_are_not_mutated(self):
        """The bundle paths reuse a shared props dict across components."""
        src = {"name": "Curler"}
        backfill.with_product(src, "1")
        self.assertNotIn("hs_product_id", src)

    def test_empty_id_is_omitted_not_sent_blank(self):
        """Sending "" risks a 400 that would cost the whole line item, where
        the PATCH it replaces was merely a no-op."""
        for empty in ("", None, "   "):
            out = backfill.with_product({"name": "X"}, empty)
            self.assertNotIn("hs_product_id", out)

    def test_no_hs_product_id_patch_remains_in_the_engine(self):
        """If anyone reintroduces the PATCH, inheritance silently stops."""
        src = Path(backfill.__file__).read_text()
        self.assertNotIn("stamp product on LI", src)
        self.assertNotIn("stamp parent LI", src)
        self.assertNotIn("stamp component LI", src)

    def test_all_five_create_sites_stamp_the_product(self):
        src = Path(backfill.__file__).read_text()
        self.assertEqual(src.count("with_product("), 6,  # 5 calls + 1 def
                         "every create_line_item site must stamp the product")


class TestNullCustomerBlock(unittest.TestCase):
    """Salla sends "customer": null on real orders. dict.get(k, {}) does NOT
    protect against that: the default only applies when the key is ABSENT, so
    c became None and c.get("id") raised
    'NoneType' object has no attribute 'get'. Orders 333560771 and 367920962
    died this way in the 2026-08-25 drain, after surviving the earlier phone
    fix, because the failure had simply moved one step down."""

    def _hs(self):
        hs = object.__new__(backfill.HubSpot)
        hs.cfg = backfill.Config()
        hs._write = lambda *a, **k: (201, {"id": "1"})
        return hs

    def test_null_customer_does_not_crash(self):
        hs = self._hs()
        self.assertEqual(hs.create_contact({"id": "1", "customer": None}), "1")

    def test_absent_customer_does_not_crash(self):
        self.assertEqual(self._hs().create_contact({"id": "1"}), "1")

    def test_null_amounts_does_not_crash(self):
        """Same shape of bug on the order side."""
        hs = self._hs()
        hs.cfg.salla_timezone_default = "Asia/Riyadh"
        try:
            hs.create_order({"id": "1", "customer": None, "amounts": None,
                             "date": {"date": "2026-01-01 00:00:00"}},
                            "123", "Asia/Riyadh")
        except AttributeError as e:
            self.fail(f"null amounts crashed: {e}")


class TestContactPhoneMatching(unittest.TestCase):
    """A contact could be invisible to the search and still reject the create
    on a uniqueness violation, because the search covered only `phone` while
    the UNIQUE property is `main_phone_number` storing the +E.164 form. The
    order then failed with no contact and no explanation. Seen live on order
    1611507997: "794345860327 already has that value"."""

    def _hs(self):
        hs = object.__new__(backfill.HubSpot)
        hs.cfg = backfill.Config()
        return hs

    def test_groups_cover_both_properties_and_both_spellings(self):
        g = self._hs()._phone_filter_groups("966", "504947749")
        pairs = {(f["propertyName"], f["value"])
                 for grp in g for f in grp["filters"]}
        self.assertIn(("phone", "504947749"), pairs)
        self.assertIn(("phone", "966504947749"), pairs)
        self.assertIn(("main_phone_number", "+966504947749"), pairs)
        self.assertIn(("main_phone_number", "966504947749"), pairs)

    def test_stays_within_hubspots_five_group_ceiling(self):
        """Retry adds salla_customer_id on top, so this must leave room."""
        self.assertLessEqual(len(self._hs()._phone_filter_groups("966", "5")), 4)

    def test_missing_phone_does_not_reach_hubspot(self):
        """HubSpot rejects an empty EQ value, search() then returned None and
        the caller dereferenced it: 'NoneType' object has no attribute 'get'.
        30 orders in the Zid corpus have no phone at all."""
        hs = self._hs()
        called = []
        hs.search = lambda *a, **k: called.append(1)
        self.assertEqual(hs.search_contact_by_phone("966", ""), (None, 0))
        self.assertEqual(hs.search_contact_by_phone("966", None), (None, 0))
        self.assertFalse(called, "no phone must not produce a search call")

    def test_a_failed_search_returns_cleanly_instead_of_crashing(self):
        hs = self._hs()
        hs.search = lambda *a, **k: None
        self.assertEqual(hs.search_contact_by_phone("966", "504947749"), (None, 0))


class TestContinuationRowsSurvive(unittest.TestCase):
    """The legacy export is one row per LINE ITEM with the order-level columns
    filled in on the first row only. Testing junk per ROW deleted every
    continuation line of every multi-item basket: 58,096 items across 48,986
    orders, showing up as a 0.09% multi-item rate for legacy against 16.25%
    for the same store in the rich file."""

    def _order(self, n_items):
        rows = [legacy_row(sku="C1")]
        for i in range(n_items - 1):
            # a continuation row: real SKU, no order-level columns
            r = list(legacy_row(sku="C2"))
            for col in ("order_status", "customer_mobile"):
                r[LEGACY_IX[col]] = None
            rows.append(tuple(r))
        return rows

    def test_a_two_line_basket_keeps_both_lines(self):
        m = zn.LegacyMapper()
        o = m.build("2793503", self._order(2), LEGACY_IX, {})
        self.assertEqual(len(o["items"]), 2)
        self.assertEqual([i["sku"] for i in o["items"]], ["C1", "C2"])

    def test_head_is_the_row_that_has_order_level_data(self):
        """The head must be picked, not assumed to be rows[0]: a continuation
        row would yield an empty status and no customer."""
        m = zn.LegacyMapper()
        rows = self._order(3)
        rows = [rows[1], rows[0], rows[2]]        # header not first
        o = m.build("1", rows, LEGACY_IX, {})
        self.assertTrue(o["customer"]["mobile"], "customer lost")
        self.assertEqual(len(o["items"]), 3)

    def test_group_is_junk_only_when_no_row_has_order_data(self):
        m = zn.LegacyMapper()
        good = self._order(2)
        self.assertFalse(m.group_is_junk(good, LEGACY_IX))
        allbad = good[1:]
        self.assertTrue(m.group_is_junk(allbad, LEGACY_IX))

    def test_continuation_row_inherits_order_currency(self):
        m = zn.LegacyMapper()
        rows = self._order(2)
        r = list(rows[1]); r[LEGACY_IX["currency"]] = None
        o = m.build("1", [rows[0], tuple(r)], LEGACY_IX, {})
        self.assertEqual(o["items"][1]["currency"],
                         o["items"][0]["currency"])


class TestRichSubTotal(unittest.TestCase):
    """sub_total_value is 0 on 167,586 of 231,497 rich orders while
    taxable_amount_value carries the real figure; where sub_total_value IS
    populated it is sometimes VAT-inclusive. The identity decides, not the
    column name."""

    def _row(self, st, tx, vat, tot):
        r = list(rich_row())
        for col, v in (("sub_total_value", st), ("taxable_amount_value", tx),
                       ("vat_value", vat), ("total_value", tot)):
            r[RICH_IX[col]] = v
        return tuple(r)

    def test_uses_taxable_when_subtotal_is_zero(self):
        v = zn.rich_sub_total(self._row(0, 780.22, 117.03, 897.25), RICH_IX)
        self.assertEqual(float(v), 780.22)

    def test_uses_subtotal_when_it_satisfies_the_identity(self):
        v = zn.rich_sub_total(self._row(469.57, 0, 70.43, 540.0), RICH_IX)
        self.assertEqual(float(v), 469.57)

    def test_refuses_a_vat_inclusive_subtotal(self):
        """st == total means the column is VAT-inclusive; taxable is right."""
        v = zn.rich_sub_total(self._row(540.0, 469.57, 70.43, 540.0), RICH_IX)
        self.assertEqual(float(v), 469.57)


class TestCurrencyVocabulary(unittest.TestCase):
    def test_iqd_is_present(self):
        """43 legacy rows carry IQD; without it repair_row_shift cannot anchor
        and 83 orders wrote the string "IQD" into hs_total_price."""
        self.assertIn("IQD", zn.CURRENCIES)

    def test_shift_repair_anchors_on_iqd(self):
        r = legacy_row(**{"coupon_code": "-", "currency": "IQD",
                          "total": 209843.281, "sub_totals": 192327.147})
        at = LEGACY_IX["coupon_name"]
        shifted = tuple(list(r[:at]) + list(r[at + 1:]) + [None])
        out, k = zn.repair_row_shift(shifted, LEGACY_IX)
        self.assertEqual(k, 1)
        self.assertEqual(out[LEGACY_IX["currency"]], "IQD")
        self.assertEqual(out[LEGACY_IX["total"]], 209843.281)


class TestReviewAliases(unittest.TestCase):
    """The client rejected 17 SKUs, but 16 of those were "no, this is really
    that one" and carry 870 orders between them. A rejected row is not a row
    to skip: with no product record and no alias those orders hold forever.
    This is the regression for reading the client's answer correctly."""

    def _mapper(self, aliases=None, excluded=frozenset()):
        m = zn.LegacyMapper()
        m.aliases = aliases or {}
        m.excluded = excluded
        return m

    def test_alias_redirects_to_the_canonical_sku(self):
        m = self._mapper({"C042": "C42"})
        o = m.build("1", [legacy_row(sku="C042")], LEGACY_IX, {})
        self.assertEqual(o["items"][0]["sku"], "C42")

    def test_alias_applies_after_case_folding(self):
        """Sheet says C042; an order may spell it c042."""
        m = self._mapper({"C042": "C42"})
        o = m.build("1", [legacy_row(sku="c042")], LEGACY_IX, {})
        self.assertEqual(o["items"][0]["sku"], "C42")

    def test_excluded_sku_drops_the_item(self):
        m = self._mapper(excluded=frozenset({"Z.40352.15934464629227045"}))
        o = m.build("1", [legacy_row(sku="Z.40352.15934464629227045")],
                    LEGACY_IX, {})
        self.assertIsNone(o, "an order of only excluded items must not survive")

    def test_no_review_loaded_is_a_no_op(self):
        o = self._mapper().build("1", [legacy_row(sku="C42")], LEGACY_IX, {})
        self.assertEqual(o["items"][0]["sku"], "C42")


class TestSkuCaseFolding(unittest.TestCase):
    """Zid writes both "C3" and "c3" for one product, across 315,249 orders
    once C1/C2/C7C3C2/C7C3C1 are counted too. Unfolded, each variant earns its
    own approval row and its own LGCY- product, splitting one product's orders
    across two records."""

    def test_both_mappers_fold_case(self):
        a = zn.LegacyMapper().build("1", [legacy_row(sku="c3")], LEGACY_IX, {})
        b = zn.LegacyMapper().build("2", [legacy_row(sku="C3")], LEGACY_IX, {})
        self.assertEqual(a["items"][0]["sku"], b["items"][0]["sku"], "C3")
        r = zn.RichMapper().build("3", [rich_row(product_sku="c18ch11")],
                                  RICH_IX, {})
        self.assertEqual(r["items"][0]["sku"], "C18CH11")

    def test_folding_survives_the_column_swap_repair(self):
        """The swap repair runs first and hands back the displaced value; the
        fold has to come after it, not instead of it."""
        o = zn.LegacyMapper().build(
            "4", [legacy_row(sku=1, **{"product name": "c18"})],
            LEGACY_IX, {})
        self.assertEqual(o["items"][0]["sku"], "C18")

    def test_barcodes_and_classification_unaffected(self):
        self.assertEqual(zn.canon_sku("6287032431307"), "6287032431307")
        self.assertEqual(zn.classify_sku("c3"), zn.classify_sku("C3"))


class TestSnapshotMatchesProductionSemantics(unittest.TestCase):
    """The snapshot's job is to answer exactly as HubSpot would. Measured
    against the live portal: hs_sku EQ search is case-INsensitive, so
    searching "BRUSHES" finds the product stored as "brushes". The live
    catalog genuinely carries mixed-case SKUs (brushes, C1cc, ccC18).

    An exact-match index would be stricter than production and report holds
    that would not really hold -- the one direction of error this design
    cannot tolerate, since every offline verdict is trusted downstream.
    """

    def _snap(self, tmp, sku):
        import sqlite3
        p = Path(tmp)
        (p / "catalog.json").write_text(json.dumps({
            "products": [{"id": "1", "properties": {
                "hs_sku": sku, "catalog_approval_status": "approved"}}],
            "templates": [], "components": []}))
        (p / "orders.json").write_text("{}")
        db = sqlite3.connect(p / "contacts.sqlite")
        db.executescript("CREATE TABLE contact(id TEXT PRIMARY KEY,"
                         " createdate TEXT, salla_customer_id TEXT);"
                         "CREATE TABLE phone(key TEXT, id TEXT,"
                         " createdate TEXT);")
        db.commit()
        db.close()
        cfg = backfill.Config()
        return zs.SnapshotHubSpot(cfg, "t", live=False, snap_dir=p)

    def test_sku_lookup_ignores_case_in_both_directions(self):
        with tempfile.TemporaryDirectory() as tmp:
            hs = self._snap(tmp, "brushes")       # as stored in HubSpot
            for q in ("brushes", "BRUSHES", "Brushes"):
                self.assertEqual(hs.gate_search_product_by_sku([q]), 1,
                                 f"{q!r} must match the stored 'brushes'")

    def test_stored_uppercase_matches_lowercase_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            hs = self._snap(tmp, "C1CC")
            self.assertEqual(hs.gate_search_product_by_sku(["C1cc"]), 1)
            # returns a response BODY, not a list: the engine reads
            # ["results"] and .get("total") off it
            body = hs.item_search_product_by_sku(["c1cc"])
            self.assertEqual(len(body["results"]), 1)
            self.assertEqual(body["results"][0]["properties"]["hs_sku"],
                             "C1CC")

    def test_a_genuinely_absent_sku_still_misses(self):
        with tempfile.TemporaryDirectory() as tmp:
            hs = self._snap(tmp, "brushes")
            self.assertEqual(hs.gate_search_product_by_sku(["C99"]), 0)


class TestPlannerIsSealed(unittest.TestCase):
    """The planner must reach nothing outside this process.

    Regression for a real incident. `PlanRecorder` intercepts `HubSpot._write`,
    so the plan is inert as far as HubSpot is concerned -- but `route_held`
    also fires a raw `http_request` POST at `cfg.held_notify_url`, gated only
    on `self.live`, and the planner must run with live=True for `_write` to
    record anything. The first canary (2023-10) therefore pushed 779 held
    notifications at the production Make webhook: 562 were accepted into its
    queue and 217 came back "Queue is full."

    Interception at the HubSpot client is the wrong altitude to catch that.
    This test works at the right one: it makes `backfill.http_request` itself
    explode, so ANY future outbound call from planner code fails loudly here
    instead of quietly in production.
    """

    def _engine(self, notify_url):
        # no apply_portal_config: it demands a fully-provisioned portal
        # (default_pipeline_stage and friends) and route_held reads none of it
        cfg = backfill.Config()
        cfg.held_notify_url = notify_url
        hs = object.__new__(zp.PlanHubSpot)
        hs.cfg, hs.plan, hs._sym = cfg, [], 0
        gio = backfill.GoogleIO(cfg, enabled=False)
        return cfg, zp.ZedPlanEngine(cfg, hs, gio, _NullMirror())

    def test_notify_url_is_blanked_on_a_copy(self):
        cfg, eng = self._engine("https://hook.eu1.make.com/REAL")
        self.assertEqual(eng.cfg.held_notify_url, "")
        # the caller's config must survive: the equivalence test builds a live
        # HubSpot from the same object in the same process
        self.assertEqual(cfg.held_notify_url, "https://hook.eu1.make.com/REAL")

    def test_route_held_makes_no_outbound_call(self):
        _, eng = self._engine("https://hook.eu1.make.com/REAL")
        order = {"id": "28170986", "reference_id": "R1",
                 "customer": {"created_at": {"date": "2023-10-01"}}}
        with mock.patch.object(backfill, "http_request",
                               side_effect=AssertionError(
                                   "planner made an outbound HTTP call")):
            eng.route_held(order, -1, [{"name": "جهاز تمويج الشعر"}])
        self.assertEqual(eng._outcome["28170986"][0], "held")


class _NullMirror:
    """LocalMirror with every write removed: the planner's mirror writes are
    irrelevant to what this file asserts, and a temp dir per test is noise."""

    def audit_event(self, *a, **kw):
        pass

    def queue_event(self, *a, **kw):
        pass


if __name__ == "__main__":
    unittest.main()
