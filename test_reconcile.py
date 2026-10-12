#!/usr/bin/env python3
"""Unit tests for the v3.0 weekly reconciliation (reconcile.py).

Everything runs offline against fakes. Under test: parity math on Riyadh
boundaries, allowance vs breach with named-id drill-down, per-field sample
drift (including the bundle-expansion rule), report-mode zero writes,
"not recorded" semantics with repairs suppressed, the insanity ceiling,
report-before-repair ordering, dead-timer sensors, and inert defaults.
"""
import csv
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

import backfill
from backfill import Config, RelayError
import reconcile
from reconcile import (Finding, RIYADH, day_bounds, phase_counts,
                       phase_samples, phase_pipeline, insane, render,
                       write_state, repair_stages)


def _chdir_tmp(test):
    tmp = tempfile.TemporaryDirectory()
    test.addCleanup(tmp.cleanup)
    old = os.getcwd()
    os.chdir(tmp.name)
    test.addCleanup(os.chdir, old)
    Path("mirror").mkdir()
    return tmp.name


def _cfg(**over):
    cfg = Config()
    cfg.reconcile_enabled = True
    cfg.reconcile_window_days = 3
    cfg.reconcile_samples_per_era = 2
    cfg.reconcile_day_allowance = 2
    cfg.reconcile_month_allowance = 10
    cfg.reconcile_insane_orders = 5000
    cfg.reconcile_insane_month_pct = 5.0
    cfg.relay_batch_size = 12
    cfg.status_stage_map = {"delivered": "STG-DEL", "canceled": "STG-CAN"}
    cfg.default_pipeline_stage = "STG-DEF"
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


TODAY = date(2026, 9, 20)   # a Sunday


class FakeSource:
    """Programmable parity world."""

    def __init__(self, salla_days=None, hs_days=None):
        self.salla_days = salla_days or {}
        self.hs_days = hs_days or {}
        self.day_lists = {}
        self.hs_day_lists = {}
        self.hs = FakeHS()
        self.relay = FakeRelay()

    def day_count(self, d):
        if isinstance(self.salla_days.get(d.isoformat()), Exception):
            raise self.salla_days[d.isoformat()]
        return self.salla_days.get(d.isoformat(), 100)

    def hs_day_count(self, d):
        return self.hs_days.get(d.isoformat(), 100)

    def range_count(self, a, b):
        return sum(self.day_count(a + timedelta(days=i))
                   for i in range((b - a).days + 1))

    def hs_range_count(self, a, b):
        return sum(self.hs_day_count(a + timedelta(days=i))
                   for i in range((b - a).days))

    def list_day_ids(self, d):
        return self.day_lists.get(d.isoformat(), {})

    def hs_day_ids(self, d):
        return self.hs_day_lists.get(d.isoformat(), set())


class FakeHS:
    def __init__(self):
        self.search_pages = []
        self.writes = []
        self.li_counts = {}

    def search(self, path, body, what):
        if self.search_pages:
            return self.search_pages.pop(0)
        return {"total": 0, "results": []}

    def order_line_item_count(self, hs_id):
        return self.li_counts.get(str(hs_id), 1)

    def _write(self, method, path, body, what):
        self.writes.append((method, path, body))
        return 200, {}


class FakeRelay:
    def __init__(self, payloads=None, error=False):
        self.payloads = dict(payloads or {})
        self.error = error

    def fetch_orders(self, ids):
        if self.error:
            raise RelayError("relay down")
        return {str(i): self.payloads[str(i)] for i in ids
                if str(i) in self.payloads}


def _payload(sid, slug="delivered", total="100", items=1):
    return {"id": int(sid), "reference_id": f"R{sid}",
            "status": {"slug": slug, "name": slug.title()},
            "amounts": {"total": {"amount": total}},
            "items": [{"id": i, "product_type": "product"}
                      for i in range(items)]}


class DayBounds(unittest.TestCase):
    def test_riyadh_day_is_2100z_to_2100z(self):
        s, lo, hi = day_bounds(date(2026, 9, 1))
        self.assertEqual(s, "2026-09-01")
        self.assertEqual(int(hi) - int(lo), 86400000)
        # 2026-09-01 00:00 +03 == 2026-08-31 21:00 UTC
        self.assertEqual(datetime.fromtimestamp(int(lo) / 1000,
                                                RIYADH).hour, 0)


class Counts(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_green_when_all_match(self):
        f = phase_counts(FakeSource(), _cfg(), today=TODAY)
        self.assertTrue(f.measured and f.ok)

    def test_within_allowance_is_green(self):
        d = (TODAY - timedelta(days=1)).isoformat()
        src = FakeSource(salla_days={d: 102}, hs_days={d: 100})
        f = phase_counts(src, _cfg(), today=TODAY)
        self.assertTrue(f.ok)

    def test_breach_names_missing_ids(self):
        d = (TODAY - timedelta(days=1)).isoformat()
        src = FakeSource(salla_days={d: 105}, hs_days={d: 100})
        src.day_lists[d] = {str(i): "delivered" for i in range(105)}
        src.hs_day_lists[d] = {str(i) for i in range(100)}
        f = phase_counts(src, _cfg(), today=TODAY)
        self.assertFalse(f.ok)
        self.assertEqual(f.data["missing_total"], 5)
        self.assertEqual(sorted(f.data["gaps"][0]["missing_ids"]),
                         sorted(str(i) for i in range(100, 105)))

    def test_structural_shapes_stay_green(self):
        # count is off but the drill-down finds no truly missing ids
        d = (TODAY - timedelta(days=1)).isoformat()
        src = FakeSource(salla_days={d: 105}, hs_days={d: 100})
        src.day_lists[d] = {str(i): "delivered" for i in range(100)}
        src.hs_day_lists[d] = {str(i) for i in range(100)}
        f = phase_counts(src, _cfg(), today=TODAY)
        self.assertTrue(f.ok)
        self.assertTrue(any("non-syncable" in x for x in f.detail))

    def test_relay_error_means_not_recorded(self):
        d = (TODAY - timedelta(days=1)).isoformat()
        src = FakeSource(salla_days={d: RelayError("down")})
        f = phase_counts(src, _cfg(), today=TODAY)
        self.assertFalse(f.measured)
        self.assertIn("not recorded", f.summary)


class Samples(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        backfill.STATUS_STAGE_MAP = {"delivered": "STG-DEL"}
        backfill.ORDER_PIPELINE_STAGE = "STG-DEF"

    def _src(self, props, payload):
        src = FakeSource()
        page = {"total": 1, "results": [
            {"id": "H1", "properties": dict({"salla_order_id": "1"}, **props)}]}
        src.hs.search_pages = [dict(page), dict(page)]
        src.relay = FakeRelay({"1": payload})
        return src

    def test_agreeing_sample_is_green(self):
        src = self._src({"hs_pipeline_stage": "STG-DEL",
                         "hs_total_price": "100",
                         "salla_order_reference": "R1"}, _payload(1))
        f = phase_samples(src, _cfg(), {}, today=TODAY)
        self.assertTrue(f.ok)

    def test_each_drift_field_detected(self):
        for props, needle in [
            ({"hs_pipeline_stage": "STG-DEF", "hs_total_price": "100",
              "salla_order_reference": "R1"}, "stage"),
            ({"hs_pipeline_stage": "STG-DEL", "hs_total_price": "999",
              "salla_order_reference": "R1"}, "total"),
            ({"hs_pipeline_stage": "STG-DEL", "hs_total_price": "100",
              "salla_order_reference": "WRONG"}, "reference"),
        ]:
            f = phase_samples(self._src(props, _payload(1)), _cfg(), {},
                              today=TODAY)
            self.assertFalse(f.ok, needle)
            self.assertIn(needle, f.data["drift"][0]["problems"][0])

    def test_bundle_expansion_passes(self):
        src = self._src({"hs_pipeline_stage": "STG-DEL",
                         "hs_total_price": "100",
                         "salla_order_reference": "R1"},
                        _payload(1, items=1))
        src.hs.li_counts["H1"] = 4    # bundle expanded: HS >= salla items
        f = phase_samples(src, _cfg(), {}, today=TODAY)
        self.assertTrue(f.ok)

    def test_missing_line_items_flagged(self):
        src = self._src({"hs_pipeline_stage": "STG-DEL",
                         "hs_total_price": "100",
                         "salla_order_reference": "R1"},
                        _payload(1, items=3))
        src.hs.li_counts["H1"] = 0
        f = phase_samples(src, _cfg(), {}, today=TODAY)
        self.assertFalse(f.ok)


class Pipeline(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def _with_queue(self, rows, live_rows=None):
        # [v2.14] the stuck check reads both queues: Queue Log (drain) and
        # Live Queue (live sync)
        gio = mock.MagicMock()
        gio.qlog_read.return_value = rows
        gio.queue_read.return_value = live_rows or []
        import contextlib

        @contextlib.contextmanager
        def both():
            with mock.patch("queue_drain.DrainGoogleIO", return_value=gio), \
                 mock.patch("backfill.GoogleIO", return_value=gio):
                yield
        return both()

    def test_clean_pipeline(self):
        with self._with_queue([]):
            f = phase_pipeline(_cfg(gift_refresh_enabled=False))
        self.assertTrue(f.ok)

    def test_dead_gift_timer_detected(self):
        Path("mirror/gift_refresh_state.json").write_text(json.dumps(
            {"ts": "2026-09-01 00:00:00"}))
        with self._with_queue([]):
            f = phase_pipeline(_cfg(gift_refresh_enabled=True),
                               now=datetime(2026, 9, 20, tzinfo=RIYADH))
        self.assertFalse(f.ok)
        self.assertTrue(any("timer may be dead" in d for d in f.detail))

    def test_stuck_queue_rows_named_from_live_sheet(self):
        rows = [{"status": "queued", "queued_at": "2026-09-01 00:00:00",
                 "order_id": "42", "items": "Blocked Product"},
                {"status": "processed", "queued_at": "2026-09-01 00:00:00",
                 "order_id": "43", "items": "x"},
                {"status": "queued", "queued_at": "2026-09-19 12:00:00",
                 "order_id": "44", "items": "young, not stuck"}]
        with self._with_queue(rows):
            f = phase_pipeline(_cfg(),
                               now=datetime(2026, 9, 20, tzinfo=RIYADH))
        self.assertFalse(f.ok)
        self.assertEqual(f.data["stuck"], 1)
        self.assertTrue(any("Blocked Product" in d for d in f.detail))

    def test_unreachable_sheet_is_not_recorded_never_zero(self):
        with mock.patch("queue_drain.DrainGoogleIO",
                        side_effect=RuntimeError("no creds")), \
             mock.patch("backfill.GoogleIO", side_effect=RuntimeError("no creds")):
            f = phase_pipeline(_cfg())
        self.assertFalse(f.measured)
        self.assertTrue(any("not recorded" in d for d in f.detail))


class InsanityAndRepairs(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_unmeasured_phase_suppresses_repairs(self):
        fs = [Finding("counts", False, False, "not recorded")]
        self.assertIsNotNone(insane(fs, _cfg()))

    def test_giant_gap_suppresses_repairs(self):
        fs = [Finding("counts", True, False, "gap",
                      data={"missing_total": 6000, "gaps": []})]
        self.assertIsNotNone(insane(fs, _cfg()))

    def test_normal_gap_allows_repairs(self):
        fs = [Finding("counts", True, False, "gap",
                      data={"missing_total": 50, "gaps": []})]
        self.assertIsNone(insane(fs, _cfg()))

    def test_insane_month_pct(self):
        fs = [Finding("counts", True, False, "gap", data={
            "missing_total": 0,
            "gaps": [{"month": "2026-03", "salla": 1000, "hs": 900}]})]
        self.assertIsNotNone(insane(fs, _cfg()))

    def test_repair_stages_batches_and_ledgers(self):
        src = FakeSource()
        stale = [{"salla_order_id": str(i), "hs_id": f"H{i}", "old": "STG-DEF",
                  "props": {"hs_pipeline_stage": "STG-DEL"}}
                 for i in range(250)]
        fs = [Finding("properties", True, False, "stale",
                      data={"stale": stale})]
        out = repair_stages(src, _cfg(), fs, {}, live=True)
        self.assertEqual(out["stage_patched"], 250)
        self.assertEqual(len(src.hs.writes), 3)   # 100+100+50
        self.assertTrue(Path("mirror/stage_resweep.csv").exists())

    def test_dry_repair_no_ledger(self):
        src = FakeSource()
        fs = [Finding("properties", True, False, "stale", data={"stale": [
            {"salla_order_id": "1", "hs_id": "H1", "old": "a",
             "props": {"hs_pipeline_stage": "b"}}]})]
        repair_stages(src, _cfg(), fs, {}, live=False)
        self.assertFalse(Path("mirror/stage_resweep.csv").exists())


class CertificateAndState(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_green_headline_is_one_sentence(self):
        fs = [Finding("counts", True, True, "all days match"),
              Finding("samples", True, True, "24/24 correct")]
        head, body = render(fs, _cfg())
        self.assertTrue(head.startswith("✅"))
        self.assertIn("all days match", head)

    def test_not_recorded_never_zero(self):
        fs = [Finding("counts", False, False, "count parity not recorded")]
        head, body = render(fs, _cfg())
        self.assertIn("NOT RECORDED", body)
        self.assertTrue(head.startswith("🟠"))

    def test_state_written_atomically_with_verdict(self):
        fs = [Finding("counts", True, True, "ok")]
        d = write_state(fs)
        on_disk = json.loads(Path("mirror/reconcile_state.json").read_text())
        self.assertTrue(on_disk["green"])
        self.assertEqual(d["phases"]["counts"]["ok"], True)


class Defaults(unittest.TestCase):
    def test_knobs_inert_by_default(self):
        self.assertFalse(Config().reconcile_enabled)
        self.assertEqual(Config().reconcile_autorepair_tier, 0)


class DigestHook(unittest.TestCase):
    def setUp(self):
        self.tmp = _chdir_tmp(self)

    def test_dead_man_switch(self):
        import report_digest
        Path("mirror/reconcile_state.json").write_text(json.dumps(
            {"ts": "2026-09-01 03:40:00", "green": True}))
        with mock.patch.object(report_digest, "ROOT", Path(self.tmp)):
            rc = report_digest._reconcile_watch()
        self.assertTrue(rc["stale"])

    def test_fresh_green(self):
        import report_digest
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Path("mirror/reconcile_state.json").write_text(json.dumps(
            {"ts": ts, "green": True}))
        with mock.patch.object(report_digest, "ROOT", Path(self.tmp)):
            rc = report_digest._reconcile_watch()
        self.assertFalse(rc["stale"])
        self.assertTrue(rc["green"])

    def test_never_ran_is_none(self):
        import report_digest
        with mock.patch.object(report_digest, "ROOT", Path(self.tmp)):
            self.assertIsNone(report_digest._reconcile_watch())


class V214(unittest.TestCase):
    """[v2.14] the Oct 2026 certificate: serial dates read as unreadable,
    the Live Queue not read at all, 1,575 missing orders left unexplained."""

    def setUp(self):
        _chdir_tmp(self)

    def test_serial_dates_parse(self):
        from reconcile import sheet_time
        self.assertEqual(sheet_time(46303.5).strftime("%Y-%m-%d %H:%M"), "2026-10-08 12:00")
        self.assertEqual(sheet_time("2026-10-08 13:01:00").day, 8)
        self.assertEqual(sheet_time("10/9/2026 2:33:44").month, 10)
        self.assertIsNone(sheet_time("not a date"))
        self.assertIsNone(sheet_time(""))

    def test_live_queue_held_rows_with_serial_dates_are_stuck(self):
        gio = mock.MagicMock()
        gio.qlog_read.return_value = []
        gio.queue_read.return_value = [
            {"order_id": "1332665269", "status": "held", "received_at": 46300.68,
             "note": "catalog gate: المجفف متعدد الاستخدام -- in review queue"},
            {"order_id": "2", "status": "done", "received_at": 46300.68, "note": ""}]
        with mock.patch("queue_drain.DrainGoogleIO", return_value=gio), \
             mock.patch("backfill.GoogleIO", return_value=gio):
            f = phase_pipeline(_cfg(), now=datetime(2026, 10, 11, 3, 33, tzinfo=RIYADH))
        self.assertFalse(f.ok)
        self.assertEqual(f.data["stuck"], 1)
        self.assertTrue(any("المجفف متعدد الاستخدام" in d for d in f.detail))

    def test_missing_orders_explained_by_catalog_hold(self):
        from reconcile import explain_missing
        counts = Finding("counts", True, False, "1 window(s) off, 3 named missing order(s)",
                         data={"gaps": [{"day": "2026-10-06", "missing_ids": ["a", "b", "c"]}],
                               "missing_total": 3})
        explain_missing([counts], _cfg(), index={"a": (None, "Multi Styler"), "b": (None, "Multi Styler")})
        self.assertEqual(counts.data["held_of_missing"], 2)
        self.assertEqual(counts.data["unexplained_missing"], 1)
        self.assertIn("on catalog hold", counts.summary)
        self.assertIn("Multi Styler (2)", counts.detail[0])

    def test_headline_states_why_repair_was_held_back(self):
        fs = [Finding("counts", True, False, "9 window(s) off")]
        head, _ = render(fs, _cfg(), held_back="month 2026-10 is 14% off")
        self.assertIn("Automatic repair held back: month 2026-10 is 14% off", head)

    def test_catalog_phase_finds_tag_trap_and_unclassified(self):
        from reconcile import phase_catalog
        src = FakeSource()
        src.relay = mock.MagicMock()
        src.relay.get_path.return_value = {"data": [
            {"id": 1982906533, "name": "Multi Styler", "type": "product", "status": "sale",
             "tags": [{"id": 784552908, "name": "bundle"}], "categories": [{"id": 294248735}]},
            {"id": 555, "name": "Real bundle", "type": "product", "status": "sale",
             "tags": [], "categories": [{"id": 1794840161}]}],
            "pagination": {"totalPages": 1}}
        src.hs = mock.MagicMock()
        src.hs.search.return_value = {"results": [
            {"properties": {"salla_product_id": "1982906533", "product_class": None, "name": "Multi Styler"}},
            {"properties": {"salla_product_id": "555", "product_class": "bundle", "name": "Real bundle"}}]}
        src.hs.gate_search_template.side_effect = lambda pid, elig: 1 if pid == "555" else 0
        f = phase_catalog(src, _cfg())
        self.assertFalse(f.ok)
        self.assertEqual(f.data["traps"], ["Multi Styler (1982906533)"])
        self.assertEqual(len(f.data["no_class"]), 1)

    def test_catalog_phase_unmeasured_on_relay_failure(self):
        from reconcile import phase_catalog
        src = FakeSource()
        src.relay = mock.MagicMock()
        src.relay.get_path.side_effect = RelayError("down")
        f = phase_catalog(src, _cfg())
        self.assertFalse(f.measured)


def _sheets(live_rows, qlog_rows):
    """Both queue reads served from one fake, as held_index makes them."""
    gio = mock.MagicMock()
    gio.queue_read.return_value = live_rows
    gio.qlog_read.return_value = qlog_rows
    import contextlib

    @contextlib.contextmanager
    def both():
        with mock.patch("queue_drain.DrainGoogleIO", return_value=gio), \
             mock.patch("backfill.GoogleIO", return_value=gio):
            yield
    return both()


class ReviewFixes(unittest.TestCase):
    """v2.14 review: an advisory section, or one bad sheet cell, must never
    withhold a week of repairs; Zid number collisions are not catalog holds."""

    NOW = datetime(2026, 10, 11, 3, 33, tzinfo=RIYADH)

    def setUp(self):
        _chdir_tmp(self)

    def _repairable(self, catalog):
        return [Finding("counts", True, True, "all days match",
                        data={"missing_total": 0, "gaps": []}),
                Finding("properties", True, False, "3 stale stage(s)",
                        data={"stale": [{}, {}, {}]}),
                catalog,
                Finding("make queue", True, True, "clear")]

    # -- #35 the catalog phase is advisory ---------------------------------
    def test_unmeasured_catalog_phase_does_not_hold_back_repairs(self):
        from reconcile import phase_catalog
        src = FakeSource()
        src.relay = mock.MagicMock()
        src.relay.get_path.side_effect = RelayError("down")
        cat = phase_catalog(src, _cfg())
        self.assertFalse(cat.measured)
        fs = self._repairable(cat)
        self.assertIsNone(insane(fs, _cfg()))
        head, body = render(fs, _cfg(), held_back=insane(fs, _cfg()))
        self.assertNotIn("Automatic repair held back", head)
        self.assertIn("*catalog* -- NOT RECORDED", body)   # still shown, never hidden

    def test_a_data_phase_still_holds_back_repairs(self):
        fs = self._repairable(Finding("catalog", True, True, "no latent catalog traps"))
        fs[0] = Finding("counts", False, False, "count parity not recorded")
        self.assertEqual(insane(fs, _cfg()), "a phase was unmeasurable")

    def test_approved_search_only_reads_salla_products(self):
        from reconcile import phase_catalog
        src = FakeSource()
        src.relay = mock.MagicMock()
        src.relay.get_path.return_value = {"data": [], "pagination": {"totalPages": 1}}
        src.hs = mock.MagicMock()
        src.hs.search.return_value = {"results": []}
        self.assertTrue(phase_catalog(src, _cfg()).measured)
        body = src.hs.search.call_args[0][1]
        filters = body["filterGroups"][0]["filters"]
        self.assertEqual(len(body["filterGroups"]), 1)      # AND, not OR
        self.assertIn({"propertyName": "salla_product_id", "operator": "HAS_PROPERTY"}, filters)
        self.assertIn({"propertyName": "catalog_approval_status", "operator": "EQ",
                       "value": "approved"}, filters)

    # -- #36 sheet_time only reads a number as a date when it is one --------
    def test_out_of_range_numbers_are_unreadable_not_errors(self):
        from reconcile import sheet_time
        for v in ("1760171234", "20261011", "1760171234000", "inf", "-inf", "nan",
                  1760171234, 10 ** 400, "1e400", "45", "0.4166", 999999, "-46300"):
            self.assertIsNone(sheet_time(v), repr(v))
        self.assertEqual(sheet_time("46303.5").strftime("%Y-%m-%d %H:%M"), "2026-10-08 12:00")
        self.assertEqual(sheet_time(46300).year, 2026)

    def test_a_bad_date_cell_does_not_cost_the_pipeline_phase(self):
        live = [{"order_id": "1", "status": "held", "received_at": "1760171234",
                 "note": "catalog gate: Dryer -- in review queue"},
                {"order_id": "2", "status": "held", "received_at": "46300.68",
                 "note": "catalog gate: Dryer -- in review queue"}]
        qlog = [{"order_id": "3", "status": "queued", "queued_at": "20261011", "items": "Brush"}]
        with _sheets(live, qlog):
            f = phase_pipeline(_cfg(), now=self.NOW)
        self.assertTrue(f.measured)
        self.assertEqual(f.data["stuck"], 1)            # the readable row; the others have no age
        self.assertFalse(any("not recorded" in d for d in f.detail))
        fs = self._repairable(Finding("catalog", True, True, "no latent catalog traps")) + [f]
        self.assertIsNone(insane(fs, _cfg()))

    def test_one_unreadable_row_is_skipped_not_the_read(self):
        real = reconcile.sheet_time

        def flaky(v):
            if v == "boom":
                raise OverflowError("date value out of range")
            return real(v)
        live = [{"order_id": "1", "status": "held", "received_at": "boom", "note": "catalog gate: A -- x"},
                {"order_id": "2", "status": "held", "received_at": "46300.68", "note": "catalog gate: B -- x"}]
        qlog = [{"order_id": "3", "status": "queued", "queued_at": "boom", "items": "C"},
                {"order_id": "4", "status": "queued", "queued_at": "46300.5", "items": "D"}]
        with _sheets(live, qlog), mock.patch.object(reconcile, "sheet_time", side_effect=flaky):
            with self.assertLogs("backfill", "WARNING"):
                idx = reconcile.held_index(_cfg())
        self.assertEqual(sorted(idx), ["2", "4"])

    # -- #37 Zid number collisions are not catalog holds --------------------
    def test_zid_collision_holds_stay_out_of_the_catalog_wording(self):
        from reconcile import explain_missing
        zid = ("zid collision: salla_order_id 222 is held by Zid order HS 9; "
               "re-key the Zid record")
        live = [{"order_id": "111", "status": "held", "received_at": "46300.68",
                 "note": "catalog gate: Dryer X -- in review queue"},
                {"order_id": "222", "status": "held", "received_at": "46300.68", "note": zid}]
        with _sheets(live, []):
            idx = reconcile.held_index(_cfg())
            f = phase_pipeline(_cfg(), now=self.NOW)
        self.assertEqual(sorted(idx), ["111"])
        self.assertEqual(f.data["stuck"], 1)
        self.assertFalse(any("zid collision" in d for d in f.detail))
        counts = Finding("counts", True, False, "1 window(s) off, 3 named missing order(s)",
                         data={"gaps": [{"day": "2026-10-06", "missing_ids": ["111", "222", "333"]}],
                               "missing_total": 3})
        explain_missing([counts], _cfg(), index=idx)
        self.assertEqual(counts.data["held_of_missing"], 1)
        self.assertEqual(counts.data["unexplained_missing"], 2)
        self.assertIn("blocking products: Dryer X (1)", counts.detail[0])
        self.assertNotIn("zid collision", counts.detail[0])

    def test_held_index_dedupes_both_queues_by_order_id(self):
        live = [{"order_id": "1", "status": "held", "received_at": "46300.68",
                 "note": "catalog gate: Dryer -- in review queue"},
                {"order_id": "5", "status": "held", "received_at": "46300.68",
                 "note": "catalog gate -- in review queue"},
                {"order_id": "2", "status": "done", "received_at": "46300.68", "note": "HS 7"}]
        long_items = "Multi Styler Pro, Dryer Brush Professional Edition 2000W"
        qlog = [{"order_id": "1", "status": "queued", "queued_at": "46290.5", "items": "Dryer"},
                {"order_id": "3", "status": "queued", "queued_at": "46290.5", "items": long_items},
                {"order_id": "4", "status": "processed", "queued_at": "46290.5", "items": "Brush"},
                {"order_id": "6", "status": "error", "queued_at": "46290.5", "items": "zid collision"}]
        with _sheets(live, qlog):
            idx = reconcile.held_index(_cfg())
        self.assertEqual(sorted(idx), ["1", "3", "5"])
        self.assertEqual(idx["1"][1], "Dryer")              # Live Queue row wins
        self.assertEqual(idx["3"][1], long_items)           # whole product names, never half of one
        self.assertEqual(idx["5"][1], "")                   # no product named: no blocker

    def test_a_hold_naming_no_product_gets_no_empty_product_list(self):
        from reconcile import explain_missing
        live = [{"order_id": "5", "status": "held", "received_at": "46300.68",
                 "note": "catalog gate -- in review queue"}]
        with _sheets(live, []):
            f = phase_pipeline(_cfg(), now=self.NOW)
        self.assertEqual(f.data["stuck"], 1)
        self.assertTrue(f.detail[0].endswith("days)"), f.detail[0])
        self.assertNotIn("catalog gate -- in review queue", f.detail[0])
        counts = Finding("counts", True, False, "1 window(s) off, 1 named missing order(s)",
                         data={"gaps": [{"day": "2026-10-06", "missing_ids": ["5"]}], "missing_total": 1})
        explain_missing([counts], _cfg(), index={"5": (None, "")})
        self.assertEqual(counts.detail[0], "1 of the 1 missing order(s) are on hold at the catalog gate, not lost")


if __name__ == "__main__":
    unittest.main(verbosity=1)
