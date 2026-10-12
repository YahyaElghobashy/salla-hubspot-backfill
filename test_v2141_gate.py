#!/usr/bin/env python3
"""v2.14.1 gate and held-thread truth (review of the v2.13/v2.14 diff).

  * The stray verdict is three-valued. An unreadable Bundles tab, a renamed
    column or a failed HubSpot read is UNKNOWN, which holds and never alerts;
    it used to count as "nobody is building this bundle" and released orders
    of a bundle being built as the single product. Active component records
    count as a build, as in tools/fix_catalog_blockers.py.
  * The line builder reuses the gate's decision for the order instead of
    re-deriving it after the cache expired or a lookup failed (which wrote a
    permanent needs_review line on an order the gate had passed). A failed
    stray check is remembered for a minute and returns the last verdict.
  * The held thread only says "already approved, do NOT approve" for an
    approved product whose record is a proven stray. An unapproved product
    gets the approval steps, a failed Bundles lookup the generic text.
  * The dry-run plan routes exempt items as the live builder does.
  * The stray thread has a closing step; resolutions name the order "#ref".
  * reconcile_stuck_hours is a real Config field.
All I/O is mocked: HubSpot is a MagicMock, Sheets a fake, Slack is patched.
"""
import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import backfill
import slack_text
from test_queue_drain import chdir_tmp, order
from test_stray_exemption import EMPTY, PID, SheetGio, dryer_order, engine

DRAFT = [{"id": "1", "properties": {"template_status": "draft",
                                    "component_count": None, "active_component_count": None}}]


def age_cache(eng, pid=PID, by=301):
    t, v = eng._stray_cache[pid]
    eng._stray_cache[pid] = (t - by, v)


def line_builder_mocks(hs, ta_line=1):
    hs.item_search_product.return_value = {"total": 1, "results": [
        {"id": "434196269276", "properties": {"name": "Multi Styler", "product_class": "device",
                                              "warranty_months": "24"}}]}
    hs.item_search_template.side_effect = lambda pid, elig: (
        {"total": 0, "results": []} if elig else {"total": ta_line, "results": EMPTY})
    hs.create_line_item.return_value = "LI1"


def sale_context(hs):
    props = hs.create_line_item.call_args[0][0]
    return props.get("sale_context") or props.get("properties", {}).get("sale_context")


# --------------------------------------------------------------- the verdict

class StrayVerdict(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_true_only_when_everything_was_read_and_is_empty(self):
        eng, _ = engine(self.tmp)
        self.assertIs(eng.stray_verdict(PID), True)

    def test_unreadable_bundles_tab_is_unknown_and_holds_without_alert(self):
        # finding 3: a Sheets 5xx used to read as "no row", cache stray=True and
        # tell the client "nobody has started building this bundle"
        eng, _ = engine(self.tmp, live=True, sheet_fail=RuntimeError("HttpError 503"))
        self.assertIsNone(eng.stray_verdict(PID))
        with mock.patch("notify.issue_open") as io:
            self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)
        io.assert_not_called()
        self.assertNotIn(PID, eng.__dict__.get("_stray_cache", {}))

    def test_renamed_bundles_column_is_unknown(self):
        eng, _ = engine(self.tmp)
        eng.sheet_gio.tabs["Bundles"] = ("template key", [])
        self.assertIsNone(eng.stray_verdict(PID))
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_renamed_bundles_tab_is_unknown(self):
        eng, _ = engine(self.tmp)
        eng.sheet_gio.tabs = {"Bundle list": ("bundle_template_key", [])}
        self.assertIsNone(eng.stray_verdict(PID))

    def test_no_validation_sheet_configured_is_unknown(self):
        eng, _ = engine(self.tmp)
        eng.cfg.validation_spreadsheet_id = ""
        self.assertIsNone(eng.stray_verdict(PID))

    def test_bundles_row_is_a_build(self):
        eng, _ = engine(self.tmp, bundles=[PID])
        self.assertIs(eng.stray_verdict(PID), False)

    def test_active_component_records_are_a_build(self):
        # the counts are plain numbers a failed activation can leave blank
        eng, hs = engine(self.tmp, components=[{"id": "c1"}])
        self.assertIs(eng.stray_verdict(PID), False)
        hs.search_active_components.assert_called_with(PID)
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_component_read_failure_is_unknown(self):
        eng, hs = engine(self.tmp)
        hs.search_active_components.side_effect = RuntimeError("HubSpot 429")
        self.assertIsNone(eng.stray_verdict(PID))

    def test_no_template_records_is_false(self):
        eng, _ = engine(self.tmp, recs=[])
        self.assertIs(eng.stray_verdict(PID), False)

    def test_given_records_are_not_fetched_again(self):
        eng, hs = engine(self.tmp)
        self.assertIs(eng.stray_verdict(PID, recs=EMPTY), True)
        hs.template_records.assert_not_called()


class StrayCache(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_failed_read_returns_the_last_verdict(self):
        eng, hs = engine(self.tmp)
        self.assertTrue(eng._stray_only(PID))
        age_cache(eng)
        hs.template_records.side_effect = RuntimeError("HubSpot 429")
        self.assertTrue(eng._stray_only(PID))           # stale is fine, never re-decided

    def test_stale_verdict_after_a_failure_never_alerts(self):
        eng, hs = engine(self.tmp, live=True)
        self.assertTrue(eng._stray_only(PID))           # primed without alerting
        age_cache(eng)
        hs.template_records.side_effect = RuntimeError("HubSpot 429")
        with mock.patch("notify.issue_open") as io:
            self.assertEqual(eng.gate_unverified_items(dryer_order()), [])
        io.assert_not_called()

    def test_failure_without_a_verdict_holds_and_is_remembered_a_minute(self):
        # finding 23: every line of a busy product retried the failing search
        eng, hs = engine(self.tmp, recs=RuntimeError("HubSpot 429"))
        for _ in range(5):
            self.assertFalse(eng._stray_only(PID))
        self.assertEqual(hs.template_records.call_count, 1)
        self.assertNotIn(PID, eng._stray_cache)         # unknown is never cached
        eng._stray_failed[PID] -= 61
        hs.template_records.side_effect = None
        hs.template_records.return_value = EMPTY
        self.assertTrue(eng._stray_only(PID))
        self.assertEqual(hs.template_records.call_count, 2)

    def test_false_is_cached_too(self):
        eng, hs = engine(self.tmp, recs=DRAFT)
        for _ in range(3):
            self.assertFalse(eng._stray_only(PID))
        self.assertEqual(hs.template_records.call_count, 1)


# ------------------------------------------------- the gate's per-order call

class GateDecision(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_line_builder_keeps_the_gates_decision_after_expiry_and_failure(self):
        # finding 4: the gate passed the order, the 300 s cache expired, the
        # refresh failed, and the line went to needs_review with flag_partial
        eng, hs = engine(self.tmp)
        o = dryer_order()
        self.assertEqual(eng.gate_unverified_items(o), [])
        self.assertEqual(eng.gate_exempt(o), {PID})
        age_cache(eng)
        hs.template_records.side_effect = RuntimeError("HubSpot 429")
        calls = hs.template_records.call_count
        line_builder_mocks(hs)
        eng.process_item(o, "HS-ORDER", o["items"][0])
        self.assertEqual(sale_context(hs), "standalone_product")
        self.assertEqual(hs.template_records.call_count, calls)     # no fresh lookup
        hs.patch_order.assert_not_called()                          # no flag_partial

    def test_a_product_the_gate_did_not_exempt_routes_on_raw_counts(self):
        # the gate passed it as a plain product (no record then); a record
        # appearing before the line is built is not re-judged by the builder
        eng, hs = engine(self.tmp, ta=0)
        o = dryer_order()
        self.assertEqual(eng.gate_unverified_items(o), [])
        self.assertEqual(eng.gate_exempt(o), set())
        line_builder_mocks(hs, ta_line=1)
        eng.process_item(o, "HS-ORDER", o["items"][0])
        self.assertEqual(sale_context(hs), "needs_review")
        hs.template_records.assert_not_called()

    def test_without_a_gate_the_builder_uses_the_shared_rule(self):
        eng, hs = engine(self.tmp)
        o = dryer_order()
        self.assertIsNone(eng.gate_exempt(o))
        line_builder_mocks(hs)
        eng.process_item(o, "HS-ORDER", o["items"][0])
        self.assertEqual(sale_context(hs), "standalone_product")

    def test_decision_is_kept_off_the_order_dict(self):
        # order dicts are dumped to the audit sheet as JSON
        eng, _ = engine(self.tmp)
        o = dryer_order()
        before = copy.deepcopy(o)
        eng.gate_unverified_items(o)
        self.assertEqual(o, before)
        json.dumps(o)

    def test_each_gate_run_starts_clean(self):
        eng, hs = engine(self.tmp)
        o = dryer_order()
        eng.gate_unverified_items(o)
        self.assertEqual(eng.gate_exempt(o), {PID})
        hs.gate_search_template.side_effect = lambda pid, elig: 1   # now an active bundle
        eng.gate_unverified_items(o)
        self.assertEqual(eng.gate_exempt(o), set())

    def test_book_is_bounded_oldest_first(self):
        eng, _ = engine(self.tmp)
        with mock.patch.object(backfill.Engine, "GATE_KEEP", 3):
            for n in range(5):
                eng.begin_gate({"id": n})
        self.assertIsNone(eng.gate_exempt({"id": 0}))
        self.assertIsNone(eng.gate_exempt({"id": 1}))
        self.assertEqual(eng.gate_exempt({"id": 4}), set())

    def test_process_order_clears_the_decision(self):
        eng, hs = engine(self.tmp)
        line_builder_mocks(hs)
        o = dryer_order()
        eng.process_order(o)
        self.assertIsNone(eng.gate_exempt(o))

    def test_engine_built_without_init_can_still_gate(self):
        # zed_catalog's probe skips __init__
        eng, hs = engine(self.tmp)
        probe = object.__new__(backfill.Engine)
        probe.hs, probe.cfg, probe._sheets, probe.live = hs, eng.cfg, eng._sheets, False
        o = dryer_order()
        self.assertEqual(probe.gate_unverified_items(o), [])
        self.assertEqual(probe.gate_exempt(o), {PID})


class PidGateState(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_states(self):
        self.assertEqual(engine(self.tmp, ta=0)[0].pid_gate_state(PID), "pass")
        self.assertEqual(engine(self.tmp, p=0, ta=0)[0].pid_gate_state(PID), "held")
        self.assertEqual(engine(self.tmp, flag=False)[0].pid_gate_state(PID), "held")
        self.assertEqual(engine(self.tmp)[0].pid_gate_state(PID), "pass")          # exempt stray
        self.assertEqual(engine(self.tmp, recs=DRAFT)[0].pid_gate_state(PID), "held")

    def test_failed_reads_are_unknown(self):
        eng, hs = engine(self.tmp)
        hs.gate_search_product_approved.side_effect = RuntimeError("HubSpot 500")
        self.assertIsNone(eng.pid_gate_state(PID))
        eng, _ = engine(self.tmp, sheet_fail=RuntimeError("HttpError 503"))
        self.assertIsNone(eng.pid_gate_state(PID))

    def test_never_alerts(self):
        eng, _ = engine(self.tmp, live=True)
        with mock.patch("notify.issue_open") as io:
            self.assertEqual(eng.pid_gate_state(PID), "pass")
        io.assert_not_called()


class DryRunPlan(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_exempt_item_is_planned_as_the_product(self):
        # finding 6: the plan said needs_review for the very orders the flag fixes
        eng, hs = engine(self.tmp, live=False)
        line_builder_mocks(hs)
        with self.assertLogs("backfill", "INFO") as logs:
            eng.process_order(dryer_order())
        plan = [m for m in logs.output if "DRY RUN plan" in m]
        self.assertTrue(plan and "i0=standalone" in plan[0], plan)


# ------------------------------------------------------------ held thread text

def gated(test, **kw):
    eng, hs = engine(test.tmp, **kw)
    o = dryer_order()
    u = eng.gate_unverified_items(o)
    test.assertEqual(len(u), 1)
    return eng, hs, o, u[0]


class HeldText(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def assertClean(self, subject, body):
        self.assertNotIn("—", subject + body)                     # no em dash in client text

    def test_unapproved_product_is_never_called_already_approved(self):
        # findings 1, 12, 27: the relisted dryer (p == 0) with an empty record
        for flag in (False, True):
            eng, hs, o, u = gated(self, flag=flag, p=0)
            self.assertIs(u["approved"], False)
            self.assertEqual(u["why"], "bundle_inactive")
            subject, body = eng.held_text(o, u)
            self.assertIn("not approved in the catalog yet", subject)
            self.assertNotIn("already approved", body)
            self.assertNotIn("Do NOT approve", body)
            self.assertIn("Approve as Standalone", body)
            self.assertIn("empty bundle record", body)            # the one extra line
            self.assertClean(subject, body)

    def test_approved_proven_stray_gets_the_stray_text(self):
        eng, hs, o, u = gated(self, flag=False)
        self.assertIs(u["approved"], True)
        subject, body = eng.held_text(o, u)
        self.assertIn("empty bundle record", subject)
        self.assertIn("Do NOT approve", body)
        self.assertClean(subject, body)

    def test_failed_bundles_lookup_gets_the_generic_text(self):
        eng, hs, o, u = gated(self, flag=False)
        eng.sheet_gio.fail = RuntimeError("HttpError 503")
        eng._sheets = slack_text.SheetIndex(eng.sheet_gio)          # nothing cached
        eng.__dict__.pop("_stray_cache", None)
        subject, body = eng.held_text(o, u)
        self.assertIn("not active yet", subject)
        self.assertNotIn("Do NOT approve", body)
        self.assertNotIn("already approved", body)

    def test_unknown_verdict_gets_the_generic_text(self):
        eng, hs, o, u = gated(self, flag=False, recs=RuntimeError("HubSpot 500"))
        subject, body = eng.held_text(o, u)
        self.assertIn("not active yet", subject)
        self.assertNotIn("Do NOT approve", body)

    def test_a_record_someone_touched_gets_the_bundle_steps(self):
        eng, hs, o, u = gated(self, recs=DRAFT)
        subject, body = eng.held_text(o, u)
        self.assertIn("not active yet", subject)
        self.assertIn("Push to HubSpot", body)
        self.assertNotIn("Do NOT approve", body)

    def test_held_text_reuses_what_the_gate_read(self):
        # finding 14: no second template search and no second component read
        eng, hs, o, u = gated(self, components=[{"id": "c1"}])
        self.assertEqual(hs.search_active_components.call_count, 1)
        recs_before = hs.template_records.call_count
        subject, body = eng.held_text(o, u)
        hs.search.assert_not_called()
        self.assertEqual(hs.search_active_components.call_count, 1)
        self.assertEqual(hs.template_records.call_count, recs_before + 1)
        self.assertIn("not active yet", subject)


class HeldIssue(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_no_catalog_reads_while_a_failed_thread_backs_off(self):
        eng, hs, o, u = gated(self, flag=False)
        rec = {"state": "failed", "retry_at": time.time() + 300, "meta": {"orders": ["1"]}}
        with mock.patch("notify.issue_get", return_value=rec), \
                mock.patch("notify.issue_update", return_value=False) as upd, \
                mock.patch("notify.issue_open") as io, \
                mock.patch.object(backfill.Engine, "held_text") as ht:
            eng.held_issue(o, [u])
        ht.assert_not_called()
        io.assert_not_called()
        self.assertIn(o["id"], upd.call_args.kwargs["meta"]["orders"])

    def test_retry_due_reopens_with_full_text(self):
        eng, hs, o, u = gated(self, flag=False)
        rec = {"state": "failed", "retry_at": time.time() - 1, "meta": {}}
        with mock.patch("notify.issue_get", return_value=rec), \
                mock.patch("notify.issue_open", return_value=True) as io:
            eng.held_issue(o, [u])
        self.assertEqual(io.call_count, 1)
        self.assertIn("Do NOT approve", io.call_args[0][2])


# ----------------------------------------------------------------- slack_text

class RowLink(unittest.TestCase):
    def index(self, **kw):
        gio = SheetGio(**kw)
        return slack_text.SheetIndex(gio), gio

    def test_found_not_found_and_failed(self):
        idx, _ = self.index(tabs={"Bundles": ("bundle_template_key", ["111", "222"])})
        link, found = idx.row_link("S", "Bundles", "bundle_template_key", "222", "x")
        self.assertIs(found, True)
        self.assertIn("range=A3", link)
        self.assertIs(idx.row_link("S", "Bundles", "bundle_template_key", "999", "x")[1], False)
        idx, _ = self.index(fail=RuntimeError("HttpError 503"))
        link, found = idx.row_link("S", "Bundles", "bundle_template_key", "222", "x")
        self.assertIsNone(found)
        self.assertIn("docs.google.com", link)

    def test_renamed_column_is_a_failed_lookup_and_read_once(self):
        idx, gio = self.index(tabs={"Bundles": ("template key", ["111"])})
        self.assertIsNone(idx.row_link("S", "Bundles", "bundle_template_key", "111", "x")[1])
        with self.assertRaises(LookupError):
            idx.column("S", "Bundles", "bundle_template_key")
        self.assertEqual(gio.reads.count("sheet header"), 1)


# ------------------------------------------------------------------ wording

class StrayThread(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_stray_thread_has_a_closing_step(self):
        # finding 33: removing the tag alone never closed the thread
        eng, _ = engine(self.tmp, live=True)
        with mock.patch("notify.issue_open", return_value=True) as io:
            eng.gate_unverified_items(dryer_order())
        body = io.call_args[0][2]
        self.assertIn("sets the empty record aside", body)
        self.assertIn("this thread confirms it", body)
        self.assertNotIn("—", io.call_args[0][1] + body)


class ResolveOrderIssues(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def resolve(self, metas, reference=None):
        eng, _ = engine(self.tmp)
        with mock.patch("notify.issue_get", side_effect=lambda k: metas.get(k)), \
                mock.patch("notify.issue_resolve", return_value=True) as res:
            eng.resolve_order_issues("1593948275", "HS9", reference)
        return {c[0][0]: c[0][1] for c in res.call_args_list}

    def test_names_the_reference_the_opener_used(self):
        # finding 34: "#239847561 is not in HubSpot" was answered with the Salla id
        out = self.resolve({"status-missing:1593948275": {"state": "open", "meta": {"ref": "239847561"}}})
        self.assertIn("order #239847561 is in HubSpot", out["status-missing:1593948275"])
        self.assertNotIn("1593948275 is in HubSpot", out["status-missing:1593948275"])

    def test_falls_back_to_the_callers_reference_then_the_id(self):
        out = self.resolve({"status-missing:1593948275": {"state": "open", "meta": {}}}, reference="239847561")
        self.assertIn("order #239847561", out["status-missing:1593948275"])
        out = self.resolve({"status-missing:1593948275": {"state": "open", "meta": {}}})
        self.assertIn("order #1593948275", out["status-missing:1593948275"])

    def test_zid_answer_names_both_numbers(self):
        out = self.resolve({"zid:1593948275": {"state": "open", "meta": {"zid_hs_id": "5"}}},
                           reference="239847561")
        self.assertIn("order #239847561 (Salla order 1593948275)", out["zid:1593948275"])
        self.assertNotIn("—", "".join(out.values()))

    def test_create_passes_the_reference(self):
        eng, hs = engine(self.tmp, live=True)
        o = order("1593948275", [(PID, "product")])
        o["reference_id"] = "239847561"
        with mock.patch.object(backfill.Engine, "_create_order_freeing_zid", return_value=("HS9", True)), \
                mock.patch.object(backfill.Engine, "process_item"), \
                mock.patch.object(backfill.Engine, "resolve_order_issues") as roi:
            eng._finish_create(o, -1, "C1", 1)
        roi.assert_called_once_with("1593948275", "HS9", "239847561")


# ------------------------------------------------------------------- config

class ReconcileStuckHours(unittest.TestCase):
    def test_is_a_config_field_and_loads(self):
        # finding 38: Config.load dropped the key with a warning
        self.assertEqual(backfill.Config.__dataclass_fields__["reconcile_stuck_hours"].default, 24)
        here = Path(__file__).resolve().parent
        raw = json.loads((here / "config.example.json").read_text())
        self.assertEqual(raw["reconcile_stuck_hours"], 24)
        raw["reconcile_stuck_hours"] = 72
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps(raw))
            self.assertEqual(backfill.Config.load(p).reconcile_stuck_hours, 72)


if __name__ == "__main__":
    unittest.main()
