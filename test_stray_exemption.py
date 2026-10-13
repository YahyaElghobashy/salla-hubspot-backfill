#!/usr/bin/env python3
"""v2.14 stray-template exemption: an approved single product can no longer be
held by an EMPTY bundle record that nobody acted on.

The replayed incident: Multi Styler 1982906533, approved standalone, gets an
empty template from Make's bundle branch (no status, no components, no
Bundles-tab row). Before v2.14 the gate rule (te == 0 and ta > 0) held every
order of it (1,760 orders, 5 to 12 Oct 2026) and the line builder routed its
lines to needs_review. Under the exemption both treat it as the approved
product. Everything a person touched (a status, components, a Bundles row)
still holds exactly as before, and an unapproved product is never exempt.
"""
import time
import unittest
from unittest import mock

import backfill
import notify
import slack_text
from test_queue_drain import chdir_tmp, make_cfg, make_engine, order

PID = "1982906533"
EMPTY = [{"id": "454482395354", "properties": {
    "template_status": None, "component_count": None, "active_component_count": None}}]


class SheetGio:
    """Just enough GoogleIO for slack_text.SheetIndex: the validation sheet's
    tabs as {tab: (header, [values])}. fail raises on every read. [v2.14.1]
    The stray check now needs the Bundles tab READ (no row there is a fact;
    a read that failed is unknown), so the tests give it a real tab."""

    def __init__(self, tabs=None, fail=None):
        self.tabs = tabs if tabs is not None else {"Bundles": ("bundle_template_key", []),
                                                   "Product Validation": ("salla_product_id", [])}
        self.fail = fail
        self.reads = []
        self.sheets_rl = None
        self.sheets = self

    # the googleapiclient request builders, returning their own kwargs
    def get(self, **kw):
        return dict(kw, kind="meta") if "fields" in kw else dict(kw)

    def values(self):
        return self

    def _gexec(self, request, what, limiter):
        self.reads.append(what)
        if self.fail:
            raise self.fail
        if what == "sheet tabs":
            return {"sheets": [{"properties": {"title": t, "sheetId": n}}
                               for n, t in enumerate(self.tabs, 7)]}
        tab = request["range"].split("!")[0].strip("'")
        if tab not in self.tabs:
            raise RuntimeError(f"HttpError 400: Unable to parse range: {request['range']}")
        header, vals = self.tabs[tab]
        if what == "sheet header":
            return {"values": [[header]]}
        return {"values": [[header]] + [[v] for v in vals]}


def engine(tmp, flag=True, recs=EMPTY, p=1, te=0, ta=1, live=False, bundles=(), sheet_fail=None,
           components=()):
    cfg = make_cfg(tmp)
    cfg.stray_template_exemption = flag
    eng, hs, relay, gio = make_engine(tmp, live=live, cfg=cfg)
    hs.gate_search_product_approved.return_value = p
    hs.gate_search_template.side_effect = lambda pid, elig: te if elig else ta
    if isinstance(recs, Exception):
        hs.template_records.side_effect = recs
    else:
        hs.template_records.return_value = recs
    hs.search_active_components.return_value = list(components)
    eng.sheet_gio = SheetGio({"Bundles": ("bundle_template_key", list(bundles)),
                              "Product Validation": ("salla_product_id", [])}, fail=sheet_fail)
    eng._sheets = slack_text.SheetIndex(eng.sheet_gio)
    return eng, hs


def dryer_order():
    return order("900000001", [(PID, "product")])


class Gate(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_flag_off_holds_as_before(self):
        eng, _ = engine(self.tmp, flag=False)
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_empty_stray_on_approved_product_passes(self):
        eng, hs = engine(self.tmp)
        self.assertEqual(eng.gate_unverified_items(dryer_order()), [])
        hs.template_records.assert_called_with(PID)

    def test_status_means_a_human_acted_so_it_holds(self):
        recs = [{"id": "1", "properties": {"template_status": "draft",
                                           "component_count": None, "active_component_count": None}}]
        eng, _ = engine(self.tmp, recs=recs)
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_components_mean_a_build_in_progress_so_it_holds(self):
        recs = [{"id": "1", "properties": {"template_status": None,
                                           "component_count": "3", "active_component_count": "0"}}]
        eng, _ = engine(self.tmp, recs=recs)
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_bundles_tab_row_means_deliberate_build_so_it_holds(self):
        eng, _ = engine(self.tmp)
        with mock.patch.object(type(eng), "_bundles_row", return_value=7):
            self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_one_stray_plus_one_real_record_holds(self):
        recs = EMPTY + [{"id": "2", "properties": {"template_status": "inactive",
                                                   "component_count": "2", "active_component_count": "0"}}]
        eng, _ = engine(self.tmp, recs=recs, ta=2)
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_unapproved_product_is_never_exempt(self):
        eng, hs = engine(self.tmp, p=0)
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)
        hs.template_records.assert_not_called()

    def test_active_template_path_untouched(self):
        eng, hs = engine(self.tmp, te=1, ta=1)
        self.assertEqual(eng.gate_unverified_items(dryer_order()), [])
        hs.template_records.assert_not_called()

    def test_lookup_failure_holds_as_before(self):
        eng, _ = engine(self.tmp, recs=RuntimeError("HubSpot 500"))
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_no_records_returned_holds(self):
        # ta > 0 but the record search came back empty (index lag): unknown, hold
        eng, _ = engine(self.tmp, recs=[])
        self.assertEqual(len(eng.gate_unverified_items(dryer_order())), 1)

    def test_verdict_is_cached(self):
        eng, hs = engine(self.tmp)
        for _ in range(5):
            eng.gate_unverified_items(dryer_order())
        self.assertEqual(hs.template_records.call_count, 1)


class DrainAgrees(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_drain_gate_matches_live_gate(self):
        for flag in (False, True):
            eng, _ = engine(self.tmp, flag=flag)
            live_verdict = len(eng.gate_unverified_items(dryer_order()))
            eng2, _ = engine(self.tmp, flag=flag)
            drain_verdict = len(eng2.gate_cached(dryer_order()))
            self.assertEqual(live_verdict, drain_verdict, f"flag={flag}")


class LineBuilder(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def _build(self, flag):
        eng, hs = engine(self.tmp, flag=flag)
        hs.item_search_product.return_value = {"total": 1, "results": [
            {"id": "434196269276", "properties": {"name": "Multi Styler", "product_class": "device",
                                                  "warranty_months": "24"}}]}
        hs.item_search_template.side_effect = lambda pid, elig: (
            {"total": 0, "results": []} if elig else {"total": 1, "results": EMPTY})
        hs.create_line_item.return_value = "LI1"
        o = dryer_order()
        eng.process_item(o, "HS-ORDER", o["items"][0])
        return hs.create_line_item.call_args

    def test_stray_routes_line_as_the_product(self):
        args = self._build(True)
        props = args[0][0]
        self.assertEqual(props.get("sale_context") or props.get("properties", {}).get("sale_context"),
                         "standalone_product")

    def test_flag_off_still_needs_review(self):
        args = self._build(False)
        props = args[0][0]
        self.assertEqual(props.get("sale_context") or props.get("properties", {}).get("sale_context"),
                         "needs_review")


class Alert(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_live_run_opens_one_keyed_thread(self):
        eng, _ = engine(self.tmp, live=True)
        with mock.patch("notify.issue_open", return_value=True) as io:
            eng.gate_unverified_items(dryer_order())
        self.assertEqual(io.call_count, 1)
        self.assertEqual(io.call_args[0][0], f"stray:{PID}")
        self.assertIn("Do NOT approve", io.call_args[0][2])

    def test_dry_run_posts_nothing(self):
        eng, _ = engine(self.tmp, live=False)
        with mock.patch("notify.issue_open") as io:
            eng.gate_unverified_items(dryer_order())
        io.assert_not_called()

    def test_line_builder_never_alerts(self):
        eng, _ = engine(self.tmp, live=True)
        with mock.patch("notify.issue_open") as io:
            eng.effective_ta(PID, 1, 0, 1, "x", alert=False)
        io.assert_not_called()


class StrayReopen(unittest.TestCase):
    """[v2.14.1] round 2, N3: the sentinel sets the dryer's stray aside every
    15 minutes, the live settle closes its thread, and Make mints a new one on
    the next listing edit. Each hourly drain is a fresh process (its 6 h
    "once per product" memory starts empty), so it reopened the thread with a
    new email every time. A thread closed within a day is not reopened."""

    def setUp(self):
        self.tmp = chdir_tmp(self)

    def gate_with(self, rec):
        eng, _ = engine(self.tmp, live=True)
        with mock.patch("notify.issue_get", return_value=rec), \
                mock.patch("notify.issue_open", return_value=True) as io:
            self.assertEqual(eng.gate_unverified_items(dryer_order()), [])   # orders still sync
        return io

    def test_closed_within_a_day_is_not_reopened(self):
        self.gate_with({"state": "closed", "closed_at": time.time() - 3600}).assert_not_called()

    def test_closed_over_a_day_ago_reopens(self):
        io = self.gate_with({"state": "closed", "closed_at": time.time() - 25 * 3600})
        self.assertEqual(io.call_count, 1)
        self.assertEqual(io.call_args[0][0], f"stray:{PID}")

    def test_a_reply_still_being_posted_is_not_reopened(self):
        self.gate_with({"state": "resolving", "closed_at": time.time() - 60}).assert_not_called()
        self.gate_with({"state": "resolving"}).assert_not_called()

    def test_no_record_or_a_failed_open_still_goes_to_notify(self):
        for rec in (None, {"state": "failed", "retry_at": time.time() - 1}):
            self.assertEqual(self.gate_with(rec).call_count, 1, rec)

    def test_hourly_drain_processes_do_not_churn_threads(self):
        # scratchpad repro_stray_churn: 3 hourly cycles gave 3 threads, 6 emails
        posts, mails = [], []
        with mock.patch.object(notify, "slack_enabled", return_value=True), \
                mock.patch.object(notify, "slack_channels", return_value=["C1"]), \
                mock.patch.object(notify, "post_slack",
                                  side_effect=lambda t, **k: posts.append(t) or "1.%d" % len(posts)), \
                mock.patch.object(notify, "email_enabled", return_value=True), \
                mock.patch.object(notify, "send_email", side_effect=lambda s, b: mails.append(s) or True), \
                mock.patch.object(notify, "add_reaction", return_value=True), \
                mock.patch.object(notify, "issue_open", wraps=notify.issue_open) as io:
            for hour in range(3):
                eng, _ = engine(self.tmp, live=True)          # a fresh drain process each hour
                self.assertEqual(eng.gate_cached(order(str(hour), [(PID, "product")])), [])
                notify.issue_resolve(f"stray:{PID}", "Resolved: set aside")   # live settle
        self.assertEqual(io.call_count, 1)
        opens = [p for p in posts if p.startswith("\U0001F7E0 An empty bundle record")]
        self.assertLessEqual(len(opens), 1)
        self.assertLessEqual(len(mails), 2)


class Defaults(unittest.TestCase):
    def test_off_by_default(self):
        self.assertFalse(backfill.Config.__dataclass_fields__["stray_template_exemption"].default)


if __name__ == "__main__":
    unittest.main()
