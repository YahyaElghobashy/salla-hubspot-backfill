#!/usr/bin/env python3
"""v2.14 drain pre-filter: during the Oct 2026 hold every hourly pass paid a
relay fetch (Make credits) for the same 120 blocked rows and never reached the
other 1,600. Rows that can only be blocked are now skipped without a fetch,
and the rows waiting longest since their last try go first.

v2.14.1 (review of the v2.14 diff):
  * Keyed on product ids, never names. Two Salla listings can share a name (a
    relisted product keeps it under a new id); a name match skipped the
    approved listing's orders for as long as the old one stayed blocked, and
    filed them under the old listing's id. drain_one now records the blocking
    ids in the row's note and a row is skipped only when every recorded id
    still fails. A product deleted in Salla is recorded by its SKU.
  * Ordered by when a row was last tried, not by its lifetime attempts: rows
    the pre-v2.14 passes retried ~170 times no longer wait days behind rows
    that can never be skipped.
  * The re-check searches run after the drain yields to live, and they seed
    the gate cache so gate_cached does not search the same product again.
  * gate_cached records the gate's per-order decision, so the line builder
    never re-decides a stray exemption the gate already made.
All I/O is mocked.
"""
import csv
import unittest
from unittest import mock

import queue_drain
from test_queue_drain import chdir_tmp, make_cfg, make_engine, order
from test_stray_exemption import engine
from test_v2141_gate import DRAFT, age_cache, line_builder_mocks, sale_context

PID = "1982906533"
OLD = "1111"                 # an older listing with the same name
STILL_BLOCKED_REC = [{"id": "1", "properties": {"template_status": "draft",
                                                "component_count": None, "active_component_count": None}}]
T_OLD, T_NEW = "2026-10-01 10:17:03", "2026-10-12 09:17:03"


def write_matrix(rows):
    queue_drain.MATRIX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(queue_drain.MATRIX_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["salla_product_id", "sku", "item_name", "blocked_orders",
                    "suggested_action", "sample_order_ids", "checked_at", "note"])
        for pid, name in rows:
            w.writerow([pid, "", name, 120, "x", "", "", ""])


def read_matrix():
    with open(queue_drain.MATRIX_FILE, newline="", encoding="utf-8") as f:
        return {r["salla_product_id"] or "sku:" + (r["sku"] or "-"): r for r in csv.DictReader(f)}


def note(names, ts, *keys):
    """A blocked row's Queue Log note, in the format drain_one writes."""
    return f"blocked: {names} @ {ts} [pids={','.join(keys)}]"


def qrow(n, oid, items, attempts, notes="", status="queued"):
    return {"row": n, "order_id": oid, "status": status, "attempts": attempts,
            "reason": "", "items": items, "notes": notes, "older": "", "reference": ""}


def deleted_item_order(oid, sku):
    o = order(oid, [])
    o["items"] = [{"id": "i0", "name": "Old dryer", "sku": sku, "product_type": "product",
                   "product": {}}]
    return o


class Pass:
    """One drain pass in verify mode over a fake Queue Log. table maps a
    product id to (approved, eligible templates, any templates); SKUs in
    sku_ok resolve to an approved legacy record."""

    def setUp(self):
        self.tmp = chdir_tmp(self)

    def drain(self, table, flag=True, sku_ok=(), live=False):
        cfg = make_cfg(self.tmp)
        cfg.drain_skip_known_blocked = flag
        cfg.stray_template_exemption = True
        eng, hs, relay, gio = make_engine(self.tmp, live=live, cfg=cfg)
        hs.gate_search_product_approved.side_effect = lambda pid: table[str(pid)][0]
        hs.gate_search_template.side_effect = lambda pid, elig: table[str(pid)][1 if elig else 2]
        hs.gate_search_product_by_sku.side_effect = lambda skus: int(skus[0] in sku_ok)
        hs.template_records.return_value = STILL_BLOCKED_REC
        hs.find_order_by_salla_id.return_value = None
        return eng, hs, relay, gio

    def run_pass(self, eng, relay, gio, rows, db, max_orders=None):
        gio.qlog_read = lambda: rows
        fetched = []
        relay.fetch_orders.side_effect = lambda ids: (fetched.extend(ids) or {i: db[i] for i in ids})
        eng.run_drain(max_orders=max_orders)
        return fetched


class PreFilter(Pass, unittest.TestCase):
    TABLE = {PID: (1, 0, 1), "2": (1, 0, 0)}     # the dryer still blocked, "2" approved

    def rows_and_db(self):
        rows = [qrow(2, "101", "Dryer", 160, note("Dryer", T_OLD, PID)),
                qrow(3, "102", "Dryer", 0),
                qrow(4, "103", "Dryer, Foam", 3, note("Dryer, Foam", T_OLD, PID, "2")),
                qrow(5, "104", "Foam", 9, note("Foam", T_OLD, "2"))]
        db = {"101": order("101", [(PID, "product")]), "102": order("102", [(PID, "product")]),
              "103": order("103", [(PID, "product"), ("2", "product")]),
              "104": order("104", [("2", "product")])}
        return rows, db

    def pass_(self, table=TABLE, flag=True):
        eng, hs, relay, gio = self.drain(table, flag=flag)
        write_matrix([(PID, "Dryer")])
        rows, db = self.rows_and_db()
        return self.run_pass(eng, relay, gio, rows, db), eng, hs

    def test_skips_only_tried_rows_whose_recorded_products_all_still_fail(self):
        fetched, _, _ = self.pass_()
        self.assertNotIn("101", fetched)          # tried, only the dryer recorded: skipped
        self.assertIn("102", fetched)             # never tried: fetched
        self.assertIn("103", fetched)             # also recorded an approved product: fetched
        self.assertIn("104", fetched)

    def test_product_now_passing_is_fetched(self):
        fetched, _, _ = self.pass_(table={PID: (1, 0, 0), "2": (1, 0, 0)})
        self.assertIn("101", fetched)

    def test_flag_off_fetches_everything_in_sheet_order(self):
        fetched, _, _ = self.pass_(flag=False)
        self.assertEqual(fetched, ["101", "102", "103", "104"])

    def test_skipped_rows_stay_in_the_next_matrix(self):
        self.pass_()
        m = read_matrix()
        self.assertIn(PID, m)
        self.assertIn("101", m[PID]["sample_order_ids"].split())
        self.assertEqual(m[PID]["item_name"], "Dryer")

    def test_a_row_without_a_recorded_id_is_fetched_even_when_its_name_is_blocked(self):
        # a note written before v2.14.1 names the product but records no id
        eng, hs, relay, gio = self.drain(self.TABLE)
        write_matrix([(PID, "Dryer")])
        rows = [qrow(2, "101", "Dryer", 160, f"blocked: Dryer @ {T_OLD}")]
        fetched = self.run_pass(eng, relay, gio, rows, {"101": order("101", [(PID, "product")])})
        self.assertEqual(fetched, ["101"])

    def test_no_record_no_search(self):
        cfg = make_cfg(self.tmp)
        cfg.drain_skip_known_blocked = True
        eng, hs, relay, gio = make_engine(self.tmp, live=False, cfg=cfg)
        rows = [qrow(2, "1", "Dryer", 5), qrow(3, "2", "Dryer", 5, f"blocked: Dryer @ {T_OLD}")]
        self.assertEqual(eng._skip_known_blocked(rows), rows)
        hs.gate_search_product_approved.assert_not_called()

    def test_a_failed_recheck_fetches_the_rows(self):
        table = dict(self.TABLE)
        eng, hs, relay, gio = self.drain(table)
        hs.gate_search_product_approved.side_effect = RuntimeError("HubSpot 502")
        rows = [qrow(2, "101", "Dryer", 160, note("Dryer", T_OLD, PID))]
        self.assertEqual(eng._skip_known_blocked(rows), rows)
        self.assertNotIn(PID, eng._gate_cache)    # an unknown is never cached

    def test_an_error_row_with_an_old_record_is_fetched(self):
        # a relay miss leaves the last blocked note in place; that row must
        # still reach "Gone" through its own attempts
        eng, hs, relay, gio = self.drain(self.TABLE)
        rows = [qrow(2, "101", "Dryer", 2, note("Dryer", T_OLD, PID), status="error")]
        self.assertEqual(eng._skip_known_blocked(rows), rows)


class SameName(Pass, unittest.TestCase):
    """Findings 2 and 19: the relisted dryer and the old listing share a name."""

    def rows_and_db(self):
        rows = [qrow(2, "201", "Dryer", 5, note("Dryer", T_OLD, PID)),
                qrow(3, "202", "Dryer", 5, note("Dryer", T_OLD, OLD))]
        db = {"201": order("201", [(PID, "product")]), "202": order("202", [(OLD, "product")])}
        return rows, db

    def test_approved_listing_is_fetched_while_the_old_one_stays_blocked(self):
        table = {OLD: (0, 0, 0), PID: (1, 0, 0)}
        rows, db = self.rows_and_db()
        write_matrix([(OLD, "Dryer"), (PID, "Dryer")])
        for _ in range(2):                        # and again on the next pass
            eng, hs, relay, gio = self.drain(table)
            self.assertEqual(self.run_pass(eng, relay, gio, rows, db), ["201"])
            m = read_matrix()
            self.assertEqual(m[OLD]["sample_order_ids"], "202")   # never filed under the old id
            self.assertEqual(m[OLD]["blocked_orders"], "1")

    def test_two_blocked_listings_with_one_name_both_stay_in_the_matrix(self):
        table = {OLD: (0, 0, 0), PID: (0, 0, 0)}
        rows, db = self.rows_and_db()
        write_matrix([(OLD, "Dryer"), (PID, "Dryer")])
        eng, hs, relay, gio = self.drain(table)
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), [])
        m = read_matrix()
        self.assertEqual((m[PID]["sample_order_ids"], m[OLD]["sample_order_ids"]), ("201", "202"))
        # approving one of them later releases exactly its own rows
        eng, hs, relay, gio = self.drain({OLD: (0, 0, 0), PID: (1, 0, 0)})
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), ["201"])


class Records(Pass, unittest.TestCase):
    def test_drain_one_records_the_blocking_ids_and_keeps_the_note_readable(self):
        eng, hs, relay, gio = self.drain({PID: (0, 0, 0), "2": (1, 0, 0)}, live=True)
        o = order("1", [(PID, "product"), ("2", "product")])
        o["items"] += deleted_item_order("1", "C18")["items"]
        with mock.patch.object(queue_drain, "now_str", return_value=T_NEW):
            st, text, d = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0}, o)
        self.assertEqual((st, d), ("Queued", 1))
        self.assertEqual(queue_drain.recorded_keys(text), {PID, "sku:C18"})
        self.assertEqual(queue_drain.last_tried(text), T_NEW)
        # tools/held_inventory.py reads the blocker names before the "@"
        self.assertEqual(text.split("@")[0].replace("blocked:", "").strip(),
                         f"item-{PID}, Old dryer")

    def test_a_key_that_cannot_be_written_back_records_nothing(self):
        eng, hs, relay, gio = self.drain({}, live=True)
        st, text, _ = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0},
                                    deleted_item_order("1", "A,B"))
        self.assertEqual(st, "Queued")
        self.assertIsNone(queue_drain.recorded_keys(text))

    def test_parsing(self):
        rk, lt = queue_drain.recorded_keys, queue_drain.last_tried
        self.assertEqual(rk(note("Dryer", T_OLD, PID, "sku:C18")), {PID, "sku:C18"})
        self.assertIsNone(rk(f"blocked: Dryer @ {T_OLD}"))          # pre-v2.14.1 note
        self.assertIsNone(rk(f"processing since {T_OLD}"))
        self.assertIsNone(rk(""))
        self.assertIsNone(rk(note("Dryer", T_OLD)))                 # empty record
        # a name cannot forge a record: only the trailing one counts
        self.assertEqual(rk(f"blocked: Odd [pids=9] A@B @ {T_OLD} [pids=1]"), {"1"})
        self.assertEqual(lt(f"blocked: A@B @ {T_OLD} [pids=1]"), T_OLD)
        self.assertEqual(lt(f"processing since {T_OLD}"), T_OLD)
        self.assertEqual(lt("blocked: Dryer"), "")
        self.assertEqual(lt(""), "")

    def test_deleted_product_rows_are_skipped_by_sku_until_it_resolves(self):
        rows = [qrow(2, "301", "Old dryer", 4, note("Old dryer", T_OLD, "sku:C18"))]
        db = {"301": deleted_item_order("301", "C18")}
        eng, hs, relay, gio = self.drain({})
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), [])
        m = read_matrix()
        self.assertEqual((m["sku:C18"]["sku"], m["sku:C18"]["sample_order_ids"]), ("C18", "301"))
        self.assertIn("LGCY-C18", m["sku:C18"]["suggested_action"])
        eng, hs, relay, gio = self.drain({}, sku_ok={"C18"})
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), ["301"])


class StrayRecheck(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def rows(self):
        return [qrow(2, "401", "Dryer", 7, note("Dryer", T_OLD, PID))]

    def test_unknown_stray_verdict_fetches(self):
        # an approved product with an empty record and an unreadable Bundles
        # tab: the gate holds it, but it may pass any minute, so it is fetched
        eng, hs = engine(self.tmp, sheet_fail=RuntimeError("HttpError 503"))
        self.assertEqual(eng._skip_known_blocked(self.rows()), self.rows())

    def test_proven_stray_passes_and_a_build_in_progress_is_skipped(self):
        eng, hs = engine(self.tmp)
        self.assertEqual(eng._skip_known_blocked(self.rows()), self.rows())
        eng, hs = engine(self.tmp, recs=DRAFT)
        self.assertEqual(eng._skip_known_blocked(self.rows()), [])


class Ordering(Pass, unittest.TestCase):
    """Finding 20: the head rows the pre-v2.14 passes retried every hour carry
    ~170 attempts; sorting by attempts parked them behind every row that can
    never be skipped, for days, even after their product was approved."""

    def test_longest_untried_first_not_least_tried(self):
        eng, hs, relay, gio = self.drain({PID: (1, 0, 0)})     # the dryer is approved now
        rows = [qrow(2, "501", "Dryer", 170, note("Dryer", T_OLD, PID))]
        rows += [qrow(3 + n, f"60{n}", "Gone thing", 1, f"blocked: Gone thing @ {T_NEW}")
                 for n in range(3)]
        rows += [qrow(9, "701", "New", 0)]
        db = {r["order_id"]: order(r["order_id"], [(PID, "product")]) for r in rows}
        fetched = self.run_pass(eng, relay, gio, rows, db, max_orders=2)
        self.assertEqual(fetched, ["701", "501"])

    def test_processing_since_counts_as_a_try_and_ties_keep_sheet_order(self):
        eng, hs, relay, gio = self.drain({PID: (1, 0, 0)})
        rows = [qrow(2, "801", "A", 1, f"blocked: A @ {T_NEW}"),
                qrow(3, "802", "B", 1, f"processing since {T_OLD}", status="processing"),
                qrow(4, "803", "C", 1, f"blocked: C @ {T_NEW}")]
        db = {r["order_id"]: order(r["order_id"], [(PID, "product")]) for r in rows}
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), ["802", "801", "803"])


class Budget(Pass, unittest.TestCase):
    """Finding 21: the re-check burst ran at the drain's full search rate
    before the first yield to live, and was searched again by gate_cached."""

    def test_yields_to_live_before_the_recheck_searches(self):
        eng, hs, relay, gio = self.drain({PID: (0, 0, 0)})
        events = []
        eng._yield_to_live = lambda: events.append("yield")
        hs.gate_search_product_approved.side_effect = lambda pid: events.append("search") or 0
        rows = [qrow(2, "101", "Dryer", 3, note("Dryer", T_OLD, PID))]
        self.run_pass(eng, relay, gio, rows, {})
        self.assertIn("search", events)
        self.assertEqual(events[0], "yield")

    def test_recheck_seeds_the_gate_cache(self):
        eng, hs, relay, gio = self.drain({PID: (0, 0, 0), "2": (1, 0, 0)})
        rows = [qrow(2, "101", "Dryer", 3, note("Dryer", T_OLD, PID)),     # skipped
                qrow(3, "102", "Dryer", 0),                                # fetched, held
                qrow(4, "103", "Foam", 2, note("Foam", T_OLD, "2"))]       # fetched, passes
        db = {"102": order("102", [(PID, "product")]), "103": order("103", [("2", "product")])}
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), ["102", "103"])
        calls = [c.args[0] for c in hs.gate_search_product_approved.call_args_list]
        self.assertEqual(sorted(calls), sorted([PID, "2"]))              # one search each
        self.assertEqual(hs.gate_search_template.call_count, 4)          # te + ta each


class GateDecision(unittest.TestCase):
    """Finding 4, drain side: gate_cached records the per-order decision the
    line builder reads, for cache hits too, and drops it when the order ends."""

    def setUp(self):
        self.tmp = chdir_tmp(self)

    def test_every_order_gets_the_decision_even_from_the_cache(self):
        eng, hs = engine(self.tmp)
        o1, o2 = order("1", [(PID, "product")]), order("2", [(PID, "product")])
        self.assertEqual(eng.gate_cached(o1), [])
        self.assertEqual(eng.gate_cached(o2), [])
        self.assertEqual((eng.gate_exempt(o1), eng.gate_exempt(o2)), ({PID}, {PID}))

    def test_line_builder_keeps_the_drain_gates_decision(self):
        # the gate passed the order as the approved product; someone starts
        # building the bundle before its line is built: the order keeps the
        # gate's decision instead of getting a needs_review line
        eng, hs = engine(self.tmp)
        o = order("1", [(PID, "product")])
        self.assertEqual(eng.gate_cached(o), [])
        age_cache(eng)
        hs.template_records.return_value = DRAFT
        line_builder_mocks(hs)
        eng.process_item(o, "HS-ORDER", o["items"][0])
        self.assertEqual(sale_context(hs), "standalone_product")
        hs.patch_order.assert_not_called()

    def test_a_regate_follows_the_current_verdict(self):
        eng, hs = engine(self.tmp)
        o = order("1", [(PID, "product")])
        eng.gate_cached(o)
        age_cache(eng)
        hs.template_records.return_value = DRAFT
        self.assertEqual(len(eng.gate_cached(o)), 1)    # building now: held, like live
        self.assertEqual(eng.gate_exempt(o), set())

    def test_drain_one_hands_the_decision_to_create_and_then_drops_it(self):
        eng, hs = engine(self.tmp, live=True)
        hs.find_order_by_salla_id.return_value = None
        eng._audit_rows = {"1": 5}
        seen = {}

        def fake_route_create(o, audit_row):
            seen["exempt"] = eng.gate_exempt(o)
            eng._outcome[str(o["id"])] = ("created", "HS42")
        eng.route_create = fake_route_create
        o = order("1", [(PID, "product")])
        with mock.patch("notify.issue_open"):
            st, _, _ = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0}, o)
        self.assertEqual((st, seen["exempt"]), ("Processed", {PID}))
        self.assertIsNone(eng.gate_exempt(o))

    def test_a_held_order_leaves_no_decision_behind(self):
        eng, hs = engine(self.tmp, p=0)
        hs.find_order_by_salla_id.return_value = None
        o = order("1", [(PID, "product")])
        st, _, _ = eng.drain_one({"row": 2, "order_id": "1", "attempts": 0}, o)
        self.assertEqual(st, "Queued")
        self.assertIsNone(eng.gate_exempt(o))


if __name__ == "__main__":
    unittest.main()
