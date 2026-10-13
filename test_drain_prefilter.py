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
  * [v2.14.1] (round 2) N2: an exemptable product's pass-cached counts are
    read again once they are as old as the stray verdict (at most once per
    TTL), so a stray the sentinel sets aside mid-pass no longer holds the rest
    of the pass. N9: a row is skipped only within 72 h of its last try, and an
    id recorded with its SKU is fetched once that SKU passes, so a listing
    deleted in Salla no longer keeps its rows skipped for good.
All I/O is mocked.
"""
import csv
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import queue_drain
from test_queue_drain import chdir_tmp, make_cfg, make_engine, order
from test_stray_exemption import engine
from test_v2141_gate import DRAFT, age_cache, line_builder_mocks, sale_context

PID = "1982906533"
OLD = "1111"                 # an older listing with the same name
DEL = "777000111"            # a listing later deleted in Salla
STILL_BLOCKED_REC = [{"id": "1", "properties": {"template_status": "draft",
                                                "component_count": None, "active_component_count": None}}]
# [v2.14.1] every test runs at NOW; a row is skipped only within 72 h of its last try
NOW = "2026-10-12 12:00:00"
T_OLD, T_NEW = "2026-10-11 10:17:03", "2026-10-12 09:17:03"
T_72H, T_UNDER_72H = "2026-10-09 12:00:00", "2026-10-09 12:00:01"


def hermetic(test):
    """chdir_tmp, plus the drain's clock frozen at NOW."""
    tmp = chdir_tmp(test)
    p = mock.patch.object(queue_drain, "now_str", return_value=NOW)
    p.start()
    test.addCleanup(p.stop)
    return tmp


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
        self.tmp = hermetic(self)

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
        self.tmp = hermetic(self)

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
        self.tmp = hermetic(self)

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


class Clock:
    """A fake time.time() shared by the drain's count cache and the stray
    verdict cache, so a test can let minutes of a long pass go by."""

    def __init__(self, test):
        self.t = 1_800_000_000.0
        p = mock.patch("time.time", lambda: self.t)
        p.start()
        test.addCleanup(p.stop)

    def advance(self, s):
        self.t += s


def dryer(oid):
    return order(oid, [(PID, "product")])


class CountsAsFreshAsTheVerdict(unittest.TestCase):
    """N2: the pass cached (approved, 0 active, 1 record) while the stray
    existed. The catalog sentinel set the record aside mid-pass; once the
    stray verdict expired it read "no record" (False), and the pass-old ta=1
    held every later order of the approved product while live passed them."""

    def setUp(self):
        self.tmp = hermetic(self)
        self.clock = Clock(self)

    def test_a_stray_set_aside_mid_pass_does_not_hold_the_rest_of_the_pass(self):
        eng, hs = engine(self.tmp)                 # approved, no active template, one empty record
        self.assertEqual(eng.gate_cached(dryer("1")), [])           # exempt
        hs.template_records.return_value = []                       # the sentinel voids the key
        hs.gate_search_template.side_effect = lambda pid, elig: 0
        self.clock.advance(eng.STRAY_TTL_S + 1)
        o2 = dryer("2")
        self.assertEqual(eng.gate_cached(o2), [])
        self.assertEqual(eng.gate_exempt(o2), set())     # passed on its own counts, no exemption
        self.assertEqual(eng.gate_unverified_items(dryer("3")), [])  # what live says

    def test_counts_read_before_the_verdict_are_read_again(self):
        # review round 2, M2: counts and verdict expired on separate clocks, so
        # a void between the counts re-read and the next verdict refresh held
        # the approved product's orders while live passed them
        eng, hs = engine(self.tmp, sheet_fail=RuntimeError("HttpError 503"))
        self.assertEqual(len(eng.gate_cached(dryer("1"))), 1)      # t0: counts read, verdict unknown
        eng.sheet_gio.fail = None
        self.clock.advance(61)
        self.assertEqual(eng.gate_cached(dryer("2")), [])           # t0+61: verdict True
        self.clock.advance(240)
        self.assertEqual(eng.gate_cached(dryer("3")), [])           # t0+301: counts re-read (1, 0, 1)
        hs.template_records.return_value = []                       # t0+330: the sentinel voids it
        hs.gate_search_template.side_effect = lambda pid, elig: 0
        self.clock.advance(61)
        o4 = dryer("4")
        self.assertEqual(eng.gate_cached(o4), [])                   # t0+362: verdict False, counts re-read
        self.assertEqual(eng.gate_exempt(o4), set())
        self.assertEqual(eng.gate_unverified_items(dryer("5")), [])  # what live says

    def test_a_build_started_mid_pass_still_holds(self):
        eng, hs = engine(self.tmp)
        self.assertEqual(eng.gate_cached(dryer("1")), [])
        hs.template_records.return_value = DRAFT
        self.clock.advance(eng.STRAY_TTL_S + 1)
        self.assertEqual(len(eng.gate_cached(dryer("2"))), 1)

    def test_read_again_at_most_once_per_ttl(self):
        eng, hs = engine(self.tmp)
        reads = hs.gate_search_product_approved
        for n in range(3):
            eng.gate_cached(dryer(str(n)))
        self.assertEqual(reads.call_count, 1)
        self.clock.advance(eng.STRAY_TTL_S - 1)
        eng.gate_cached(dryer("3"))
        self.assertEqual(reads.call_count, 1)
        self.clock.advance(2)
        for n in range(4, 9):
            eng.gate_cached(dryer(str(n)))
        self.assertEqual((reads.call_count, hs.gate_search_template.call_count), (2, 4))
        self.clock.advance(eng.STRAY_TTL_S)
        eng.gate_cached(dryer("9"))
        self.assertEqual(reads.call_count, 3)

    def test_one_lane_reads_again_while_the_others_wait(self):
        eng, hs = engine(self.tmp)
        eng.gate_cached(dryer("0"))
        self.clock.advance(eng.STRAY_TTL_S + 1)
        hs.gate_search_product_approved.side_effect = lambda pid: time.sleep(0.02) or 1
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(eng.gate_cached, [dryer(str(n)) for n in range(1, 33)]))
        self.assertEqual(hs.gate_search_product_approved.call_count, 2)

    def test_shapes_the_exemption_cannot_touch_are_read_once_a_pass(self):
        for flag, p, te, ta in ((True, 0, 0, 0), (True, 0, 0, 1), (True, 1, 1, 1),
                                (True, 1, 0, 0), (False, 1, 0, 1)):
            eng, hs = engine(self.tmp, flag=flag, p=p, te=te, ta=ta)
            eng.gate_cached(dryer("1"))
            self.clock.advance(10 * 3600)
            eng.gate_cached(dryer("2"))
            self.assertEqual(hs.gate_search_product_approved.call_count, 1, (flag, p, te, ta))

    def test_a_failed_reread_keeps_the_counts_for_another_ttl(self):
        eng, hs = engine(self.tmp)
        eng.gate_cached(dryer("1"))
        self.clock.advance(eng.STRAY_TTL_S + 1)
        hs.gate_search_product_approved.side_effect = RuntimeError("HubSpot 502")
        self.assertEqual(eng.gate_cached(dryer("2")), [])   # no error row: old counts, live verdict
        self.assertEqual(eng.gate_cached(dryer("3")), [])
        self.assertEqual(hs.gate_search_product_approved.call_count, 2)
        hs.gate_search_product_approved.side_effect = None
        hs.gate_search_product_approved.return_value = 1
        self.clock.advance(eng.STRAY_TTL_S + 1)
        eng.gate_cached(dryer("4"))
        self.assertEqual(hs.gate_search_product_approved.call_count, 3)

    def test_a_failed_first_read_still_raises_and_caches_nothing(self):
        eng, hs = engine(self.tmp)
        hs.gate_search_product_approved.side_effect = RuntimeError("HubSpot 502")
        with self.assertRaises(RuntimeError):
            eng.gate_cached(dryer("1"))
        self.assertNotIn(PID, eng._gate_cache)


def sku_order(oid, pid, sku):
    o = order(oid, [(pid, "product")])
    o["items"][0]["sku"] = sku
    return o


class DeletedListing(Pass, unittest.TestCase):
    """N9: order 301 was held by Salla listing DEL and recorded under it. The
    client then deleted DEL in Salla: the order's item now has no product id
    and the gate resolves it by SKU (C18). Once LGCY-C18 was approved the order
    would pass, but the pre-filter re-checked only DEL, which still fails, so
    the row was skipped on every pass and the matrix kept asking for DEL."""

    TABLE = {DEL: (0, 0, 0)}

    def test_a_row_is_fetched_again_72_hours_after_its_last_try(self):
        db = {"301": deleted_item_order("301", "C18")}
        for ts, want in ((T_UNDER_72H, []), (T_72H, ["301"])):
            rows = [qrow(2, "301", "Old dryer", 1, note("Old dryer", ts, DEL))]
            eng, hs, relay, gio = self.drain(self.TABLE, sku_ok={"C18"})
            self.assertEqual(self.run_pass(eng, relay, gio, rows, db), want, ts)
        self.assertEqual(eng.stats.held, 0)             # fetched, and it passes by SKU
        self.assertNotIn(DEL, read_matrix())

    def test_the_sku_is_recorded_and_the_row_fetched_once_it_passes(self):
        eng, hs, relay, gio = self.drain(self.TABLE, live=True)
        st, text, _ = eng.drain_one({"row": 2, "order_id": "301", "attempts": 0},
                                    sku_order("301", DEL, "C18"))
        self.assertEqual(st, "Queued")
        self.assertEqual(queue_drain.recorded_keys(text), {f"{DEL}/C18"})
        self.assertEqual(text.split("@")[0].replace("blocked:", "").strip(), f"item-{DEL}")
        rows = [qrow(2, "301", f"item-{DEL}", 1, text)]
        db = {"301": deleted_item_order("301", "C18")}
        eng, hs, relay, gio = self.drain(self.TABLE)                 # nothing changed yet
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), [])
        m = read_matrix()[DEL]
        self.assertEqual((m["sku"], m["blocked_orders"], m["sample_order_ids"]), ("C18", "1", "301"))
        eng, hs, relay, gio = self.drain(self.TABLE, sku_ok={"C18"})  # LGCY-C18 approved
        self.assertEqual(self.run_pass(eng, relay, gio, rows, db), ["301"])
        self.assertEqual(eng.stats.held, 0)

    def test_a_sku_that_already_passes_is_not_recorded(self):
        # an approved relisting can carry the same SKU: recording it would get
        # the old listing's rows fetched every pass
        eng, hs, relay, gio = self.drain(self.TABLE, live=True, sku_ok={"C18"})
        _, text, _ = eng.drain_one({"row": 2, "order_id": "301", "attempts": 0},
                                   sku_order("301", DEL, "C18"))
        self.assertEqual(queue_drain.recorded_keys(text), {DEL})

    def test_no_sku_search_when_the_pre_filter_is_off(self):
        eng, hs, relay, gio = self.drain(self.TABLE, flag=False, live=True)
        _, text, _ = eng.drain_one({"row": 2, "order_id": "301", "attempts": 0},
                                   sku_order("301", DEL, "C18"))
        self.assertEqual(queue_drain.recorded_keys(text), {DEL})
        hs.gate_search_product_by_sku.assert_not_called()

    def test_a_failed_sku_check_records_the_sku(self):
        eng, hs, relay, gio = self.drain(self.TABLE, live=True)
        hs.gate_search_product_by_sku.side_effect = RuntimeError("HubSpot 502")
        st, text, _ = eng.drain_one({"row": 2, "order_id": "301", "attempts": 0},
                                    sku_order("301", DEL, "C18"))
        self.assertEqual((st, queue_drain.recorded_keys(text)), ("Queued", {f"{DEL}/C18"}))

    def test_a_failed_sku_recheck_fetches_the_row(self):
        eng, hs, relay, gio = self.drain(self.TABLE)
        hs.gate_search_product_by_sku.side_effect = RuntimeError("HubSpot 502")
        rows = [qrow(2, "301", "Old dryer", 1, note("Old dryer", T_OLD, f"{DEL}/C18"))]
        self.assertEqual(eng._skip_known_blocked(rows), rows)

    def test_a_skipped_row_counts_once_per_product(self):
        rows = [qrow(2, "301", "Old dryer", 1, note("Old dryer", T_OLD, DEL, f"{DEL}/C18", f"{DEL}/C19"))]
        eng, hs, relay, gio = self.drain(self.TABLE)
        self.assertEqual(self.run_pass(eng, relay, gio, rows, {}), [])
        m = read_matrix()
        self.assertEqual(list(m), [DEL])
        self.assertEqual((m[DEL]["blocked_orders"], m[DEL]["sku"], m[DEL]["item_name"]),
                         ("1", "C18", "Old dryer"))
        # either SKU passing now is enough to fetch it
        eng, hs, relay, gio = self.drain(self.TABLE, sku_ok={"C19"})
        self.assertEqual(self.run_pass(eng, relay, gio, rows, {"301": deleted_item_order("301", "C19")}),
                         ["301"])

    def test_parsing(self):
        self.assertEqual(queue_drain.split_key(f"{DEL}/C18"), (DEL, "C18"))
        self.assertEqual(queue_drain.split_key(f"{DEL}/A/B"), (DEL, "A/B"))
        self.assertEqual(queue_drain.split_key(DEL), (DEL, ""))
        self.assertEqual(queue_drain.split_key("sku:A/B"), ("sku:A/B", "A/B"))
        self.assertEqual(queue_drain.split_key("sku:-"), ("sku:-", ""))
        u = {"pid": DEL, "sku": "C18", "name": "x"}
        self.assertEqual(queue_drain.pids_note([u], {"C18"}), f" [pids={DEL}/C18]")
        self.assertEqual(queue_drain.pids_note([u]), f" [pids={DEL}]")
        # a SKU that cannot be written back is left off its id
        self.assertEqual(queue_drain.pids_note([dict(u, sku="A,B")], {"A,B"}), f" [pids={DEL}]")
        # an id that could be read as id/sku records nothing
        self.assertEqual(queue_drain.pids_note([dict(u, pid="1/2")]), "")
        tw = queue_drain.tried_within
        self.assertTrue(tw(note("x", T_UNDER_72H, DEL), queue_drain.SKIP_MAX_AGE_S, NOW))
        self.assertFalse(tw(note("x", T_72H, DEL), queue_drain.SKIP_MAX_AGE_S, NOW))
        self.assertFalse(tw("blocked: x", queue_drain.SKIP_MAX_AGE_S, NOW))


if __name__ == "__main__":
    unittest.main()


class FailuresAfterApproval(Pass, unittest.TestCase):
    """[v2.14.1] drain review: every blocked pass adds an attempt, so a row
    held for a week carried ~170 and, once its product was approved, one
    relay miss or create error parked it for good (Gone, or an Error row
    claimable never picks up), and the order was never created."""

    @staticmethod
    def marks(gio):
        out = {}
        for c in gio.qlog_mark_batch.call_args_list:
            for _row, oid, upd in c.args[0]:
                out[oid] = upd
        return out

    def miss_pass(self, rows):
        eng, hs, relay, gio = self.drain({PID: (1, 0, 0)}, live=True)    # approved now
        gio.qlog_read = lambda: rows
        gio.qlog_mark_batch = mock.Mock(return_value=1)
        relay.fetch_orders.side_effect = lambda ids: {}                # the relay misses
        eng.run_drain()
        return eng, self.marks(gio)

    def test_a_miss_after_a_long_hold_starts_counting_from_zero(self):
        eng, m = self.miss_pass([qrow(2, "601", "Dryer", 170, note("Dryer", T_OLD, PID))])
        u = m["601"]
        self.assertEqual((u[queue_drain.QC_STATUS], u[queue_drain.QC_ATTEMPTS], u[queue_drain.QC_NOTES]),
                         ("Error", 1, f"failed @ {NOW}"))       # timestamped: sorts by this try
        self.assertTrue(eng.claimable([qrow(2, "601", "Dryer", 1, "", status="error")]))

    def test_an_interrupted_row_past_the_cap_starts_from_zero(self):
        # round 4: a wave stopped mid-row leaves "processing since" and ~170
        eng, m = self.miss_pass([qrow(2, "603", "Dryer", 170, f"processing since {T_OLD}", status="processing")])
        self.assertEqual((m["603"][queue_drain.QC_STATUS], m["603"][queue_drain.QC_ATTEMPTS]), ("Error", 1))

    def test_a_create_error_is_timestamped_and_sorts_by_that_try(self):
        # round 4: a failure wrote an empty note, which sorts as never tried,
        # so failing rows took every slot ahead of the approved backlog
        eng, hs, relay, gio = self.drain({PID: (1, 0, 0)}, live=True)
        rows = [qrow(2, "604", "Dryer", 170, note("Dryer", T_OLD, PID))]
        gio.qlog_read = lambda: rows
        gio.qlog_mark_batch = mock.Mock(return_value=1)
        relay.fetch_orders.side_effect = lambda ids: {i: order(i, [(PID, "product")]) for i in ids}
        eng.route_create = lambda o, r: None                     # no outcome: a create error
        eng._audit_rows = {"604": 5}
        eng.run_drain()
        u = self.marks(gio)["604"]
        self.assertEqual((u[queue_drain.QC_STATUS], u[queue_drain.QC_ATTEMPTS], u[queue_drain.QC_NOTES]),
                         ("Error", 1, f"failed @ {NOW}"))
        self.assertGreater(queue_drain.last_tried(u[queue_drain.QC_NOTES]),
                           queue_drain.last_tried(note("Foam", T_OLD, "2")))

    def test_real_failures_still_reach_the_cap(self):
        eng, hs, relay, gio = self.drain({PID: (1, 0, 0)})
        cap = eng.cfg.live_max_attempts
        eng, m = self.miss_pass([qrow(2, "602", "Dryer", cap - 1, "", status="error")])
        self.assertEqual(m["602"][queue_drain.QC_STATUS], "Gone")


class AgeRecheckWaves(Pass, unittest.TestCase):
    """[v2.14.1] drain review: rows a big hold recorded came due for the 72 h
    re-check together and filled every hourly slot for hours, ahead of the
    orders of a product approved meanwhile (tried more recently)."""

    def test_approved_rows_first_and_age_rechecks_capped(self):
        eng, hs, relay, gio = self.drain({PID: (1, 0, 1), "2": (1, 0, 0)})   # PID blocked, "2" approved
        old = [qrow(10 + i, f"70{i}", "Dryer", 5, note("Dryer", T_72H, PID)) for i in range(10)]
        new = [qrow(30 + i, f"80{i}", "Foam", 2, note("Foam", T_NEW, "2")) for i in range(2)]
        db = {r["order_id"]: order(r["order_id"], [(PID, "product")]) for r in old}
        db.update({r["order_id"]: order(r["order_id"], [("2", "product")]) for r in new})
        fetched = self.run_pass(eng, relay, gio, old + new, db, max_orders=4)
        self.assertEqual(fetched[:2], ["800", "801"])          # the approved product first
        self.assertEqual(len(fetched), 3)                       # one age re-check (4 // 5 -> 1)
        self.assertIn(fetched[2], {r["order_id"] for r in old})

    def test_without_a_cap_every_due_row_is_fetched_last(self):
        eng, hs, relay, gio = self.drain({PID: (1, 0, 1), "2": (1, 0, 0)})
        old = [qrow(10 + i, f"70{i}", "Dryer", 5, note("Dryer", T_72H, PID)) for i in range(3)]
        new = [qrow(30, "800", "Foam", 2, note("Foam", T_NEW, "2"))]
        db = {r["order_id"]: order(r["order_id"], [(PID, "product")]) for r in old}
        db["800"] = order("800", [("2", "product")])
        fetched = self.run_pass(eng, relay, gio, old + new, db)
        self.assertEqual(fetched[0], "800")
        self.assertEqual(sorted(fetched[1:]), ["700", "701", "702"])
