#!/usr/bin/env python3
"""v2.14.1 live settle: the hourly step that answers the held threads.

Findings #5 and #29: a stray-record thread was closed with "someone has
started building it as a bundle" when the second HubSpot read failed (or the
Bundles tab could not be read). It is now judged from the records already in
hand, and only a positive sign of a build closes it.

Findings #11 and #31: a held:<product> thread waited for every order it ever
listed to reach HubSpot, so one order deleted in Salla, or one also blocked
by a second product, kept it open for good. It now closes when its product
passes the gate and no order waits for it alone; orders that will never be
created are dropped from it.

Finding #30 (and the rest of #11): still_held counted Zid collision parks and
orders the drain had given up on (Gone), so held-backlog and every daily
held-escalation thread never closed.

Everything is offline: Slack, email, HubSpot and the sheets are fakes; the
issue registry is the real one in a temporary directory.
"""
import types
import unittest
from unittest import mock

import backfill
import live
import notify
import queue_drain
from test_realtime_ext import FakeSlack, _chdir_tmp

EMPTY = [{"id": "T1", "properties": {"template_status": None, "component_count": "0",
                                     "active_component_count": ""}}]
ZID_NOTE = ("zid collision: salla_order_id 9 is held by Zid order HS 77; Salla order not created, "
            "awaiting Zid re-key")


def row(n, oid, status, ref="", note="", attempts=0):
    return {"row": n, "order_id": oid, "reference_id": ref, "status": status,
            "attempts": attempts, "note": note, "received_at": "2026-10-10 10:00:00"}


class SettleCase(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        self.slack = FakeSlack()
        p = self.slack.patch()
        p.start()
        self.addCleanup(p.stop)
        self.ledger, self.approved, self.qlog = {}, {}, []
        self.qlog_read = mock.Mock(side_effect=lambda gio: list(self.qlog))
        q = mock.patch.object(queue_drain.DrainGoogleIO, "qlog_read", self.qlog_read)
        q.start()
        self.addCleanup(q.stop)

    def engine(self, rows=()):
        e = object.__new__(live.LiveEngine)
        e.cfg = backfill.Config()
        e.live, e.qsid = True, "Q"
        e.created_ledger = types.SimpleNamespace(get=lambda oid: self.ledger.get(str(oid)))
        e.gio = types.SimpleNamespace(queue_read_all=lambda q, tab=None: list(rows),
                                      queue_mark_batch=mock.Mock(side_effect=lambda q, m: len(m)))
        e.hs = mock.Mock()

        def approved(pid):
            v = self.approved.get(str(pid), 0)
            if isinstance(v, Exception):
                raise v
            return v
        e.hs.gate_search_product_approved.side_effect = approved
        e.hs.gate_search_template.return_value = 0
        e.hs.search_active_components.return_value = []
        return e

    def settle(self, e):
        with mock.patch("realtime_base.trim_lock_active", return_value=False):
            return live.LiveEngine._maybe_settle_held(e)

    def open_held(self, pid, orders, refs, name="Hair Dryer"):
        self.assertTrue(notify.issue_open(f"held:{pid}", f"{name} holds orders", "detail",
                                          meta={"orders": orders, "refs": refs, "name": name, "pid": pid}))

    def state(self, key):
        return (notify.issue_get(key) or {}).get("state")

    def replies(self):
        return [t for _, t, _ in self.slack.replies]

    def assertPlain(self):
        for t in self.replies():
            self.assertNotIn("—", t)


class StrayThreads(SettleCase):
    """#5, #29: only a positive sign of a build closes a stray thread."""

    def setUp(self):
        super().setUp()
        self.assertTrue(notify.issue_open("stray:42", "An empty bundle record appeared", "detail",
                                          meta={"pid": "42", "name": "Multi Styler"}))

    def test_second_read_failure_no_longer_closes_as_building(self):
        e = self.engine()
        e.hs.template_records.side_effect = [EMPTY, RuntimeError("HubSpot search 502")]
        e._bundles_row = mock.Mock(return_value=False)        # tab read, no row: a stray
        self.settle(e)
        self.assertEqual(self.state("stray:42"), "open")
        self.assertFalse(any("started building" in t for t in self.replies()))
        self.assertEqual(e.hs.template_records.call_count, 1)    # judged from the records in hand

    def test_unreadable_bundles_tab_leaves_the_thread_untouched(self):
        e = self.engine()
        e.hs.template_records.return_value = EMPTY
        e._bundles_row = mock.Mock(return_value=None)          # Sheets failed: unknown
        self.settle(e)
        self.assertEqual(self.state("stray:42"), "open")
        self.assertEqual(self.replies(), [])

    def test_component_read_failure_leaves_the_thread_open(self):
        e = self.engine()
        e.hs.template_records.return_value = EMPTY
        e.hs.search_active_components.side_effect = RuntimeError("HubSpot 429")
        e._bundles_row = mock.Mock(return_value=False)
        self.settle(e)
        self.assertEqual(self.state("stray:42"), "open")

    def test_a_status_closes_it_as_building(self):
        e = self.engine()
        e.hs.template_records.return_value = [{"id": "T1", "properties": {"template_status": "draft"}}]
        e._bundles_row = mock.Mock(return_value=False)
        e._stray_cache = {"42": (0, True)}
        self.settle(e)
        self.assertEqual(self.state("stray:42"), "closed")
        self.assertIn("someone has started building Multi Styler", self.replies()[-1])
        self.assertNotIn("42", e._stray_cache)                  # the gate re-checks at once
        self.assertPlain()

    def test_a_bundles_tab_row_closes_it_as_building(self):
        e = self.engine()
        e.hs.template_records.return_value = EMPTY
        e._bundles_row = mock.Mock(return_value=7)
        self.settle(e)
        self.assertEqual(self.state("stray:42"), "closed")

    def test_record_gone_resolves_as_set_aside(self):
        e = self.engine()
        e.hs.template_records.return_value = []
        self.settle(e)
        self.assertEqual(self.state("stray:42"), "closed")
        self.assertIn("has been set aside", self.replies()[-1])


class BacklogThreads(SettleCase):
    """#30 and #11: the backlog and escalation threads close when no catalog
    hold is left, whatever Zid parks or given-up orders remain."""

    def setUp(self):
        super().setUp()
        self.assertTrue(notify.issue_open("held-backlog", "Orders on hold get delivery updates", "detail"))
        self.assertTrue(notify.issue_open("held-escalation:2026-10-12", "Orders on hold", "detail"))

    def test_zid_collision_park_does_not_keep_them_open(self):
        rows = [row(5, "1", "held", "1001", "catalog gate: Hair Dryer -- in review queue"),
                row(6, "9", "held", "1009", ZID_NOTE)]
        self.ledger["1"] = "H1"
        self.assertEqual(self.settle(self.engine(rows)), 1)
        self.assertEqual(self.state("held-backlog"), "closed")
        self.assertEqual(self.state("held-escalation:2026-10-12"), "closed")
        self.assertIn("Every one of them is in HubSpot now.", self.replies()[0])
        self.qlog_read.assert_not_called()                      # nothing held: no Queue Log read
        self.assertPlain()

    def test_order_the_drain_gave_up_on_does_not_keep_them_open(self):
        rows = [row(5, "2", "held", "1002", "catalog gate: Hair Dryer -- in review queue")]
        self.qlog = [{"order_id": "2", "status": "gone", "attempts": 8}]
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held-backlog"), "closed")
        self.assertEqual(self.state("held-escalation:2026-10-12"), "closed")
        self.assertIn("except 1 order that left the queue without being created", self.replies()[0])
        self.assertPlain()

    def test_out_of_attempts_counts_as_given_up_but_retries_left_do_not(self):
        rows = [row(5, "2", "held", "1002", "catalog gate: X -- in review queue")]
        self.qlog = [{"order_id": "2", "status": "error", "attempts": 3}]
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held-backlog"), "open")
        self.qlog = [{"order_id": "2", "status": "error", "attempts": 8}]
        e = self.engine(rows)
        self.settle(e)
        self.assertEqual(self.state("held-backlog"), "closed")

    def test_an_order_still_queued_for_the_drain_keeps_them_open(self):
        rows = [row(5, "2", "held", "1002", "catalog gate: X -- in review queue")]
        self.qlog = [{"order_id": "2", "status": "gone", "attempts": 8},     # an old row
                     {"order_id": "2", "status": "queued", "attempts": 1}]   # and a live one
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held-backlog"), "open")
        self.assertEqual(self.state("held-escalation:2026-10-12"), "open")

    def test_unreadable_queue_log_closes_nothing(self):
        rows = [row(5, "2", "held", "1002", "catalog gate: X -- in review queue")]
        self.qlog_read.side_effect = RuntimeError("Sheets 503")
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held-backlog"), "open")


class ProductThreads(SettleCase):
    """#11, #31: a product thread closes when its product passes and no order
    waits for it alone."""

    def test_order_deleted_in_salla_no_longer_blocks_the_release(self):
        rows = [row(5, "A", "held", "1001", "catalog gate: Hair Dryer -- in review queue"),
                row(6, "B", "held", "1002", "catalog gate: Hair Dryer -- in review queue")]
        self.ledger["A"] = "H1"
        self.qlog = [{"order_id": "B", "status": "gone", "attempts": 8}]
        self.approved["P1"] = 1
        self.open_held("P1", ["A", "B"], ["1001", "1002"])
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "closed")
        text = self.replies()[-1]
        self.assertIn("Released: Hair Dryer passes the catalog check now, and 1 order", text)
        self.assertIn("|#1001>", text)                          # the reference the client knows, linked
        self.assertIn("Order #1002 was not created", text)
        self.assertPlain()

    def test_order_blocked_by_two_products_does_not_hold_the_passing_ones_thread(self):
        rows = [row(5, "C", "held", "1003", "catalog gate: Hair Dryer, Comb -- in review queue"),
                row(6, "D", "held", "1004", "catalog gate: Hair Dryer -- in review queue")]
        self.ledger["D"] = "H4"
        self.qlog = [{"order_id": "C", "status": "queued", "attempts": 2}]
        self.approved["X"] = 1                                   # X passes, Y still fails
        self.open_held("X", ["C", "D"], ["1003", "1004"], name="Hair Dryer")
        self.open_held("Y", ["C"], ["1003"], name="Comb")
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:X"), "closed")
        self.assertEqual(self.state("held:Y"), "open")
        text = self.replies()[-1]
        self.assertIn("|#1004>", text)
        self.assertIn("Still waiting for another product: #1003. That product's own thread confirms it.", text)
        self.assertEqual(notify.issue_get("held:Y")["meta"]["orders"], ["C"])
        self.assertPlain()

    def test_both_products_fixed_wait_for_the_drain_together(self):
        rows = [row(5, "C", "held", "1003", "catalog gate: Hair Dryer, Comb -- in review queue")]
        self.approved.update({"X": 1, "Y": 1})
        self.open_held("X", ["C"], ["1003"])
        self.open_held("Y", ["C"], ["1003"], name="Comb")
        e = self.engine(rows)
        self.settle(e)
        self.assertEqual((self.state("held:X"), self.state("held:Y")), ("open", "open"))
        self.ledger["C"] = "H3"                                  # the drain created it
        e._next_settle = 0
        self.settle(e)
        self.assertEqual((self.state("held:X"), self.state("held:Y")), ("closed", "closed"))

    def test_order_waiting_only_for_a_fixed_product_keeps_its_thread_until_created(self):
        rows = [row(5, "E", "held", "1005", "catalog gate: Hair Dryer -- in review queue")]
        self.approved["P1"] = 1
        self.open_held("P1", ["E"], ["1005"])
        e = self.engine(rows)
        self.settle(e)
        self.assertEqual(self.state("held:P1"), "open")
        self.qlog_read.assert_called_once()
        self.ledger["E"] = "H5"
        e._next_settle = 0
        self.settle(e)
        self.assertEqual(self.state("held:P1"), "closed")
        self.assertIn("|#1005>", self.replies()[-1])

    def test_failing_product_keeps_its_thread_and_drops_orders_that_will_never_come(self):
        rows = [row(5, "A", "gone", "1001", "relay returned nothing after 8 attempts"),
                row(6, "B", "held", "1002", "catalog gate: Hair Dryer -- in review queue")]
        self.open_held("P1", ["A", "B"], ["1001", "1002"])     # P1 is not approved
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "open")
        meta = notify.issue_get("held:P1")["meta"]
        self.assertEqual((meta["orders"], meta["refs"], meta["closed"]), (["B"], ["1002"], ["1001"]))
        self.assertEqual(self.replies(), [])

    def test_unknown_gate_state_keeps_the_thread_open(self):
        rows = [row(5, "A", "done", "1001", "HS H1")]
        self.ledger["A"] = "H1"
        self.approved["P1"] = RuntimeError("HubSpot 502")
        self.open_held("P1", ["A"], ["1001"])
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "open")

    def test_released_reply_names_orders_by_reference_when_refs_are_out_of_step(self):
        rows = [row(5, "777001", "done", "5001", "HS H1"), row(6, "777002", "done", "5002", "HS H2")]
        self.ledger.update({"777001": "H1", "777002": "H2"})
        self.approved["P1"] = 1
        self.open_held("P1", ["777001", "777002"], ["5001"])   # refs out of step with orders
        self.settle(self.engine(rows))
        text = self.replies()[-1]
        self.assertIn("|#5001>", text)
        self.assertIn("|#5002>", text)
        self.assertNotIn("#777002", text)

    def test_order_released_from_hold_is_dropped_and_named_later(self):
        # a person closed the row by hand ("cancelled"): it never reaches HubSpot
        rows = [row(5, "A", "cancelled", "1001", "closed by hand"),
                row(6, "B", "held", "1002", "catalog gate: Hair Dryer -- in review queue")]
        self.open_held("P1", ["A", "B"], ["1001", "1002"])
        e = self.engine(rows)
        self.settle(e)                                           # P1 still fails
        self.assertEqual(notify.issue_get("held:P1")["meta"]["orders"], ["B"])
        self.approved["P1"] = 1
        self.ledger["B"] = "H2"
        e._next_settle = 0
        self.settle(e)
        text = self.replies()[-1]
        self.assertIn("|#1002>", text)
        self.assertIn("Order #1001 was not created", text)

    def test_deleted_product_thread_follows_the_gates_sku_rule(self):
        rows = [row(5, "F", "done", "1006", "HS H6")]
        self.ledger["F"] = "H6"
        self.assertTrue(notify.issue_open("held:sku:ABC-1", "Deleted product holds orders", "detail",
                                          meta={"orders": ["F"], "refs": ["1006"], "name": "Old Brush"}))
        e = self.engine(rows)
        e.hs.gate_search_product_by_sku.return_value = 0
        self.settle(e)
        self.assertEqual(self.state("held:sku:ABC-1"), "open")
        e.hs.gate_search_product_by_sku.return_value = 1
        e._next_settle = 0
        self.settle(e)
        self.assertEqual(self.state("held:sku:ABC-1"), "closed")
        e.hs.gate_search_product_by_sku.assert_called_with(["ABC-1", "LGCY-ABC-1"])

    def test_no_queue_log_read_when_no_held_thread_is_open(self):
        rows = [row(5, "B", "held", "1002", "catalog gate: Hair Dryer -- in review queue")]
        self.settle(self.engine(rows))
        self.qlog_read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
