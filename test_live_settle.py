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

Round 2 (N4 and integration concern 2): the Live Queue rows of orders the
drain finished with read "held" for good, so the status relay, the Sunday
held index and the digest kept counting them after the threads closed, the
relay's escalation came back every day, and a give-up for an order the drain
had given up on reopened held-backlog. The settle now marks those rows
"gone" and every reader agrees. N7: a held thread whose orders are all
created or gone closes whatever the gate says, without claiming the product
passes. N8: a held record Slack refused is closed quietly once nothing it
listed waits, and the settle posts what Slack refused (notify.retry_failed).

Everything is offline: Slack, email, HubSpot and the sheets are fakes; the
issue registry is the real one in a temporary directory.
"""
import json
import time
import types
import unittest
from contextlib import ExitStack
from datetime import datetime
from unittest import mock

import backfill
import live
import notify
import queue_drain
from test_realtime_ext import FakeHS, FakeSlack, IndexedGIO, _cfg, _chdir_tmp, _row

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
        notify._breaker["until"] = 0.0
        self.addCleanup(notify._breaker.update, until=0.0)
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

        def mark(q, marks):                     # the marks land on the sheet, as queue_mark_batch's do
            by_row = {r["row"]: r for r in rows}
            for n, oid, status, attempts, note in marks:
                if n in by_row and by_row[n]["order_id"] == oid:
                    by_row[n].update(status=status, attempts=attempts, note=note)
            return len(marks)
        e.gio = types.SimpleNamespace(queue_read_all=lambda q, tab=None: [dict(r) for r in rows],
                                      queue_mark_batch=mock.Mock(side_effect=mark))
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

    def reply_with(self, part):
        found = [t for t in self.replies() if part in t]
        self.assertEqual(len(found), 1, self.replies())
        return found[0]


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


    def test_unreadable_queue_log_writes_no_held_count(self):
        # round 4: with the Queue Log unreadable every drain-closed row counted
        # as held, and the stamp reopened a cleared escalation in the relay
        rows = [row(5, "2", "held", "1002", "catalog gate: X -- in review queue")]
        live.stamp_settle_held(0)
        self.qlog_read.side_effect = RuntimeError("Sheets 503")
        self.settle(self.engine(rows))
        self.assertEqual(json.loads(live.SETTLE_HELD.read_text())["count"], 0)
        self.qlog_read.side_effect = lambda gio: list(self.qlog)
        self.settle(self.engine(rows))
        self.assertEqual(json.loads(live.SETTLE_HELD.read_text())["count"], 1)

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

    def test_unknown_gate_state_keeps_a_thread_with_an_order_waiting_open(self):
        # B waits for both products; P2 passes, P1's gate cannot be read
        rows = [row(5, "B", "held", "1002", "catalog gate: Hair Dryer, Comb -- in review queue")]
        self.qlog = [{"order_id": "B", "status": "queued", "attempts": 1}]
        self.approved.update({"P1": RuntimeError("HubSpot 502"), "P2": 1})
        self.open_held("P1", ["B"], ["1002"])
        self.open_held("P2", ["B"], ["1002"], name="Comb")
        self.settle(self.engine(rows))
        self.assertEqual((self.state("held:P1"), self.state("held:P2")), ("open", "open"))
        self.assertEqual(self.replies(), [])

    def test_unknown_gate_state_closes_a_thread_with_nothing_waiting_without_claiming_a_pass(self):
        # [v2.14.1] N7: every order it listed is in HubSpot, so the gate has nothing to decide
        rows = [row(5, "A", "done", "1001", "HS H1")]
        self.ledger["A"] = "H1"
        self.approved["P1"] = RuntimeError("HubSpot 502")
        self.open_held("P1", ["A"], ["1001"])
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "closed")
        text = self.replies()[-1]
        self.assertTrue(text.startswith("✅ Every order that waited for Hair Dryer is in HubSpot now: "), text)
        self.assertIn("|#1001>", text)
        self.assertNotIn("passes", text)
        self.assertNotIn("does not pass", text)               # unknown: no claim either way
        self.assertPlain()

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
        # [v2.14.1] N7: both close once their orders are in HubSpot; only the SKU the
        # gate matches is called a pass
        rows = [row(5, "F", "done", "1006", "HS H6"), row(6, "G", "done", "1007", "HS H7")]
        self.ledger.update({"F": "H6", "G": "H7"})
        self.assertTrue(notify.issue_open("held:sku:ABC-1", "Deleted product holds orders", "detail",
                                          meta={"orders": ["F"], "refs": ["1006"], "name": "Old Brush"}))
        self.assertTrue(notify.issue_open("held:sku:XYZ-2", "Deleted product holds orders", "detail",
                                          meta={"orders": ["G"], "refs": ["1007"], "name": "Old Comb"}))
        e = self.engine(rows)
        e.hs.gate_search_product_by_sku.side_effect = lambda skus: 1 if skus[0] == "ABC-1" else 0
        self.settle(e)
        self.assertEqual((self.state("held:sku:ABC-1"), self.state("held:sku:XYZ-2")), ("closed", "closed"))
        self.assertIn("|#1006>", self.reply_with("Released: Old Brush passes the catalog check now"))
        text = self.reply_with("Old Comb")
        self.assertTrue(text.startswith("✅ Every order that waited for Old Comb is in HubSpot now: "), text)
        self.assertIn("A new order with Old Comb would wait again, in a new thread: the product still does "
                      "not pass the catalog check.", text)
        e.hs.gate_search_product_by_sku.assert_any_call(["ABC-1", "LGCY-ABC-1"])
        e.hs.gate_search_product_by_sku.assert_any_call(["XYZ-2", "LGCY-XYZ-2"])
        self.assertPlain()

    def test_queue_log_read_once_per_settle_and_never_when_nothing_is_held(self):
        # [v2.14.1] N4 needs the Queue Log whenever a row is held, thread or not
        rows = [row(5, "B", "held", "1002", "catalog gate: Hair Dryer -- in review queue")]
        self.open_held("P1", ["B"], ["1002"])
        self.settle(self.engine(rows))
        self.qlog_read.assert_called_once()                     # the marks and the threads share it
        self.qlog_read.reset_mock()
        self.settle(self.engine([row(5, "B", "done", "1002", "HS H2")]))
        self.qlog_read.assert_not_called()


HELD = "catalog gate: Hair Dryer -- in review queue"


class Clock:
    """time.time() and the relay's day, on one fake clock."""

    def __init__(self, start):
        self.t = start.timestamp()

    def patch(self):
        import status_relay
        clock = self

        class Now(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromtimestamp(clock.t, tz)

        stack = ExitStack()
        stack.enter_context(mock.patch("time.time", lambda: clock.t))
        stack.enter_context(mock.patch.object(status_relay, "datetime", Now))
        return stack


class DrainClosedRows(SettleCase):
    """[v2.14.1] N4 and integration concern 2: the held rows of orders the
    drain finished with are marked "gone", so the relay, the held index (and
    the digest that reads it) and the live threads count the same holds."""

    def drained_queue(self):
        rows = [row(2, "1", "held", "284000001", HELD),       # deleted in Salla: Queue Log gone
                row(3, "2", "held", "284000002", HELD),       # out of attempts
                row(4, "3", "held", "284000003", HELD),       # a Zid park in the drain (error at the cap)
                row(5, "4", "held", "284000004", HELD),       # attempts left: still the drain's
                row(6, "5", "held", "284000005", HELD),       # queued in the Queue Log
                row(7, "6", "held", "284000006", HELD),       # created since: done
                row(8, "7", "held", "284000007", ZID_NOTE),   # the live engine's own Zid park
                row(9, "8", "held", "284000008", HELD),       # never reached the Queue Log
                row(10, "9", "error", "284000009", "processing failed", attempts=8)]
        self.qlog = [{"order_id": "1", "status": "gone", "attempts": 8},
                     {"order_id": "2", "status": "error", "attempts": 8},
                     {"order_id": "3", "status": "error", "attempts": 8},
                     {"order_id": "4", "status": "error", "attempts": 3},
                     {"order_id": "5", "status": "queued", "attempts": 1},
                     {"order_id": "7", "status": "gone", "attempts": 8},
                     {"order_id": "9", "status": "gone", "attempts": 8}]
        self.ledger["6"] = "H6"
        return rows

    def test_rows_the_drain_closed_are_marked_gone_and_the_rest_stay(self):
        rows = self.drained_queue()
        e = self.engine(rows)
        self.assertEqual(self.settle(e), 4)
        marks = e.gio.queue_mark_batch.call_args[0][1]
        self.assertEqual([m[:3] for m in marks], [(7, "6", "done"), (2, "1", "gone"), (3, "2", "gone"),
                                                  (4, "3", "gone")])
        self.assertEqual(marks[1][4], f"drain closed (Queue Log: gone; was held: {HELD})")
        self.assertEqual(marks[2][4], f"drain closed (Queue Log: error after 8 attempts; was held: {HELD})")
        self.assertEqual({r["order_id"]: r["status"] for r in rows},
                         {"1": "gone", "2": "gone", "3": "gone", "4": "held", "5": "held", "6": "done",
                          "7": "held", "8": "held", "9": "error"})
        self.qlog_read.assert_called_once()

    def test_any_status_the_drain_never_claims_again_closes_the_row(self):
        # error-final, cancelled (by hand), duplicate: claimable skips them all
        rows = [row(2, "1", "held", "1", HELD), row(3, "2", "held", "2", HELD), row(4, "3", "held", "3", HELD)]
        self.qlog = [{"order_id": "1", "status": "error-final", "attempts": 1},
                     {"order_id": "2", "status": "cancelled", "attempts": 0},
                     {"order_id": "3", "status": "processing", "attempts": 2}]
        e = self.engine(rows)
        self.assertEqual(self.settle(e), 2)
        self.assertEqual([r["status"] for r in rows], ["gone", "gone", "held"])
        self.assertTrue(rows[0]["note"].startswith("drain closed (Queue Log: error-final; was held: "))

    def test_marked_with_alerts_off_too(self):
        rows = self.drained_queue()
        e = self.engine(rows)
        e.cfg.alerts_enabled = False
        self.assertEqual(self.settle(e), 4)
        self.assertEqual(sorted(r["order_id"] for r in rows if r["status"] == "gone"), ["1", "2", "3"])

    def test_a_drain_closed_order_created_after_all_is_marked_done(self):
        rows = [row(2, "1", "gone", "284000001", f"drain closed (Queue Log: error after 8 attempts; was held: {HELD})")]
        self.ledger["1"] = "H1"
        e = self.engine(rows)
        self.assertEqual(self.settle(e), 1)
        self.assertEqual(rows[0]["status"], "done")
        self.assertTrue(rows[0]["note"].startswith("HS H1 (created later; was gone: drain closed"))
        rows[0].update(status="gone", note="relay returned nothing after 8 attempts")   # the live 404 path
        e._next_settle = 0
        self.assertEqual(self.settle(e), 0)                    # not the settle's row: left as it was

    def test_all_clear_still_names_orders_the_drain_closed_at_an_earlier_settle(self):
        # their rows read "gone" after the first settle, so the second no longer sees them
        self.assertTrue(notify.issue_open("held-backlog", "Orders on hold get delivery updates", "detail"))
        self.assertTrue(notify.issue_open("held-escalation:2026-10-12", "Orders on hold", "detail"))
        rows = [row(2, "1", "held", "284000001", HELD), row(3, "2", "held", "284000002", HELD),
                row(4, "3", "held", "284000003", HELD)]
        self.qlog = [{"order_id": "1", "status": "queued", "attempts": 1},
                     {"order_id": "2", "status": "gone", "attempts": 8},
                     {"order_id": "3", "status": "error", "attempts": 8}]
        e = self.engine(rows)
        self.settle(e)                                           # 1 still waits: nothing to say yet
        self.assertEqual(self.replies(), [])
        self.assertEqual([r["status"] for r in rows], ["held", "gone", "gone"])
        self.assertEqual(notify.issue_get("held-backlog")["meta"]["left"], ["2", "3"])
        self.ledger.update({"1": "H1", "3": "H3"})               # 3 was created after all
        e._next_settle = 0
        self.settle(e)
        self.assertEqual((self.state("held-backlog"), self.state("held-escalation:2026-10-12")),
                         ("closed", "closed"))
        self.assertEqual(len(self.replies()), 2)
        for text in self.replies():
            self.assertTrue(text.endswith("HubSpot now, except 1 order that left the queue without being "
                                          "created (for example, deleted in Salla)."), text)
        self.assertEqual(self.slack.sent[2:], [])                # the stored ids were never posted
        self.assertPlain()

    def test_a_failed_mark_still_answers_the_threads_and_posts_what_slack_refused(self):
        self.assertTrue(notify.issue_open("held-backlog", "Orders on hold get delivery updates", "detail"))
        rows = [row(2, "1", "held", "284000001", HELD)]
        self.qlog = [{"order_id": "1", "status": "gone", "attempts": 8}]
        e = self.engine(rows)
        e.gio.queue_mark_batch.side_effect = RuntimeError("Sheets 503")
        with mock.patch.object(notify, "retry_failed", return_value=0) as retry:
            self.assertEqual(self.settle(e), 0)
        self.assertEqual(rows[0]["status"], "held")               # the write waits for the next settle
        self.assertEqual(self.state("held-backlog"), "closed")
        self.assertIn("except 1 order that left the queue", self.replies()[-1])
        retry.assert_called_once_with(max_n=50)        # [v2.14.1] every due hold key, not the first five

    def relay(self, rows, **kw):
        import status_relay
        kw.setdefault("live_min_reference", 280000000)
        return status_relay.StatusRelay(_cfg(live_queue_tab="Live Queue", **kw), FakeHS(),
                                        IndexedGIO(rows), live=True)

    def held_index(self, rows):
        import reconcile
        qlog = self.qlog

        class Drain:
            def __init__(self, cfg, enabled=True):
                pass

            def qlog_read(self):
                return list(qlog)
        sheets = types.SimpleNamespace(queue_read=lambda qsid, start_row=2, tab=None: [dict(r) for r in rows])
        with mock.patch("backfill.GoogleIO", lambda cfg, enabled=True: sheets), \
                mock.patch("queue_drain.DrainGoogleIO", Drain):
            return set(reconcile.held_index(backfill.Config()))

    def test_relay_held_index_and_live_threads_count_the_same_holds(self):
        rows = self.drained_queue()
        e = self.engine(rows)
        self.settle(e)
        r = self.relay(rows)
        r._refresh_live_index()
        live_held = e._held_now([dict(x) for x in rows])[0]
        self.assertEqual(live_held, {"4", "5", "8"})
        self.assertEqual(r._held_summary()[0], len(live_held))
        self.assertEqual(self.held_index(rows), live_held)       # the digest counts with held_index too

    def test_a_giveup_for_an_order_the_drain_gave_up_on_opens_no_held_backlog(self):
        rows = self.drained_queue()
        self.settle(self.engine(rows))
        r = self.relay(rows)
        self.assertEqual(r._classify("2", "284000002"), "recent")      # out of attempts: nothing creates it
        self.assertEqual(r._classify("4", "284000004"), "held")
        r.handle_row(_row(oid="2", ref="284000002", attempts=5))
        self.assertIsNone(notify.issue_get("held-backlog"))
        self.assertEqual(self.state("status-missing:2"), "open")      # its own thread: a person re-creates it
        r.handle_row(_row(oid="4", ref="284000004", attempts=5))
        self.assertEqual(self.state("held-backlog"), "open")

    def test_relay_and_settle_over_a_night_raise_one_escalation(self):
        """The relay every 10 minutes, the settle every hour, 21:00 to 03:00,
        120 held rows the drain closed and no order really on hold."""
        rows = [row(i + 1, str(i), "held", str(284000000 + i), HELD) for i in range(1, 121)]
        self.qlog = ([{"order_id": str(i), "status": "gone", "attempts": 8} for i in range(1, 81)]
                     + [{"order_id": str(i), "status": "error", "attempts": 8} for i in range(81, 121)])
        r = self.relay(rows, held_escalate_count=100, escalation_mentions=["U1"])
        e = self.engine(rows)
        emails = []
        clock = Clock(datetime(2026, 10, 12, 21, 0))
        with clock.patch(), mock.patch.multiple(notify, email_enabled=lambda: True,
                                                send_email=lambda s, b, dry_run=False: emails.append(s) or True):
            for step in range(36):
                r.periodic()
                if step % 6 == 3:
                    e._next_settle = 0
                    self.settle(e)
                clock.t += 600
        red = [h for h, _ in self.slack.sent if "orders are on hold and not in HubSpot" in h]
        self.assertEqual(len(red), 1, [h for h, _ in self.slack.sent])         # before the first settle only
        self.assertEqual(len([s for s in emails if not s.startswith("Resolved")]), 1, emails)
        self.assertLessEqual(len([s for s in emails if s.startswith("Resolved")]), 1, emails)
        self.assertEqual(r._held_summary()[0], 0)
        self.assertTrue(all(x["status"] == "gone" for x in rows))


class EmptyThreads(SettleCase):
    """[v2.14.1] N7: a held thread whose orders are all created or gone closes
    whatever the gate says, and only a pass is called one."""

    def test_no_sku_thread_closes_once_every_order_is_in_hubspot(self):
        rows = [row(5, "F", "done", "1006", "HS H6"), row(6, "G", "done", "1007", "HS H7")]
        self.ledger.update({"F": "H6", "G": "H7"})
        self.assertTrue(notify.issue_open("held:sku:-", "Deleted product holds orders", "detail",
                                          meta={"orders": ["F", "G"], "refs": ["1006", "1007"],
                                                "name": "Old Brush"}))
        e = self.engine(rows)
        self.settle(e)
        self.assertEqual(self.state("held:sku:-"), "closed")
        text = self.replies()[-1]
        self.assertTrue(text.startswith("✅ Every order that waited for Old Brush is in HubSpot now: "), text)
        self.assertIn("|#1006>", text)
        self.assertIn("|#1007>", text)
        self.assertTrue(text.endswith(" A new order with Old Brush would wait again, in a new thread: the "
                                      "product still does not pass the catalog check."), text)
        self.assertNotIn("passes", text)
        e.hs.gate_search_product_by_sku.assert_not_called()   # no SKU: the gate can never match it
        self.assertPlain()

    def test_product_that_fails_again_closes_without_claiming_a_pass(self):
        rows = [row(5, "A", "done", "1001", "HS H1"), row(6, "B", "gone", "1002", "relay returned nothing")]
        self.ledger["A"] = "H1"
        self.open_held("P1", ["A", "B"], ["1001", "1002"])     # P1 is not approved (again)
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "closed")
        text = self.replies()[-1]
        self.assertTrue(text.startswith("✅ No order is waiting for Hair Dryer any more. 1 order that waited "
                                        "for it is in HubSpot: "), text)
        self.assertIn("|#1001>", text)
        self.assertIn("Order #1002 was not created", text)
        self.assertIn("A new order with Hair Dryer would wait again", text)
        self.assertNotIn("passes", text)
        self.assertPlain()

    def test_every_order_gone_names_them_and_claims_nothing(self):
        rows = [row(5, "A", "held", "1001", HELD)]
        self.qlog = [{"order_id": "A", "status": "gone", "attempts": 8}]
        self.open_held("P1", ["A"], ["1001"])
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "closed")
        text = self.replies()[-1]
        self.assertTrue(text.startswith("✅ No order is waiting for Hair Dryer any more. Order #1001 was not "
                                        "created"), text)
        self.assertNotIn("in HubSpot:", text)

    def test_a_passing_product_keeps_the_released_wording(self):
        rows = [row(5, "A", "done", "1001", "HS H1")]
        self.ledger["A"] = "H1"
        self.approved["P1"] = 1
        self.open_held("P1", ["A"], ["1001"])
        self.settle(self.engine(rows))
        text = self.replies()[-1]
        self.assertTrue(text.startswith("✅ Released: Hair Dryer passes the catalog check now, and 1 order "
                                        "that waited for it is in HubSpot: "), text)
        self.assertNotIn("does not pass", text)


class FailedHeldRecords(SettleCase):
    """[v2.14.1] N8 and notify.retry_failed from the settle: a held record no
    Slack channel took is closed without a post once nothing it listed
    waits, and otherwise the settle posts it."""

    def refused(self, key, subject, meta=None):
        with mock.patch.object(notify, "post_slack", lambda *a, **k: None):
            self.assertFalse(notify.issue_open(key, subject, "detail", meta=meta))
        self.assertEqual(self.state(key), "failed")
        with notify._issues() as d:                            # its retry is due
            d[key]["retry_at"] = time.time() - 1

    def test_failed_product_record_closes_quietly_once_its_orders_are_created_or_gone(self):
        self.refused("held:P1", "Hair Dryer holds orders",
                     {"orders": ["A", "B"], "refs": ["1001", "1002"], "name": "Hair Dryer", "pid": "P1"})
        rows = [row(5, "A", "done", "1001", "HS H1"), row(6, "B", "gone", "1002", "relay returned nothing")]
        self.ledger["A"] = "H1"
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "closed")
        self.assertEqual((self.slack.sent, self.replies()), ([], []))
        self.assertEqual(notify.retry_failed(), 0)
        self.assertEqual(self.slack.sent, [])

    def test_failed_product_record_with_an_order_waiting_is_posted_by_the_settle(self):
        self.refused("held:P1", "Hair Dryer holds orders",
                     {"orders": ["B"], "refs": ["1002"], "name": "Hair Dryer", "pid": "P1"})
        rows = [row(6, "B", "held", "1002", HELD)]
        self.qlog = [{"order_id": "B", "status": "queued", "attempts": 1}]
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held:P1"), "open")
        self.assertEqual(self.slack.sent, [["Hair Dryer holds orders", "detail"]])

    def test_failed_backlog_and_escalation_close_when_nothing_is_held(self):
        self.refused("held-backlog", "Orders on hold get delivery updates")
        self.refused("held-escalation:2026-10-12", "120 orders are on hold")
        self.settle(self.engine([row(5, "A", "done", "1001", "HS H1")]))
        self.assertEqual((self.state("held-backlog"), self.state("held-escalation:2026-10-12")),
                         ("closed", "closed"))
        self.assertEqual(self.slack.sent, [])

    def test_failed_backlog_is_posted_while_an_order_is_held(self):
        self.refused("held-backlog", "Orders on hold get delivery updates")
        rows = [row(6, "B", "held", "1002", HELD)]
        self.settle(self.engine(rows))
        self.assertEqual(self.state("held-backlog"), "open")
        self.assertEqual(len(self.slack.sent), 1)

    def test_retry_runs_once_per_settle_after_the_closes(self):
        self.refused("held:P1", "Hair Dryer holds orders", {"orders": ["A"], "name": "Hair Dryer"})
        self.ledger["A"] = "H1"
        calls = []
        with mock.patch.object(notify, "close_failed", side_effect=lambda k, why="", **kw: calls.append("close")), \
                mock.patch.object(notify, "retry_failed", side_effect=lambda *a, **k: calls.append("retry") or 0):
            e = self.engine([row(5, "A", "done", "1001", "HS H1")])
            self.settle(e)
            self.assertEqual(calls, ["close", "retry"])
            e._next_settle = 0
            e.cfg.alerts_enabled = False
            self.settle(e)
            self.assertEqual(calls, ["close", "retry"])         # alerts off: nothing posts


if __name__ == "__main__":
    unittest.main()
