#!/usr/bin/env python3
"""v2.14 drain pre-filter: during the Oct 2026 hold every hourly pass paid a
relay fetch (Make credits) for the same 120 blocked rows and never reached the
other 1,600. Rows that can only be blocked are now skipped without a fetch,
and the least-tried rows go first.
"""
import csv
import unittest

import queue_drain
from test_queue_drain import chdir_tmp, make_cfg, make_engine, order

PID = "1982906533"
STILL_BLOCKED_REC = [{"id": "1", "properties": {"template_status": "draft",
                                                "component_count": None, "active_component_count": None}}]


def write_matrix(rows):
    queue_drain.MATRIX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(queue_drain.MATRIX_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["salla_product_id", "sku", "item_name", "blocked_orders",
                    "suggested_action", "sample_order_ids", "checked_at", "note"])
        for pid, name in rows:
            w.writerow([pid, "", name, 120, "x", "", "", ""])


def qrow(n, oid, items, attempts):
    return {"row": n, "order_id": oid, "status": "queued", "attempts": attempts,
            "reason": "", "items": items, "notes": "", "older": "", "reference": ""}


class PreFilter(unittest.TestCase):
    def setUp(self):
        self.tmp = chdir_tmp(self)

    def run_pass(self, flag=True, blocked_now=True):
        cfg = make_cfg(self.tmp)
        cfg.drain_skip_known_blocked = flag
        cfg.stray_template_exemption = True
        eng, hs, relay, gio = make_engine(self.tmp, live=False, cfg=cfg)
        hs.gate_search_product_approved.return_value = 1
        hs.gate_search_template.side_effect = lambda pid, elig: (0 if elig else (1 if blocked_now else 0))
        hs.template_records.return_value = STILL_BLOCKED_REC
        hs.find_order_by_salla_id.return_value = None
        rows = [qrow(2, "101", "Dryer", 160),
                qrow(3, "102", "Dryer", 0),
                qrow(4, "103", "Dryer, Foam", 3),
                qrow(5, "104", "Foam", 9)]
        gio.qlog_read = lambda: rows
        db = {"101": order("101", [(PID, "product")]), "102": order("102", [(PID, "product")]),
              "103": order("103", [(PID, "product"), ("2", "product")]),
              "104": order("104", [("2", "product")])}
        fetched = []
        relay.fetch_orders.side_effect = lambda ids: (fetched.extend(ids) or {i: db[i] for i in ids})
        write_matrix([(PID, "Dryer")])
        eng.run_drain()
        return fetched, eng

    def test_skips_only_tried_rows_naming_only_blocked_products(self):
        fetched, eng = self.run_pass()
        self.assertNotIn("101", fetched)          # tried 160 times, only the dryer: skipped
        self.assertIn("102", fetched)             # never tried: fetched
        self.assertIn("103", fetched)             # also names an approved product: fetched
        self.assertIn("104", fetched)

    def test_least_tried_first(self):
        fetched, _ = self.run_pass()
        self.assertEqual(fetched[0], "102")       # attempts 0 before 3 and 9

    def test_product_now_passing_is_fetched(self):
        fetched, _ = self.run_pass(blocked_now=False)
        self.assertIn("101", fetched)

    def test_flag_off_fetches_everything_in_sheet_order(self):
        fetched, _ = self.run_pass(flag=False)
        self.assertEqual(sorted(fetched), ["101", "102", "103", "104"])

    def test_skipped_rows_stay_in_the_next_matrix(self):
        self.run_pass()
        with open(queue_drain.MATRIX_FILE, newline="", encoding="utf-8") as f:
            pids = [r["salla_product_id"] for r in csv.DictReader(f)]
        self.assertIn(PID, pids)

    def test_no_matrix_no_filter(self):
        cfg = make_cfg(self.tmp)
        cfg.drain_skip_known_blocked = True
        eng, hs, relay, gio = make_engine(self.tmp, live=False, cfg=cfg)
        rows = [qrow(2, "1", "Dryer", 5)]
        self.assertEqual(eng._skip_known_blocked(rows), rows)
        hs.gate_search_product_approved.assert_not_called()


if __name__ == "__main__":
    unittest.main()
