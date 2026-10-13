#!/usr/bin/env python3
"""v2.14.1 tools/fix_delivery_dates.py: released orders get the day Salla
says they were delivered, not the day they were released into HubSpot."""
import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "tools"))
import fix_delivery_dates as fd


class FakeGIO:
    def __init__(self, rows):
        self.rows, self.sheets_rl = rows, None
        vals = types.SimpleNamespace(get=lambda **kw: ("get", kw))
        self.sheets = types.SimpleNamespace(values=lambda: vals)

    def _gexec(self, request, what, limiter):
        assert request[1]["range"] == "'Status Queue'!A:D"
        return {"values": [["received_at", "order_id", "reference_id", "event"]] + self.rows}


class RealDays(unittest.TestCase):
    def days(self, rows, wanted):
        cfg = types.SimpleNamespace(queue_spreadsheet_id="Q")
        return fd.real_days(FakeGIO(rows), cfg, wanted)

    def test_first_delivered_event_wins_over_completed(self):
        rows = [["t", "11", "r", "status:completed@2026-10-09 10:00:00.000000"],
                ["t", "11", "r", "status:delivered@2026-10-08 18:00:00.000000"],
                ["t", "11", "r", "status:delivered@2026-10-07 09:00:00.000000"]]
        self.assertEqual(self.days(rows, {"11"}), {"11": "2026-10-07"})

    def test_completed_only_uses_its_first_completed_event(self):
        rows = [["t", 22.0, "r", "status:shipped@2026-10-06 10:00:00"],
                ["t", 22.0, "r", "status:completed@2026-10-10 10:00:00"],
                ["t", 22.0, "r", "status:completed@2026-10-09 10:00:00"]]
        self.assertEqual(self.days(rows, {"22"}), {"22": "2026-10-09"})

    def test_orders_without_an_event_and_other_orders_are_left_out(self):
        rows = [["t", "33", "r", "status:shipped@2026-10-06 10:00:00"],
                ["t", "44", "r", "status:delivered@2026-10-06 10:00:00"],
                ["t", "55"]]
        self.assertEqual(self.days(rows, {"33", "55"}), {})


if __name__ == "__main__":
    unittest.main()
