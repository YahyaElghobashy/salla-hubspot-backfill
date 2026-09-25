"""customer_sweep.py (v2.11): day refusal rules, presence, queued row shape,
the cap, dry runs, and the consent filler. Offline, against fakes.

Run: python3 -m unittest test_customer_sweep -v
"""
import csv
import json
import re
import unittest
from pathlib import Path
from unittest import mock

import customer_sweep as sw
from test_realtime_ext import _cfg, _chdir_tmp


def cust(cid, day="2026-09-24", flag=True, mobile="500000001"):
    return {"id": cid, "first_name": "N", "last_name": "K", "mobile": mobile,
            "mobile_code": "+966", "email": "x@store.fake", "city": "Riyadh",
            "gender": "female", "lang": "ar", "location": "",
            "birthday": {"date": "1990-01-02 00:00:00.000000"},
            "is_notifications_enabled": flag,
            "created_at": {"date": f"{day} 10:00:00.000000", "timezone": "Asia/Riyadh"}}


class FakeRelay:
    def __init__(self, pages=None, total=None, singles=None):
        self.pages, self.total, self.singles, self.paths = pages or [], total, singles or {}, []

    def get_path(self, path):
        self.paths.append(path)
        if path.startswith("customers?"):
            page = int(re.search(r"[?&]page=(\d+)", path).group(1))
            items = self.pages[page - 1] if page <= len(self.pages) else []
            total = self.total if self.total is not None else sum(len(p) for p in self.pages)
            return {"status": 200, "data": items,
                    "pagination": {"total": total, "totalPages": max(1, len(self.pages))}}
        cid = path.split("customers/")[1].split("?")[0]
        rec = self.singles.get(cid)
        return {"status": 200, "data": rec}


class FakeHS:
    def __init__(self, existing=(), no_flag=()):
        self.existing, self.no_flag, self.writes = set(existing), list(no_flag), []

    def search(self, path, body, what):
        f = body["filterGroups"][0]["filters"]
        if f[0]["operator"] == "IN":
            vals = f[0]["values"]
            return {"results": [{"id": f"C{v}", "properties": {"salla_customer_id": v}}
                                for v in vals if v in self.existing]}
        return {"results": [{"id": cid, "properties": {"salla_customer_id": sid}}
                            for cid, sid in self.no_flag]}

    def _write(self, method, path, body, what):
        self.writes.append((method, path, body))
        return 200, {}


class FakeGIO:
    def __init__(self, queue=()):
        self.queue, self.appends = list(queue), []

    def queue_read_all(self, qsid, tab=None, chunk=20000):
        return self.queue

    def queue_append_rows(self, qsid, rows, tab=None):
        self.appends.append((tab, rows))


class TestSallaDay(unittest.TestCase):
    def test_pages_and_refusals(self):
        relay = FakeRelay(pages=[[cust(1), cust(2)], [cust(3)]])
        recs, total = sw.salla_day(relay, "2026-09-24")
        self.assertEqual([r["id"] for r in recs], [1, 2, 3])
        self.assertIn("date_from=2026-09-24&date_to=2026-09-24", relay.paths[0])
        self.assertIn("fields[]=is_notifications_enabled", relay.paths[0])
        with self.assertRaises(ValueError):
            sw.salla_day(FakeRelay(pages=[[cust(1)]], total=10000), "2026-09-24")
        with self.assertRaises(ValueError):
            sw.salla_day(FakeRelay(pages=[[cust(1, day="2026-09-23")]]), "2026-09-24")


class TestSweep(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        with open("mirror/customers.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ts", "salla_customer_id", "contact_id", "action"])
            w.writerow(["x", "1", "c1", "created"])

    def queue(self):
        return [{"row": 5, "order_id": "2", "status": "held"},
                {"row": 6, "order_id": "9", "status": "done"}]

    def test_only_truly_missing_customers_are_queued(self):
        relay = FakeRelay(pages=[[cust(1), cust(2), cust(3), cust(4, flag=False)]])
        hs, gio = FakeHS(existing={"3"}), FakeGIO(self.queue())
        with mock.patch("notify.send_alert", create=True):
            t = sw.sweep_days(_cfg(), relay, hs, gio, ["2026-09-24"], live=True, cap=10, force=False)
        self.assertEqual(t["missing"], 1)
        tab, rows = gio.appends[0]
        self.assertEqual(tab, "Customer Queue")
        row = rows[0]
        self.assertEqual(row[1:7], ["4", "+966500000001", "customer.created", "queued", 0, "sweep"])
        payload = json.loads(row[7])
        self.assertEqual(payload["is_notifications_enabled"], "false")
        self.assertEqual(payload["birthday"], "1990-01-02 00:00:00.000000")
        state = json.loads(Path("mirror/customer_sweep_state.json").read_text())
        self.assertEqual(state["swept"]["2026-09-24"]["queued"], 1)

    def test_cap_and_dry_run(self):
        relay = FakeRelay(pages=[[cust(11), cust(12), cust(13)]])
        gio = FakeGIO([])
        with mock.patch("notify.send_alert", create=True):
            sw.sweep_days(_cfg(), relay, FakeHS(), gio, ["2026-09-24"], live=True, cap=2, force=False)
        self.assertEqual(gio.appends, [])
        sw.sweep_days(_cfg(), relay, FakeHS(), gio, ["2026-09-24"], live=False, cap=10, force=False)
        self.assertEqual(gio.appends, [])
        self.assertFalse(Path("mirror/customer_sweep_state.json").exists())


class TestConsentFiller(unittest.TestCase):
    def test_fills_known_flags_only(self):
        _chdir_tmp(self)
        relay = FakeRelay(singles={"51": {"id": 51, "is_notifications_enabled": True},
                                   "52": {"id": 52, "is_notifications_enabled": False},
                                   "53": {"id": 53}})
        hs = FakeHS(no_flag=[("A", "51"), ("B", "52"), ("C", "53")])
        out = sw.fill_recent(_cfg(), relay, hs, live=True)
        self.assertEqual((out["resolved"], out["unknown"], out["written"]), (2, 1, 2))
        inputs = hs.writes[0][2]["inputs"]
        self.assertEqual({(i["id"], i["properties"]["salla_consent_status"]) for i in inputs},
                         {("A", "true"), ("B", "false")})
        self.assertIn("fields[]=is_notifications_enabled", relay.paths[0])

    def test_dry_run_writes_nothing(self):
        _chdir_tmp(self)
        relay = FakeRelay(singles={"51": {"id": 51, "is_notifications_enabled": True}})
        hs = FakeHS(no_flag=[("A", "51")])
        out = sw.fill_recent(_cfg(), relay, hs, live=False)
        self.assertEqual((out["resolved"], out["written"]), (1, 0))
        self.assertEqual(hs.writes, [])


if __name__ == "__main__":
    unittest.main()
