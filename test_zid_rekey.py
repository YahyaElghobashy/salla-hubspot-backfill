"""zid_rekey.py (v2.12): the plan a Zid holder gets, what a live apply
writes and in which order, what a dry run never touches, the ledgers, and
the engine hook that re-keys then creates. Offline, against fakes.

Run: python3 -m unittest test_zid_rekey -v
"""
import csv
import json
import types
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import backfill
import zid_rekey as zr
from test_realtime_ext import _chdir_tmp

N, Z = "4938528", "1337741441227"
DELIVERED, COMPLETED, SHIPPED = "3725360f-519b-4b18-a593-494d60a29c9f", "5656450292", "aa99e8d0"


class FakeHS:
    """Answers the reads the planner makes; records every write."""

    def __init__(self, order_day="2020-12-18", original=COMPLETED, current=DELIVERED, live=True):
        self.live, self.writes = live, []
        self.order = {"salla_order_id": N, "salla_order_reference": N, "hs_external_order_id": N,
                      "hs_source_store": "Zid", "salla_store": "Zid", "last_salla_sync_status": "partial",
                      "hs_pipeline_stage": current, "hs_external_created_date": order_day + "T15:42:00Z",
                      "hs_order_name": f"RID{N} | Zid | Manal"}
        self.history = [{"value": current, "timestamp": "2026-09-23"}, {"value": SHIPPED, "timestamp": "2026-09-21"},
                        {"value": original, "timestamp": "2026-08-27"}]
        self.lis = {"481431164103": {"salla_order_item_id": f"Z{N}-1", "salla_order_id": N},
                    "487000000001": {"salla_order_item_id": "346417258", "salla_order_id": N}}
        self.warranties = {
            "W1": {"warranty_key": f"{N}:346417258:u1", "origin": "live_engine", "hs_pipeline_stage": "5913821399"},
            "W2": {"warranty_key": f"{N}:Z{N}-1:u1", "origin": "backfill_cutoff_2024-03-01",
                   "hs_pipeline_stage": "5913821399"},
            "W3": {"warranty_key": f"{N}:Z{N}-1:u2", "origin": "live_engine", "hs_pipeline_stage": "5913821399",
                   "warranty_months_snapshot": "24"},
            "W4": {"warranty_key": "999:Z999-1:u1", "origin": "live_engine", "hs_pipeline_stage": "5913821399"}}
        self.void_options = [{"value": "returned", "label": "Order returned"}]

    def orders_by_salla_id(self, sid):
        return (None, None)

    def _req(self, method, path, body=None, is_search=False, what=""):
        if path.startswith(f"/crm/v3/objects/orders/{Z}?"):
            return 200, {"properties": self.order, "propertiesWithHistory": {"hs_pipeline_stage": self.history}}
        if "/associations/line_items" in path:
            return 200, {"results": [{"toObjectId": int(i)} for i in self.lis]}
        if f"/associations/{zr.WARRANTY_OBJ}" in path:
            return 200, {"results": [{"toObjectId": i} for i in self.warranties]}
        if path == "/crm/v3/objects/line_items/batch/read":
            return 200, {"results": [{"id": x["id"], "properties": self.lis[x["id"]]} for x in body["inputs"]]}
        if path == f"/crm/v3/objects/{zr.WARRANTY_OBJ}/batch/read":
            return 200, {"results": [{"id": x["id"], "properties": self.warranties[x["id"]]} for x in body["inputs"]]}
        if path.endswith("/void_reason"):
            return 200, {"options": self.void_options}
        raise AssertionError(f"unexpected read {method} {path}")

    def _write(self, method, path, body, what):
        self.writes.append((method, path, body))
        return 200, {}


class TestPlan(unittest.TestCase):
    def test_classification(self):
        rk = zr.ZidRekey(FakeHS(), live=False, today=date(2026, 9, 27))
        plan = rk.plan(Z, N)
        self.assertEqual(plan["order_props"], {"salla_order_id": "Z4938528", "salla_order_reference": "Z4938528",
                                               "hs_external_order_id": "Z4938528", "last_salla_sync_status": "synced"})
        self.assertEqual([it["id"] for it in plan["zid_items"]], ["481431164103"])
        self.assertEqual([it["id"] for it in plan["salla_items"]], ["487000000001"])
        acts = {w["id"]: w["action"] for w in plan["warranties"]}
        # W3: 2020 order, out of the backfill scope -> phantom -> void
        self.assertEqual(acts, {"W1": "detach", "W2": "rekey", "W3": "void", "W4": "leave"})
        w2 = next(w for w in plan["warranties"] if w["id"] == "W2")
        self.assertEqual(w2["props"], {"warranty_key": f"Z{N}:Z{N}-1:u1"})
        self.assertEqual((plan["stage_current"], plan["stage_original"], plan["stage_changes"]),
                         (DELIVERED, COMPLETED, 2))

    def test_in_scope_live_engine_record_is_redated(self):
        hs = FakeHS(order_day="2025-01-10", original=DELIVERED)
        plan = zr.ZidRekey(hs, live=False, today=date(2026, 9, 27)).plan(Z, N)
        w3 = next(w for w in plan["warranties"] if w["id"] == "W3")
        self.assertEqual(w3["action"], "redate")
        self.assertEqual(w3["props"]["warranty_start_date"], "2025-01-10")
        self.assertEqual(w3["props"]["warranty_end_date"], "2027-01-10")
        self.assertEqual(w3["props"]["hs_pipeline_stage"], zr.W_STAGE["active"])

    def test_refuses_a_salla_order_or_a_taken_key(self):
        hs = FakeHS()
        hs.order["hs_source_store"] = "Salla"
        with self.assertRaises(RuntimeError):
            zr.ZidRekey(hs).plan(Z, N)
        hs = FakeHS()
        hs.orders_by_salla_id = lambda sid: ("1378000000009", None)
        with self.assertRaises(RuntimeError):
            zr.ZidRekey(hs).plan(Z, N)

    def test_dates(self):
        self.assertEqual(zr.add_months("2024-01-31", 1), "2024-02-29")
        today = date(2026, 9, 27)
        self.assertEqual(zr.warranty_stage("2026-09-26", today), zr.W_STAGE["expired"])
        self.assertEqual(zr.warranty_stage("2026-10-20", today), zr.W_STAGE["expiring"])
        self.assertEqual(zr.warranty_stage("2027-01-01", today), zr.W_STAGE["active"])


class TestApply(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)
        led = backfill.CreatedLedger("mirror")
        led.add(N, Z)                                  # the pre-v2.12 wrong entry
        Path("mirror/zid_collisions.json").write_text(json.dumps({N: {"zid_hs_id": Z, "first_seen": "x"}}))

    def test_dry_run_writes_nothing(self):
        hs = FakeHS(live=False)
        plan = zr.ZidRekey(hs, live=False).rekey(Z, N)
        self.assertFalse(plan["applied"])
        self.assertEqual(hs.writes, [])
        self.assertFalse(Path("mirror/zid_rekeys.csv").exists())
        self.assertEqual(backfill.CreatedLedger("mirror").get(N), Z)

    def test_live_apply_order_and_ledgers(self):
        hs = FakeHS()
        plan = zr.ZidRekey(hs, live=True, today=date(2026, 9, 27)).rekey(Z, N)
        self.assertTrue(plan["applied"])
        paths = [p for _, p, _ in hs.writes]
        self.assertEqual(paths, [f"/crm/v3/objects/orders/{Z}",                       # 1 free the number
                                 "/crm/v3/objects/line_items/batch/update",            # 2 Zid items follow
                                 "/crm/v4/associations/orders/line_items/batch/archive",   # 3 Salla item leaves
                                 f"/crm/v4/associations/{zr.WARRANTY_OBJ}/orders/batch/archive",
                                 f"/crm/v3/properties/{zr.WARRANTY_OBJ}/void_reason",  # option added once
                                 f"/crm/v3/objects/{zr.WARRANTY_OBJ}/batch/update"])
        self.assertNotIn("hs_pipeline_stage", hs.writes[0][2]["properties"])          # no stage restore by default
        self.assertEqual(hs.writes[2][2]["inputs"], [{"from": {"id": Z}, "to": {"id": "487000000001"}}])
        self.assertEqual({o["value"] for o in hs.writes[4][2]["options"]}, {"returned", "wrong_order"})
        upd = {x["id"]: x["properties"] for x in hs.writes[5][2]["inputs"]}
        self.assertEqual(upd["W3"]["hs_pipeline_stage"], zr.W_STAGE["voided"])
        self.assertEqual(upd["W3"]["void_reason"], "wrong_order")
        self.assertIsNone(backfill.CreatedLedger("mirror").get(N))
        self.assertTrue(json.loads(Path("mirror/zid_collisions.json").read_text())[N]["resolved"])
        rows = list(csv.DictReader(open("mirror/zid_rekeys.csv")))
        self.assertIn(("order", "salla_order_id", N, "Z4938528"),
                      {(r["object"], r["field"], r["old"], r["new"]) for r in rows})
        self.assertTrue(any(r["object"] == "created_ledger" and r["note"] == "revoked" for r in rows))

    def test_stage_restore_only_on_request(self):
        hs = FakeHS()
        zr.ZidRekey(hs, live=True).rekey(Z, N, restore_stages=True)
        stage_patch = [b for m, p, b in hs.writes if p == f"/crm/v3/objects/orders/{Z}"
                       and "hs_pipeline_stage" in b["properties"]]
        self.assertEqual(stage_patch, [{"properties": {"hs_pipeline_stage": COMPLETED}}])

    def test_void_option_added_once(self):
        hs = FakeHS()
        hs.void_options.append({"value": "wrong_order", "label": "x"})
        zr.ZidRekey(hs, live=True).rekey(Z, N)
        self.assertFalse(any(p.endswith("/void_reason") for _, p, _ in hs.writes))


class TestEngineHook(unittest.TestCase):
    def engine(self, live=True, switch=True):
        e = object.__new__(backfill.Engine)
        e.cfg = backfill.Config()
        e.cfg.zid_auto_rekey = switch
        e.live = live
        e.mirror = types.SimpleNamespace(dir=Path("mirror"))
        e.hs = types.SimpleNamespace(live=live)
        return e

    def test_create_retries_after_a_successful_rekey(self):
        e = self.engine()
        calls = []

        def create(order, cid, tz):
            calls.append(1)
            if len(calls) == 1:
                raise backfill.ZidCollision(N, Z)
            return ("HS-NEW", True)
        e.hs.create_order = create
        with mock.patch("zid_rekey.ZidRekey") as rk:
            self.assertEqual(e._create_order_freeing_zid({"id": N}, None), ("HS-NEW", True))
            rk.assert_called_once()
            rk.return_value.rekey.assert_called_once_with(Z, N)
        self.assertEqual(len(calls), 2)

    def test_dry_run_and_switch_off_still_park(self):
        for e in (self.engine(live=False), self.engine(switch=False)):
            e.hs.create_order = mock.Mock(side_effect=backfill.ZidCollision(N, Z))
            with mock.patch("zid_rekey.ZidRekey") as rk, self.assertRaises(backfill.ZidCollision):
                e._create_order_freeing_zid({"id": N}, None)
            rk.assert_not_called()

    def test_failed_rekey_parks(self):
        e = self.engine()
        e.hs.create_order = mock.Mock(side_effect=backfill.ZidCollision(N, Z))
        with mock.patch("zid_rekey.ZidRekey") as rk, self.assertRaises(backfill.ZidCollision):
            rk.return_value.rekey.side_effect = RuntimeError("HubSpot 500")
            e._create_order_freeing_zid({"id": N}, None)
        self.assertEqual(e.hs.create_order.call_count, 1)


if __name__ == "__main__":
    unittest.main()
