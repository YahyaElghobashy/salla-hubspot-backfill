"""tools/warranty_sweep.py (v2.12, Repair Batch 1.0): the vendored action is
the deployed one, dry runs intercept every write, HTTP retries, candidate
filtering, and the stale-warranty void. Offline, against fakes.

Run: python3 -m unittest test_warranty_sweep -v
"""
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.modules.setdefault("requests", types.SimpleNamespace(get=None, post=None))
sys.path.insert(0, str(Path(__file__).resolve().parent / "tools"))

import backfill
import warranty_sweep as ws
from test_realtime_ext import _chdir_tmp


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._b, self.headers = status, body or {}, headers or {}
        self.text = str(self._b)

    def json(self):
        return self._b


class TestAction(unittest.TestCase):
    def test_vendored_action_is_the_guarded_one(self):
        src = (Path(__file__).resolve().parent / "warranty_engine_action.py").read_text()
        self.assertIn('zid_order_skipped', src)          # the 27 Sep guard ships with it
        self.assertIn('"%s:%s:u%d" % (salla_order_id, item_id, unit)', src)   # same keys

    def test_load_sets_the_token_before_import(self):
        mod = ws.load_action("tok-x")
        self.assertEqual(mod.TOKEN, "tok-x")
        self.assertEqual(mod.HDR["Authorization"], "Bearer tok-x")


class TestHttp(unittest.TestCase):
    def mod(self, get=None, post=None):
        m = types.SimpleNamespace(_get=get or mock.Mock(return_value=Resp(200)),
                                  _post=post or mock.Mock(return_value=Resp(200, {"results": []})))
        return m

    def test_dry_run_intercepts_writes_only(self):
        post = mock.Mock(return_value=Resp(200, {"results": [{"id": "r"}]}))
        m, planned = self.mod(post=post), []
        ws.wrap_http(m, live=False, pacer=ws.Pacer(1000), planned=planned)
        r = m._post("/crm/v3/objects/2-252148104/batch/upsert",
                    {"inputs": [{"id": "N:I:u1", "properties": {"warranty_key": "N:I:u1"}}]})
        self.assertEqual(r.json()["results"][0]["properties"]["warranty_key"], "N:I:u1")
        m._post("/crm/v4/associations/2-252148104/0-123/batch/create", {"inputs": []})
        post.assert_not_called()                          # no write reached HubSpot
        m._post("/crm/v3/objects/0-8/batch/read", {"inputs": []})
        post.assert_called_once()                         # reads pass through
        self.assertEqual(planned, ["N:I:u1"])

    def test_live_passes_writes_and_retries_429(self):
        post = mock.Mock(side_effect=[Resp(429, headers={"Retry-After": "0"}), Resp(200, {"results": []})])
        m = self.mod(post=post)
        ws.wrap_http(m, live=True, pacer=ws.Pacer(1000), planned=[])
        with mock.patch("time.sleep"):
            r = m._post("/crm/v3/objects/2-252148104/batch/upsert", {"inputs": []})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(post.call_count, 2)


class FakeHS:
    def __init__(self, orders, warranties):
        self.orders, self.warranties, self.writes, self.live = orders, warranties, [], True

    def search(self, path, body, what):
        f = {x["propertyName"]: x.get("value") for x in body["filterGroups"][0]["filters"]}
        res = [{"id": oid, "properties": p} for oid, p in self.orders.items()
               if p["hs_pipeline_stage"] == f.get("hs_pipeline_stage")]
        return {"results": res}

    def _req(self, method, path, body=None, is_search=False, what=""):
        if "/associations/" in path and path.endswith("/batch/read"):
            return 200, {"results": [{"from": {"id": x["id"]},
                                      "to": [{"toObjectId": w} for w in self.warranties.get(x["id"], {})]}
                                     for x in body["inputs"]]}
        if path.endswith("batch/read"):
            ids = [x["id"] for x in body["inputs"]]
            if "2-252148104" in path:
                return 200, {"results": [{"id": w, "properties": {"hs_pipeline_stage": s}}
                                         for ws_ in self.warranties.values() for w, s in ws_.items() if w in ids]}
            return 200, {"results": [{"id": i, "properties": self.orders[i]} for i in ids if i in self.orders]}
        raise AssertionError(path)

    def _write(self, method, path, body, what):
        self.writes.append((path, body))
        return 200, {}


def cfg():
    c = backfill.Config()
    c.status_stage_map = {"delivered": "D", "completed": "C", "canceled": "X", "restored": "R"}
    return c


class TestPasses(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_candidates_are_delivered_salla_orders_without_a_warranty(self):
        orders = {"1": {"hs_pipeline_stage": "D", "hs_source_store": "Salla", "delivery_date": "2026-09-20"},
                  "2": {"hs_pipeline_stage": "C", "hs_source_store": "Salla", "delivery_date": "2026-09-20"},
                  "3": {"hs_pipeline_stage": "D", "hs_source_store": "Zid", "delivery_date": "2021-01-01"},
                  "4": {"hs_pipeline_stage": "X", "hs_source_store": "Salla"}}
        hs = FakeHS(orders, {"2": {"w1": "5913821399"}})
        self.assertEqual([r["id"] for r in ws.candidates(hs, cfg(), 3, [])], ["1"])
        self.assertEqual([r["id"] for r in ws.candidates(hs, cfg(), 3, ["1", "3", "4"])], ["1"])

    def test_missing_pass_runs_the_action_and_skips_undated(self):
        orders = {"1": {"hs_pipeline_stage": "D", "hs_source_store": "Salla", "delivery_date": "2026-09-20",
                        "salla_order_id": "S1"},
                  "2": {"hs_pipeline_stage": "D", "hs_source_store": "Salla", "salla_order_id": "S2"}}
        mod = types.SimpleNamespace(main=mock.Mock(return_value={"outputFields": {"status": "ok", "warranties_created": 2}}))
        out = ws.sweep_missing(FakeHS(orders, {}), cfg(), mod, live=True, days=3, ids=[], workers=2)
        self.assertEqual((out["orders"], out["no_delivery_date"], out["ok"], out["warranties"]), (2, 1, 1, 2))
        mod.main.assert_called_once_with({"object": {"objectId": "1"}})
        self.assertTrue(Path("mirror/warranty_sweep.csv").exists())

    def test_stale_pass_voids_active_warranties_with_the_reason(self):
        orders = {"7": {"hs_pipeline_stage": "X", "salla_order_id": "S7"},
                  "8": {"hs_pipeline_stage": "R", "salla_order_id": "S8"}}
        hs = FakeHS(orders, {"7": {"w7": "5913821399", "w7b": "5913821402"}, "8": {"w8": "5913821400"}})
        out = ws.sweep_stale(hs, cfg(), live=True, days=30)
        self.assertEqual(out["voided"], 2)
        sent = {x["id"]: x["properties"] for x in hs.writes[0][1]["inputs"]}
        self.assertEqual(sent["w7"], {"hs_pipeline_stage": "5913821402", "void_reason": "cancelled"})
        self.assertEqual(sent["w8"]["void_reason"], "returned")
        self.assertNotIn("w7b", sent)                     # already voided: untouched


if __name__ == "__main__":
    unittest.main()
