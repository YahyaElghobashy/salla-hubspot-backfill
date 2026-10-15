"""v2.15 tools/switch_bundle_dryer.py against a fake HubSpot.

  * The old-dryer component is patched in place: Salla id, HubSpot product id,
    key, name and sku change, component_code does not, the record stays active,
    and its product link moves from the old product to the new one.
  * A template already on the new dryer is skipped; two old-dryer components,
    an inactive template or a clashing component key are refused, and nothing
    is written.
  * The bag is copied from the source bundle onto the Volume & Gloss template,
    linked with the source's association types, and the template counts go up
    by one; a second run skips it.
  * Dry run writes nothing.

Offline. Run: python3 -m unittest test_switch_bundle_dryer -v
"""
import os
import tempfile
import unittest

import backfill
from tools import switch_bundle_dryer as sw

TPL, COMP = "2-T", "2-C"


class FakeHS:
    def __init__(self, live=True):
        self.live = live
        self.tpl, self.comp, self.links = {}, {}, {}   # links: (cid, to_type) -> {to_id: types}
        self.n = 900
        self.writes = []

    # data setup
    def add_tpl(self, key, status="active", a="3", c="3"):
        tid = f"t{key}"
        self.tpl[tid] = {"bundle_template_key": key, "template_status": status,
                         "active_component_count": a, "component_count": c}
        return tid

    def add_comp(self, key, sid, hs_id, status="active", code="X", sku="S", name="n", **extra):
        self.n += 1
        cid = str(self.n)
        self.comp[cid] = dict(bundle_template_key=key, component_salla_product_id=sid,
                              component_hubspot_product_id=hs_id, component_status=status,
                              component_key=f"{key}::{sid}", component_code=code,
                              component_product_sku=sku, component_product_name_snapshot=name,
                              quantity_in_bundle="1", **extra)
        self.links[(cid, "products")] = {hs_id: [{"category": "USER_DEFINED", "typeId": 77}]}
        self.links[(cid, TPL)] = {f"t{key}": [{"category": "USER_DEFINED", "typeId": 55}]}
        return cid

    # HubSpot surface used by the tool
    def search(self, path, body, what):
        f = {x["propertyName"]: x["value"] for x in body["filterGroups"][0]["filters"]}
        store = self.tpl if f"/{TPL}/" in path else self.comp
        res = [{"id": i, "properties": dict(v)} for i, v in store.items()
               if all(v.get(k) == val for k, val in f.items())]
        return {"results": res}

    def _req(self, method, path, body=None, what=""):
        if path.startswith(f"/crm/v3/properties/{COMP}"):
            return 200, {"results": [{"name": n} for n in (
                "bundle_template_key", "component_salla_product_id", "component_hubspot_product_id",
                "component_status", "component_key", "component_code", "component_product_sku",
                "component_product_name_snapshot", "quantity_in_bundle")] + [{"name": "hs_object_id"}]}
        parts = path.split("/")             # /crm/v4/objects/COMP/cid/associations/to
        cid, to = parts[5], parts[7]
        return 200, {"results": [{"toObjectId": k, "associationTypes": v}
                                 for k, v in self.links.get((cid, to), {}).items()]}

    def _write(self, method, path, body, what):
        if not self.live:
            return 200, {"id": "DRY"}
        self.writes.append((method, path))
        parts = path.split("/")
        if method == "PATCH" and f"/{COMP}/" in path:
            self.comp[parts[-1]].update(body["properties"])
        elif method == "PATCH" and f"/{TPL}/" in path:
            self.tpl[parts[-1]].update(body["properties"])
        elif method == "POST":
            self.n += 1
            cid = str(self.n)
            self.comp[cid] = dict(body["properties"])
            return 201, {"id": cid}
        elif method == "PUT":
            self.links.setdefault((parts[5], parts[7]), {})[parts[8]] = [
                {"category": b["associationCategory"], "typeId": b["associationTypeId"]} for b in body]
        elif method == "DELETE":
            self.links.get((parts[5], parts[7]), {}).pop(parts[8], None)
        return 200, {}


def seed(hs):
    hs.add_tpl(sw.REFERENCE_TEMPLATE)
    hs.add_comp(sw.REFERENCE_TEMPLATE, sw.NEW_SALLA, sw.NEW_HS, code="", sku="", name="NEWNAME")
    for key, _ in sw.SWITCH:
        hs.add_tpl(key)
        hs.add_comp(key, sw.OLD_SALLA, sw.OLD_HS, code="C18", sku="OLDSKU", name="OLD")
        hs.add_comp(key, "111", "h111")
    hs.add_comp(sw.BAG_SOURCE, sw.BAG_SALLA, "hbag", code="C45", sku="C45", name="Bag")


class Base(unittest.TestCase):
    def setUp(self):
        backfill.OBJ_COMPONENT, backfill.OBJ_BUNDLE_TEMPLATE = COMP, TPL
        sw.time.sleep = lambda s: None
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def ref(self, hs):
        return [{"id": i, "properties": v} for i, v in hs.comp.items()
                if v["bundle_template_key"] == sw.REFERENCE_TEMPLATE][0]

    def props(self):
        return {"component_status", "bundle_template_key", *sw.SWITCH_FIELDS}


class Switch(Base):
    def test_in_place_switch(self):
        hs = FakeHS()
        seed(hs)
        key = sw.SWITCH[0][0]
        cid = [i for i, v in hs.comp.items() if v["bundle_template_key"] == key
               and v["component_salla_product_id"] == sw.OLD_SALLA][0]
        ok = sw.switch_one(hs, key, "x", self.ref(hs), self.props(), True, [])
        self.assertTrue(ok)
        c = hs.comp[cid]
        self.assertEqual((c["component_salla_product_id"], c["component_hubspot_product_id"]),
                         (sw.NEW_SALLA, sw.NEW_HS))
        self.assertEqual(c["component_key"], f"{key}::{sw.NEW_SALLA}")
        self.assertEqual(c["component_product_name_snapshot"], "NEWNAME")
        self.assertEqual(c["component_code"], "C18")                 # untouched
        self.assertEqual(c["component_status"], "active")
        self.assertIn(sw.NEW_HS, hs.links[(cid, "products")])
        self.assertNotIn(sw.OLD_HS, hs.links[(cid, "products")])
        self.assertEqual(hs.tpl[f"t{key}"]["active_component_count"], "3")
        # second run skips
        n = len(hs.writes)
        self.assertTrue(sw.switch_one(hs, key, "x", self.ref(hs), self.props(), True, []))
        self.assertEqual(len(hs.writes), n)

    def test_refusals_write_nothing(self):
        hs = FakeHS()
        seed(hs)
        k0, k1, k2 = (k for k, _ in sw.SWITCH[:3])
        hs.add_comp(k0, sw.OLD_SALLA, sw.OLD_HS)                    # two old dryers
        hs.tpl[f"t{k1}"]["template_status"] = "inactive"
        hs.add_comp("other", sw.NEW_SALLA, sw.NEW_HS)
        hs.comp[str(hs.n)]["component_key"] = f"{k2}::{sw.NEW_SALLA}"  # key clash
        for k in (k0, k1, k2):
            self.assertFalse(sw.switch_one(hs, k, "x", self.ref(hs), self.props(), True, []))
        self.assertEqual(hs.writes, [])

    def test_dry_run_writes_nothing(self):
        hs = FakeHS(live=False)
        seed(hs)
        before = {i: dict(v) for i, v in hs.comp.items()}
        for k, _ in sw.SWITCH:
            sw.switch_one(hs, k, "x", self.ref(hs), self.props(), False, [])
        sw.add_bag(hs, self.props(), [p["name"] for p in hs._req("GET", f"/crm/v3/properties/{COMP}")[1]["results"]
                                      if not p["name"].startswith("hs_")], False, [])
        self.assertEqual(hs.comp, before)


class Bag(Base):
    def test_bag_added_once(self):
        hs = FakeHS()
        seed(hs)
        writable = [p["name"] for p in hs._req("GET", f"/crm/v3/properties/{COMP}")[1]["results"]
                    if not p["name"].startswith("hs_")]
        self.assertTrue(sw.add_bag(hs, self.props(), writable, True, []))
        bags = [(i, v) for i, v in hs.comp.items() if v["bundle_template_key"] == sw.BAG_TARGET
                and v["component_salla_product_id"] == sw.BAG_SALLA]
        self.assertEqual(len(bags), 1)
        cid, v = bags[0]
        self.assertEqual(v["component_key"], f"{sw.BAG_TARGET}::{sw.BAG_SALLA}")
        self.assertEqual((v["component_status"], v["component_code"]), ("active", "C45"))
        self.assertEqual(hs.links[(cid, TPL)], {f"t{sw.BAG_TARGET}": [{"category": "USER_DEFINED", "typeId": 55}]})
        self.assertIn("hbag", hs.links[(cid, "products")])
        self.assertEqual(hs.tpl[f"t{sw.BAG_TARGET}"]["active_component_count"], "4")
        self.assertEqual(hs.tpl[f"t{sw.BAG_TARGET}"]["component_count"], "4")
        n = len(hs.writes)
        self.assertTrue(sw.add_bag(hs, self.props(), writable, True, []))   # skip
        self.assertEqual(len(hs.writes), n)


if __name__ == "__main__":
    unittest.main()
