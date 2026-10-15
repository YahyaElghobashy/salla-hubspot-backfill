#!/usr/bin/env python3
"""Point Multi Styler bundles at the new dryer listing, and add the bag to the Volume & Gloss set (v2.15).

Why: the Multi Styler was relisted on 21 Sep (Salla 1982906533, HubSpot product
434196269276). Five bundles on Clara's updated Multi Styler list still carry the
retired listing (Salla 1268166868, HubSpot 419301315812) as their device
component. The retired product is still an approved device, so these orders do
get a warranty; this is tidying so reporting and the catalog name the product
that is actually sold. Clara's list also puts the gift bag (C45, Salla
1973407079) in the Mousse + Gloss bundle, whose template lacks it.

How, chosen so nothing breaks while it runs:
  * Dryer switch: the old-dryer component record is edited IN PLACE (Salla id,
    HubSpot product id, component key, and the name snapshot (and sku, when
    it is not blank) copied
    from the new-dryer component Make built for 1448268221 on 15 Oct;
    component_code stays, because line item ids are built from it). The order
    engine reads components by search (template key + status active) and uses
    only these fields, so there is never a moment with a missing component and
    the template's component counts do not change. Its product association is
    moved from the old product to the new one with the same association type.
  * Bag: a new active component record is created from the bag component of
    bundle 102885086 (all writable custom properties copied, key fields
    changed), associated to the template and the bag product with the same
    association types the source uses, then the template's counts go up by one.

Refuses: a template that is missing or not active, more than one active old
dryer component, a component key that already exists on another record. A
template already on the new dryer, or already holding the bag, is skipped.
Every old value is written to mirror/bundle_dryer_switch.csv before the write;
rollback is a PATCH back from the ledger (and archiving the created bag record).
Dry run by default.

    python3 tools/switch_bundle_dryer.py --config config.live.json
    python3 tools/switch_bundle_dryer.py --config config.live.json --apply
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import backfill
from backfill import Config, HubSpot, now_str

LEDGER = Path("mirror/bundle_dryer_switch.csv")

OLD_SALLA, OLD_HS = "1268166868", "419301315812"      # retired Multi Styler listing
NEW_SALLA, NEW_HS = "1982906533", "434196269276"      # relisted Multi Styler (device)
REFERENCE_TEMPLATE = "1448268221"                     # its new-dryer component was built by Make

# bundles on Clara's updated Multi Styler list still on the retired dryer
SWITCH = [
    ("102885086", "Multi Styler + Mousse (+ bag)"),
    ("1307694046", "Multi Styler + Mousse + Gloss (Volume & Gloss set)"),
    ("599685400", "Multi Styler + Leave-in + Serum"),
    ("266454889", "Multi Styler + Repair Cream"),
    ("1092951511", "Multi Styler + Slim Ionic Brush"),
]

BAG_SALLA = "1973407079"                              # Compact Modern Bag, C45
BAG_TARGET = "1307694046"                             # Volume & Gloss set
BAG_SOURCE = "102885086"                              # bundle whose bag component is copied

SWITCH_FIELDS = ("component_salla_product_id", "component_hubspot_product_id",
                 "component_key", "component_product_name_snapshot",
                 "component_product_sku", "component_code")
SKIP_ON_COPY = {"bundle_template_key", "component_key"}


# ---- HubSpot helpers --------------------------------------------------------

def comp_props(hs):
    """Writable custom property names of the component object."""
    st, data = hs._req("GET", f"/crm/v3/properties/{backfill.OBJ_COMPONENT}", what="component props")
    if st != 200:
        raise SystemExit(f"cannot read component properties: HTTP {st}")
    out = []
    for p in data.get("results") or []:
        meta = p.get("modificationMetadata") or {}
        if p["name"].startswith("hs_") or meta.get("readOnlyValue") or p.get("calculated"):
            continue
        out.append(p["name"])
    return out


def template(hs, key):
    data = hs.search(f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/search", {
        "filterGroups": [{"filters": [{"propertyName": "bundle_template_key",
                                       "operator": "EQ", "value": key}]}],
        "properties": ["bundle_template_name", "template_status",
                       "component_count", "active_component_count"],
        "limit": 5}, "template")
    res = data.get("results") or []
    return res[0] if len(res) == 1 else (None if not res else "many")


def components(hs, key, props):
    data = hs.search(f"/crm/v3/objects/{backfill.OBJ_COMPONENT}/search", {
        "filterGroups": [{"filters": [{"propertyName": "bundle_template_key",
                                       "operator": "EQ", "value": key}]}],
        "properties": list(props), "limit": 100}, "components")
    return data.get("results") or []


def by_key(hs, ckey):
    data = hs.search(f"/crm/v3/objects/{backfill.OBJ_COMPONENT}/search", {
        "filterGroups": [{"filters": [{"propertyName": "component_key",
                                       "operator": "EQ", "value": ckey}]}],
        "properties": ["component_key", "component_status"], "limit": 5}, "component key")
    return data.get("results") or []


def assoc(hs, cid, to_type):
    """[(to_id, [type dicts])] for a component's associations to to_type."""
    st, data = hs._req("GET", f"/crm/v4/objects/{backfill.OBJ_COMPONENT}/{cid}/associations/{to_type}",
                       what="component assoc")
    if st != 200:
        return []
    return [(str(r.get("toObjectId")), r.get("associationTypes") or [])
            for r in data.get("results") or []]


def link(hs, cid, to_type, to_id, types, what):
    body = [{"associationCategory": t["category"], "associationTypeId": t["typeId"]} for t in types]
    return hs._write("PUT", f"/crm/v4/objects/{backfill.OBJ_COMPONENT}/{cid}/associations/{to_type}/{to_id}",
                     body, what)


def unlink(hs, cid, to_type, to_id, what):
    return hs._write("DELETE", f"/crm/v4/objects/{backfill.OBJ_COMPONENT}/{cid}/associations/{to_type}/{to_id}",
                     None, what)


def readback(hs, key, props, check, tries=(0, 3, 6, 10)):
    rows = []
    for wait in tries:
        time.sleep(wait)
        rows = components(hs, key, props)
        if check(rows):
            return True, rows
    return False, rows


# ---- steps --------------------------------------------------------------------

def p(x, k):
    return (x.get("properties") or {}).get(k) or ""


def switch_one(hs, key, label, ref, props, apply, ledger):
    tpl = template(hs, key)
    if tpl in (None, "many"):
        print(f"REFUSE {key} {label}: template {'missing' if tpl is None else 'not unique'}")
        return False
    if p(tpl, "template_status") != "active":
        print(f"REFUSE {key} {label}: template status {p(tpl, 'template_status')!r}")
        return False
    comps = components(hs, key, props)
    active = [c for c in comps if p(c, "component_status") == "active"]
    old = [c for c in active if p(c, "component_salla_product_id") == OLD_SALLA]
    new = [c for c in active if p(c, "component_salla_product_id") == NEW_SALLA]
    if not old and new:
        print(f"SKIP   {key} {label}: already on the new dryer")
        return True
    if len(old) != 1:
        print(f"REFUSE {key} {label}: {len(old)} active old-dryer components")
        return False
    c = old[0]
    new_key = f"{key}::{NEW_SALLA}"
    clash = [r for r in by_key(hs, new_key) if r["id"] != c["id"]]
    if clash:
        print(f"REFUSE {key} {label}: component key {new_key} already on record "
              f"{', '.join(r['id'] for r in clash)}")
        return False
    want = {"component_salla_product_id": NEW_SALLA, "component_hubspot_product_id": NEW_HS,
            "component_key": new_key,
            "component_product_name_snapshot": p(ref, "component_product_name_snapshot")}
    if p(ref, "component_product_sku"):
        # a blank reference sku would blank reporting_product_key on future dryer lines
        want["component_product_sku"] = p(ref, "component_product_sku")
    # component_code is left as it is: the engine builds each line item's
    # salla_order_item_id from it, so changing it could duplicate the dryer
    # line on an order that is reprocessed later
    diff = {k: v for k, v in want.items() if p(c, k) != v}
    before = {k: p(c, k) for k in diff}
    prod_links = [t for to, t in assoc(hs, c["id"], "products") if to == OLD_HS]
    print(f"{'SWITCH' if apply else 'WOULD '} {key} {label}: component {c['id']} "
          f"{json.dumps(before, ensure_ascii=False)} -> {json.dumps(diff, ensure_ascii=False)}"
          f"{' + move product link' if prod_links else ''}")
    if not apply:
        return True
    ledger.append([now_str(), "switch", key, c["id"], json.dumps(before, ensure_ascii=False),
                   json.dumps(diff, ensure_ascii=False), ""])
    st, resp = hs._write("PATCH", f"/crm/v3/objects/{backfill.OBJ_COMPONENT}/{c['id']}",
                         {"properties": diff}, "switch dryer component")
    if st not in (200, 201):
        print(f"       FAILED (HTTP {st}): {json.dumps(resp)[:200]}")
        ledger[-1][-1] = f"fail {st}"
        return False
    if prod_links:
        link(hs, c["id"], "products", NEW_HS, prod_links[0], "link new dryer product")
        unlink(hs, c["id"], "products", OLD_HS, "unlink old dryer product")
    ok, rows = readback(hs, key, props, lambda rs: any(
        r["id"] == c["id"] and p(r, "component_salla_product_id") == NEW_SALLA
        and p(r, "component_status") == "active" for r in rs))
    act = [r for r in rows if p(r, "component_status") == "active"]
    print(f"       {'readback matches' if ok else 'READBACK MISMATCH'}; "
          f"{len(act)} active component(s): {', '.join(p(r, 'component_salla_product_id') for r in act)}")
    ledger[-1][-1] = "ok" if ok else "readback mismatch"
    return ok


def add_bag(hs, props, writable, apply, ledger):
    tpl = template(hs, BAG_TARGET)
    if tpl in (None, "many") or p(tpl, "template_status") != "active":
        print(f"REFUSE bag: target template {BAG_TARGET} missing or not active")
        return False
    comps = components(hs, BAG_TARGET, props)
    if any(p(c, "component_salla_product_id") == BAG_SALLA and p(c, "component_status") == "active"
           for c in comps):
        print(f"SKIP   bag: {BAG_TARGET} already holds the bag")
        return True
    src = [c for c in components(hs, BAG_SOURCE, set(props) | set(writable))
           if p(c, "component_salla_product_id") == BAG_SALLA and p(c, "component_status") == "active"]
    if len(src) != 1:
        print(f"REFUSE bag: {len(src)} active bag components on source {BAG_SOURCE}")
        return False
    s = src[0]
    new_key = f"{BAG_TARGET}::{BAG_SALLA}"
    if by_key(hs, new_key):
        print(f"REFUSE bag: component key {new_key} already exists")
        return False
    body = {k: p(s, k) for k in writable if k not in SKIP_ON_COPY and p(s, k) != ""}
    body.update({"bundle_template_key": BAG_TARGET, "component_key": new_key})
    tpl_links = [t for to, t in assoc(hs, s["id"], backfill.OBJ_BUNDLE_TEMPLATE)]
    prod_links = [(to, t) for to, t in assoc(hs, s["id"], "products")]
    a, c = p(tpl, "active_component_count"), p(tpl, "component_count")
    counts = {}
    for name, val in (("active_component_count", a), ("component_count", c)):
        if str(val).strip().isdigit():
            counts[name] = str(int(val) + 1)
    print(f"{'ADD   ' if apply else 'WOULD '} bag to {BAG_TARGET} (template {tpl['id']}): "
          f"{json.dumps(body, ensure_ascii=False)}; template link types "
          f"{[x['typeId'] for x in tpl_links[0]] if tpl_links else 'none'}; product link "
          f"{prod_links[0][0] if prod_links else 'none'}; counts {a}/{c} -> {counts}")
    if not apply:
        return True
    if not tpl_links:
        print("       REFUSE: source bag has no template association to copy")
        return False
    st, resp = hs._write("POST", f"/crm/v3/objects/{backfill.OBJ_COMPONENT}",
                         {"properties": body}, "create bag component")
    cid = str((resp or {}).get("id") or "")
    if st not in (200, 201) or not cid:
        print(f"       FAILED (HTTP {st}): {json.dumps(resp)[:200]}")
        ledger.append([now_str(), "bag", BAG_TARGET, "", "", json.dumps(body, ensure_ascii=False), f"fail {st}"])
        return False
    ledger.append([now_str(), "bag", BAG_TARGET, cid, json.dumps({"counts": {"active_component_count": a,
                   "component_count": c}}), json.dumps(body, ensure_ascii=False), ""])
    link(hs, cid, backfill.OBJ_BUNDLE_TEMPLATE, tpl["id"], tpl_links[0], "link bag to template")
    if prod_links:
        link(hs, cid, "products", prod_links[0][0], prod_links[0][1], "link bag product")
    if counts:
        hs._write("PATCH", f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/{tpl['id']}",
                  {"properties": counts}, "bump template counts")
    ok, rows = readback(hs, BAG_TARGET, props, lambda rs: any(
        r["id"] == cid and p(r, "component_status") == "active" for r in rs))
    act = [r for r in rows if p(r, "component_status") == "active"]
    print(f"       created {cid}; {'readback matches' if ok else 'READBACK MISMATCH'}; "
          f"{len(act)} active component(s): {', '.join(p(r, 'component_salla_product_id') for r in act)}")
    ledger[-1][-1] = "ok" if ok else "readback mismatch"
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    cfg = Config.load(args.config)
    backfill.apply_portal_config(cfg)
    if not (backfill.OBJ_COMPONENT and backfill.OBJ_BUNDLE_TEMPLATE):
        sys.exit("object_type_ids.component / bundle_template missing from config")
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, tok, live=args.apply)

    props = {"component_status", "bundle_template_key", *SWITCH_FIELDS}
    ref = [c for c in components(hs, REFERENCE_TEMPLATE, props)
           if p(c, "component_salla_product_id") == NEW_SALLA and p(c, "component_status") == "active"]
    if len(ref) != 1:
        sys.exit(f"reference new-dryer component on {REFERENCE_TEMPLATE}: found {len(ref)}, need 1")
    ref = ref[0]
    print(f"reference new-dryer component {ref['id']}: name {p(ref, 'component_product_name_snapshot')!r}, "
          f"sku {p(ref, 'component_product_sku')!r}, code {p(ref, 'component_code')!r}")

    ledger, failed = [], 0
    for key, label in SWITCH:
        if not switch_one(hs, key, label, ref, props, args.apply, ledger):
            failed += 1
    if not add_bag(hs, props, comp_props(hs), args.apply, ledger):
        failed += 1

    if ledger:
        new = not LEDGER.exists()
        LEDGER.parent.mkdir(exist_ok=True)
        with open(LEDGER, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "step", "bundle_template_key", "component_id",
                            "old_values", "new_values", "result"])
            w.writerows(ledger)
        print(f"ledger: {LEDGER} (+{len(ledger)})")
    if not args.apply:
        print("DRY RUN -- rerun with --apply to write.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
