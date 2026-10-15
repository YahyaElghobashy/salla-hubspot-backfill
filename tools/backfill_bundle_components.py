#!/usr/bin/env python3
"""Add the missing component line items to past orders of named bundles (v2.15).

Why: an order's bundle item becomes one parent line (price = bundle price)
plus one zero-price line per ACTIVE component of its template, created at the
moment the order syncs (backfill.py, M126-M130). The component line is the one
that carries the device, so it is what earns the warranty and the device
flags. Templates whose components were never built (the Make "Bundle Template
Sync" stop for bundles with no parent product, found 2026-10-15) produced
orders with the parent line only: no dryer line, no warranty.

This builds, for every purchased-bundle record of the named templates that has
no component line yet, the component lines the engine would have built: same
properties, price 0, hs_product_id in the CREATE so the product's class and
warranty term are inherited, salla_order_item_id = "<item>_<component code>"
exactly as the engine writes it (that property is unique, so a re-run or a
later engine top-up can never double a line). Each line is associated to its
order and to its purchased-bundle record.

Which device: orders placed before the Multi Styler relist (PRE_RELIST_CUTOFF)
were sold with the retired dryer (Salla 1268166868), so for those the current
dryer component (1982906533) is swapped for the retired one. Both are classed
device; the swap keeps reporting true to what shipped.

Every created line goes to mirror/bundle_component_backfill.csv first-thing
after its batch returns; a re-run skips items already in the ledger or already
holding a component line. Rollback is archiving the ledger's line ids. It also
writes mirror/bundle_component_backfill_orders.txt: the order ids to hand to
tools/warranty_sweep.py --ids-file. Dry run by default.

    python3 tools/backfill_bundle_components.py --config config.live.json --keys 1448268221,119765676
    python3 tools/backfill_bundle_components.py --config config.live.json --keys 1448268221,119765676 --apply
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import backfill
from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")

LEDGER = Path("mirror/bundle_component_backfill.csv")
ORDERS_OUT = Path("mirror/bundle_component_backfill_orders.txt")
BATCH = 100
# The relisted Multi Styler (1982906533) first landed 2026-09-21 22:42 UTC; earlier
# orders shipped the retired dryer (1268166868). Midnight Riyadh on the 22nd.
PRE_RELIST_CUTOFF_MS = int(datetime(2026, 9, 21, 21, 0, tzinfo=timezone.utc).timestamp() * 1000)
DEVICE_SWAP = {"1982906533": "1268166868"}       # current component -> what shipped before


# ---- pure helpers (tested) ----------------------------------------------------

def parse_instance_key(key):
    """bundle_instance_key "<order>::<item>::<template>" -> (order, item, template)."""
    parts = str(key or "").split("::")
    return tuple(parts) if len(parts) == 3 and all(parts) else None


def components_for(order_ms, components, cutoff_ms=PRE_RELIST_CUTOFF_MS, swap=DEVICE_SWAP):
    """The component list that applies to an order placed at order_ms.

    components: [{"salla_id", "qty"}] from the template's active components.
    Before the cutoff, a swapped component is replaced by what shipped then."""
    out = []
    for c in components:
        sid = c["salla_id"]
        if order_ms and order_ms < cutoff_ms and sid in swap:
            sid = swap[sid]
        out.append({"salla_id": sid, "qty": c["qty"]})
    return out


def line_props(item_id, order_id, template_key, template_name, bundle_qty, comp, product, currency):
    """The component line item the engine writes (backfill.py M127), for one component."""
    try:
        qty = float(bundle_qty or 1) * float(comp["qty"] or 1)
        qty = int(qty) if qty == int(qty) else qty
    except (TypeError, ValueError):
        qty = bundle_qty or 1
    sku = product.get("hs_sku", "") or ""
    props = {
        "name": product.get("name", ""), "price": "0",
        "hs_sku": sku, "salla_sku": sku,
        "quantity": str(qty), "sale_context": "bundle_component",
        "salla_order_id": str(order_id),
        "salla_product_id": str(comp["salla_id"]),
        "bundle_template_key": str(template_key), "is_bundle_component": True,
        "salla_order_item_id": f"{item_id}_{comp['salla_id']}",
        "reporting_product_key": sku,
        "revenue_attribution_method": "component_quantity_only",
        "allocated_component_revenue": "0",
        "bundle_template_name_snapshot": template_name or "",
    }
    if currency:
        props["hs_line_item_currency_code"] = currency
    return backfill.with_product(props, product.get("id", ""))


# ---- HubSpot reads ---------------------------------------------------------------

def search_all(hs, obj, filters, props, what):
    out, after = [], None
    while True:
        body = {"filterGroups": [{"filters": filters}], "properties": props, "limit": 200}
        if after:
            body["after"] = after
        data = hs.search(f"/crm/v3/objects/{obj}/search", body, what)
        out.extend(data.get("results") or [])
        after = ((data.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            return out


def batch_read(hs, obj, ids, props, what):
    out = {}
    ids = list(dict.fromkeys(str(i) for i in ids if i))
    for i in range(0, len(ids), BATCH):
        status, data = hs._req("POST", f"/crm/v3/objects/{obj}/batch/read",
                               {"inputs": [{"id": x} for x in ids[i:i + BATCH]],
                                "properties": props}, what=what)
        if status not in (200, 207):
            raise RuntimeError(f"{what}: HTTP {status} {json.dumps(data)[:200]}")
        for r in data.get("results") or []:
            out[str(r["id"])] = r.get("properties") or {}
    return out


def assoc_read(hs, from_obj, to_obj, ids, what):
    out = {}
    ids = list(dict.fromkeys(str(i) for i in ids if i))
    for i in range(0, len(ids), BATCH):
        status, data = hs._req("POST", f"/crm/v4/associations/{from_obj}/{to_obj}/batch/read",
                               {"inputs": [{"id": x} for x in ids[i:i + BATCH]]}, what=what)
        if status not in (200, 207):
            raise RuntimeError(f"{what}: HTTP {status} {json.dumps(data)[:200]}")
        for r in data.get("results") or []:
            out[str(r["from"]["id"])] = [str(t["toObjectId"]) for t in r.get("to") or []]
    return out


def ledger_done():
    if not LEDGER.exists():
        return set()
    with open(LEDGER, encoding="utf-8") as f:
        return {row["salla_order_item_id"] for row in csv.DictReader(f) if row.get("li_id")}


# ---- main ------------------------------------------------------------------------

def plan(hs, keys):
    """Every component line still missing for the purchased bundles of `keys`."""
    templates = {}
    for k in keys:
        tpl = search_all(hs, backfill.OBJ_BUNDLE_TEMPLATE,
                         [{"propertyName": "bundle_template_key", "operator": "EQ", "value": k}],
                         ["bundle_template_name", "template_status"], "template")
        comps = hs.search_active_components(k)
        if not comps:
            raise SystemExit(f"Template {k} has no active components yet: push it from the sheet first.")
        templates[k] = {"name": (tpl[0]["properties"].get("bundle_template_name") if tpl else ""),
                        "components": [{"salla_id": str(c["properties"]["component_salla_product_id"]),
                                        "qty": c["properties"].get("quantity_in_bundle") or 1}
                                       for c in comps]}
    bundles = search_all(hs, backfill.OBJ_BUNDLE,
                         [{"propertyName": "bundle_template_key", "operator": "IN", "values": list(keys)}],
                         ["bundle_instance_key", "bundle_quantity", "currency", "bundle_template_key"],
                         "purchased bundles")
    existing = search_all(hs, "line_items",
                          [{"propertyName": "bundle_template_key", "operator": "IN", "values": list(keys)},
                           {"propertyName": "sale_context", "operator": "EQ", "value": "bundle_component"}],
                          ["salla_order_item_id"], "existing component lines")
    have_items = {str(r["properties"].get("salla_order_item_id", "")).rsplit("_", 1)[0] for r in existing}
    done = ledger_done()
    b2o = assoc_read(hs, backfill.OBJ_BUNDLE, "order", [b["id"] for b in bundles], "bundle->order")
    orders = batch_read(hs, "orders", [o for v in b2o.values() for o in v],
                        ["hs_external_created_date", "createdate", "hs_pipeline_stage"], "orders")
    salla_ids = {c["salla_id"] for t in templates.values() for c in t["components"]} | set(DEVICE_SWAP.values())
    prods = search_all(hs, "products",
                       [{"propertyName": "salla_product_id", "operator": "IN", "values": sorted(salla_ids)}],
                       ["name", "hs_sku", "salla_product_id", "product_class", "catalog_approval_status"],
                       "component products")
    by_salla = {}
    for p in prods:
        pr = p.get("properties") or {}
        if pr.get("catalog_approval_status") == "approved" or pr.get("salla_product_id") not in by_salla:
            by_salla[str(pr.get("salla_product_id"))] = {"id": p["id"], **pr}
    missing_prod = salla_ids - set(by_salla)
    if missing_prod:
        raise SystemExit(f"No HubSpot product for component Salla ids: {sorted(missing_prod)}")

    lines, stats, order_ids = [], Counter(), set()
    for b in bundles:
        pr = b.get("properties") or {}
        parsed = parse_instance_key(pr.get("bundle_instance_key"))
        if not parsed:
            stats["skip_bad_key"] += 1
            continue
        oid, item_id, tkey = parsed
        if item_id in have_items:
            stats["skip_has_components"] += 1
            continue
        hs_orders = b2o.get(str(b["id"])) or []
        if len(hs_orders) != 1:
            stats["skip_order_assoc_%d" % len(hs_orders)] += 1
            continue
        hs_oid = hs_orders[0]
        o = orders.get(hs_oid) or {}
        try:
            ms = int(o.get("hs_external_created_date") or 0) or int(
                datetime.fromisoformat(str(o.get("createdate")).replace("Z", "+00:00")).timestamp() * 1000)
        except (TypeError, ValueError):
            ms = 0
        tpl = templates[tkey]
        for comp in components_for(ms, tpl["components"]):
            props = line_props(item_id, oid, tkey, tpl["name"], pr.get("bundle_quantity"),
                               comp, by_salla[comp["salla_id"]], pr.get("currency"))
            if props["salla_order_item_id"] in done:
                stats["skip_in_ledger"] += 1
                continue
            lines.append({"bundle_id": str(b["id"]), "order_id": hs_oid, "props": props,
                          "device": by_salla[comp["salla_id"]].get("product_class") == "device"})
            stats["pre_relist_device" if ms and ms < PRE_RELIST_CUTOFF_MS and comp["salla_id"] in DEVICE_SWAP.values()
                  else "lines_" + comp["salla_id"]] += 1
        stats["bundles_to_fix"] += 1
        order_ids.add(hs_oid)
    stats["bundles_total"] = len(bundles)
    stats["orders_to_fix"] = len(order_ids)
    return lines, stats, sorted(order_ids)


def apply(hs, lines):
    LEDGER.parent.mkdir(exist_ok=True)
    new = not LEDGER.exists()
    created = failed = 0
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "bundle_id", "order_id", "li_id", "salla_order_item_id", "salla_product_id"])
        for i in range(0, len(lines), BATCH):
            chunk = lines[i:i + BATCH]
            status, data = hs._write("POST", "/crm/v3/objects/line_items/batch/create",
                                     {"inputs": [{"properties": c["props"]} for c in chunk]},
                                     "create component lines")
            ids = {}
            if status in (200, 201, 207):
                for r in data.get("results") or []:
                    ids[str((r.get("properties") or {}).get("salla_order_item_id"))] = str(r["id"])
            if len(ids) < len(chunk):
                # one-by-one for whatever the batch did not create (the engine's own
                # create reuses an existing line on the unique salla_order_item_id)
                for c in chunk:
                    key = c["props"]["salla_order_item_id"]
                    if key not in ids:
                        li = hs.create_line_item(c["props"], "create component line")
                        if li:
                            ids[key] = str(li)
            pairs = []
            for c in chunk:
                li = ids.get(c["props"]["salla_order_item_id"])
                if not li:
                    failed += 1
                    continue
                created += 1
                pairs.append((c, li))
                w.writerow([now_str(), c["bundle_id"], c["order_id"], li,
                            c["props"]["salla_order_item_id"], c["props"]["salla_product_id"]])
            f.flush()
            for from_obj, assoc_id, cat, pick in (
                    ("order", backfill.ASSOC_ORDER_LI, "HUBSPOT_DEFINED", lambda c: c["order_id"]),
                    (backfill.OBJ_BUNDLE, backfill.ASSOC_BUNDLE_COMP, "USER_DEFINED", lambda c: c["bundle_id"])):
                inputs = [{"from": {"id": pick(c)}, "to": {"id": li},
                           "types": [{"associationCategory": cat, "associationTypeId": assoc_id}]}
                          for c, li in pairs]
                if inputs:
                    st, d = hs._write("POST", f"/crm/v4/associations/{from_obj}/line_items/batch/create",
                                      {"inputs": inputs}, "associate component lines")
                    if st not in (200, 201, 207):
                        log.error("association %s->line_items failed (%s): %s", from_obj, st,
                                  json.dumps(d)[:300])
            log.info("batch %d-%d: %d created so far, %d failed", i, i + len(chunk), created, failed)
    return created, failed


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--keys", required=True, help="comma-separated bundle template keys")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="bundle_component_backfill.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, tok, live=args.apply)
    keys = [k.strip() for k in args.keys.split(",") if k.strip()]
    lines, stats, order_ids = plan(hs, keys)
    print()
    print("BUNDLE COMPONENT BACKFILL" + ("" if args.apply else " (DRY RUN)"))
    for k, v in sorted(stats.items()):
        print(f"  {k:<28} {v:>8,}")
    print(f"  {'component lines to create':<28} {len(lines):>8,}"
          f"  (device lines: {sum(1 for l in lines if l['device']):,})")
    if lines:
        print("  sample:", json.dumps(lines[0]["props"], ensure_ascii=False)[:400])
    ORDERS_OUT.parent.mkdir(exist_ok=True)
    ORDERS_OUT.write_text("\n".join(order_ids) + ("\n" if order_ids else ""), encoding="utf-8")
    print(f"  order ids for the warranty sweep: {ORDERS_OUT} ({len(order_ids):,})")
    if not args.apply:
        print("DRY RUN -- rerun with --apply to write.")
        return
    created, failed = apply(hs, lines)
    print(f"created {created:,} component lines, {failed:,} failed; ledger {LEDGER}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
