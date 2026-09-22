#!/usr/bin/env python3
"""Two catalog blockers that approve_legacy.py must NOT touch.

Both hold orders, neither is a missing product, and each fails a different
clause of the gate. Running them through the legacy tool would make things
worse, which is why they live here instead.

  ROW 2 -- approve an EXISTING product (C031, Salla id 2142608306)
    The product record is already in HubSpot with the right SKU and Salla id;
    its catalog_approval_status is "pending_review". The gate's SKU search
    filters on catalog_approval_status == "approved", so a pending record is
    invisible to it and reads as absent. Feeding this SKU to approve_legacy.py
    would therefore "SKIP already in HubSpot"? No -- worse: that check uses the
    same approved-only search, gets 0, and CREATES A SECOND C031. One PATCH is
    the correct fix; a create is a duplicate.

  ROW 3 -- neutralise an EMPTY bundle template (CH12, Salla id 2113977878)
    The CH12 product exists and is approved (p=1), but the gate also holds when
    a template exists for the product and none is eligible:

        hold when (te == 0 and ta > 0)

    Template 441823658210 carries bundle_template_key=2113977878 with no
    components, no status, and a last-modified stamp 0.6s after creation -- an
    abandoned draft. The client confirmed CH12 is a single product, so the
    template should stop matching.

    This REPOINTS bundle_template_key rather than deleting the record.
    Deleting is irreversible and destroys the audit trail of what someone
    intended in July; changing the key drops `ta` to 0, which is all the gate
    needs, and the old value is written to a ledger so it can be restored with
    a single PATCH. Setting template_status alone would NOT work -- `ta` counts
    templates regardless of status.

    The key is set to a VOID- sentinel rather than emptied, because
    bundle_template_key is a required property on this object and HubSpot
    rejects the clear outright:

        400 "Error updating bundle_template. Some required properties were
             cleared." context.properties=["bundle_template_key"]

    It is also hasUniqueValue=True, so the sentinel must be unique. Embedding
    the original Salla id in it (VOID-2113977878) satisfies both constraints,
    can never equal a real Salla product id, and states plainly what the record
    used to point at.

    python3 tools/fix_catalog_blockers.py            # dry run, default
    python3 tools/fix_catalog_blockers.py --apply
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import backfill
from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")

LEDGER = Path("mirror/catalog_blocker_fixes.csv")

# (salla_product_id, expected_sku, human label)
APPROVE_EXISTING = [("2142608306", "C031", "فرشاة الشعر الكيرلي / Curly Hair Brush")]
NEUTRALISE_STUB = [
    ("2113977878", "CH12", "Clara Daily Hydrating Shampoo & Conditioner"),
    # Relisted 21 Sep as a single product but tagged "bundle" in Salla, so the
    # Make bundle branch creates an empty template for it on every edit and the
    # gate holds its orders (te == 0, ta > 0). Standalone 434196269276 is approved.
    ("1982906533", "", "المجفف متعدد الاستخدام / Multi Styler dryer (tagged bundle in Salla)"),
]


def ledger_write(rows):
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "action", "object", "object_id",
                        "property", "old_value", "new_value"])
        for r in rows:
            w.writerow([now_str()] + r)


def approve_existing(hs, pid, sku, label, apply_):
    """PATCH catalog_approval_status -> approved on the product already there."""
    d = hs.search("/crm/v3/objects/products/search", {
        "filterGroups": [{"filters": [
            {"propertyName": "salla_product_id", "operator": "EQ", "value": pid}]}],
        "properties": ["hs_sku", "catalog_approval_status", "name", "salla_product_id"],
        "limit": 5}, "find product")
    results = d.get("results") or []
    if not results:
        log.error("  pid %s: no product found -- expected one to approve", pid)
        return None
    if len(results) > 1:
        log.error("  pid %s: %d products share this Salla id; refusing to guess",
                  pid, len(results))
        return None

    rec = results[0]
    props = rec.get("properties") or {}
    have_sku = (props.get("hs_sku") or "").strip()
    old = props.get("catalog_approval_status")

    if have_sku.upper() != sku.upper():
        log.error("  pid %s: record carries hs_sku %r, sheet says %r -- refusing",
                  pid, have_sku, sku)
        return None
    if old == "approved":
        log.info("  pid %-12s %-6s already approved -- nothing to do", pid, sku)
        return None

    log.info("  %s pid %-12s %-6s id=%s  %s -> approved",
             "PATCH" if apply_ else "WOULD PATCH", pid, sku, rec["id"], old)
    log.info("        (%s)", label)
    if not apply_:
        return None

    status, _ = hs._write("PATCH", f"/crm/v3/objects/products/{rec['id']}",
                          {"properties": {"catalog_approval_status": "approved"}},
                          "approve existing product")
    if status not in (200, 201):
        log.error("        PATCH failed: HTTP %s", status)
        return None
    log.info("        gate approved-product search now = %s",
             hs.gate_search_product_approved(pid))
    return ["approve_existing_product", "product", rec["id"],
            "catalog_approval_status", old or "", "approved"]


def neutralise_stub(hs, pid, sku, label, apply_):
    """Blank bundle_template_key on an empty template so the gate stops seeing it."""
    found = hs.item_search_template(pid, eligible_only=False)
    results = found.get("results") or []
    if not results:
        log.info("  pid %-12s no template -- nothing to neutralise", pid)
        return None
    if len(results) > 1:
        log.error("  pid %s: %d templates; refusing to guess", pid, len(results))
        return None

    tpl_id = results[0]["id"]
    status, data = hs._req(
        "GET", f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/{tpl_id}"
        "?properties=bundle_sku,bundle_template_key,template_status,"
        "component_count,active_component_count",
        what="stub read")
    p = (data.get("properties") or {}) if status == 200 else {}

    # Refuse anything that is not demonstrably empty. A template with real
    # components is a build in progress, not a stray, and belongs to
    # activate_templates.py or to the client.
    n_active = p.get("active_component_count")
    n_total = p.get("component_count")
    if (n_active not in (None, 0, "0")) or (n_total not in (None, 0, "0")):
        log.error("  pid %s: template %s has components (total=%s active=%s) -- refusing",
                  pid, tpl_id, n_total, n_active)
        return None
    if hs.search_active_components(pid):
        log.error("  pid %s: template %s still has active components -- refusing",
                  pid, tpl_id)
        return None

    old_key = p.get("bundle_template_key") or ""
    new_key = f"VOID-{old_key or tpl_id}"
    # bundle_template_key is unique. A product neutralised once before already
    # owns the plain sentinel (the Make bundle branch recreates a stray for any
    # product still sitting in the Salla bundle category), so a later stray gets
    # its own record id appended: VOID-<key>-<template id>.
    taken = hs.search(
        f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/search",
        {"filterGroups": [{"filters": [
            {"propertyName": "bundle_template_key", "operator": "EQ", "value": new_key}]}],
         "properties": ["hs_object_id"], "limit": 1},
        "void sentinel already used?")
    if taken.get("results"):
        new_key = f"{new_key}-{tpl_id}"
    log.info("  %s pid %-12s %-6s template=%s  bundle_template_key %r -> %r",
             "PATCH" if apply_ else "WOULD PATCH", pid, sku, tpl_id, old_key, new_key)
    log.info("        (%s) -- record kept, key repointed, reversible", label)
    if not apply_:
        return None

    status, _ = hs._write(
        "PATCH", f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/{tpl_id}",
        {"properties": {"bundle_template_key": new_key}},
        "neutralise empty bundle template")
    if status not in (200, 201):
        log.error("        PATCH failed: HTTP %s", status)
        return None
    ta = hs.item_search_template(pid, eligible_only=False).get("total", 0)
    log.info("        templates ANY now = %s %s", ta,
             "OK -- gate no longer blocked by it" if ta == 0 else "STILL MATCHING")
    return ["neutralise_empty_template", "bundle_template", tpl_id,
            "bundle_template_key", old_key, new_key]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="fix_catalog_blockers.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    hs = HubSpot(cfg, token, live=args.apply)
    log.info("mode=%s", "APPLY" if args.apply else "DRY RUN")
    written = []

    log.info("")
    log.info("ROW 2 -- approve products that already exist but sit at pending_review")
    for pid, sku, label in APPROVE_EXISTING:
        r = approve_existing(hs, pid, sku, label, args.apply)
        if r:
            written.append(r)

    log.info("")
    log.info("ROW 3 -- neutralise empty bundle templates that block an approved product")
    for pid, sku, label in NEUTRALISE_STUB:
        r = neutralise_stub(hs, pid, sku, label, args.apply)
        if r:
            written.append(r)

    if written:
        ledger_write(written)
        log.info("")
        log.info("recorded %d change(s) in %s (old values kept for rollback)",
                 len(written), LEDGER)
    if not args.apply:
        log.info("")
        log.info("DRY RUN -- rerun with --apply to write to HubSpot.")


if __name__ == "__main__":
    main()
