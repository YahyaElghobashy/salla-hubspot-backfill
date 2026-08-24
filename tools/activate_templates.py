#!/usr/bin/env python3
"""Flip fully-built bundle templates from draft to active.

Why this exists as a tool rather than three clicks in HubSpot: the catalog
gate holds an order when a bundle template EXISTS for the item's Salla product
id but none is eligible, i.e.

    hold when (te == 0 and ta > 0)

where `ta` counts every template carrying that bundle_template_key and `te`
counts only those with template_status == "active" AND
active_component_count > 0. A template left in draft therefore does not fail
open -- it actively blocks every order containing that product, forever. That
is deliberate: a half-built bundle would otherwise write the wrong line items,
which is far more expensive to undo than a hold.

So the safe fix is narrow and must be verified rather than assumed. This tool
refuses to activate a template unless its components already resolve:

  * template_status is draft (never touches an already-active one)
  * active_component_count > 0
  * every active component carries a component_hubspot_product_id
  * the component SKUs, sorted, reconstruct the template's own bundle_sku

That last check is the one that matters. It is what separates "someone
finished building this and forgot to flip the switch" from "someone started
building this and stopped". Only the former is safe to activate without asking
the client, because the end state is already unambiguous in the data.

A template with zero components is NOT handled here. That is an abandoned
draft, and whether it should be deleted or completed is a catalog decision
belonging to the client, not a mechanical one.

    python3 tools/activate_templates.py                 # dry run, default
    python3 tools/activate_templates.py --apply
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import backfill
from backfill import Config, HubSpot, apply_portal_config, setup_logging

log = logging.getLogger("backfill")

TOKEN_RE = re.compile(r"[A-Z]{1,3}\d{1,3}")


def sku_tokens(sku):
    """C41CH7 -> ['C41', 'CH7'] only when the tokens rebuild the string.

    A partial match means the SKU is not a clean composite and the component
    cross-check below cannot be trusted, so it returns nothing and the
    template is skipped rather than guessed at.
    """
    up = str(sku or "").strip().upper()
    toks = TOKEN_RE.findall(up)
    return toks if toks and "".join(toks) == up else []


def inspect(hs, pid):
    """Everything needed to judge one product id, read straight from HubSpot."""
    any_t = hs.item_search_template(pid, eligible_only=False)
    results = any_t.get("results") or []
    if not results:
        return None
    tpl_id = results[0]["id"]
    status, data = hs._req(
        "GET", f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/{tpl_id}"
        "?properties=bundle_template_name,bundle_sku,template_status,"
        "active_component_count,component_count,bundle_template_key",
        what="template read")
    props = (data.get("properties") or {}) if status == 200 else {}
    return {"id": tpl_id, "props": props,
            "components": hs.search_active_components(pid)}


def verifies(tpl, comps):
    """Is this template complete enough to activate unattended?

    Returns (ok, reason). The reason is logged either way so a refusal is as
    legible as an approval.
    """
    if (tpl.get("template_status") or "").lower() == "active":
        return False, "already active"
    if not comps:
        return False, "no active components -- abandoned draft, needs a catalog decision"

    missing = [c for c in comps
               if not (c.get("properties") or {}).get("component_hubspot_product_id")]
    if missing:
        return False, f"{len(missing)} component(s) have no HubSpot product id"

    want = sku_tokens(tpl.get("bundle_sku"))
    if not want:
        return False, f"bundle_sku {tpl.get('bundle_sku')!r} does not tokenise cleanly"

    have = sorted(str((c.get("properties") or {}).get("component_product_sku") or "").upper()
                  for c in comps)
    if sorted(want) != have:
        return False, (f"components {have} do not reconstruct bundle_sku "
                       f"{tpl.get('bundle_sku')!r} (expected {sorted(want)})")
    return True, f"{len(comps)} component(s) match {tpl.get('bundle_sku')}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--pids", default="1209728408,771366295,2113977878",
                    help="comma separated Salla product ids to consider")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="activate_templates.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    if not backfill.OBJ_BUNDLE_TEMPLATE:
        sys.exit("bundle_template object type is not configured.")

    hs = HubSpot(cfg, token, live=args.apply)
    log.info("mode=%s", "APPLY" if args.apply else "DRY RUN")

    activated = refused = 0
    for pid in [p.strip() for p in args.pids.split(",") if p.strip()]:
        found = inspect(hs, pid)
        if not found:
            log.info("pid %-12s no template found -- nothing to do", pid)
            continue
        tpl, comps = found["props"], found["components"]
        ok, why = verifies(tpl, comps)
        name = str(tpl.get("bundle_template_name") or "")[:38]
        if not ok:
            log.info("REFUSE  pid %-12s tpl %-14s %s", pid, found["id"], why)
            log.info("        (%s)", name)
            refused += 1
            continue

        log.info("%s pid %-12s tpl %-14s %s",
                 "ACTIVATE" if args.apply else "WOULD ACTIVATE",
                 pid, found["id"], why)
        log.info("        (%s)", name)
        if args.apply:
            status, _ = hs._write(
                "PATCH",
                f"/crm/v3/objects/{backfill.OBJ_BUNDLE_TEMPLATE}/{found['id']}",
                {"properties": {"template_status": "active"}},
                "activate bundle template")
            if status not in (200, 201):
                log.error("        PATCH failed: HTTP %s", status)
                refused += 1
                continue
            # prove it to the gate, not to ourselves
            te = hs.item_search_template(pid, eligible_only=True).get("total", 0)
            log.info("        gate eligible now = %s %s", te,
                     "OK" if te else "STILL NOT ELIGIBLE")
        activated += 1

    log.info("")
    log.info("%s: %d   refused: %d",
             "activated" if args.apply else "would activate", activated, refused)
    if not args.apply:
        log.info("DRY RUN -- rerun with --apply to write to HubSpot.")


if __name__ == "__main__":
    main()
