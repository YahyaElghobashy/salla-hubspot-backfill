#!/usr/bin/env python3
"""Ingest the client's REVIEWED approval sheet and turn it into machine inputs.

The first sheet asked one question ("create this product: yes or no"). The
reviewed sheet answers three, and only the first is a plain yes:

  1. approval          -> create an LGCY- record, or do not
  2. duplicate_of_sku  -> "no, because this SKU is really that one"
  3. device_or_consumable + the line-item columns -> the warranty classification

Point 2 is the one that silently breaks an import. A rejected row is NOT a row
to skip: 871 orders carry those SKUs, and with no record and no alias they
hold forever. `--apply` only ever created approved rows, so left alone the
rejects would have been read as "nothing to do" -- the failure mode where the
client answers correctly and the pipeline discards the answer.

Aliasing happens at NORMALISATION rather than by creating alias products,
because that is what the client asked for ("map its 27 orders onto LGCY-C13").
One canonical product ends up owning the history instead of two records
splitting it.

Two alias targets are written in the client's own catalogue names rather than
the portal's spelling, and both resolve against live HubSpot:

    "CH1 (new SKU C110)"   -> CH01           Daily Shampoo
    "CH11 (new SKU S220)"  -> 6287032431307  Volumizing Mousse

Those are recorded as explicit overrides below rather than guessed at by
fuzzy-matching, so the mapping is reviewable.

Outputs:
    approvals/sku_aliases.json      alias SKU -> canonical SKU
    approvals/sku_excluded.json     SKUs to drop entirely (test records)
    approvals/warranty_input.csv    per-SKU product_class + component detail

Usage:
    python3 zed_review.py --sheet <reviewed.csv>            # validate + write
    python3 zed_review.py --sheet <reviewed.csv> --check    # validate only
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path

from backfill import Config, apply_portal_config, setup_logging
from zed_snapshot import SnapshotHubSpot
import zed_normalize as zn

log = logging.getLogger("backfill")

APPROVALS = Path("approvals")

# Client wrote their new catalogue's SKU; the portal still carries the old one.
# Explicit, not fuzzy-matched, so a human can check the pairing at a glance.
TARGET_OVERRIDES = {
    "CH1": "CH01",                  # Daily Shampoo
    "CH11": "6287032431307",        # Volumizing Mousse (barcode-as-SKU)
}

# SKUs the client's review could not have caught, because that review
# deduplicated WITHIN the sheet and never had the live HubSpot catalogue in
# scope. A blank duplicate_of_sku on these rows was therefore not a decision
# that the SKU is distinct.
#
# Each pair was verified the same way: byte-identical product name, identical
# modal ex-VAT price, date ranges that abut at a cutover, and ZERO orders
# containing both SKUs. That combination is a SKU rename, not two products.
#
# These live here rather than being hand-edited into sku_aliases.json because
# that file is regenerated from the sheet on every run -- an edit made only
# there is silently discarded the next time anyone runs this tool.
LIVE_DUPLICATES = {
    "C6":   "BRUSHES",          # مجموعة فرش الشعر, 65.22, relisted 2024-08
    "C011": "C11",              # مشد للعين والوجه, 33.91, cutover 2023-04-02
    "C012": "C12",              # مشبك الشعر, 25.22, cutover 2023-04-02
    "C021": "C21",              # مدلك لفروة الرأس, 21.74, cutover 2025-02-10
    "CH14": "6287032432144",    # Hair Wax Stick, 94.78, cutover 2025-10-09
}

# device_or_consumable -> product_class. "Device + Consumable" is a mixed
# bundle: HubSpot's product_class enum calls that 'bundle', and the component
# columns carry what is actually inside.
CLASS_MAP = {
    "device": "device",
    "consumable": "consumable",
    "accessory": "accessory",
    "device + consumable": "bundle",
}


def parse_target(raw):
    """'CH1 (new SKU C110)' -> 'CH1', then through TARGET_OVERRIDES."""
    m = re.match(r"^\s*([A-Za-z0-9.\-]+)", str(raw or ""))
    if not m:
        return ""
    t = zn.canon_sku(m.group(1))
    return TARGET_OVERRIDES.get(t, t)


def load(sheet):
    with open(sheet, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def build(rows, hs):
    """Returns (approved, aliases, excluded, problems)."""
    approved, aliases, excluded, problems = set(), {}, set(), []

    for r in rows:
        sku = zn.canon_sku(r.get("original_zid_sku"))
        if not sku:
            continue
        ok = (r.get("approval (Yes/No)") or "").strip().lower() in ("yes", "y")
        if ok:
            approved.add(sku)
            continue
        target = parse_target(r.get("duplicate_of_sku"))
        note = (r.get("duplicate_of_product") or "").strip()
        held = int(r.get("held_orders_released_est") or 0)
        if not target:
            # a reject with no target is only safe when it is deliberate
            if "exclude" in note.lower() or "test" in note.lower():
                excluded.add(sku)
            else:
                problems.append(
                    f"{sku}: rejected with no duplicate_of_sku and no EXCLUDE "
                    f"note; {held} orders would hold forever")
            continue
        aliases[sku] = target

    for sku, target in LIVE_DUPLICATES.items():
        sku = zn.canon_sku(sku)
        if sku in approved:
            approved.discard(sku)      # do not mint a duplicate product
        aliases[sku] = zn.canon_sku(target)

    # every alias target must be resolvable, or its orders still hold
    for sku, target in sorted(aliases.items()):
        in_sheet = target in approved
        in_portal = hs.gate_search_product_by_sku([target]) > 0
        if not (in_sheet or in_portal):
            problems.append(
                f"{sku} -> {target}: target is neither approved in this sheet "
                f"nor an approved product in HubSpot")

    # an alias must not point at another alias (one hop only)
    for sku, target in aliases.items():
        if target in aliases:
            problems.append(f"{sku} -> {target} -> {aliases[target]}: "
                            f"alias chain; resolve to a single hop")
    return approved, aliases, excluded, problems


def warranty_rows(rows, aliases):
    out = []
    for r in rows:
        sku = zn.canon_sku(r.get("original_zid_sku"))
        if not sku or sku in aliases:
            continue           # aliased SKUs inherit their target's class
        raw = (r.get("device_or_consumable") or "").strip().lower()
        cls = CLASS_MAP.get(raw, "")
        # device_or_consumable alone cannot express a DEVICE + DEVICE set, so
        # a two-styler pack answers "Device" and would collapse to a single
        # device record -- one warranty for a customer who owns two. The
        # sheet's own `type` column already says it is a set, and
        # component_proposal carries the decomposition, so trust those.
        is_set = (r.get("type") or "").strip().lower().startswith("set /")
        if cls == "device" and is_set:
            cls = "bundle"
        out.append({
            "sku": sku,
            "proposed_sku": r.get("proposed_sku", ""),
            "product_name": r.get("product_name", ""),
            "device_or_consumable_raw": r.get("device_or_consumable", ""),
            "product_class": cls,
            "device_line_items": r.get("device_line_items", ""),
            "consumable_line_items": r.get("consumable_line_items", ""),
            "component_proposal": (r.get("component_proposal") or "").strip(),
            "type": (r.get("type") or "").strip(),
            "warranty_months": "",       # the client still owes us this
            "needs_decision": "" if cls else "UNMAPPED CLASS",
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--sheet", required=True)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="zed_review.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    if not (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip():
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = SnapshotHubSpot(cfg, os.environ["HUBSPOT_ACCESS_TOKEN"].strip(),
                         live=False)

    rows = load(args.sheet)
    approved, aliases, excluded, problems = build(rows, hs)

    log.info("sheet rows        %d", len(rows))
    log.info("approved          %d", len(approved))
    log.info("aliased           %d  (%d orders redirected)", len(aliases),
             sum(int(r.get("held_orders_released_est") or 0) for r in rows
                 if zn.canon_sku(r.get("original_zid_sku")) in aliases))
    log.info("excluded          %d", len(excluded))

    cls = Counter(r["product_class"] or "UNMAPPED"
                  for r in warranty_rows(rows, aliases))
    log.info("product_class     %s", dict(cls))

    if problems:
        log.error("%d PROBLEM(S):", len(problems))
        for p in problems:
            log.error("  %s", p)
        return 1
    log.info("every alias target resolves; no alias chains")

    if args.check:
        return 0

    APPROVALS.mkdir(parents=True, exist_ok=True)
    (APPROVALS / "sku_aliases.json").write_text(
        json.dumps(aliases, indent=1, ensure_ascii=False, sort_keys=True))
    (APPROVALS / "sku_excluded.json").write_text(
        json.dumps(sorted(excluded), indent=1))
    wr = warranty_rows(rows, aliases)
    with open(APPROVALS / "warranty_input.csv", "w", newline="",
              encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(wr[0].keys()))
        w.writeheader()
        w.writerows(wr)
    log.info("wrote %s, %s, %s", APPROVALS / "sku_aliases.json",
             APPROVALS / "sku_excluded.json",
             APPROVALS / "warranty_input.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
