#!/usr/bin/env python3
"""Per-month catalog approval sheets: the gate that stands between the Zid
data and the client's CRM.

The rule the client set: each month yields a sheet of the products and bundles
that are NOT in HubSpot and would therefore hold that month's orders, and no
month imports until those are approved. Crucially the sheets must not be
redundant -- a SKU approved in March must not reappear in April.

How the "unique per month" requirement is met without a second gate
implementation: `--report` runs the REAL `Engine.gate_unverified_items`
against the current catalog snapshot. A SKU approved last month is in the
snapshot, so it simply does not come back unverified. The non-redundancy is a
property of the data, not a filter someone has to maintain.

Sheet shape is deliberately identical to the one the client already signed off
(legacy_approval_2026-08-10.csv), plus a component_proposal column for
composite SKUs, so nothing about the review process has to be relearned.

    python3 zed_catalog.py --report 2023-10        # writes the CSV, exit 1 if pending
    python3 zed_catalog.py --apply  2023-10        # creates approved records
    python3 zed_catalog.py --report-all            # every month, oldest first
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

import backfill
from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging
from zed_snapshot import SnapshotHubSpot
import zed_normalize as zn

log = logging.getLogger("backfill")

NORM = Path("mirror/zed")
APPROVALS = Path("approvals")

HEADER = ["#", "proposed_sku", "original_zid_sku", "product_name", "type",
          "last_known_price_sar", "held_orders_released_est",
          "orders_sampled_as_evidence", "name_consistency",
          "component_proposal", "proposed_action", "approval (Yes/No)",
          "client_notes"]


def load_month(month):
    p = NORM / f"{month}.jsonl.gz"
    if not p.exists():
        raise SystemExit(f"no normalised data for {month} at {p}")
    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def single_token_universe():
    """Every SKU that appears on its own somewhere in the corpus.

    A composite is only proposed as a bundle when all of its tokens exist as
    standalone SKUs; otherwise the tokenisation is a guess and the client gets
    it as a plain product to decide.
    """
    singles = set()
    for p in sorted(NORM.glob("*.jsonl.gz")):
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                for it in json.loads(line).get("items", []):
                    kind, _ = zn.classify_sku(it.get("sku"))
                    if kind == "single":
                        singles.add(str(it["sku"]).strip().upper())
    return singles


def gather(month, hs, singles):
    """Run the real gate; aggregate what it holds, by SKU."""
    eng_cls = backfill.Engine
    stats = {"orders": 0, "held_orders": 0, "clean_orders": 0}
    by_sku = defaultdict(lambda: {"orders": 0, "names": Counter(),
                                  "prices": Counter()})

    class _Probe(eng_cls):
        """Only the gate is needed; nothing is written."""
        def __init__(self):
            pass

    probe = _Probe()
    probe.hs = hs
    probe.cfg = hs.cfg

    for order in load_month(month):
        stats["orders"] += 1
        unverified = eng_cls.gate_unverified_items(probe, order)
        if not unverified:
            stats["clean_orders"] += 1
            continue
        stats["held_orders"] += 1
        seen = set()
        for u in unverified:
            # match the held item back to its source item for sku + price
            for it in order.get("items", []):
                if str(it.get("id")) == str(u.get("id")):
                    sku = str(it.get("sku") or "").strip()
                    if not sku or sku in seen:
                        continue
                    seen.add(sku)
                    rec = by_sku[sku]
                    rec["orders"] += 1
                    nm = str(it.get("name") or "").strip()
                    if nm:
                        rec["names"][nm[:70]] += 1
                    pr = str(((it.get("amounts") or {})
                              .get("price_without_tax") or {}).get("amount") or "")
                    if pr:
                        rec["prices"][pr] += 1
                    break
    return stats, by_sku


def dominant(counter):
    if not counter:
        return "", 0.0
    total = sum(counter.values())
    top, n = counter.most_common(1)[0]
    return top, (n / total if total else 0.0)


def write_sheet(month, by_sku, singles, stats):
    APPROVALS.mkdir(parents=True, exist_ok=True)
    path = APPROVALS / f"{month}_catalog.csv"
    rows = sorted(by_sku.items(), key=lambda kv: -kv[1]["orders"])
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for i, (sku, rec) in enumerate(rows, 1):
            name, name_share = dominant(rec["names"])
            price, _ = dominant(rec["prices"])
            kind, toks = zn.classify_sku(sku)
            resolvable = kind == "composite" and all(
                t in singles for t in toks)
            if kind == "composite":
                kind_label = ("Set / bundle (components known)" if resolvable
                              else "Set / bundle (components unclear)")
                comp = " | ".join(f"{t} x1" for t in toks) if resolvable else \
                    "UNCLEAR: confirm the components"
            elif kind == "barcode":
                kind_label, comp = "Single product (barcode SKU)", ""
            elif kind == "malformed":
                kind_label, comp = "Needs your mapping (SKU not recognised)", ""
            else:
                kind_label, comp = "Single product", ""
            w.writerow([
                i, f"LGCY-{sku}", sku, name, kind_label, price,
                rec["orders"], sum(rec["names"].values()),
                round(name_share, 3), comp,
                "Create legacy record in HubSpot (approved, LGCY namespace, "
                "no Salla id) so these historical orders can sync",
                "", ""])
    return path, len(rows)


def report(month, hs, singles):
    stats, by_sku = gather(month, hs, singles)
    if not by_sku:
        log.info("%s: %d orders, ALL pass the catalog gate -- no approval "
                 "needed", month, stats["orders"])
        return 0, None, stats
    path, n = write_sheet(month, by_sku, singles, stats)
    blocked = sum(r["orders"] for r in by_sku.values())
    log.info("%s: %d orders, %d held by %d SKU(s) -> %s",
             month, stats["orders"], stats["held_orders"], n, path)
    return n, path, stats


def apply_approvals(month, hs, live):
    """Create the approved LGCY- records. Idempotent: existing SKUs skip."""
    path = APPROVALS / f"{month}_catalog.csv"
    if not path.exists():
        raise SystemExit(f"no sheet at {path}; run --report first")
    made = skipped = pending = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            ok = (row.get("approval (Yes/No)") or "").strip().lower()
            if ok not in ("yes", "y"):
                pending += 1
                continue
            hs_sku = row["proposed_sku"]
            if hs.gate_search_product_by_sku([row["original_zid_sku"], hs_sku]) > 0:
                skipped += 1
                continue
            props = {"name": row["product_name"] or hs_sku,
                     "hs_sku": hs_sku,
                     "catalog_approval_status": "approved",
                     "description": (
                         "Legacy catalog record for historical Zid orders. "
                         f"Original Zid SKU {row['original_zid_sku']}. "
                         "Carries no Salla product id, so it can never "
                         "collide with a product created in Salla later.")}
            if row.get("last_known_price_sar"):
                try:
                    props["price"] = str(float(row["last_known_price_sar"]))
                except ValueError:
                    pass
            if not live:
                log.info("DRY RUN would create %s (%s)", hs_sku,
                         row["product_name"][:40])
                made += 1
                continue
            hid = hs.create_product(props, f"zed legacy {hs_sku}")
            if hid:
                made += 1
                log.info("created %s -> %s", hs_sku, hid)
    log.info("%s: %d created, %d already existed, %d still awaiting approval",
             month, made, skipped, pending)
    return pending


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--report")
    ap.add_argument("--report-all", action="store_true")
    ap.add_argument("--apply")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="zed_catalog.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    if args.apply:
        hs = HubSpot(cfg, token, live=args.live)
        pending = apply_approvals(args.apply, hs, args.live)
        return 1 if pending else 0

    hs = SnapshotHubSpot(cfg, token, live=False)
    singles = single_token_universe()
    log.info("single-token SKU universe: %d", len(singles))

    months = ([args.report] if args.report else
              sorted(p.name.split(".")[0] for p in NORM.glob("*.jsonl.gz")))
    total_pending = 0
    for m in months:
        n, path, stats = report(m, hs, singles)
        total_pending += n
    if total_pending:
        log.warning("%d SKU(s) await approval across %d month(s). No month "
                    "may emit until its sheet comes back approved.",
                    total_pending, len(months))
        return 1
    log.info("every month passes the catalog gate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
