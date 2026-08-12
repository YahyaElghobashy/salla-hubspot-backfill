#!/usr/bin/env python3
"""Per-month catalog approval sheets: the gate that stands between the Zid
data and the client's CRM.

The rule the client set: each month yields a sheet of the products and bundles
that are NOT in HubSpot and would therefore hold that month's orders, and no
month imports until those are approved. Crucially the sheets must not be
redundant -- a SKU approved in March must not reappear in April.

"Not redundant" turns out to mean two different things, and only one of them
is free:

  * ACROSS APPROVAL CYCLES it costs nothing. `--report` runs the REAL
    `Engine.gate_unverified_items` against the current catalog snapshot, so a
    SKU approved in March is in the snapshot by April and simply does not come
    back unverified. No filter for anyone to maintain.
  * WITHIN ONE SWEEP it is not free. Generating all 69 sheets today, before
    any approval exists, the snapshot is identical for every month -- so C1,
    which holds orders in 2020 and again in 2023, lands on both sheets. That
    is exactly the redundancy the client asked us to avoid. `--report-all`
    therefore carries a `seen` set forward and assigns each SKU to the FIRST
    month it appears in.

Once a SKU is assigned to one month, its impact figure has to be corpus-wide:
approving C1 on the 2020-06 sheet releases every held order carrying C1 across
all six years, so a row claiming "releases 3" when the true figure is in the
thousands would get the decision badly wrong.

Sheet shape is the one the client already signed off
(legacy_approval_2026-08-10.csv), plus component_proposal for composites and
first_month/months_affected so a six-year figure is not mistaken for a
one-month one.

    python3 zed_catalog.py --report 2023-10        # one month, exit 1 if pending
    python3 zed_catalog.py --report-all            # every month + ALL_catalog.csv
    python3 zed_catalog.py --apply  2023-10        # creates approved records
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
          "first_month", "months_affected",
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


def sweep(months, hs, singles):
    """Chronological corpus scan. Returns (by_sku, per_month_stats).

    Months arrive sorted, so the first month a SKU is seen in is simply the
    month it is first inserted. Counters merge across months, which is what
    makes the impact figure corpus-wide.
    """
    by_sku, per_month = {}, {}
    for m in months:
        stats, month_by_sku = gather(m, hs, singles)
        per_month[m] = stats
        for sku, rec in month_by_sku.items():
            g = by_sku.get(sku)
            if g is None:
                g = by_sku[sku] = {"orders": 0, "names": Counter(),
                                   "prices": Counter(), "months": Counter(),
                                   "first_month": m}
            g["orders"] += rec["orders"]
            g["names"] += rec["names"]
            g["prices"] += rec["prices"]
            g["months"][m] += rec["orders"]
        log.info("  %s: %d orders, %d held, %d SKU(s) (%d new)", m,
                 stats["orders"], stats["held_orders"], len(month_by_sku),
                 sum(1 for s in month_by_sku if by_sku[s]["first_month"] == m))
    return by_sku, per_month


def write_sheet(label, by_sku, singles, stats):
    APPROVALS.mkdir(parents=True, exist_ok=True)
    path = APPROVALS / f"{label}_catalog.csv"
    rows = sorted(by_sku.items(), key=lambda kv: -kv[1]["orders"])
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for i, (sku, rec) in enumerate(rows, 1):
            name, name_share = dominant(rec["names"])
            price, _ = dominant(rec["prices"])
            # Zid stores ex-VAT prices, so the dominant value arrives as
            # 327.82608695652 (== 377.00 / 1.15). Eleven decimal places in a
            # column a human has to eyeball reads as a bug and invites the
            # client to query the number instead of the approval. Round for
            # display only; --apply re-reads this same column, and 2dp is
            # already finer than the currency.
            try:
                price = f"{float(price):.2f}"
            except (TypeError, ValueError):
                pass
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
            months = rec.get("months") or {}
            w.writerow([
                i, f"LGCY-{sku}", sku, name, kind_label, price,
                rec["orders"], rec.get("first_month", ""), len(months) or 1,
                sum(rec["names"].values()),
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


def verify_sheet(hs, months, singles, sheet=APPROVALS / "ALL_catalog.csv"):
    """Prove the sheet is SUFFICIENT: approve every row, re-run the real gate,
    and require that nothing is left held.

    Writing the sheet proves each row is needed. It does not prove the set is
    complete, and those are different claims. If one SKU is missing, the
    client approves 156 items, we import, and orders still hold -- discovered
    only after a second round-trip through their review.

    So: inject every sheet row into the snapshot as an approved product and
    re-run `gate_unverified_items` over all 974k orders. Residual holds are
    SKUs the sheet failed to ask for. Costs one more sweep (~6s) and turns
    "we listed what we found" into "approving this list unblocks everything".

    Injection mirrors what --apply creates: the engine's candidate list is
    [bare_sku, LGCY-bare_sku], so an entry under the bare key is what the gate
    will match once the record exists.
    """
    if not sheet.exists():
        raise SystemExit(f"no sheet at {sheet}; run --report-all first")
    rows = list(csv.DictReader(open(sheet, newline="", encoding="utf-8-sig")))
    for r in rows:
        sku = zn.canon_sku(r["original_zid_sku"])
        rec = {"id": f"SIMULATED-{sku}",
               "properties": {"hs_sku": f"LGCY-{sku}",
                              "catalog_approval_status": "approved",
                              "name": r["product_name"]}}
        hs.by_sku.setdefault(sku, []).append((True, rec))

    log.info("simulating approval of %d sheet rows, re-running the gate over "
             "every month", len(rows))
    by_sku, per_month = sweep(months, hs, singles)
    held = sum(s["held_orders"] for s in per_month.values())
    orders = sum(s["orders"] for s in per_month.values())
    if by_sku:
        log.error("SHEET INCOMPLETE: %d order(s) still held by %d SKU(s) the "
                  "sheet does not list", held, len(by_sku))
        for sku, rec in sorted(by_sku.items(),
                               key=lambda kv: -kv[1]["orders"])[:20]:
            log.error("  missing %-28s %7d orders", sku, rec["orders"])
        return 1
    log.info("SHEET COMPLETE: all %d orders pass the gate once these %d SKUs "
             "are approved; 0 left held", orders, len(rows))
    return 0


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
    ap.add_argument("--verify-sheet", action="store_true",
                    help="approve every sheet row in a simulation and prove "
                         "nothing is left held")
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

    if args.report:
        n, path, stats = report(args.report, hs, singles)
        if n:
            log.warning("%d SKU(s) await approval. %s may not emit until its "
                        "sheet comes back approved.", n, args.report)
            return 1
        return 0

    months = sorted(p.name.split(".")[0] for p in NORM.glob("*.jsonl.gz"))
    if args.verify_sheet:
        return verify_sheet(hs, months, singles)

    log.info("sweeping %d months, oldest first", len(months))
    by_sku, per_month = sweep(months, hs, singles)
    if not by_sku:
        log.info("every month passes the catalog gate")
        return 0

    # per-month sheets carry only the SKUs that FIRST appear in that month
    empty = 0
    for m in months:
        mine = {s: r for s, r in by_sku.items() if r["first_month"] == m}
        if not mine:
            empty += 1
            continue
        write_sheet(m, mine, singles, per_month[m])

    # the consolidated sheet is the one that actually goes to the client
    master, n = write_sheet("ALL", by_sku, singles, {})
    held_total = sum(s["held_orders"] for s in per_month.values())
    order_total = sum(s["orders"] for s in per_month.values())
    log.info("%d orders across %d months, %d held by %d distinct SKU(s)",
             order_total, len(months), held_total, n)
    log.info("%d month sheets written, %d months needed none",
             len(months) - empty, empty)
    log.info("consolidated sheet for the client -> %s", master)
    log.warning("no month may emit until its sheet comes back approved.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
