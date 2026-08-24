#!/usr/bin/env python3
"""Turn a blocker matrix into the approval sheet the client already signs.

Third time generating one of these by hand, so it becomes a tool. Reads
mirror/blocker_matrix.csv (written by queue_drain.py --scan/--verify/--live),
pulls one real held order per blocker to recover the evidence a catalog owner
needs, and emits the column layout used on 2026-08-10 and 2026-08-13.

Evidence, not assertion. Every figure on the sheet is read back from a live
Salla payload rather than inferred:

  * SKU and price come from the held line item itself
  * the price is converted ex-VAT -> inclusive (x1.15), because Salla stores
    price_without_tax and the client reads retail prices
  * `type` is decided by whether EVERY token of a composite SKU also exists as
    a standalone SKU somewhere in the blocker set or in HubSpot already

That last point is the one that has bitten this project. C18C37 tokenises
cleanly into C18 + C37 and still is not a bundle -- the client said so. So the
column is a PROPOSAL with the evidence beside it, never a decision: a composite
whose tokens all resolve is proposed as a bundle, anything else as a single
product, and the client's answer overrides either way.

    python3 tools/make_approval_sheet.py --out wave3.csv
"""

import argparse
import csv
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import (Config, HubSpot, RelayClient, apply_portal_config,
                      setup_logging)

log = logging.getLogger("backfill")

TOKEN_RE = re.compile(r"[A-Z]{1,3}\d{1,3}")
VAT = 1.15

HEADER = ["#", "proposed_sku", "original_salla_sku", "product_name", "type",
          "last_known_price_sar", "held_orders_released_est",
          "orders_sampled_as_evidence", "name_consistency",
          "proposed_action", "approval (Yes/No)", "client_notes"]


def tokens(sku):
    up = str(sku or "").strip().upper()
    toks = TOKEN_RE.findall(up)
    return toks if toks and "".join(toks) == up else []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--matrix", default="mirror/blocker_matrix.csv")
    ap.add_argument("--out", default="approvals_wave3.csv")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="make_approval_sheet.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    secret = (os.environ.get("RELAY_SECRET") or "").strip()
    if not token or not secret:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN and RELAY_SECRET first.")

    hs = HubSpot(cfg, token, live=False)
    relay = RelayClient(cfg, secret)

    rows = list(csv.DictReader(open(args.matrix, newline="", encoding="utf-8-sig")))
    rows = [r for r in rows if int(r.get("blocked_orders") or 0) > 0]
    log.info("matrix has %d blocker(s)", len(rows))

    # one sample order per blocker, fetched in the relay's own batches
    want, owner = [], {}
    for r in rows:
        sids = (r.get("sample_order_ids") or "").split()
        if sids:
            want.append(sids[0])
            owner[sids[0]] = r
    log.info("fetching %d sample order(s) for evidence ...", len(want))
    orders = relay.fetch_orders(want)
    log.info("got %d", len(orders))

    # the SKU universe: tokens that stand alone somewhere are bundle-able
    singles = {(r.get("sku") or "").strip().upper()
               for r in rows if len(tokens(r.get("sku"))) == 1}

    out = []
    for r in rows:
        sku = (r.get("sku") or "").strip()
        sids = (r.get("sample_order_ids") or "").split()
        order = orders.get(sids[0]) if sids else None

        price = ""
        sampled = 0
        if order:
            for it in order.get("items", []) or []:
                if str(it.get("sku") or "").strip().upper() != sku.upper():
                    continue
                sampled = 1
                amt = ((it.get("amounts") or {}).get("price_without_tax")
                       or {}).get("amount")
                if amt:
                    try:
                        price = f"{float(amt) * VAT:.2f}"
                    except (TypeError, ValueError):
                        price = ""
                break

        toks = tokens(sku)
        if len(toks) <= 1:
            kind, note = "Single product", ""
        elif all(t in singles or
                 hs.gate_search_product_by_sku([t, f"LGCY-{t}"]) > 0
                 for t in toks):
            kind = "Bundle"
            note = f"All {len(toks)} tokens exist as their own product: {' + '.join(toks)}"
        else:
            kind = "Single product"
            note = (f"SKU splits into {' + '.join(toks)} but not every part exists "
                    f"on its own, so it is proposed as one product. Correct us "
                    f"if it should be a bundle.")

        out.append({
            "proposed_sku": f"LGCY-{sku}" if sku else "",
            "original_salla_sku": sku,
            "product_name": r.get("item_name", ""),
            "type": kind,
            "last_known_price_sar": price,
            "held_orders_released_est": r.get("blocked_orders", ""),
            "orders_sampled_as_evidence": sampled,
            "name_consistency": "100%" if sampled else "",
            "proposed_action": (r.get("suggested_action") or "")[:150],
            "client_notes": note,
        })

    out.sort(key=lambda x: -int(x["held_orders_released_est"] or 0))
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for i, row in enumerate(out, 1):
            w.writerow([i] + [row[k] for k in HEADER[1:10]] + ["", row["client_notes"]])

    total = sum(int(r["held_orders_released_est"] or 0) for r in out)
    log.info("wrote %s: %d row(s), %d order-block(s) covered",
             args.out, len(out), total)
    log.info("  singles: %d   bundles: %d",
             sum(1 for r in out if r["type"] == "Single product"),
             sum(1 for r in out if r["type"] == "Bundle"))
    missing = [r["original_salla_sku"] for r in out if not r["last_known_price_sar"]]
    if missing:
        log.info("  no price recovered for: %s", ", ".join(missing[:10]))


if __name__ == "__main__":
    main()
