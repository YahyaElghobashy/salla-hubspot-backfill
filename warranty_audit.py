#!/usr/bin/env python3
"""Warranty readiness: which SKUs actually sell, and which can be classified.

The warranty system classifies on the PRODUCT and HubSpot copies that onto
each new order line. So a product with no `product_class` produces no warranty,
and an order line whose SKU has no product record can never be classified at
all -- it has nothing to point at.

That makes the real question not "are the 80 products tagged" but "does every
SKU that has ever sold resolve to a product record we can tag". Those are very
different sets: the products were all created in 2026 during the sync build,
while the order lines reach back to 2020.

Three populations, unioned by canonical SKU:

  SOLD-HUBSPOT   SKUs on line items already in HubSpot
  SOLD-ZID       SKUs in the Zid corpus, about to be imported
  CATALOGUE      the product records that classification can be written to

A SKU in either SOLD set with no CATALOGUE match is a hole: orders exist, no
warranty can ever attach. Recency is reported separately because a device sold
last month matters more than one discontinued in 2021.

Usage:
    python3 warranty_audit.py --dump      # pull line items + orders (slow)
    python3 warranty_audit.py --report    # analyse what was dumped
"""

import argparse
import csv
import glob
import gzip
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from backfill import Config, HubSpot, apply_portal_config, setup_logging
from zed_snapshot import list_all
import zed_normalize as zn

log = logging.getLogger("backfill")

SNAP = Path("mirror/snapshot")
OUT = Path("approvals")
LI = SNAP / "line_items.jsonl"
ORD = SNAP / "orders_dates.json"
NORM = Path("mirror/zed")

RECENT_MONTHS = 24


def dump(hs):
    SNAP.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(LI, "w", encoding="utf-8") as f:
        for r in list_all(hs, "line_items",
                          ["hs_sku", "name", "quantity", "salla_order_id",
                           "product_class", "warranty_months", "createdate",
                           "hs_product_id"], label="line_items"):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    log.info("line items dumped: %d -> %s", n, LI)

    dates = {}
    for r in list_all(hs, "orders",
                      ["salla_order_id", "hs_external_created_date"],
                      label="orders"):
        p = r.get("properties") or {}
        sid = str(p.get("salla_order_id") or "")
        if sid:
            dates[sid] = str(p.get("hs_external_created_date") or "")
    ORD.write_text(json.dumps(dates))
    log.info("order dates: %d -> %s", len(dates), ORD)


def _month(v):
    if not v:
        return ""
    s = str(v)
    if s.isdigit():
        return datetime.fromtimestamp(int(s) / 1000,
                                      timezone.utc).strftime("%Y-%m")
    return s[:7]


def sheet_proposals(path=OUT / "warranty_input.csv"):
    """SKU -> the class the client already proposed on the reviewed sheet.

    The reviewed sheet only ever covered SKUs that were MISSING from HubSpot,
    so it answers the warranty question for the legacy Zid catalogue and says
    nothing about the products that were already there. Merging it in here is
    what separates "Clara still has to decide this" from "Clara already did".
    """
    out = {}
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            sku = zn.canon_sku(r.get("sku"))
            if sku:
                out[sku] = {
                    "product_class": (r.get("product_class") or "").strip(),
                    "devices": (r.get("device_line_items") or "").strip(),
                    "consumables": (r.get("consumable_line_items") or "").strip(),
                }
    return out


def n_devices(desc):
    """How many distinct devices the client says are inside.

    Matters because the warranty object is one record PER DEVICE. A bundle
    holding two devices must yield two warranties, so a bundle imported as a
    single flat line item under-counts the customer's cover.
    """
    d = (desc or "").strip()
    if not d or d == "-":
        return 0
    return len([x for x in d.split(" + ") if x.strip()])


def report():
    prods = json.loads((SNAP / "products_full.json").read_text())
    proposals = sheet_proposals()
    catalogue = {}
    for p in prods:
        pr = p["properties"]
        raw = str(pr.get("hs_sku") or "").strip()
        if not raw:
            continue
        key = zn.canon_sku(raw)
        # LGCY-C18 and C18 are the same catalogue entry as far as the gate is
        # concerned, so index both spellings
        for k in {key, key[5:] if key.startswith("LGCY-") else key}:
            catalogue[k] = {
                "id": p["id"], "hs_sku": raw, "name": pr.get("name"),
                "product_class": (pr.get("product_class") or "").strip(),
                "warranty_months": str(pr.get("warranty_months") or "").strip(),
                "salla_product_id": str(pr.get("salla_product_id") or ""),
            }

    dates = json.loads(ORD.read_text()) if ORD.exists() else {}
    sold = defaultdict(lambda: {"hs_lines": 0, "zid_orders": 0,
                                "months": set(), "name": ""})

    if LI.exists():
        with open(LI, encoding="utf-8") as f:
            for line in f:
                pr = (json.loads(line).get("properties") or {})
                sku = zn.canon_sku(pr.get("hs_sku"))
                if not sku:
                    continue
                bare = sku[5:] if sku.startswith("LGCY-") else sku
                rec = sold[bare]
                rec["hs_lines"] += 1
                rec["name"] = rec["name"] or (pr.get("name") or "")
                m = _month(dates.get(str(pr.get("salla_order_id") or "")))
                if m:
                    rec["months"].add(m)

    for fp in sorted(glob.glob(str(NORM / "*.jsonl.gz"))):
        mo = Path(fp).name[:7]
        with gzip.open(fp, "rt", encoding="utf-8") as f:
            for line in f:
                for it in json.loads(line).get("items", []):
                    sku = zn.canon_sku(it.get("sku"))
                    if not sku:
                        continue
                    rec = sold[sku]
                    rec["zid_orders"] += 1
                    rec["name"] = rec["name"] or (it.get("name") or "")
                    rec["months"].add(mo)

    cutoff = sorted({m for r in sold.values() for m in r["months"]})
    recent_from = cutoff[-RECENT_MONTHS] if len(cutoff) >= RECENT_MONTHS \
        else (cutoff[0] if cutoff else "")

    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "warranty_coverage.csv"
    stats = Counter()
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["sku", "product_name", "hs_line_items", "zid_orders",
                    "first_month", "last_month", "sold_recently",
                    "catalogue_record", "product_class", "warranty_months",
                    "client_proposed_class", "devices_inside",
                    "device_count", "status", "action_needed"])
        for sku, r in sorted(sold.items(),
                             key=lambda kv: -(kv[1]["hs_lines"]
                                              + kv[1]["zid_orders"])):
            cat = catalogue.get(sku)
            prop = proposals.get(sku, {})
            last = max(r["months"]) if r["months"] else ""
            recent = bool(last and last >= recent_from)
            nd = n_devices(prop.get("devices"))
            if not cat:
                status = "NO PRODUCT RECORD - cannot be classified"
            elif not cat["product_class"]:
                status = "product exists, class EMPTY"
            elif cat["product_class"] == "device" and not cat["warranty_months"]:
                status = "device with NO warranty_months"
            else:
                status = "ready"
            # what a human actually has to do about it
            if not cat:
                action = ("create the product record (the Zid import will) "
                          "then classify")
            elif prop.get("product_class"):
                action = f"apply proposed class '{prop['product_class']}'"
            else:
                action = "CLARA MUST CLASSIFY - no proposal from any source"
            if nd >= 2:
                action += f" | contains {nd} devices: needs components or it "\
                          f"yields 1 warranty not {nd}"
            stats[status] += 1
            if recent:
                stats[f"[recent] {status}"] += 1
            if "CLARA MUST CLASSIFY" in action:
                stats["needs a decision from Clara"] += 1
                if recent:
                    stats["[recent] needs a decision from Clara"] += 1
            if nd >= 2:
                stats["multi-device: needs component expansion"] += 1
            w.writerow([sku, (r["name"] or (cat or {}).get("name") or "")[:60],
                        r["hs_lines"], r["zid_orders"],
                        min(r["months"]) if r["months"] else "", last,
                        "yes" if recent else "no",
                        (cat or {}).get("hs_sku", ""),
                        (cat or {}).get("product_class", ""),
                        (cat or {}).get("warranty_months", ""),
                        prop.get("product_class", ""),
                        prop.get("devices", ""), nd or "",
                        status, action])

    log.info("SKUs that have ever sold: %d   (recency cutoff %s)",
             len(sold), recent_from)
    for k, v in sorted(stats.items()):
        log.info("  %-46s %d", k, v)
    log.info("-> %s", path)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--dump", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="warranty_audit.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    if args.dump:
        token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
        if not token:
            sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
        dump(HubSpot(cfg, token, live=False))
    if args.report:
        return report()
    return 0


if __name__ == "__main__":
    sys.exit(main())
