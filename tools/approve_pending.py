#!/usr/bin/env python3
"""Approve a named list of catalogue records, with a rollback ledger.

Deliberately takes an explicit id list rather than a filter. "Approve
everything unapproved" is exactly the operation nobody should be able to run by
accident: approval is what lets an order write to the CRM, so the set has to be
one a human read.

Each id carries the reason it qualifies, and the reason is printed in the dry
run so the person confirming sees the argument, not just a count.

    python3 tools/approve_pending.py            # dry run
    python3 tools/approve_pending.py --live
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, HubSpot, apply_portal_config, now_str, setup_logging

log = logging.getLogger("backfill")
LEDGER = Path("approvals") / "approved_products.csv"

# id -> (label, why it qualifies)
APPROVE = {
    "422900763896": (
        "C41CH7",
        "Real Salla product (id 1209728408) with an ACTIVE bundle template "
        "carrying 2 components, and 117 line items already sold. It has been "
        "syncing through the template path all along; approving the product "
        "makes the catalogue consistent and gives it a fallback if that "
        "template is ever deactivated."),
    "421781197030": (
        "C18CH58c",
        "Real Salla product (id 770385112) with an ACTIVE bundle template "
        "carrying 3 components. Same reasoning as C41CH7."),
    "422828644595": (
        "C18CH11C45C46",
        "Real Salla product (id 1018965024). No template, no line items yet, "
        "so any future order containing it would hold on the product path "
        "alone."),
    "427428090094": (
        "C26CH5800",
        "Real Salla product (id 1235051226). No template, same reasoning."),
    # One half of a duplicate pair. Approving exactly one is deliberate: both
    # carry salla_product_id 2039567592, and the gate matches on that id, so a
    # single approved record releases the 138 held orders while leaving no
    # ambiguity about which record wins.
    "429785448673": (
        "Multi-head Hot Brush",
        "Real device blocking 138 queued orders. Its twin (429789091060) is "
        "left UNAPPROVED on purpose so exactly one record can satisfy the "
        "gate. The twin should be archived separately, which is a deletion "
        "and therefore a decision, not a cleanup."),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="approve_pending.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, token, live=args.live)

    ids = list(APPROVE)
    st, data = hs._req(
        "POST", "/crm/v3/objects/products/batch/read",
        body={"inputs": [{"id": i} for i in ids],
              "properties": ["hs_sku", "name", "catalog_approval_status",
                             "salla_product_id"]},
        what="approve read")
    if st != 200:
        sys.exit(f"batch read HTTP {st}")
    before = {str(r["id"]): (r.get("properties") or {})
              for r in data.get("results", [])}

    missing = [i for i in ids if i not in before]
    if missing:
        sys.exit(f"these ids do not exist, refusing to continue: {missing}")

    done = skipped = 0
    lf = lw = None
    if args.live:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        new = not LEDGER.exists()
        lf = open(LEDGER, "a", newline="", encoding="utf-8")
        lw = csv.writer(lf)
        if new:
            lw.writerow(["ts", "hubspot_id", "label", "was_status",
                         "now_status"])

    for hid, (label, why) in APPROVE.items():
        prev = before[hid].get("catalog_approval_status") or ""
        if prev == "approved":
            log.info("%s (%s) already approved, skipping", label, hid)
            skipped += 1
            continue
        if not args.live:
            log.info("DRY RUN approve %s (%s), currently %r", label, hid,
                     prev or None)
            log.info("        because: %s", why)
            done += 1
            continue
        st, _ = hs._req("PATCH", f"/crm/v3/objects/products/{hid}",
                        body={"properties":
                              {"catalog_approval_status": "approved"}},
                        what=f"approve {label}")
        if st in (200, 201):
            done += 1
            log.info("approved %s (%s), was %r", label, hid, prev or None)
            lw.writerow([now_str(), hid, label, prev, "approved"])
            lf.flush()
        else:
            log.error("PATCH %s failed HTTP %s", hid, st)
    if lf:
        lf.close()
    log.info("%s: %d approved, %d already were",
             "APPLIED" if args.live else "DRY RUN", done, skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
