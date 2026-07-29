#!/usr/bin/env python3
"""Re-gate every HELD order against the CURRENT catalog and relaunch the ones that
now pass (i.e. their bundle template was activated / product approved since).

Held rows are deliberately never auto-reclaimed by the live engine (a held order
must not be retried every poll), and the hourly sweep skips ids already in the
queue -- so after the catalog team approves a bundle/product, its held orders need
an explicit nudge. This tool is that nudge, and it is selective: an order is only
requeued when the gate actually passes now, so nothing bounces straight back to
held and no relay/HubSpot budget is wasted.

Safe by construction:
  * dry run by default; --apply writes
  * only orders whose gate now PASSES are requeued
  * orders already created (Live Queue 'done') are skipped -- no duplicates
  * gate results cached per product id (159 orders share few products)
  * requeue = append a fresh 'queued' row (the engine's ledger + dedup +
    duplicate-400 guardrails make reprocessing idempotent)

Usage:
  python3 tools/relaunch_held_orders.py                 # DRY RUN: what would relaunch
  python3 tools/relaunch_held_orders.py --apply         # requeue the unblocked ones
  python3 tools/relaunch_held_orders.py --limit 20 --apply
"""
import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backfill import Config, GoogleIO, HubSpot, RelayClient, dig, now_str


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="only consider the first N held orders")
    ap.add_argument("--batch", type=int, default=12, help="relay fetch batch size")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    gio = GoogleIO(cfg, enabled=True)
    relay = RelayClient(cfg, os.environ["RELAY_SECRET"])
    hs = HubSpot(cfg, os.environ["HUBSPOT_ACCESS_TOKEN"], live=False)

    rows = gio.queue_read(cfg.queue_spreadsheet_id)
    done_ids = {r["order_id"] for r in rows if r["status"] == "done"}
    ref_of = {}
    held = []
    for r in rows:
        if r["status"] == "held" and r["order_id"] and r["order_id"] not in done_ids:
            if r["order_id"] not in ref_of:
                ref_of[r["order_id"]] = r["reference_id"]
                held.append(r["order_id"])
    if args.limit:
        held = held[:args.limit]
    print(f"held orders still uncreated: {len(held)}")
    if not held:
        return

    # ---- gate cache: product id -> (passes, reason) ----
    cache = {}

    def gate(pid):
        pid = str(pid)
        if pid in cache:
            return cache[pid]
        p = hs.gate_search_product_approved(pid)
        te = hs.gate_search_template(pid, True)
        ta = hs.gate_search_template(pid, False)
        if (p == 0 and te == 0 and ta == 0):
            res = (False, "unknown/unapproved product")
        elif (te == 0 and ta > 0):
            res = (False, "bundle template not active")
        else:
            res = (True, "ok")
        cache[pid] = res
        return res

    now_ok, still, missing = [], [], []
    blockers = defaultdict(lambda: {"orders": set(), "reason": "", "name": ""})

    for i in range(0, len(held), args.batch):
        chunk = held[i:i + args.batch]
        try:
            orders = relay.fetch_orders(chunk)
        except Exception as e:
            print(f"  relay batch failed ({e}); skipping this batch")
            continue
        for oid in chunk:
            o = orders.get(oid)
            if not o:
                missing.append(oid)
                continue
            bad = []
            for it in (o.get("items") or []):
                if str(it.get("product_type", "")) == "group_products":
                    continue
                pid = str(dig(it, "product.id"))
                ok, why = gate(pid)
                if not ok:
                    bad.append((pid, why, it.get("name", "")))
            if bad:
                still.append(oid)
                for pid, why, nm in bad:
                    b = blockers[pid]
                    b["orders"].add(oid); b["reason"] = why; b["name"] = nm[:44]
            else:
                now_ok.append(oid)
        print(f"  checked {min(i+args.batch, len(held))}/{len(held)}  "
              f"(unblocked={len(now_ok)} still_held={len(still)})")

    print(f"\n=== RESULT ===")
    print(f"  NOW UNBLOCKED (will relaunch): {len(now_ok)}")
    print(f"  still blocked               : {len(still)}")
    if missing:
        print(f"  relay returned nothing for  : {len(missing)} {missing[:5]}")

    if blockers:
        print(f"\n=== remaining blockers (fix these to free more orders) ===")
        for pid, b in sorted(blockers.items(), key=lambda kv: -len(kv[1]["orders"])):
            print(f"  product {pid:<12} | {b['reason']:<28} | {len(b['orders']):>3} order(s) | {b['name']!r}")

    if not args.apply:
        print(f"\nDRY RUN. Re-run with --apply to requeue the {len(now_ok)} unblocked order(s).")
        return
    if not now_ok:
        print("\nnothing to relaunch.")
        return

    append = [[now_str(), oid, ref_of.get(oid, ""), "relaunch", "queued", 0, "relaunch",
               "catalog approved -- requeued by relaunch_held_orders"] for oid in now_ok]
    for k in range(0, len(append), 500):
        gio.queue_append_rows(cfg.queue_spreadsheet_id, append[k:k + 500])
    print(f"\nAPPLIED: requeued {len(append)} order(s). The live engine will pick them up "
          f"within a poll cycle; watch live.log or the Live Queue sheet.")


if __name__ == "__main__":
    main()
