#!/usr/bin/env python3
"""Phase 1 collision report: which Zid orders are ALREADY in HubSpot as
Salla orders under a different id?

Why this gates everything: Zid relayed into Salla, and our engine synced
Salla into HubSpot from Nov 2025 onward. The same real-world order can
therefore exist twice -- once in the Zid export (Zid id) and once in HubSpot
(Salla id). Importing the overlap window without resolving this duplicates
up to four months of revenue, and at 974k scale that is not undoable.

Two tests, cheap first:

  1. DIRECT ID: does any Zid order id appear as a HubSpot salla_order_id?
     If the relay preserved ids this answers everything in seconds.
  2. FINGERPRINT: (normalised phone, order day, total to 2dp) built from the
     engine's own archive JSONs -- the exact Salla payloads the HubSpot
     orders were created from -- matched against the same triple on the Zid
     side. Day tolerance +/-1 covers timezone drift.

Output:
  mirror/zed_overlap.json          counts, coverage, per-month breakdown
  mirror/zed_relabel_worklist.csv  salla_order_id,hs_id,zid_order_id,match
     -- the point-2 (source = Zed) worklist, produced here because a
        fingerprint match IS the proof an order originated in Zid.

Read-only. No HubSpot calls: everything comes from local snapshots/archives.
"""

import argparse
import glob
import gzip
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import zed_normalize as zn

WINDOW_LO = "2025-10"      # a month early on purpose: coverage check
WINDOW_HI = "2026-03"


def day_shift(day, delta):
    return (datetime.strptime(day, "%Y-%m-%d")
            + timedelta(days=delta)).strftime("%Y-%m-%d")


def salla_fingerprints(archive_dir, orders_idx):
    """(phone, day, total) -> [salla_order_id] from the engine's archives,
    window-limited. Also returns how many of the window's HubSpot orders we
    could fingerprint at all (archive coverage, reported honestly)."""
    fps = defaultdict(list)
    seen = set()
    n_files = 0
    for fp in glob.glob(str(Path(archive_dir) / "order_*.json")):
        n_files += 1
        try:
            o = json.loads(Path(fp).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        day = str((o.get("date") or {}).get("date") or "")[:10]
        if not (WINDOW_LO <= day[:7] < WINDOW_HI):
            continue
        sid = str(o.get("id") or "")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        c = o.get("customer") or {}
        phone = zn.zed_phone_key(f"{c.get('mobile_code','')}{c.get('mobile','')}")
        try:
            total = round(float((o.get("amounts") or {})
                                .get("total", {}).get("amount") or 0), 2)
        except (TypeError, ValueError):
            total = 0.0
        if phone:
            fps[(phone, day, total)].append(sid)
    in_hubspot = sum(1 for sid in seen if sid in orders_idx)
    return fps, seen, in_hubspot, n_files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", default="archive")
    ap.add_argument("--months", default="mirror/zed")
    ap.add_argument("--orders-snapshot", default="mirror/snapshot/orders.json")
    args = ap.parse_args()

    orders_idx = json.loads(Path(args.orders_snapshot).read_text())
    print(f"HubSpot orders index: {len(orders_idx):,}")

    month_files = sorted(
        p for p in Path(args.months).glob("*.jsonl.gz")
        if WINDOW_LO <= p.name.split(".")[0] < WINDOW_HI)
    if not month_files:
        sys.exit("no overlap months found")
    print(f"Zid overlap months: {[p.name.split('.')[0] for p in month_files]}")

    # ---- test 1: direct id ------------------------------------------------
    direct = Counter()
    zed_orders = []
    for p in month_files:
        month = p.name.split(".")[0]
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                o = json.loads(line)
                zed_orders.append(o)
                if str(o["id"]) in orders_idx:
                    direct[month] += 1
    print(f"\nTEST 1 -- direct id match: {sum(direct.values()):,} "
          f"Zid ids present as HubSpot salla_order_id")
    for m, n in sorted(direct.items()):
        print(f"  {m}: {n:,}")

    # ---- test 2: fingerprint ---------------------------------------------
    fps, salla_seen, salla_in_hs, n_files = salla_fingerprints(
        args.archive, orders_idx)
    print(f"\narchive: {n_files:,} files, {len(salla_seen):,} unique Salla "
          f"orders in window, {salla_in_hs:,} of them in HubSpot "
          f"({len(fps):,} fingerprintable)")

    matched, ambiguous, unmatched = [], 0, 0
    per_month = defaultdict(lambda: [0, 0])
    for o in zed_orders:
        month = (o["date"]["date"] or "")[:7]
        phone = o.get("_zed", {}).get("phone_e164") or zn.zed_phone_key(
            f"{o['customer'].get('mobile_code','')}"
            f"{o['customer'].get('mobile','')}")
        day = (o["date"]["date"] or "")[:10]
        try:
            total = round(float(o["amounts"]["total"]["amount"] or 0), 2)
        except (TypeError, ValueError):
            total = 0.0
        hit = None
        if phone and day:
            for d in (day, day_shift(day, 1), day_shift(day, -1)):
                cands = fps.get((phone, d, total))
                if cands:
                    if len(cands) == 1:
                        hit = cands[0]
                    else:
                        ambiguous += 1
                    break
        if hit:
            matched.append((hit, orders_idx.get(hit, ""), str(o["id"])))
            per_month[month][0] += 1
        else:
            unmatched += 1
            per_month[month][1] += 1

    print(f"\nTEST 2 -- fingerprint (phone, day+/-1, total):")
    print(f"  matched    {len(matched):,}")
    print(f"  ambiguous  {ambiguous:,} (same fingerprint, multiple Salla orders)")
    print(f"  unmatched  {unmatched:,}")
    for m in sorted(per_month):
        hit, miss = per_month[m]
        t = hit + miss
        print(f"  {m}: {hit:,}/{t:,} matched ({100.0*hit/t if t else 0:.1f}%)")

    out = Path("mirror/zed_overlap.json")
    out.write_text(json.dumps({
        "direct_id_matches": sum(direct.values()),
        "fingerprint_matched": len(matched),
        "ambiguous": ambiguous,
        "unmatched": unmatched,
        "archive_window_orders": len(salla_seen),
        "archive_window_in_hubspot": salla_in_hs,
        "per_month": {m: {"matched": v[0], "unmatched": v[1]}
                      for m, v in per_month.items()},
    }, indent=1))

    wl = Path("mirror/zed_relabel_worklist.csv")
    with open(wl, "w", encoding="utf-8") as f:
        f.write("salla_order_id,hubspot_id,zid_order_id,match\n")
        for sid, hid, zid in matched:
            f.write(f"{sid},{hid},{zid},fingerprint\n")
    print(f"\n-> {out}\n-> {wl}  ({len(matched):,} relabel rows)")


if __name__ == "__main__":
    main()
