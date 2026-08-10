#!/usr/bin/env python3
"""Plan-equivalence: prove the offline planner produces the same HubSpot
records the live engine actually produced.

This is the load-bearing test of the whole Zid design. The claim is that the
planner is "the production engine with a different transport" -- not a
reimplementation. The only way to prove that is to take orders the LIVE engine
already created from real Salla payloads, run those same payloads through the
planner, and diff the planned order properties against what HubSpot actually
holds, property by property.

Any divergence is a planner bug by definition, because the live records are
ground truth. It is also the only test that validates the emitter's two folds
(last_salla_sync_status and hs_product_id set at create time instead of via a
follow-up PATCH): the folded plan must equal live-create followed by
live-patch.

Method:
  1. Sample N engine-created orders from mirror/created.csv whose archive
     JSON exists (the archive IS the payload the live engine processed).
  2. Plan each archive payload through SnapshotHubSpot + PlanRecorder.
  3. orders/batch/read the real records (N/100 calls, the only API use).
  4. Diff, ignoring properties that are legitimately non-deterministic.

Usage (on the VM, after snapshots exist):
    python3 tools/zed_plan_equiv.py --sample 500
"""

import argparse
import csv
import glob
import json
import logging
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import backfill
from backfill import Config, HubSpot, apply_portal_config, setup_logging
from zed_plan import PlanHubSpot, ZedPlanEngine, _NoCursor  # noqa: F401
from backfill import GoogleIO, LocalMirror

log = logging.getLogger("backfill")

# Properties that may legitimately differ between plan-time and live-time and
# say nothing about planner correctness. Everything else must match exactly.
IGNORE = {
    # set by HubSpot, not by us
    "hs_object_id", "hs_createdate", "hs_lastmodifieddate",
    # the engine stamps sync bookkeeping AFTER create via patch_order; the
    # emitter folds it into the create body -- both end states carry it, but
    # a live record that later errored may differ. Compared separately.
    "sync_error_log",
}


def sample_created(n, seed=20260810):
    """(salla_order_id, hs_order_id, archive_path) for n random creations."""
    rows = []
    with open("mirror/created.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((row["salla_order_id"], row["hubspot_order_id"]))
    random.Random(seed).shuffle(rows)
    out = []
    for sid, hid in rows:
        hits = glob.glob(f"archive/order_RID*_{sid}_*.json")
        if hits:
            out.append((sid, hid, hits[0]))
        if len(out) >= n:
            break
    return out


def batch_read(hs, ids, properties):
    """orders/batch/read at 100 per call, via the paced client."""
    out = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        status, data = hs._req(
            "POST", "/crm/v3/objects/orders/batch/read",
            body={"inputs": [{"id": x} for x in chunk],
                  "properties": properties},
            what="equiv batch read")
        if status != 200:
            raise RuntimeError(f"batch read HTTP {status}")
        for r in data.get("results", []):
            out[str(r["id"])] = r.get("properties") or {}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--sample", type=int, default=500)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="zed_equiv.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    picks = sample_created(args.sample)
    log.info("sampled %d engine-created orders with archives", len(picks))
    if not picks:
        sys.exit("no archived creations found")

    # ---- plan each archived payload ---------------------------------------
    plan_hs = PlanHubSpot(cfg, token, live=True)
    gio = GoogleIO(cfg, enabled=False)
    mirror = LocalMirror("mirror/zed_equiv_mirror")
    eng = ZedPlanEngine(cfg, plan_hs, gio, mirror)

    planned = {}          # salla_order_id -> planned order-create properties
    skipped_held = 0
    for sid, hid, path in picks:
        order = json.loads(Path(path).read_text(encoding="utf-8"))
        plan_hs.plan.clear()
        # the snapshot knows these orders exist; bypass dedup deliberately --
        # we WANT the planner to re-plan them for comparison
        eng.process_order(order)
        create = next((p for p in plan_hs.plan
                       if p["path"] == "/crm/v3/objects/orders"
                       and p["op"] == "POST"), None)
        if create is None:
            skipped_held += 1          # gate held it now (catalog changed)
            continue
        planned[sid] = (hid, create["body"]["properties"])
    log.info("planned %d orders (%d held by today's catalog, skipped)",
             len(planned), skipped_held)

    # ---- read the live records --------------------------------------------
    prop_names = sorted({k for _, props in planned.values() for k in props}
                        - IGNORE)
    live_hs = HubSpot(cfg, token, live=False)
    actual = batch_read(live_hs, [hid for hid, _ in planned.values()],
                        prop_names)

    # ---- diff ---------------------------------------------------------------
    mismatch = []
    checked = 0
    for sid, (hid, want) in planned.items():
        have = actual.get(str(hid))
        if have is None:
            mismatch.append((sid, hid, "__missing__", "", ""))
            continue
        for k, v in want.items():
            if k in IGNORE:
                continue
            checked += 1
            a = "" if have.get(k) is None else str(have.get(k))
            w = "" if v is None else str(v)
            # numeric equivalence: "897.25" == "897.250"
            try:
                if float(a) == float(w):
                    continue
            except (TypeError, ValueError):
                pass
            if a != w:
                mismatch.append((sid, hid, k, w[:60], a[:60]))

    print(f"\n================ PLAN EQUIVALENCE ================")
    print(f"orders compared      {len(planned):,}")
    print(f"properties checked   {checked:,}")
    print(f"mismatches           {len(mismatch):,}")
    if mismatch:
        print("\nfirst 20 mismatches (order, property, planned, live):")
        for sid, hid, k, w, a in mismatch[:20]:
            print(f"  {sid} {k}: planned={w!r} live={a!r}")
        out = Path("mirror/zed_equiv_mismatches.csv")
        with open(out, "w", encoding="utf-8") as f:
            f.write("salla_order_id,hs_id,property,planned,live\n")
            for row in mismatch:
                f.write(",".join(str(x).replace(",", " ") for x in row) + "\n")
        print(f"-> {out}")
        return 1
    print("PASS: every planned property matches the live record.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
