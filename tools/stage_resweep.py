#!/usr/bin/env python3
"""Stage re-sweep: reconcile stale pipeline stages against Salla's CURRENT state.

Why this exists: an order's stage is written at creation and then only moved
by the status relay, which applies EVENTS. Events only exist from the moment
the thin status capture went live (Sept 2026), so any order that closed
before then -- the June/July populations created mid-outage or by the drain
-- sits frozen at whatever stage creation baked in. The audit measured 67% of
June orders and ~18% of July orders stuck in non-terminal stages their Salla
originals left months ago.

What one run does:

  1. HubSpot search: Salla-store orders in NON-TERMINAL stages (the config's
     default/in_progress, under_review, delivering, shipped), created before
     --before (default 2026-08-15; younger orders belong to the live status
     relay and are left alone). Each stage bucket is paged separately to stay
     under the search API's 10k-per-query ceiling.
  2. Hydrate each order's CURRENT payload from Salla through the relay
     (batched, paced by the engine's own adaptive limiter).
  3. Where map(current slug) differs from the stored stage: ONE batch PATCH
     per 100 orders setting hs_pipeline_stage, and refreshing the status text
     properties (hs_fulfillment_status, hs_external_order_status) from the
     fresh payload so the text stops lying too.

Safety:
  - Dry by default; --live asks for RUN (pipe `echo RUN |` for supervised
    non-interactive runs).
  - Idempotent and resumable: mirror/stage_resweep.csv records every patched
    order; re-runs skip ledgered ids, and a patched order leaves the search
    result set by its own stage anyway.
  - A degraded relay payload (id mismatch / no status block) decides nothing.
  - STOP.resweep halts between batches; nothing needs unwinding.
  - Orders missing from the relay response are reported, never patched.

Run (from the working dir that owns mirror/, e.g. the VM app dir):
    set -a; . ./.env; set +a
    venv/bin/python3 tools/stage_resweep.py                # dry: plan + histogram
    echo RUN | venv/bin/python3 tools/stage_resweep.py --live
"""

import argparse
import csv
import json
import os
import socket
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import backfill
from backfill import Config, HubSpot, RelayClient, RelayError, dig, now_str

STOP_FILE = Path("STOP.resweep")
LEDGER = Path("mirror/stage_resweep.csv")

TERMINAL_KEYS = ("delivered", "completed", "canceled", "deleted", "restored")


def ms(day):
    return str(int(datetime.fromisoformat(day + "T00:00:00+03:00").timestamp() * 1000))


def load_ledger():
    done = set()
    if LEDGER.exists():
        with open(LEDGER, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(str(row["salla_order_id"]))
    return done


def append_ledger(rows):
    new = not LEDGER.exists()
    LEDGER.parent.mkdir(exist_ok=True)
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "salla_order_id", "hs_order_id",
                        "old_stage", "new_stage", "slug"])
        for r in rows:
            w.writerow(r)


def collect(hs, cfg, before_ms):
    """All non-terminal Salla orders older than the cutoff, one stage bucket
    at a time (each bucket stays under the 10k search ceiling)."""
    stage_map = {str(k).lower(): v for k, v in (cfg.status_stage_map or {}).items()}
    terminal = {stage_map[k] for k in TERMINAL_KEYS if k in stage_map}
    non_terminal = sorted(({v for v in stage_map.values()}
                           | {cfg.default_pipeline_stage}) - terminal - {""})
    out = {}
    for stage in non_terminal:
        _collect_bucket(hs, stage, None, before_ms, out)
        print(f"  stage {stage}: cumulative {len(out)}")
    return out


def _collect_bucket(hs, stage, after_ms, before_ms, out):
    """Page one (stage, date-window) bucket into `out`. The search API stops
    paging at 10k results per filter set; a bucket that reports >=10k on its
    first page is split in half by created-date and each half collected
    recursively, so no straggler can hide past the ceiling."""
    filters = [
        {"propertyName": "salla_store", "operator": "EQ", "value": "Salla"},
        {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": stage},
        {"propertyName": "hs_external_created_date", "operator": "LT",
         "value": before_ms},
    ]
    if after_ms:
        filters.append({"propertyName": "hs_external_created_date",
                        "operator": "GTE", "value": after_ms})
    after = None
    while True:
        body = {"filterGroups": [{"filters": filters}],
                "properties": ["salla_order_id", "hs_pipeline_stage"],
                "sorts": [{"propertyName": "hs_object_id",
                           "direction": "ASCENDING"}],
                "limit": 100}
        if after:
            body["after"] = after
        data = hs.search("/crm/v3/objects/orders/search", body, "resweep collect")
        if after is None and int(data.get("total") or 0) >= 10000:
            lo = int(after_ms or ms("2026-02-01"))
            hi = int(before_ms)
            mid = str((lo + hi) // 2)
            _collect_bucket(hs, stage, str(lo), mid, out)
            _collect_bucket(hs, stage, mid, str(hi), out)
            return
        for r in data.get("results") or []:
            sid = (r.get("properties") or {}).get("salla_order_id")
            if sid:
                out[str(sid)] = {"hs_id": r["id"], "stage": stage}
        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after:
            return


def plan_patch(payload, current_stage, stage_map, default_stage):
    """The one decision both the CLI and the weekly reconciler share: given a
    FRESH Salla payload and the stored stage, return the property dict to
    PATCH, or None when the stage is already right. Includes the status text
    refresh (nothing else ever updates it after creation)."""
    slug = str(dig(payload, "status.slug")).lower()
    want = stage_map.get(slug, default_stage)
    if want == current_stage:
        return None
    props = {"hs_pipeline_stage": want}
    name = str(dig(payload, "status.name") or "").strip()
    if name:
        props["hs_fulfillment_status"] = name
        props["hs_external_order_status"] = name
    return props


def flush_batch(hs, batch, ledger_live):
    """One batch/update for up to 100 planned patches; ledger on success when
    ledger_live. `batch` rows: (sid, hs_id, old_stage, new_stage, slug, props).
    Returns (ok_count, fail_count)."""
    if not batch:
        return 0, 0
    inputs = [{"id": h, "properties": p} for _, h, _, _, _, p in batch]
    status, resp = hs._write("POST", "/crm/v3/objects/orders/batch/update",
                             {"inputs": inputs}, f"resweep batch x{len(inputs)}")
    if status in (200, 201):
        if ledger_live:
            append_ledger([(now_str(), s, h, o, n, sl)
                           for s, h, o, n, sl, _ in batch])
        return len(batch), 0
    print(f"BATCH FAILED {status}: {json.dumps(resp)[:200]}")
    return 0, len(batch)


def main():
    ap = argparse.ArgumentParser(description="Retroactive stage re-sweep")
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--before", default="2026-08-15",
                    help="only orders created before this day (YYYY-MM-DD)")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--max-orders", type=int, default=None)
    args = ap.parse_args()

    socket.setdefaulttimeout(180)
    cfg = Config.load(args.config)
    backfill.apply_portal_config(cfg)
    stage_map = backfill.STATUS_STAGE_MAP

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN and RELAY_SECRET first.")
    if args.live:
        if input("LIVE stage re-sweep will PATCH HubSpot orders. Type RUN: ").strip() != "RUN":
            sys.exit("Aborted.")

    hs = HubSpot(cfg, token, live=args.live)
    relay = RelayClient(cfg, secret)
    done = load_ledger()

    print("collecting non-terminal orders ...")
    work = collect(hs, cfg, ms(args.before))
    todo = [sid for sid in work if sid not in done]
    if args.max_orders:
        todo = todo[:args.max_orders]
    print(f"candidates {len(work)}, after ledger skip {len(todo)}")

    stats = Counter()
    plan_hist = Counter()
    batch_props = []   # (sid, hs_id, old, new, slug, props)
    missing = []

    def flush():
        ok, failed = flush_batch(hs, batch_props, ledger_live=args.live)
        stats["patched"] += ok
        stats["batch_failed"] += failed
        batch_props.clear()

    for i in range(0, len(todo), cfg.relay_batch_size):
        if STOP_FILE.exists():
            print("STOP.resweep present -- halting cleanly")
            break
        chunk = todo[i:i + cfg.relay_batch_size]
        try:
            payloads = relay.fetch_orders(chunk)
        except RelayError as e:
            print(f"relay unavailable, halting: {e}")
            break
        for sid in chunk:
            row = work[sid]
            p = payloads.get(sid)
            if not (isinstance(p, dict) and str(p.get("id")) == sid
                    and isinstance(p.get("status"), dict)):
                missing.append(sid)
                stats["unfetchable"] += 1
                continue
            plan_hist[str(dig(p, "status.slug")).lower()] += 1
            props = plan_patch(p, row["stage"], stage_map,
                               backfill.ORDER_PIPELINE_STAGE)
            if props is None:
                stats["already_right"] += 1
                continue
            batch_props.append((sid, row["hs_id"], row["stage"],
                                props["hs_pipeline_stage"],
                                str(dig(p, "status.slug")).lower(), props))
            if len(batch_props) >= 100:
                flush()
        if (i // cfg.relay_batch_size) % 25 == 0:
            print(f"  {i+len(chunk)}/{len(todo)} hydrated; "
                  f"planned {stats.get('patched',0)+len(batch_props)} patches")
    flush()

    print("\n== current Salla status of the swept orders ==")
    for slug, n in plan_hist.most_common():
        print(f"  {slug:<16} {n}")
    print("\n== outcome ==")
    for k, v in stats.items():
        print(f"  {k:<16} {v}")
    if missing:
        print(f"  first unfetchable ids: {missing[:10]}")
    if not args.live:
        print("\nDRY RUN: nothing was written. Re-run with --live to apply.")


if __name__ == "__main__":
    main()
