#!/usr/bin/env python3
"""Create the Zid-corpus orders that never reached HubSpot.

The one-time inventory (tools/zed_gap_ids.py) diffed every HubSpot Zid
record against the normalised corpus and found exactly 58 orders missing,
zero extras -- the residue of the import's 2026-08-28 outage window that
its lost-response recovery could not see. This creates precisely those 58,
through the engine's own create path (create_order + process_item), so
properties, line items, the catalog gate and associations behave exactly
as an import create would. The import's status fold is applied the same
way (last_salla_sync_status).

Run from the VM app dir:
    venv/bin/python3 tools/zed_create_missing.py            # dry
    echo RUN | venv/bin/python3 tools/zed_create_missing.py --live

Ledger mirror/zed_missing_created.csv makes re-runs skip completed ids.
"""
import argparse
import csv
import glob
import gzip
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, "/home/integrations_claragroup_com/app")
os.chdir("/home/integrations_claragroup_com/app")
import backfill
from backfill import (Config, GoogleIO, HubSpot, LocalMirror, RelayClient,
                      apply_portal_config, dig, now_str)

LEDGER = Path("mirror/zed_missing_created.csv")
MISSING_FILE = "/tmp/zed_missing_ids.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    missing = set(json.load(open(MISSING_FILE))["missing"])
    done = set()
    if LEDGER.exists():
        with open(LEDGER, newline="") as f:
            done = {r["salla_order_id"] for r in csv.DictReader(f)}
    todo = missing - done
    print(f"{len(missing)} missing, {len(done)} already created, {len(todo)} to do")
    if not todo:
        return

    payloads = {}
    for p in sorted(glob.glob("mirror/zed/20*.jsonl.gz")):
        with gzip.open(p, "rt") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except ValueError:
                    continue
                if str(o.get("id")) in todo:
                    payloads[str(o.get("id"))] = o
    print(f"extracted {len(payloads)} payloads from the corpus")

    if args.live:
        if input(f"LIVE: create {len(payloads)} Zid orders in HubSpot. "
                 f"Type RUN: ").strip() != "RUN":
            sys.exit("Aborted.")

    cfg = Config.load("config.live.json")
    apply_portal_config(cfg)
    hs = HubSpot(cfg, os.environ["HUBSPOT_ACCESS_TOKEN"], live=args.live)
    relay = RelayClient(cfg, os.environ.get("RELAY_SECRET", "x"))

    class _Cur:
        data = {"status": "zed-missing"}
        status = "zed-missing"

    eng = backfill.Engine(cfg, _Cur(), relay, hs, GoogleIO(cfg, enabled=False),
                          LocalMirror("mirror"), live=args.live, workers=1)
    eng.is_live_sync = False
    eng.health = None

    ok = failed = held = 0
    for sid, o in sorted(payloads.items()):
        eng._order_signals = set()
        unverified = eng.gate_unverified_items(o)
        if unverified:
            held += 1
            names = [u.get("name", "?") for u in unverified][:2]
            print(f"HELD {sid}: {names}")
            continue
        try:
            cid = eng.hs.search_contact_retry(o)
        except Exception:
            cid = None
        hs_id, fresh = hs.create_order(o, cid, cfg.salla_timezone_default)
        if not hs_id:
            failed += 1
            print(f"FAILED create {sid}")
            continue
        li = 0
        for item in o.get("items") or []:
            if str(item.get("product_type", "")).lower() == "group_products":
                continue
            try:
                eng.process_item(o, hs_id, item)
                li += 1
            except Exception as e:
                print(f"  item error on {sid}: {e}")
        slug = str(dig(o, "status.slug") or "").lower()
        if slug and args.live:
            hs.update_order(hs_id, {"last_salla_sync_status": slug},
                            f"zed status fold {sid}")
        if args.live:   # the ledger records reality, never rehearsals
            new = not LEDGER.exists()
            with open(LEDGER, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["ts", "salla_order_id", "hs_order_id",
                                "line_items", "fresh"])
                w.writerow([now_str(), sid, hs_id, li, fresh])
        ok += 1
        print(f"CREATED {sid} -> {hs_id} ({li} line items)")
    print(f"done: created={ok} failed={failed} held={held}")


if __name__ == "__main__":
    main()
