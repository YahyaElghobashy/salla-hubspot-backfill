#!/usr/bin/env python3
"""Retroactive pipeline-stage sweep for backfill-created orders (v1.3, plan step 3).

Re-stages every previously backfill-created order from its CURRENT Salla status:
  - engine-created orders: status read from local archive/*.json (zero Salla calls)
  - Make-created orders (May 1 - May 13 wave-1 + resweep): status via throttled
    relay light-list sweep (cached to mirror/retro_salla_status_cache.json)
  - salla_id -> hs_id map: local audit mirror join + one Sheets read of the
    audit tab (order-date window May 1 - Jun 4 keeps live v19 orders untouchable)
  - writes: HubSpot batch/update, 100 orders per call, throttled, idempotent
    (batch/read first, no-ops skipped), STOP-file aware, ledger + verification.

Modes:
    (default)      dry run: build the full plan, print histogram, write nothing
    --sample N     patch N random planned orders (type RUN), then re-read + verify
    --live         patch everything planned (type RUN), then verify
Run:
    set -a; source secrets.env; set +a
    ./venv/bin/python3 tools/retro_status.py [--sample 50 | --live]
"""
import argparse
import csv
import glob
import json
import os
import random
import socket
import ssl
import sys
import time
import urllib.request

socket.setdefaulttimeout(120)  # googleapiclient has none; a dead socket must not hang the sweep
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
from backfill import STATUS_STAGE_MAP, ORDER_PIPELINE_STAGE  # single source of truth

def _labels():
    """stage id -> friendly label, from config's status_stage_map (best effort)."""
    cfg = json.load(open("config.json"))
    out = {cfg.get("default_pipeline_stage", ""): "default"}
    for slug, stage in (cfg.get("status_stage_map") or {}).items():
        out.setdefault(stage, slug)
    return out

STAGE_LABELS = None  # populated in main()
BASE = "https://api.hubapi.com"
CTX = ssl.create_default_context()
CACHE = Path("mirror/retro_salla_status_cache.json")
LEDGER = Path("mirror/retro_status_report.csv")
WINDOW_LO = None   # set from --window-from
WINDOW_HI = None   # set from --window-to
SWEEP_DAYS = []    # set from --sweep-from/--sweep-to
HS_BATCH_GAP_S = 2.0
RELAY_GAP_S = 6.0


def http(method, url, body=None, headers=None, timeout=120):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                 method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return json.loads(r.read())


def hs(method, path, body=None):
    tok = os.environ["HUBSPOT_ACCESS_TOKEN"]
    for attempt in range(6):
        try:
            return http(method, BASE + path, body,
                        {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (attempt + 1))
                continue
            raise
    raise RuntimeError("HubSpot 429 retry budget exhausted")


def relay(path):
    cfg = json.load(open("config.json"))
    body = {"secret": os.environ["RELAY_SECRET"], "path": path}
    for attempt in range(4):
        try:
            p = http("POST", cfg["relay_url"], body, {"Content-Type": "application/json"}, timeout=180)
            if p.get("ok"):
                return p.get("data") or {}
        except Exception:
            pass
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"relay failed: {path[:80]}")


def stop_requested():
    return Path("STOP.retro").exists()


# ---------------------------------------------------------------------------
# 1. salla_id -> hs_id map
# ---------------------------------------------------------------------------

def id_map_from_mirror():
    rows = list(csv.reader(open("mirror/audit_mirror.csv")))
    hdr = rows[0]
    arrived, processed = {}, {}
    for r in rows[1:]:
        d = dict(zip(hdr, r))
        if d["sheet_row"] in ("", "-1"):
            continue
        if d["event"] == "arrived_append" and d["c0"]:
            arrived[d["sheet_row"]] = d["c0"]
        elif d["event"] == "processed_update" and d["c14"]:
            processed[d["sheet_row"]] = d["c14"]
    return {arrived[sr]: processed[sr] for sr in arrived if sr in processed}


def id_map_from_sheet(local_ids):
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    cfg = json.load(open("config.json"))
    creds = Credentials.from_authorized_user_file(
        "token.json", ["https://www.googleapis.com/auth/spreadsheets",
                       "https://www.googleapis.com/auth/drive"])
    sheets = build("sheets", "v4", credentials=creds).spreadsheets()
    resp = sheets.values().get(spreadsheetId=cfg["spreadsheet_id"],
                               range=f"'{cfg['audit_tab']}'!A:O").execute()
    out = {}
    for row in resp.get("values", []):
        if len(row) < 15:
            continue
        salla_id, order_date, hs_id = str(row[0]).strip(), str(row[2]).strip(), str(row[14]).strip()
        if not (salla_id.isdigit() and hs_id.isdigit()):
            continue
        if not (WINDOW_LO <= order_date[:10] < WINDOW_HI):
            continue  # outside backfill window: live v19 rows, never touch
        if salla_id in local_ids:
            continue
        out[salla_id] = hs_id
    return out


# ---------------------------------------------------------------------------
# 2. status source
# ---------------------------------------------------------------------------

def statuses_from_archive():
    out = {}
    for f in glob.glob("archive/*.json"):
        try:
            d = json.load(open(f))
            s = d.get("status", {}) or {}
            if d.get("id") is not None and s.get("slug"):
                out[str(d["id"])] = (str(s["slug"]).lower(), s.get("name", ""))
        except Exception:
            continue
    return out


def statuses_from_relay_sweep(wanted_ids):
    cached = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    if "__swept_all__" in cached:
        print("sweep already completed once; ids still absent are Salla-side "
              "deleted/hidden orders (left at their current stage)")
        cached.pop("__swept_all__")
        return {k: tuple(v) for k, v in cached.items()}
    missing = wanted_ids - set(cached)
    if not missing:
        return {k: tuple(v) for k, v in cached.items()}
    print(f"relay sweep: {len(missing)} Make-created orders need status "
          f"(throttled {RELAY_GAP_S}s/call, cache at {CACHE})")
    probe_checked = False
    for day in SWEEP_DAYS:
        if not (missing - set(cached)):
            break
        page = 1
        while True:
            if stop_requested():
                print("STOP.retro present: sweep paused (cache retained)")
                break
            path = (f"orders?from_date={day}T00:00:00&to_date={day}T23:59:59"
                    f"&per_page=100&format=light&page={page}")
            body = relay(path)
            data = body.get("data", []) or []
            if not probe_checked:
                probe_checked = True
                if data and not (data[0].get("status") or {}).get("slug"):
                    raise SystemExit("light list has no status field: rerun after "
                                     "switching sweep to expanded batch fetches")
            for o in data:
                oid = str(o.get("id"))
                s = o.get("status", {}) or {}
                if s.get("slug"):
                    cached[oid] = [str(s["slug"]).lower(), s.get("name", "")]
            CACHE.write_text(json.dumps(cached))
            total_pages = int((body.get("pagination", {}) or {}).get("totalPages", 1) or 1)
            got = len(missing - set(cached))
            print(f"  {day} page {page}/{total_pages} -> {len(data)} orders, {got} still missing")
            if page >= total_pages:
                break
            page += 1
            time.sleep(RELAY_GAP_S)
        time.sleep(RELAY_GAP_S)
    if not stop_requested():
        cached["__swept_all__"] = ["done", ""]
        CACHE.write_text(json.dumps(cached))
        cached.pop("__swept_all__")
    return {k: tuple(v) for k, v in cached.items()}


# ---------------------------------------------------------------------------
# 3. plan + write
# ---------------------------------------------------------------------------

def batch_read_stages(hs_ids):
    out = {}
    ids = list(hs_ids)
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        r = hs("POST", "/crm/v3/objects/orders/batch/read",
               {"properties": ["hs_pipeline_stage"],
                "inputs": [{"id": x} for x in chunk]})
        for res in r.get("results", []):
            out[str(res["id"])] = res["properties"].get("hs_pipeline_stage", "")
        time.sleep(HS_BATCH_GAP_S)
    return out


def batch_update(plan_rows):
    """plan_rows: list of dicts with hs_id, stage, name. Returns per-id result."""
    results = {}
    for i in range(0, len(plan_rows), 100):
        if stop_requested():
            print("STOP.retro present: halting between batches")
            break
        chunk = plan_rows[i:i + 100]
        body = {"inputs": [{"id": p["hs_id"],
                            "properties": {"hs_pipeline_stage": p["stage"],
                                           "hs_fulfillment_status": p["name"],
                                           "hs_external_order_status": p["name"]}}
                           for p in chunk]}
        try:
            r = hs("POST", "/crm/v3/objects/orders/batch/update", body)
            ok_ids = {str(res["id"]) for res in r.get("results", [])}
            for p in chunk:
                results[p["hs_id"]] = "ok" if p["hs_id"] in ok_ids else "missing-in-response"
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:200]
            for p in chunk:
                results[p["hs_id"]] = f"batch-error {e.code}: {detail}"
        print(f"  batch {i // 100 + 1}/{(len(plan_rows) + 99) // 100} done")
        time.sleep(HS_BATCH_GAP_S)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None, help="patch N random planned orders")
    ap.add_argument("--live", action="store_true", help="patch everything planned")
    ap.add_argument("--window-from", required=True, help="YYYY-MM-DD backfill window start")
    ap.add_argument("--window-to", required=True, help="YYYY-MM-DD backfill window end (exclusive)")
    ap.add_argument("--sweep-from", default=None, help="YYYY-MM-DD first day needing a relay status sweep")
    ap.add_argument("--sweep-to", default=None, help="YYYY-MM-DD last day (inclusive)")
    args = ap.parse_args()

    global WINDOW_LO, WINDOW_HI, SWEEP_DAYS, STAGE_LABELS
    WINDOW_LO, WINDOW_HI = args.window_from, args.window_to
    STAGE_LABELS = _labels()
    if args.sweep_from and args.sweep_to:
        from datetime import timedelta
        d0 = datetime.fromisoformat(args.sweep_from)
        d1 = datetime.fromisoformat(args.sweep_to)
        SWEEP_DAYS = [(d0 + timedelta(days=i)).strftime("%Y-%m-%d")
                      for i in range((d1 - d0).days + 1)]

    local_map = id_map_from_mirror()
    sheet_map = id_map_from_sheet(set(local_map))
    id_map = {**sheet_map, **local_map}
    print(f"id map: {len(local_map)} engine-created (mirror) + {len(sheet_map)} "
          f"Make-created (sheet) = {len(id_map)} orders in scope")

    st = statuses_from_archive()
    make_ids = {sid for sid in id_map if sid not in st}
    if make_ids:
        st.update(statuses_from_relay_sweep(make_ids))

    plan, unknown, no_status = [], Counter(), 0
    for sid, hid in id_map.items():
        tup = st.get(sid)
        if not tup:
            no_status += 1
            continue
        slug, name = tup
        stage = STATUS_STAGE_MAP.get(slug)
        if stage is None:
            unknown[slug] += 1
            continue  # unmapped slugs stay at their creation stage
        plan.append({"salla_id": sid, "hs_id": hid, "slug": slug,
                     "name": name, "stage": stage})

    hist = Counter(f"{p['slug']} -> {STAGE_LABELS.get(p['stage'], p['stage'])}" for p in plan)
    print("\nPLAN histogram:")
    for k, v in hist.most_common():
        print(f"  {k}: {v}")
    print(f"planned: {len(plan)} | unmapped slugs (left as-is): {dict(unknown) or 0} "
          f"| no status found: {no_status}")

    todo = plan
    if args.sample:
        todo = random.sample(plan, min(args.sample, len(plan)))
    elif not args.live:
        print("\nDRY RUN complete: nothing written. Use --sample 50 or --live.")
        return

    print(f"\nreading current stages for {len(todo)} orders (batch/read)...")
    current = batch_read_stages([p["hs_id"] for p in todo])
    todo2 = [p for p in todo if current.get(p["hs_id"]) != p["stage"]]
    print(f"already correct (skipped): {len(todo) - len(todo2)} | to patch: {len(todo2)}")
    if args.sample:
        for p in todo2[:60]:
            print(f"  {p['salla_id']} [{p['slug']}] "
                  f"{STAGE_LABELS.get(current.get(p['hs_id'], ''), current.get(p['hs_id'], '?'))}"
                  f" -> {STAGE_LABELS.get(p['stage'], p['stage'])}")
    if not todo2:
        print("nothing to do")
        return

    confirm = input(f"LIVE PATCH of {len(todo2)} orders in your HubSpot portal. Type RUN: ")
    if confirm.strip() != "RUN":
        sys.exit("aborted")

    results = batch_update(todo2)

    new_hdr = not LEDGER.exists()
    with open(LEDGER, "a", newline="") as f:
        w = csv.writer(f)
        if new_hdr:
            w.writerow(["ts", "salla_order_id", "hs_id", "slug", "from_stage", "to_stage", "result"])
        for p in todo2:
            w.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), p["salla_id"], p["hs_id"],
                        p["slug"], current.get(p["hs_id"], ""), p["stage"],
                        results.get(p["hs_id"], "not-attempted")])

    print("\nverifying (re-read)...")
    check = random.sample(todo2, min(100, len(todo2)))
    after = batch_read_stages([p["hs_id"] for p in check])
    okn = sum(1 for p in check if after.get(p["hs_id"]) == p["stage"])
    fails = [r for r in results.values() if r != "ok"]
    print(f"verification: {okn}/{len(check)} sampled orders at target stage")
    print(f"write results: {Counter(results.values()).most_common()}")
    print("RETRO SWEEP", "PASS" if okn == len(check) and not fails else "HAS FAILURES (see ledger)")


if __name__ == "__main__":
    main()
