#!/usr/bin/env python3
"""Give orders that were created late (released from a catalog hold) their
REAL delivery date, before the warranty sweep runs (v2.14.1).

HubSpot flow 4717426891 ("Set Delivery Date when Order stage becomes
Delivered") stamps delivery_date with the day the order ENTERS Delivered in
HubSpot. An order held at the catalog gate for a week and released already
delivered therefore gets the release day, not the day the customer received
it; one released already Completed gets nothing. The warranty engine starts
each warranty on delivery_date (warranty_sweep dates an undated order from
its HubSpot stage history, which for a released order is the release day
too), so both cases would start warranties on the wrong day.

The real day comes from the Status Queue: the webhook capture writes every
Salla status change there as "status:<slug>@<Salla time>", even while the
order is held. The first "delivered" event wins; an order that went straight
to "completed" uses its first "completed" event. An order with neither is
left alone and counted. Salla times are Riyadh time, so the date part is the
Riyadh day. The Status Queue is trimmed (status_trim_days), so an order
placed before its oldest row may have lost its "delivered" row while a later
"completed" row survived: such an order is never dated from "completed", it
is listed for a Salla order-history check instead.

Only orders whose current delivery_date differs from the real day are
written. Old values are ledgered in mirror/delivery_date_fixes.csv BEFORE the
batch that changes them, so the job can be reversed from the ledger and a
re-run touches only what is still wrong. No HubSpot workflow enrols on
delivery_date (checked 2026-10-12: only 4717426891 writes it and the warranty
flow reads it; both enrol on the stage), so the write sends nothing to
customers.

    python3 tools/fix_delivery_dates.py --config config.live.json \
        --created-since "2026-10-12 11:00" --ordered-since 2026-10-05          # dry run
    ... --apply
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backfill import Config, GoogleIO, HubSpot, apply_portal_config, now_str

LEDGER = Path("mirror/delivery_date_fixes.csv")
STOP_FILE = Path("STOP.datefix")
RIYADH = timezone(timedelta(hours=3))
EVENT = re.compile(r"status:([a-z_]+)@(\d{4}-\d\d-\d\d) \d\d:\d\d:\d\d")


def ms(day_time):
    fmt = "%Y-%m-%d %H:%M" if " " in day_time else "%Y-%m-%d"
    return str(int(datetime.strptime(day_time, fmt).replace(tzinfo=RIYADH).timestamp() * 1000))


def collect(hs, cfg, created_since, ordered_since):
    """Released Salla orders now in Delivered or Completed."""
    stage_map = cfg.status_stage_map or {}
    out = []
    for stage in (stage_map.get("delivered"), stage_map.get("completed")):
        if not stage:
            continue
        after = None
        while True:
            body = {"filterGroups": [{"filters": [
                {"propertyName": "salla_store", "operator": "EQ", "value": "Salla"},
                {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": stage},
                {"propertyName": "hs_external_created_date", "operator": "GTE", "value": ms(ordered_since)},
                {"propertyName": "hs_createdate", "operator": "GTE", "value": ms(created_since)}]}],
                "properties": ["salla_order_id", "delivery_date", "hs_pipeline_stage",
                               "hs_external_created_date"],
                "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
                "limit": 200}
            if after:
                body["after"] = after
            data = hs.search("/crm/v3/objects/orders/search", body, "datefix collect")
            if after is None and int(data.get("total") or 0) >= 10000:
                sys.exit(f"{data['total']} orders match one stage: over the 10k search "
                         "ceiling, narrow --created-since")
            out += data.get("results") or []
            after = (data.get("paging") or {}).get("next", {}).get("after")
            if not after:
                break
    return out


def real_days(gio, cfg, wanted, ordered=None):
    """{salla order id: YYYY-MM-DD} from the Status Queue events. ordered
    ({salla order id: YYYY-MM-DD order date}) enables the trim guard: an
    order placed before the queue's oldest row gets no "completed" fallback."""
    vals = gio._gexec(gio.sheets.values().get(
        spreadsheetId=cfg.queue_spreadsheet_id, range="'Status Queue'!A:D",
        valueRenderOption="UNFORMATTED_VALUE"), "datefix status queue", gio.sheets_rl).get("values") or []
    floor = min((str(r[0])[:10] for r in vals[1:]
                 if r and re.match(r"\d{4}-\d\d-\d\d", str(r[0]))), default="")
    first = {}
    for r in vals[1:]:
        if len(r) < 4:
            continue
        oid = re.sub(r"\.0$", "", str(r[1]).strip())
        m = EVENT.match(str(r[3]).strip())
        if oid not in wanted or not m or m.group(1) not in ("delivered", "completed"):
            continue
        key = (oid, m.group(1))
        if key not in first or m.group(2) < first[key]:
            first[key] = m.group(2)
    out = {}
    for o in wanted:
        day = first.get((o, "delivered"))
        if not day and first.get((o, "completed")):
            placed = (ordered or {}).get(o, "")
            if ordered is None or (placed and floor and placed > floor):
                day = first[(o, "completed")]
        if day:
            out[o] = day
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--created-since", required=True, help='HubSpot create time, "YYYY-MM-DD HH:MM" Riyadh')
    ap.add_argument("--ordered-since", required=True, help="Salla order date, YYYY-MM-DD")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
    if not tok:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, tok, live=args.apply)
    gio = GoogleIO(cfg, enabled=True)

    orders = collect(hs, cfg, args.created_since, args.ordered_since)
    by_sid = {str((o.get("properties") or {}).get("salla_order_id") or ""): o for o in orders}
    by_sid.pop("", None)
    ordered = {sid: ((o.get("properties") or {}).get("hs_external_created_date") or "")[:10]
               for sid, o in by_sid.items()}
    real = real_days(gio, cfg, set(by_sid), ordered)
    change, kinds, no_event = [], Counter(), []
    for sid, o in by_sid.items():
        cur = ((o.get("properties") or {}).get("delivery_date") or "")[:10]
        day = real.get(sid)
        if not day:
            no_event.append(sid)
            continue
        if cur == day:
            kinds["already right"] += 1
            continue
        kinds["blank -> real day" if not cur else f"{cur} -> real day"] += 1
        change.append((str(o["id"]), sid, cur, day))
    print(f"{len(by_sid)} released delivered/completed order(s); {len(change)} to change; "
          f"{len(no_event)} with no usable delivered/completed event (left alone: check their "
          "Salla order history)")
    for k, n in sorted(kinds.items()):
        print(f"  {k}: {n}")
    print("  new days:", dict(sorted(Counter(d for *_, d in change).items())))
    if no_event:
        print("  no event:", no_event[:10])
    if not args.apply:
        print("sample:", change[:3])
        print("DRY RUN -- rerun with --apply to write.")
        return

    new = not LEDGER.exists()
    LEDGER.parent.mkdir(exist_ok=True)
    done = failed = 0
    with open(LEDGER, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["ts", "hs_order_id", "salla_order_id", "old_delivery_date", "new_delivery_date", "result"])
        for i in range(0, len(change), 100):
            if STOP_FILE.exists():
                print("STOP.datefix present -- halting cleanly")
                break
            chunk = change[i:i + 100]
            ts = now_str()
            for hid, sid, cur, day in chunk:
                w.writerow([ts, hid, sid, cur, day, "sent"])
            fh.flush()
            status, resp = hs._write(
                "POST", "/crm/v3/objects/orders/batch/update",
                {"inputs": [{"id": hid, "properties": {"delivery_date": day}} for hid, _, _, day in chunk]},
                f"datefix x{len(chunk)}")
            if status in (200, 201):
                done += len(chunk)
            else:
                failed += len(chunk)
                print(f"BATCH FAILED {status}: {json.dumps(resp)[:200]}")
            time.sleep(1.0)
    print(f"done: {done} dated, {failed} failed; ledger {LEDGER}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
