#!/usr/bin/env python3
"""Customer gap sweep and consent filler (v2.11).

Two daily jobs that close what the realtime customer path can miss:

SWEEP. The Make capture scenario is the only way a new Salla customer reaches
the Customer Queue. When its sheet append fails (Google Sheets 409/429; five
times between 5 and 22 Sep), the event is gone. The sweep re-derives truth
from Salla for each ENDED day: every customer created that day
(`customers?date_from=D&date_to=D&fields[]=is_notifications_enabled`) is
checked against the engine ledger (mirror/customers.csv), the Customer Queue
(any non-final row) and HubSpot (salla_customer_id). Anyone missing from all
three is appended to the Customer Queue as `queued`, source `sweep`, with the
payload in the capture's exact 12-key shape, so the realtime consumer creates
the contact through its normal path. Sweep rows never auto-merge.
A day is refused (and alerted) when Salla reports 10,000 or more customers
for it (the list caps there) or when any returned customer was created
outside that day (the date filter did not apply).

CONSENT FILLER. Contacts created by paths whose event carries no
notifications flag (the abandoned-cart scenario creates about 95 a day) get
`salla_consent_status` from the Merchant API, which returns the flag when
asked (`customers/{id}?fields[]=is_notifications_enabled`). Only contacts
with a Salla id and no flag, created in the last `consent_filler_days`.
`--consent-history FROM TO` fills older contacts from the day lists instead
(one list call per 60 customers); its dry run reports the relay-call cost.

    python3 customer_sweep.py --config config.live.json                  # dry run: yesterday
    python3 customer_sweep.py --config config.live.json --live
    python3 customer_sweep.py --since 2026-09-05 --until 2026-09-25      # dry run of a backfill
    python3 customer_sweep.py --consent-history 2026-02-23 2026-09-21    # dry run, cost report
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from backfill import (Config, GoogleIO, HubSpot, RelayClient, now_str,
                      setup_logging)
from customer_payload import API_FIELDS, from_api, phone_of
from realtime_base import trim_lock_active

log = logging.getLogger("backfill")

STATE = Path("mirror/customer_sweep_state.json")
LEDGER = Path("mirror/customers.csv")
SWEEP_LEDGER = Path("mirror/customer_sweep.csv")
CONSENT_LEDGER = Path("mirror/consent_filled.csv")
LIST_CAP = 10000
PER_PAGE = 60
QUEUE_FINAL = ("done", "superseded", "gone")


# ----------------------------------------------------------------------------
# Salla side
# ----------------------------------------------------------------------------

def salla_day(relay, day):
    """Every customer Salla reports as created on `day` (Riyadh), newest first.
    Returns (records, total) or raises ValueError when the day cannot be
    trusted (cap reached, or the filter visibly did not apply)."""
    out, page, total = [], 1, None
    while True:
        env = relay.get_path(f"customers?date_from={day}&date_to={day}"
                             f"&per_page={PER_PAGE}&page={page}&{API_FIELDS}")
        items = env.get("data") or []
        pg = env.get("pagination") or {}
        total = int(pg.get("total") or 0)
        if total >= LIST_CAP:
            raise ValueError(f"{day}: Salla reports {total} customers (list cap {LIST_CAP})")
        for c in items:
            created = str(((c.get("created_at") or {}).get("date")) or "")[:10]
            if created and created != day:
                raise ValueError(f"{day}: customer {c.get('id')} created {created}; "
                                 f"the date filter did not apply")
        out.extend(items)
        if not items or page >= int(pg.get("totalPages") or 1):
            return out, total
        page += 1


# ----------------------------------------------------------------------------
# presence
# ----------------------------------------------------------------------------

def ledger_ids(path=LEDGER):
    ids = set()
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ids.add(str(row.get("salla_customer_id") or "").strip())
    return ids


def queue_ids(gio, cfg):
    """Customer ids with a NON-final row in the Customer Queue: a held or
    queued customer is already on its way and must not get a second row."""
    return {r["order_id"] for r in gio.queue_read_all(cfg.queue_spreadsheet_id,
                                                      tab=cfg.customer_queue_tab)
            if r["status"] not in QUEUE_FINAL}


def hubspot_ids(hs, ids):
    """Which of `ids` already have a contact carrying that salla_customer_id."""
    found, ids = set(), sorted(ids)
    for i in range(0, len(ids), 100):
        chunk, after = ids[i:i + 100], None
        while True:
            body = {"filterGroups": [{"filters": [{"propertyName": "salla_customer_id",
                                                   "operator": "IN", "values": chunk}]}],
                    "properties": ["salla_customer_id"], "limit": 100}
            if after:
                body["after"] = after
            d = hs.search("/crm/v3/objects/contacts/search", body, "sweep presence")
            for r in d.get("results") or []:
                found.add(str((r.get("properties") or {}).get("salla_customer_id") or ""))
            after = ((d.get("paging") or {}).get("next") or {}).get("after")
            if not after:
                break
    return found


# ----------------------------------------------------------------------------
# sweep
# ----------------------------------------------------------------------------

def load_state():
    try:
        return json.loads(STATE.read_text())
    except (OSError, json.JSONDecodeError):
        return {"swept": {}}


def save_state(state):
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=1, sort_keys=True))


def append_csv(path, header, rows):
    path.parent.mkdir(exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        w.writerows(rows)


def alert(cfg, live, subject, body):
    if not live or not getattr(cfg, "alerts_enabled", True):
        return
    try:
        import notify
        notify.send_alert(subject, body)
    except Exception as e:
        log.warning("sweep alert failed: %s", e)


def sweep_days(cfg, relay, hs, gio, days, live, cap, force):
    state = load_state()
    have_ledger = ledger_ids()
    in_queue = queue_ids(gio, cfg)
    totals = {"days": 0, "customers": 0, "missing": 0, "queued": 0, "refused": 0}
    for day in days:
        try:
            records, total = salla_day(relay, day)
        except ValueError as e:
            totals["refused"] += 1
            log.error("SWEEP refused %s", e)
            alert(cfg, live, "🟠 Customer sweep refused a day", str(e))
            continue
        ids = {str(c.get("id")) for c in records if c.get("id") is not None}
        unknown = ids - have_ledger - in_queue
        present = hubspot_ids(hs, unknown) if unknown else set()
        missing = sorted(unknown - present)
        totals["days"] += 1
        totals["customers"] += len(ids)
        totals["missing"] += len(missing)
        log.info("SWEEP %s: %d customers (Salla total %d), %d not in ledger/queue, "
                 "%d of those in HubSpot, %d missing", day, len(ids), total,
                 len(unknown), len(present), len(missing))
        if len(missing) > cap and not force:
            log.error("SWEEP %s: %d missing is above the cap %d -- nothing queued "
                      "(rerun with --force after checking)", day, len(missing), cap)
            alert(cfg, live, "🟠 Customer sweep over its cap",
                  f"{day}: {len(missing)} Salla customers have no contact, above the "
                  f"cap of {cap}. Nothing was queued. Check whether Salla imported "
                  f"customers in bulk, then rerun customer_sweep.py --force.")
            continue
        by_id = {str(c.get("id")): c for c in records}
        rows = []
        for cid in missing:
            p = from_api(by_id[cid])
            rows.append([now_str(), cid, phone_of(p), "customer.created", "queued", 0,
                         "sweep", json.dumps(p, ensure_ascii=False)])
        if rows and live:
            gio.queue_append_rows(cfg.queue_spreadsheet_id, rows, tab=cfg.customer_queue_tab)
            append_csv(SWEEP_LEDGER, ["ts", "day", "salla_customer_id"],
                       [[now_str(), day, r[1]] for r in rows])
            totals["queued"] += len(rows)
            alert(cfg, live, "🔎 Customer sweep queued missed customers",
                  f"{day}: {len(rows)} Salla customer(s) never reached HubSpot "
                  f"(the capture most likely failed on the sheet append). They are "
                  f"queued for the customer sync: {', '.join(r[1] for r in rows[:10])}"
                  f"{' ...' if len(rows) > 10 else ''}")
        elif rows:
            log.info("DRY RUN would queue %d: %s", len(rows), [r[1] for r in rows[:20]])
        if live:
            state["swept"][day] = {"customers": len(ids), "missing": len(missing),
                                   "queued": len(rows), "ts": now_str()}
            save_state(state)
    return totals


# ----------------------------------------------------------------------------
# consent
# ----------------------------------------------------------------------------

def flag_text(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return None


def contacts_without_flag(hs, since_ms, cap):
    out, after = [], None
    while len(out) < cap:
        body = {"filterGroups": [{"filters": [
            {"propertyName": "salla_customer_id", "operator": "HAS_PROPERTY"},
            {"propertyName": "salla_consent_status", "operator": "NOT_HAS_PROPERTY"},
            {"propertyName": "createdate", "operator": "GTE", "value": str(since_ms)}]}],
            "properties": ["salla_customer_id"], "limit": 100,
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}]}
        if after:
            body["after"] = after
        d = hs.search("/crm/v3/objects/contacts/search", body, "consent gap")
        out += [(r["id"], str((r.get("properties") or {}).get("salla_customer_id") or ""))
                for r in d.get("results") or []]
        after = ((d.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            break
    return out[:cap]


def batch_update_flags(hs, pairs):
    """pairs: [(contact_id, "true"|"false")]. Returns contacts written."""
    written = 0
    for i in range(0, len(pairs), 100):
        chunk = pairs[i:i + 100]
        status, _ = hs._write("POST", "/crm/v3/objects/contacts/batch/update",
                              {"inputs": [{"id": cid, "properties": {"salla_consent_status": v}}
                                          for cid, v in chunk]}, "consent batch update")
        if status in (200, 201):
            written += len(chunk)
        else:
            log.error("consent batch update failed HTTP %s (%d contacts)", status, len(chunk))
    return written


def fill_recent(cfg, relay, hs, live):
    days = int(getattr(cfg, "consent_filler_days", 3))
    cap = int(getattr(cfg, "consent_filler_cap", 400))
    since = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    todo = contacts_without_flag(hs, since, cap)
    pairs, unknown = [], 0
    for contact_id, sid in todo:
        try:
            env = relay.get_path(f"customers/{sid}?{API_FIELDS}")
        except Exception as e:
            log.warning("consent lookup %s failed: %s", sid, e)
            unknown += 1
            continue
        data = env.get("data") if env.get("status") == 200 else None
        v = flag_text((data or {}).get("is_notifications_enabled"))
        if v is None:
            unknown += 1
            continue
        pairs.append((contact_id, v))
    written = batch_update_flags(hs, pairs) if (pairs and live) else 0
    if live and written:
        append_csv(CONSENT_LEDGER, ["ts", "contact_id", "salla_consent_status"],
                   [[now_str(), c, v] for c, v in pairs])
    log.info("CONSENT recent: %d contacts without the flag (last %d days), %d resolved, "
             "%d unknown, %d written%s", len(todo), days, len(pairs), unknown, written,
             "" if live else " (dry run)")
    return {"checked": len(todo), "resolved": len(pairs), "unknown": unknown, "written": written}


def fill_history(cfg, relay, hs, first, last, live):
    """Fill older contacts from the day lists: one relay call per 60 customers."""
    calls, flags = 0, {}
    d = first
    while d <= last:
        day = d.isoformat()
        try:
            records, total = salla_day(relay, day)
        except ValueError as e:
            log.error("CONSENT history refused %s", e)
            d += timedelta(days=1)
            continue
        calls += max(1, -(-total // PER_PAGE))
        for c in records:
            v = flag_text(c.get("is_notifications_enabled"))
            if v is not None and c.get("id") is not None:
                flags[str(c["id"])] = v
        d += timedelta(days=1)
    pairs, ids = [], sorted(flags)
    for i in range(0, len(ids), 100):
        chunk, after = ids[i:i + 100], None
        while True:
            body = {"filterGroups": [{"filters": [
                {"propertyName": "salla_customer_id", "operator": "IN", "values": chunk},
                {"propertyName": "salla_consent_status", "operator": "NOT_HAS_PROPERTY"}]}],
                "properties": ["salla_customer_id"], "limit": 100}
            if after:
                body["after"] = after
            r = hs.search("/crm/v3/objects/contacts/search", body, "consent history")
            pairs += [(x["id"], flags[str((x.get("properties") or {}).get("salla_customer_id"))])
                      for x in r.get("results") or []
                      if str((x.get("properties") or {}).get("salla_customer_id")) in flags]
            after = ((r.get("paging") or {}).get("next") or {}).get("after")
            if not after:
                break
    written = batch_update_flags(hs, pairs) if (pairs and live) else 0
    if live and written:
        append_csv(CONSENT_LEDGER, ["ts", "contact_id", "salla_consent_status"],
                   [[now_str(), c, v] for c, v in pairs])
    log.info("CONSENT history %s..%s: %d relay calls, %d Salla flags, %d contacts to fill, "
             "%d written%s", first, last, calls, len(flags), len(pairs), written,
             "" if live else " (dry run)")
    return {"relay_calls": calls, "flags": len(flags), "contacts": len(pairs), "written": written}


# ----------------------------------------------------------------------------

def parse_day(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--live", action="store_true", help="write (default: dry run)")
    ap.add_argument("--since", help="sweep every ended day from this date (backfill)")
    ap.add_argument("--until", help="last day to sweep (default: yesterday)")
    ap.add_argument("--no-sweep", action="store_true")
    ap.add_argument("--no-consent", action="store_true")
    ap.add_argument("--consent-history", nargs=2, metavar=("FROM", "TO"))
    ap.add_argument("--force", action="store_true", help="queue even above the cap")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="customer_sweep.log")
    cfg = Config.load(args.config)
    if Path("STOP.sweep").exists():
        log.info("STOP.sweep present -- not running")
        return
    if args.live and not getattr(cfg, "customer_sweep_enabled", False):
        log.info("customer_sweep_enabled is false -- live run skipped")
        return
    if trim_lock_active():
        log.info("a queue trim is running -- sweep skipped this time")
        return
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    secret = (os.environ.get("RELAY_SECRET") or "").strip()
    if not token or not secret:
        log.warning("HUBSPOT_ACCESS_TOKEN or RELAY_SECRET missing -- idle")
        return
    hs = HubSpot(cfg, token, live=args.live)
    relay = RelayClient(cfg, secret)
    gio = GoogleIO(cfg, enabled=True)

    yesterday = date.today() - timedelta(days=1)
    last = parse_day(args.until) if args.until else yesterday
    last = min(last, yesterday)                    # only ended days
    first = parse_day(args.since) if args.since else last - timedelta(days=1)
    days = []
    d = first
    while d <= last:
        days.append(d.isoformat())
        d += timedelta(days=1)

    t0 = time.time()
    if args.consent_history:
        fill_history(cfg, relay, hs, parse_day(args.consent_history[0]),
                     parse_day(args.consent_history[1]), args.live)
        return
    if not args.no_sweep:
        totals = sweep_days(cfg, relay, hs, gio, days, args.live,
                            int(getattr(cfg, "customer_sweep_cap", 300)), args.force)
        log.info("SWEEP %s..%s: %s (%s)", days[0] if days else "-", days[-1] if days else "-",
                 totals, "live" if args.live else "dry run")
    if not args.no_consent:
        fill_recent(cfg, relay, hs, args.live)
    log.info("customer_sweep finished in %.0fs", time.time() - t0)


if __name__ == "__main__":
    main()
