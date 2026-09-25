#!/usr/bin/env python3
"""Drain held rows of the Customer Queue (v2.11).

A customer row is parked as `held` when its payload could not be read,
salvaged or looked up, or when it failed on every retry. The consumer walks
past held rows and never retries them; this tool is how they get resolved,
once the cause is understood.

Each held row goes through the SAME path the consumer uses
(CustomerSync.handle_row, v2.11: JSON, template salvage, Salla lookup with
the consent flag), so contacts are created or updated exactly as a live row
would be. Differences, all deliberate:
  * auto-merge is OFF unless --allow-merge (HubSpot merges cannot be undone);
    a row that matches two contacts updates the most recent one and alerts;
  * a row that still fails stays `held`, with the new reason in column I;
  * dry run by default: it prints what it would do, including merge pairs.

Refuses to run while a queue trim holds mirror/trim.lock (row numbers move).

    python3 tools/drain_held_customers.py                 # dry run, all held rows
    python3 tools/drain_held_customers.py --apply
    python3 tools/drain_held_customers.py --rows 52846 --apply
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, GoogleIO, HubSpot, RelayClient, now_str, setup_logging
from customer_sync import CustomerSync
from realtime_base import trim_lock_active

log = logging.getLogger("backfill")
LEDGER = Path("mirror/held_customer_drain.csv")


def ledger_write(rows):
    LEDGER.parent.mkdir(exist_ok=True)
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "sheet_row", "salla_customer_id", "outcome", "note"])
        for r in rows:
            w.writerow([now_str()] + r)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--rows", default="", help="only these sheet rows (comma-separated)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--allow-merge", action="store_true",
                    help="let a row that matches 2+ contacts auto-merge (irreversible)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="drain_held_customers.log")
    if trim_lock_active():
        sys.exit("a queue trim is running (mirror/trim.lock): try again later")
    cfg = Config.load(args.config)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    secret = (os.environ.get("RELAY_SECRET") or "").strip()
    if not token:
        sys.exit("HUBSPOT_ACCESS_TOKEN missing")
    hs = HubSpot(cfg, token, live=args.apply)
    gio = GoogleIO(cfg, enabled=True)
    relay = RelayClient(cfg, secret) if secret and cfg.relay_url else None
    sync = CustomerSync(cfg, hs, gio, live=args.apply, relay=relay)
    sync.no_merge = not args.allow_merge

    only = {int(x) for x in args.rows.split(",") if x.strip()}
    rows = [r for r in gio.queue_read_all(cfg.queue_spreadsheet_id, tab=cfg.customer_queue_tab)
            if r["status"] == "held" and (not only or r["row"] in only)]
    log.info("%s -- %d held row(s)%s", "APPLY" if args.apply else "DRY RUN", len(rows),
             "" if args.allow_merge else "; auto-merge off")
    written, outcomes = [], {}
    for r in rows:
        work = dict(r, status="queued")
        if not args.allow_merge:
            work["source"] = "drain"
        try:
            state, note = sync.handle_row(work)
        except Exception as e:
            state, note = "error", f"{type(e).__name__}: {e}"[:180]
        final = state in sync.terminal
        outcomes[state] = outcomes.get(state, 0) + 1
        log.info("  row %d customer %s -> %s | %s", r["row"], r["order_id"], state, note[:120])
        if not args.apply:
            continue
        if final:
            ok = sync._mark(r, state, r["attempts"] + 1, f"drained: {note}")
        else:
            ok = gio.queue_mark(cfg.queue_spreadsheet_id, r["row"], r["order_id"], "held",
                                r["attempts"], f"drain {now_str()[:16]}: {note}"[:180],
                                tab=cfg.customer_queue_tab, note_col="I",
                                expect_received_at=r["received_at"])
        written.append([r["row"], r["order_id"], state if ok else "mark refused", note[:150]])
    if written:
        ledger_write(written)
        log.info("ledger: %s (+%d)", LEDGER, len(written))
    log.info("done: %s", outcomes or "nothing to drain")


if __name__ == "__main__":
    main()
