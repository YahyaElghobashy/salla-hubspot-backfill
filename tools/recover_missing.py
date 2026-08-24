#!/usr/bin/env python3
"""Repair orders that exist in HubSpot without their line items.

Two code paths deliberately create an order and then decline to add its line
items, each deferring the repair to "tools/recover_missing.py":

  * backfill.Engine._finish_create -- when create_order resolves to a
    PRE-EXISTING record (was_fresh False), adding items would duplicate
    whatever the earlier writer added, so it adds none.
  * live.LiveSync._resolve_preexisting -- when an order found by search has
    fewer line items than the source has items, it flags 'partial'.

That tool was never written, so neither path could ever complete. The order
sits at zero line items and the live queue row is re-marked 'error' every poll
forever (the mark writes back r["attempts"] unchanged, so the
live_max_attempts circuit breaker never trips). This closes that loop.

The repair is the engine's own item router, not a reimplementation:
`Engine.process_item(order, hs_order_id, item)` creates each line item,
stamps hs_product_id and associates it to the order, handling the standalone,
legacy-SKU and bundle routes exactly as a fresh create would.

Two safety properties, both deliberate:

  * THE CATALOG GATE STILL APPLIES. An order whose items are not approved is
    reported and skipped, never repaired. Attaching line items for unapproved
    products would put catalog the client has not signed off into the CRM
    through a side door -- precisely what the gate exists to prevent.
  * ONLY ZERO-LINE-ITEM ORDERS ARE REPAIRED by default. With 0 < li <
    expected some items already landed and re-running the whole item loop
    would duplicate them; there is no per-item marker to tell which. Those
    are reported for a human. --allow-topup overrides, and says what it will
    duplicate first.

Success adds the order to mirror/created.csv. That is what actually stops the
retry loop: _resolve_preexisting consults the created-ledger FIRST and returns
"done" on a hit, so the queue row is marked done on the next poll.

    python3 tools/recover_missing.py                    # dry run, from errors.csv
    python3 tools/recover_missing.py --apply
    python3 tools/recover_missing.py --order 1063985595 --apply
"""

import argparse
import csv
import logging
import os
import socket
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import backfill
from backfill import (Config, CreatedLedger, GoogleIO, HubSpot, LocalMirror,
                      RelayClient, install_dns_cache, setup_logging)

log = logging.getLogger("backfill")

REPAIRABLE = ("partial", "duplicate_create")


def load_dotenv(path=".env"):
    """Tiny .env loader (KEY=VALUE lines); never overrides an existing var.
    Same helper queue_drain.py carries; duplicated rather than imported so
    this tool does not pull in the whole drain module."""
    try:
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


class RecoverEngine(backfill.Engine):
    """The engine with intake and Sheets removed; only process_item is used."""

    def __init__(self, cfg, relay, hs, mirror, live):
        class _NoCursor:
            data = {"status": "recover"}
            status = "recover"
        super().__init__(cfg, _NoCursor(), relay, hs,
                         GoogleIO(cfg, enabled=False), mirror,
                         live=live, workers=1)
        self.is_live_sync = False
        self.health = None


def orders_from_errors(path="mirror/errors.csv"):
    """Distinct order ids flagged by either deferring path, newest first."""
    p = Path(path)
    if not p.exists():
        return []
    seen, out = set(), []
    with open(p, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            # ts, order_id, kind, note
            if len(row) < 3:
                continue
            oid, kind = str(row[1]).strip(), str(row[2]).strip().lower()
            if kind in REPAIRABLE and oid and oid not in seen:
                seen.add(oid)
                out.append(oid)
    return out


def repair(oid, engine, hs, relay, ledger, apply_, allow_topup):
    """Returns a one-word outcome for the summary counter."""
    hs_id = hs.find_order_by_salla_id(oid)
    if not hs_id:
        log.info("SKIP   %-12s no HubSpot order found (never created)", oid)
        return "not_in_hubspot"

    li = hs.order_line_item_count(hs_id)
    if li < 0:
        log.error("ERROR  %-12s line-item count unavailable (HubSpot error)", oid)
        return "count_failed"

    fetched = relay.fetch_orders([oid]).get(oid)
    if fetched is None:
        log.error("ERROR  %-12s relay returned nothing; cannot rebuild items", oid)
        return "no_source"
    items = fetched.get("items", []) or []
    expected = len(items) or 1

    if li >= expected:
        # already whole: the ledger entry is the missing piece, and it is what
        # stops the retry loop
        log.info("OK     %-12s HS %s already has %d/%d LIs%s",
                 oid, hs_id, li, expected, "" if apply_ else " (would ledger)")
        if apply_:
            ledger.add(oid, hs_id)
        return "already_complete"

    unverified = engine.gate_unverified_items(fetched)
    if unverified:
        names = ", ".join(str(u.get("name", ""))[:34] for u in unverified[:3])
        log.warning("HELD   %-12s HS %s gate holds %d item(s): %s",
                    oid, hs_id, len(unverified), names)
        return "held_by_gate"

    if li > 0 and not allow_topup:
        log.warning("MIXED  %-12s HS %s has %d/%d LIs -- partial top-up would "
                    "duplicate the %d already there; skipped (--allow-topup "
                    "to force)", oid, hs_id, li, expected, li)
        return "mixed_skipped"

    if not apply_:
        log.info("WOULD  %-12s HS %s attach %d line item(s)", oid, hs_id, expected)
        return "would_repair"

    before = li
    errors = 0
    for item in items:
        try:
            engine.process_item(fetched, hs_id, item)
        except Exception as e:
            errors += 1
            log.error("item %s on order %s raised: %s", item.get("id"), oid, e)

    after = hs.order_line_item_count(hs_id)
    if errors or after < expected:
        log.error("PARTIAL %-11s HS %s now %d/%d LIs (was %d, %d item error(s)) "
                  "-- NOT ledgered", oid, hs_id, after, expected, before, errors)
        return "repair_incomplete"

    # match a clean create: the engine stamps this right after create_order
    hs.patch_order(hs_id, {"last_salla_sync_status": "synced"}, "recover set synced")
    ledger.add(oid, hs_id)
    log.info("REPAIRED %-10s HS %s %d -> %d line items", oid, hs_id, before, after)
    return "repaired"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--order", action="append", default=[],
                    help="salla order id; repeatable. Default: every "
                         "partial/duplicate_create row in mirror/errors.csv")
    ap.add_argument("--apply", action="store_true",
                    help="write to HubSpot (default is a dry run)")
    ap.add_argument("--allow-topup", action="store_true",
                    help="also repair orders that already have SOME line "
                         "items -- may duplicate them")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    threading.current_thread().name = "main"
    setup_logging(args.verbose, logfile="recover.log")
    socket.setdefaulttimeout(180)
    install_dns_cache()
    load_dotenv()

    cfg = Config.load(args.config)
    if hasattr(backfill, "apply_portal_config"):
        backfill.apply_portal_config(cfg)

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN and RELAY_SECRET first (.env).")

    oids = args.order or orders_from_errors()
    if not oids:
        print("Nothing to repair: no partial/duplicate_create rows in "
              "mirror/errors.csv and no --order given.")
        return

    mirror = LocalMirror("mirror")
    relay = RelayClient(cfg, secret)
    hs = HubSpot(cfg, token, live=args.apply)
    ledger = CreatedLedger("mirror")
    engine = RecoverEngine(cfg, relay, hs, mirror, live=args.apply)
    engine.created_ledger = ledger

    log.info("%d order(s) to inspect; mode=%s",
             len(oids), "APPLY" if args.apply else "DRY RUN")

    counts = {}
    for oid in oids:
        if ledger.get(oid):
            log.info("SKIP   %-12s already in the created-ledger", oid)
            counts["already_ledgered"] = counts.get("already_ledgered", 0) + 1
            continue
        try:
            outcome = repair(oid, engine, hs, relay, ledger,
                             args.apply, args.allow_topup)
        except Exception as e:
            log.error("ERROR  %-12s %s", oid, e)
            outcome = "exception"
        counts[outcome] = counts.get(outcome, 0) + 1

    print("\n" + "=" * 62)
    for k in sorted(counts):
        print(f"  {k:<20} {counts[k]}")
    print("=" * 62)
    if not args.apply:
        print("DRY RUN -- rerun with --apply to write to HubSpot.")


if __name__ == "__main__":
    main()
