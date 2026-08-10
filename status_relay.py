#!/usr/bin/env python3
"""Delivery-Status Relay (v2.7, sub-product A).

Moves a Salla order's delivery status onto its HubSpot order as a pipeline
stage change, in realtime, from the `Status Queue` tab that the thin-capture
Make scenario appends to (webhook -> one addRow -> done).

Why the engine owns this instead of Make (which did it all in-scenario):
the Make version slept 120s when the order wasn't in HubSpot yet and then
gave up after ONE retry -- precisely wrong for this store, where tens of
thousands of historical orders enter HubSpot hours-to-days after their status
events (catalog holds, backfill). This consumer defers with a real ladder
(status_retry_ladder: 30s/2m/10m/1h/6h by default) and recognizes the
`superseded` case: if the order got created after the event, its stage was
already set from the fresh payload at creation time, so the late event needs
no write at all.

Row contract (A:H, shared queue schema):
  A received_at | B order_id | C reference_id | D "status:<slug>@<event_ts>"
  E state       | F attempts | G source        | H note

Idempotency: mirror/status_applied.csv (append-only) records every stage
write as (ts, order_id, slug, stage, hs_order_id, event_ts). A replayed or
older event never regresses the stage: events apply only when their event_ts
is >= the last applied event_ts for that order.

Usage:
    python3 status_relay.py --live            # the service entrypoint
    python3 status_relay.py --once --dry      # one poll cycle, no writes
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from backfill import Config, HubSpot, GoogleIO, dig, now_str, setup_logging
from realtime_base import RealtimeConsumer, TabLock

log = logging.getLogger("backfill")

LEDGER = Path("mirror/status_applied.csv")
EXCEPTIONS_TAB = "Delivery Status Exceptions"


class StatusLedger:
    """last applied (event_ts, slug) per order id; append-only CSV behind it."""

    def __init__(self, path=LEDGER):
        self.path = Path(path)
        self.last = {}
        if self.path.exists():
            try:
                with open(self.path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        self.last[str(row["order_id"])] = (
                            row.get("event_ts") or "", row.get("slug") or "")
            except OSError as e:
                log.warning("status ledger unreadable: %s", e)

    def newer_than_applied(self, order_id, event_ts):
        prev = self.last.get(str(order_id))
        return prev is None or str(event_ts) >= prev[0]

    def record(self, order_id, slug, stage, hs_order_id, event_ts):
        new = not self.path.exists()
        self.path.parent.mkdir(exist_ok=True)
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "order_id", "slug", "stage",
                            "hs_order_id", "event_ts"])
            w.writerow([now_str(), order_id, slug, stage, hs_order_id,
                        event_ts])
        self.last[str(order_id)] = (str(event_ts), slug)


class StatusRelay(RealtimeConsumer):
    name = "status"

    def __init__(self, cfg, hs, gio, live=True):
        super().__init__(cfg, hs, gio, tab=cfg.status_queue_tab, live=live)
        self.ledger = StatusLedger()
        self.stage_map = {str(k).lower(): v
                          for k, v in (cfg.status_stage_map or {}).items()}
        self.ladder = list(getattr(cfg, "status_retry_ladder",
                                   (30, 120, 600, 3600, 21600)))
        self._alert_last = {}

    # -- helpers ---------------------------------------------------------------

    @staticmethod
    def parse_event(event):
        """'status:<slug>@<event_ts>' -> (slug, event_ts). Tolerant of a bare
        slug (older capture rows): event_ts falls back to ''. """
        body = event.split(":", 1)[1] if ":" in event else event
        slug, _, ts = body.partition("@")
        return slug.strip().lower(), ts.strip()

    def _find_order(self, order_id, reference_id):
        """salla_order_id EQ, fallback salla_order_reference EQ -- the same
        two searches the Make scenario ran, through the paced client."""
        d = self.hs.search("/crm/v3/objects/orders/search", {
            "filterGroups": [
                {"filters": [{"propertyName": "salla_order_id",
                              "operator": "EQ", "value": str(order_id)}]},
                {"filters": [{"propertyName": "salla_order_reference",
                              "operator": "EQ",
                              "value": str(reference_id or order_id)}]}],
            "properties": ["hs_object_id", "hs_pipeline_stage"],
            "limit": 1}, "status find order")
        res = d.get("results") or []
        return res[0] if res else None

    def _alert(self, key, subject, body):
        """Cooldown-respecting Slack alert (per key, alert_cooldown_minutes)."""
        cool = float(getattr(self.cfg, "alert_cooldown_minutes", 30)) * 60
        if time.time() - self._alert_last.get(key, 0) < cool:
            return
        self._alert_last[key] = time.time()
        try:
            import notify
            notify.send_alert(subject, body)
        except Exception as e:
            log.warning("status alert failed: %s", e)

    def _exception_row(self, row, slug, why, action):
        """Client-visible surface stays identical to the Make design: one row
        in the Delivery Status Exceptions tab of the audit workbook."""
        if not self.live:
            return
        try:
            self.gio.queue_append_rows(
                self.cfg.spreadsheet_id,
                [[now_str(), row["order_id"], row["reference_id"], slug,
                  row.get("note", ""), why, action, "engine"]],
                tab=EXCEPTIONS_TAB)
        except Exception as e:
            log.warning("exceptions tab append failed: %s", e)

    # -- core ------------------------------------------------------------------

    def handle_row(self, row):
        slug, event_ts = self.parse_event(row["event"])
        oid = row["order_id"]

        stage = self.stage_map.get(slug)
        if not stage:
            self._exception_row(row, slug, "Unmapped status",
                                "Review mapping or fix manually")
            self._alert(f"unmapped:{slug}",
                        f"🟠 Delivery status “{slug}” has no stage mapping",
                        f"Order {oid} arrived with status “{slug}”, which is "
                        f"not in status_stage_map. It is logged in the "
                        f"Delivery Status Exceptions tab; the stage was NOT "
                        f"changed. Add the mapping and the next event will "
                        f"apply cleanly.")
            return "error-final", f"unmapped status {slug}"

        if not self.ledger.newer_than_applied(oid, event_ts):
            return "superseded", f"older than applied event ({slug})"

        hs_order = self._find_order(oid, row["reference_id"])
        if hs_order is None:
            attempts = int(row["attempts"] or 0)
            if attempts >= len(self.ladder):
                self._exception_row(row, slug, "Order not in HubSpot after "
                                    f"{attempts} retries over ~8h",
                                    "Check whether the order is held/failed")
                self._alert("order-missing",
                            "🟠 Status events arriving for orders HubSpot "
                            "does not have",
                            f"Latest: order {oid} (status “{slug}”). Retried "
                            f"{attempts} times over ~8 hours. Usual causes: "
                            f"the order is catalog-held, or its creation "
                            f"failed. The event is logged in the exceptions "
                            f"tab and will apply automatically if the order "
                            f"appears later via drain/backfill (stage is "
                            f"baked in at creation).")
                return "error-final", f"order absent after {attempts} tries"
            nb = time.time() + self.ladder[min(attempts, len(self.ladder) - 1)]
            return "deferred", f"order not in HS yet nb={int(nb)} ({slug})"

        hs_id = str(dig(hs_order, "properties.hs_object_id")
                    or hs_order.get("id"))
        current = str(dig(hs_order, "properties.hs_pipeline_stage") or "")
        if current == stage:
            self.ledger.record(oid, slug, stage, hs_id, event_ts)
            return "superseded", f"stage already {slug} (creation baked it in)"

        status, _ = self.hs.update_order(hs_id, {"hs_pipeline_stage": stage},
                                         f"status {slug} -> order {hs_id}")
        if status not in (200, 201):
            return "error", f"PATCH failed HTTP {status}"
        self.ledger.record(oid, slug, stage, hs_id, event_ts)
        log.info("STATUS applied %s -> %s (HS %s, stage %s)",
                 oid, slug, hs_id, stage[:12])
        return "done", f"{slug} applied"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--live", action="store_true",
                    help="write to HubSpot/sheets (omit for dry-run)")
    ap.add_argument("--once", action="store_true", help="one poll cycle")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="status_relay.log")
    cfg = Config.load(args.config)
    if not getattr(cfg, "status_relay_enabled", True):
        sys.exit("status_relay_enabled=false in config -- not starting.")
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    TabLock("status_relay").acquire()
    hs = HubSpot(cfg, token, live=args.live)
    gio = GoogleIO(cfg, enabled=True)
    relay = StatusRelay(cfg, hs, gio, live=args.live)
    relay.run(once=args.once)


if __name__ == "__main__":
    main()
