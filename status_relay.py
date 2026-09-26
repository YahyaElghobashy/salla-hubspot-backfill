#!/usr/bin/env python3
"""Delivery-Status Relay (v2.8, sub-product A).

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

from backfill import (Config, HubSpot, GoogleIO, dig, is_zid_order, now_str,
                      setup_logging)
from realtime_base import RealtimeConsumer, TabLock

log = logging.getLogger("backfill")

LEDGER = Path("mirror/status_applied.csv")
EXCEPTIONS_TAB = "Delivery Status Exceptions"


def _sheet_url(sheet_id):
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}" if sheet_id else ""


def _hubspot_search_url(term):
    portal = (os.environ.get("HUBSPOT_PORTAL_ID") or "").strip()
    if not portal:
        return ""
    return f"https://app.hubspot.com/search/{portal}/search?term={term}"


def _slack_link(url, label):
    return f"<{url}|{label}>" if url else label


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
    # An order legitimately produces several status events (under_review ->
    # completed). Collapsing them would apply only the first and silently
    # discard the rest; the ledger's event-ts guard already handles ordering.
    collapse_twins = False
    # [v2.11] daily trim of the Status Queue: 7 days kept by default, an hour
    # before the live engine's own trim so the two never delete at once.
    trim_days_attr = "status_trim_days"
    trim_hour_offset = -1

    def on_exhausted(self, row, note):
        """[v2.11] A status row that failed on every retry lands in the
        Delivery Status Exceptions tab like every other give-up."""
        slug, _ = self.parse_event(row["event"])
        self._exception_row(row, slug, f"Retries exhausted: {note}"[:180],
                            "Apply the stage by hand and check the order")

    def __init__(self, cfg, hs, gio, live=True):
        super().__init__(cfg, hs, gio, tab=cfg.status_queue_tab, live=live)
        self.ledger = StatusLedger()
        self.stage_map = {str(k).lower(): v
                          for k, v in (cfg.status_stage_map or {}).items()}
        self.ladder = list(getattr(cfg, "status_retry_ladder",
                                   (30, 120, 600, 3600, 21600)))
        self._alert_last = {}
        # v2.8: classification state. The Live Queue index answers "did the
        # live engine see this order, and is it catalog-held right now?" --
        # which turns the old one-size-fits-all orange alert into three honest
        # stories (held / live-order-missing / awaiting-backfill).
        self._live_index = {}          # order_id -> (state, note)
        self._live_index_at = 0.0
        self._digest = []              # backfill-era give-ups awaiting flush
        self._digest_sent_day = ""

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
            "properties": ["hs_object_id", "hs_pipeline_stage", "salla_store",
                           "hs_source_store"],
            "limit": 10}, "status find order")
        res = d.get("results") or []
        # [v2.12] a Zid-import order carries a Zid order number in
        # salla_order_id and salla_order_reference; it is never the Salla
        # order, whatever number it shares with it
        mine = [r for r in res if not is_zid_order(r.get("properties"))]
        self._zid_holder = None
        if res and not mine:
            self._zid_holder = str(res[0].get("id"))
            log.warning("ZID COLLISION: status for salla order %s (ref %s) "
                        "matched only Zid order HS %s -- not applied",
                        order_id, reference_id, self._zid_holder)
        return mine[0] if mine else None

    def _refresh_live_index(self):
        every = float(getattr(self.cfg, "held_index_refresh_s", 600))
        if time.time() - self._live_index_at < every:
            return
        self._live_index_at = time.time()
        try:
            rows = self.gio.queue_read(
                self.cfg.queue_spreadsheet_id, start_row=2,
                tab=getattr(self.cfg, "live_queue_tab", "Live Queue")) or []
            self._live_index = {str(r.get("order_id")):
                                (str(r.get("status") or r.get("state") or ""),
                                 str(r.get("note") or ""))
                                for r in rows if r.get("order_id")}
        except Exception as e:
            log.warning("live-queue index refresh failed: %s", e)

    def _classify(self, oid, reference_id):
        """'held' | 'recent' | 'backfill' | None (classification disabled)."""
        min_ref = int(getattr(self.cfg, "live_min_reference", 0) or 0)
        if min_ref <= 0:
            return None
        self._refresh_live_index()
        state, note = self._live_index.get(str(oid), ("", ""))
        if state == "held" and not str(note).startswith("zid collision"):
            return "held"
        if state:
            # the live engine SAW this order (done/error/gone) yet HubSpot
            # cannot find it: that is a genuine anomaly, never digest fodder
            return "recent"
        try:
            return "recent" if int(reference_id or 0) >= min_ref else "backfill"
        except (TypeError, ValueError):
            return "backfill"

    def _held_summary(self):
        held = [(o, note) for o, (st, note) in self._live_index.items()
                if st == "held" and not str(note).startswith("zid collision")]
        names = {}
        for _, note in held:
            if note.startswith("catalog gate:"):
                for n in note[len("catalog gate:"):].split("--")[0].split(","):
                    n = n.strip()
                    if n:
                        names[n] = names.get(n, 0) + 1
        top = ", ".join(f"{n} ({c})" for n, c in
                        sorted(names.items(), key=lambda kv: -kv[1])[:4])
        return len(held), top

    def _links_line(self):
        parts = [
            _slack_link(_sheet_url(self.cfg.queue_spreadsheet_id),
                        "Live Queue sheet"),
            _slack_link(_sheet_url(self.cfg.spreadsheet_id),
                        "Audit workbook (exceptions tab)"),
        ]
        return "  ·  ".join(x for x in parts if x)

    def _flush_digest(self, force=False):
        if not self._digest:
            return
        today = datetime.now().strftime("%Y-%m-%d")
        due = (datetime.now().hour >=
               int(getattr(self.cfg, "status_digest_hour", 18))
               and self._digest_sent_day != today)
        if not (due or force or len(self._digest) >= 50):
            return
        items = self._digest
        self._digest = []
        self._digest_sent_day = today
        sample = ", ".join(f"{o} ({s})" for o, s in items[:10])
        more = f" and {len(items) - 10} more" if len(items) > 10 else ""
        self._alert(
            "backfill-digest",
            f"🟠 {len(items)} status event(s) waiting on historical backfill",
            f"These orders predate the live sync and their backfill window "
            f"has not run yet, so their delivery statuses cannot apply: "
            f"{sample}{more}. No action needed: each stage is baked in "
            f"automatically when its order is created by the backfill. If "
            f"this number grows day over day, check that the backfill is "
            f"actually progressing.\n{self._links_line()}")

    def _giveup_alert(self, kind, oid, reference_id, slug, attempts):
        """One give-up, three honest stories. kind=None preserves the pre-v2.8
        single orange alert so an unconfigured deployment behaves as before."""
        hs_link = _slack_link(_hubspot_search_url(oid), "search HubSpot")
        if kind == "held":
            self._refresh_live_index()
            count, top = self._held_summary()
            blockers = f" Blocking products: {top}." if top else ""
            self._alert(
                "held-backlog",
                f"🟡 {count} order(s) held for catalog approval are getting "
                f"delivery updates",
                f"Latest: order {oid} (status “{slug}”). It cannot be created "
                f"until its products are approved in the catalog review, so "
                f"its delivery status cannot apply yet.{blockers} Once "
                f"approved and drained, the order is created with the CURRENT "
                f"status baked in, so nothing is lost. The backlog ages "
                f"until someone approves: that is the action.\n"
                f"{self._links_line()}  ·  {hs_link}")
            return
        if kind == "recent":
            self._alert(
                f"missing:{oid}",
                f"🔴 Live order {oid} is missing from HubSpot",
                f"This order is from the live era (reference "
                f"{reference_id or '?'}), its delivery status “{slug}” has "
                f"been arriving for ~8 hours, and the order itself is not in "
                f"HubSpot. This is NOT catalog-hold and NOT backfill lag: "
                f"the intake likely lost the creation event. Check the Live "
                f"Queue row and mirror/errors.csv on the server, then "
                f"re-drain or create manually.\n"
                f"{self._links_line()}  ·  {hs_link}")
            return
        if kind == "backfill":
            self._digest.append((oid, slug))
            self._flush_digest()
            return
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

    def _alert(self, key, subject, body):
        """Cooldown-respecting Slack alert (per key, alert_cooldown_minutes)."""
        cool = float(getattr(self.cfg, "alert_cooldown_minutes", 30)) * 60
        if key == "held-backlog":
            cool = max(cool, 6 * 3600)
        elif key.startswith("missing:"):
            cool = max(cool, 24 * 3600)
        elif key == "backfill-digest":
            cool = max(cool, 20 * 3600)
        if time.time() - self._alert_last.get(key, 0) < cool:
            return
        self._alert_last[key] = time.time()
        try:
            import notify
            notify.send_alert(subject, body)
        except Exception as e:
            log.warning("status alert failed: %s", e)

    def _exception_row(self, row, slug, why, action, hs_order_id=""):
        """Client-visible surface stays identical to the Make design: one row
        in the Delivery Status Exceptions tab of the audit workbook."""
        if not self.live:
            return
        try:
            # column order matches the tab the Make design established:
            # ts | salla order | reference | slug | status name | type |
            # action | hubspot order id (blank when we never found one)
            self.gio.queue_append_rows(
                self.cfg.spreadsheet_id,
                [[now_str(), row["order_id"], row["reference_id"], slug,
                  row.get("note", ""), why, action, hs_order_id or ""]],
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
            log.error("STATUS unmapped slug %r on order %s -- logged to the "
                      "exceptions tab, stage unchanged", slug, oid)
            return "error-final", f"unmapped status {slug}"

        if not self.ledger.newer_than_applied(oid, event_ts):
            return "superseded", f"older than applied event ({slug})"

        hs_order = self._find_order(oid, row["reference_id"])
        zid = getattr(self, "_zid_holder", None)
        if hs_order is None and zid:
            # [v2.12] the only match is the imported Zid order with the same
            # number: the Salla order cannot exist until that record is
            # re-keyed, so waiting ~8 h for it helps nobody
            self._exception_row(row, slug, "Order number held by an imported Zid "
                                "order", "Salla order is created after the Zid "
                                "order is re-keyed; nothing changed", hs_order_id="")
            self._alert(f"zid:{oid}",
                        "🟠 Delivery status for an order blocked by a Zid number",
                        f"Order {oid} got status “{slug}”, but HubSpot order {zid} "
                        f"is the imported Zid order with the same number, so the "
                        f"Salla order is not in HubSpot yet. Nothing was changed "
                        f"on the Zid order.\n{self._links_line()}")
            return "error-final", f"zid collision: number held by Zid HS {zid}"
        if hs_order is None:
            attempts = int(row["attempts"] or 0)
            if attempts >= len(self.ladder):
                kind = self._classify(oid, row["reference_id"])
                why = {"held": "Order is catalog-held (products awaiting "
                               "approval)",
                       "recent": "LIVE order missing from HubSpot",
                       "backfill": "Order awaits its backfill window"}.get(
                    kind, f"Order not in HubSpot after {attempts} retries "
                          "over ~8h")
                self._exception_row(row, slug, why,
                                    "Check whether the order is held/failed",
                                    hs_order_id="")
                self._giveup_alert(kind, oid, row["reference_id"], slug,
                                   attempts)
                log.error("STATUS order %s absent from HubSpot after %d "
                          "retries (class=%s) -- giving up on %r",
                          oid, attempts, kind or "legacy", slug)
                return "error-final", f"order absent after {attempts} tries"
            nb = time.time() + self.ladder[min(attempts, len(self.ladder) - 1)]
            return "deferred", f"order not in HS yet nb={int(nb)} ({slug})"

        hs_id = str(dig(hs_order, "properties.hs_object_id")
                    or hs_order.get("id"))
        current = str(dig(hs_order, "properties.hs_pipeline_stage") or "")
        if current == stage:
            if self.live:
                self.ledger.record(oid, slug, stage, hs_id, event_ts)
            return "superseded", f"stage already {slug} (creation baked it in)"

        status, _ = self.hs.update_order(hs_id, {"hs_pipeline_stage": stage},
                                         f"status {slug} -> order {hs_id}")
        if status not in (200, 201):
            return "error", f"PATCH failed HTTP {status}"
        if self.live:
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
