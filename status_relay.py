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
import slack_text as st

log = logging.getLogger("backfill")

LEDGER = Path("mirror/status_applied.csv")
EXCEPTIONS_TAB = "Delivery Status Exceptions"
# [v2.14.1] the one shared thread for give-ups the relay cannot classify
UNCLASSIFIED_KEY = "status-unclassified"
UNCLASSIFIED_KEEP = 500          # orders remembered in its meta
UNCLASSIFIED_QUIET_S = 24 * 3600  # closed after a day with nothing new


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
            f"🟠 {len(items)} delivery updates are waiting for older orders to be imported",
            st.message(
                f"These orders are older than the live sync and are not in HubSpot yet, so their "
                f"delivery updates cannot apply: {sample}{more}. Nothing is lost: each order gets "
                f"its latest status when it is imported.",
                "Nothing to do. If this number grows every day, tell the integration team.",
                [self._tab_link(self.cfg.spreadsheet_id, EXCEPTIONS_TAB, "Delivery Status Exceptions tab")]))

    def _tab_link(self, sheet_id, tab, label):
        """[v2.13] A link straight to a tab of a sheet; the sheet when the tab
        id cannot be read."""
        try:
            if getattr(self, "_sheets", None) is None:
                self._sheets = st.SheetIndex(self.gio)
            return st.sheet(sheet_id, label, self._sheets.tabs(sheet_id).get(tab))
        except Exception:
            return st.sheet(sheet_id, label)

    def _issue(self, key, subject, body, meta=None):
        if not (self.live and getattr(self.cfg, "alerts_enabled", True)):
            return False
        try:
            import notify
            return notify.issue_open(key, subject, body, meta=meta)
        except Exception as e:
            log.warning("status issue %s failed: %s", key, e)
            return False

    def _escalate_held(self, count, top):
        """[v2.14] The Oct 2026 hold was flagged within 7 hours and posted 26
        times, and nobody acted: a yellow thread in a busy channel is easy to
        miss. Past `held_escalate_count` held orders, post a red thread that
        tags the named owners (and email it when email is configured). Keyed
        per day, so it repeats daily while the hold lasts, once per day even
        across restarts; the live settle step closes it when the hold clears.
        [v2.14.1] Runs from every held give-up and from periodic(): the live sync
        puts every order it holds in its product's thread, and the give-up for
        such an order used to return before ever getting here. The email goes
        once per day's thread, with no Slack mention markup in its subject,
        and also when Slack refused the thread (email is then the only way the
        owners hear of it). A day's thread that closed is not reopened that
        day, and a failed email waits notify's failed backoff before the next
        try."""
        threshold = int(getattr(self.cfg, "held_escalate_count", 0) or 0)
        if not threshold or count < threshold:
            return
        if not (self.live and getattr(self.cfg, "alerts_enabled", True)):
            return
        mentions = " ".join(f"<@{u}>" for u in (getattr(self.cfg, "escalation_mentions", None) or []))
        headline = f"{count} orders are on hold and not in HubSpot. This needs a person today."
        subject = f"🔴 {mentions + ' ' if mentions else ''}{headline}"
        body = st.message(
            f"The catalog hold has passed {threshold} orders. Nothing is lost (the orders wait safely and "
            "sync by themselves once the cause is fixed), but every hour adds more."
            + (f" Products holding the most orders: {top}." if top else ""),
            ["Open each blocking product's own thread in this channel and follow its fix.",
             "If nobody on your side can act on it today, reply here and the integration team will."],
            [])
        key = f"held-escalation:{datetime.now().strftime('%Y-%m-%d')}"
        try:
            import notify
            rec = notify.issue_get(key) or {}
            if rec.get("state") in ("closed", "resolving"):
                # [v2.14.1] closed today (the live settle's all-clear, or its
                # reply still waiting to post): never reopened the same day.
                # This count included "held" rows of orders the catalog drain
                # has finished with, which the settle leaves out, so a reopen
                # came back every hour with a red thread and an email for a
                # hold that had cleared. The settle now marks those rows gone;
                # this holds when a mark does not reach the sheet. A hold
                # still there tomorrow escalates on tomorrow's key.
                return
            opened = notify.issue_open(key, subject, body, meta={"count": count}, email=False)
            rec = notify.issue_get(key) or {}
            m = rec.get("meta") or {}
            if ((opened or rec.get("state") == "failed") and not m.get("emailed")
                    and notify.email_enabled() and self._email_due(m)):
                # [v2.14.1] the attempt is recorded before the send: with Slack
                # down and SMTP unreachable (one try can block for
                # SMTP_TIMEOUT_S), every held give-up tried again and stalled
                # the relay
                self._escalation_meta(key, {"email_tried_at": time.time(),
                                            "email_tries": int(m.get("email_tries") or 0) + 1})
                if notify.send_email(f"🔴 {headline}", body):
                    # an open or failed key keeps this, and so does the thread
                    # retry_failed() posts later, so the email never goes twice
                    self._escalation_meta(key, {"emailed": True})
        except Exception as e:
            log.warning("held escalation failed: %s", e)

    @staticmethod
    def _email_due(meta):
        """[v2.14.1] The escalation email's next try, on notify's failed
        backoff: 5, 10, 20, 40 minutes after each failed one, capped."""
        import notify
        n = int(meta.get("email_tries") or 0)
        if not n:
            return True
        wait = notify._retry_delay(n)     # the one backoff a refused thread uses too
        return time.time() - float(meta.get("email_tried_at") or 0) >= wait

    @staticmethod
    def _escalation_meta(key, meta):
        """[v2.14.1] Merge into the day's escalation record and never post.
        issue_update merges into an open or failed record and, with a throttle
        that never runs out, posts nothing; it leaves a closed record alone,
        where issue_open would post a fresh red thread if the live settle had
        closed the key a moment earlier."""
        import notify
        notify.issue_update(key, "", every_s=float("inf"), meta=meta)

    def _giveup_alert(self, kind, oid, reference_id, slug, attempts):
        """[v2.13] Every give-up becomes (or joins) a Slack thread that is
        answered when the order finally exists: held orders join their
        product's thread, a missing live order gets its own. [v2.14.1] With
        classification off, give-ups share one thread (_unclassified_giveup)."""
        import notify
        ref = reference_id or oid
        exceptions = self._tab_link(self.cfg.spreadsheet_id, EXCEPTIONS_TAB, "Delivery Status Exceptions tab")
        if kind == "held":
            if not (self.live and getattr(self.cfg, "alerts_enabled", True)):
                return
            self._refresh_live_index()
            count, top = self._held_summary()
            joined = False
            try:
                for key, rec in notify.issues_open("held:").items():
                    if str(oid) in ((rec.get("meta") or {}).get("orders") or []):
                        notify.issue_update(key, f"Order #{ref} also got a delivery update (“{slug}”). It "
                                                 "applies by itself once the order is created.", every_s=6 * 3600)
                        joined = True
                        break
            except Exception as e:
                # [v2.14.1] a registry fault must not turn the give-up into a
                # retried error that appends an exceptions row on every attempt
                log.warning("held thread lookup for order %s failed: %s", oid, e)
            vs = getattr(self.cfg, "validation_spreadsheet_id", "") or st.VALIDATION_SHEET
            if not joined and not self._issue(
                    "held-backlog",
                    f"🟡 {st.plural(count, 'order')} on hold for catalog approval "
                    f"{'is' if count == 1 else 'are'} getting delivery updates",
                    st.message(
                        "These orders include products that are not approved in the catalog, so they are "
                        "not in HubSpot yet and their delivery updates cannot apply. Nothing is lost: when "
                        "an order is created it gets its latest status."
                        + (f" Products holding the most orders: {top}." if top else ""),
                        # [v2.14] point at each product's own thread: it carries the
                        # fix for THAT cause. "Approve the product" was wrong for the
                        # Oct 2026 hold, where the product was already approved.
                        ["Each product holding orders has its own thread in this channel with the exact "
                         "fix for its case. Follow that thread.",
                         "If a product is already approved and its orders are still on hold, do not approve "
                         "it again: its thread explains the real cause (usually a \"bundle\" tag in Salla).",
                         f"Approvals themselves happen on the {st.sheet(vs, 'Bundle & Product Validation sheet')}. "
                         "This thread confirms when no order is left on hold."],
                        [exceptions])):
                try:
                    notify.issue_update("held-backlog", f"Now {st.plural(count, 'order')} on hold. Latest: order "
                                                        f"#{ref} got “{slug}”.", every_s=6 * 3600)
                except Exception as e:
                    log.warning("held-backlog update failed: %s", e)
            # [v2.14.1] checked on every held give-up, including an order that
            # joined its product's thread above (the live sync puts every order
            # it holds in one, and that early return used to skip this for good)
            self._escalate_held(count, top)
            return
        if kind == "backfill":
            self._digest.append((oid, slug))
            self._flush_digest()
            return
        if kind is None:
            self._unclassified_giveup(oid, ref, slug, exceptions)
            return
        # "recent": a live order whose creation was lost
        self._issue(
            f"status-missing:{oid}",
            f"🔴 Order #{ref} is getting delivery updates but is not in HubSpot",
            st.message(
                f"Salla has sent delivery updates for order #{ref} (latest: “{slug}”) for about "
                f"{max(1, round(sum(self.ladder) / 3600))} hours, but the order itself never reached HubSpot. "
                "It is not on hold for the catalog, so its creation was most likely lost on the way in.",
                ["Nothing for now: the integration team is alerted and will re-create it.",
                 "This thread confirms when the order is in HubSpot. Its latest status is applied then."],
                [exceptions, st.hubspot_search(oid, "Search HubSpot for it")]),
            meta={"ref": str(ref), "slug": slug})

    def _unclassified_giveup(self, oid, ref, slug, exceptions):
        """[v2.14.1] Classification off (live_min_reference unset): the relay
        cannot tell a held order from a lost one. A red thread per order (as
        "recent" gets) would open one per held order, about 1,700 in a hold
        like Oct 2026's, each saying "not on hold". So these share one thread
        with neutral wording; later orders join its meta and post news at most
        every 6 hours. periodic() closes it after a quiet day."""
        if not (self.live and getattr(self.cfg, "alerts_enabled", True)):
            return
        try:
            import notify
            rec = notify.issue_get(UNCLASSIFIED_KEY) or {}
            m = (rec.get("meta") or {}) if rec.get("state") in ("open", "opening", "failed") else {}
            orders = [str(o) for o in (m.get("orders") or [])]
            seen = str(oid) in orders
            # past the cap orders are counted but not listed (an order with
            # several updates may then count twice)
            n = int(m.get("count") or len(orders)) + (0 if seen else 1)
            meta = {"count": n, "last": time.time()}
            if not seen and len(orders) < UNCLASSIFIED_KEEP:
                meta.update(orders=[str(oid)], refs=[str(ref)])
            if notify.issue_open(
                    UNCLASSIFIED_KEY,
                    "🟠 Delivery updates are arriving for orders that are not in HubSpot",
                    st.message(
                        f"Salla has sent delivery updates for order #{ref} (latest: “{slug}”) for about "
                        f"{max(1, round(sum(self.ladder) / 3600))} hours, but the order is not in HubSpot. It may "
                        "be on hold for the catalog, waiting for older orders to be imported, or lost on the way "
                        "in. Nothing is lost for an order that is created later: it gets its latest status then.",
                        ["Nothing for now: the integration team is alerted and checks these orders.",
                         "Other orders like this join this thread, with news at most every 6 hours."],
                        [exceptions]),
                    meta=meta):
                return
            notify.issue_update(UNCLASSIFIED_KEY, f"Order #{ref} also got a delivery update (“{slug}”) and "
                                                  f"is not in HubSpot. {st.plural(n, 'order')} like this so far.",
                                every_s=6 * 3600, meta=meta)
        except Exception as e:
            log.warning("status issue %s failed: %s", UNCLASSIFIED_KEY, e)

    def periodic(self):
        """[v2.13] Every 10 minutes: answer "unmapped status" threads whose
        status now has a stage (the mapping was added and the relay restarted).
        [v2.14.1] Also check the held escalation, so a hold escalates even when
        no delivery update gives up, close the shared "not in HubSpot"
        thread once a day passes with nothing new, and post what Slack
        refused earlier (notify.retry_failed)."""
        if time.time() - getattr(self, "_periodic_at", 0) < 600:
            return
        self._periodic_at = time.time()
        if not self.live:
            return
        try:
            import notify
            for key in notify.issues_open("unmapped:"):
                slug = key.split(":", 1)[1]
                if self.stage_map.get(slug):
                    notify.issue_resolve(key, f"✅ Resolved: the Salla status “{slug}” now moves orders to a "
                                              "HubSpot stage. New updates with this status apply by themselves.")
        except Exception as e:
            log.warning("status periodic failed: %s", e)
        if not getattr(self.cfg, "alerts_enabled", True):
            return
        if int(getattr(self.cfg, "held_escalate_count", 0) or 0) > 0:
            # the Live Queue read is cached for held_index_refresh_s, and the
            # escalation key is per day, so a lasting hold posts once a day
            self._refresh_live_index()
            self._escalate_held(*self._held_summary())
        try:
            import notify
            m = (notify.issues_open(UNCLASSIFIED_KEY).get(UNCLASSIFIED_KEY) or {}).get("meta") or {}
            if m and time.time() - float(m.get("last") or 0) > UNCLASSIFIED_QUIET_S:
                notify.issue_resolve(UNCLASSIFIED_KEY, "✅ Closed: no new delivery updates for orders missing "
                                                       "from HubSpot in the last day. An order listed here that "
                                                       "is created later gets its latest status then.")
        except Exception as e:
            log.warning("status periodic close of %s failed: %s", UNCLASSIFIED_KEY, e)
        try:
            import notify
            # [v2.14.1] a thread or reply Slack refused (from any service) is
            # posted only from a main loop, never by the caller that raised it:
            # this pass, the live settle and the credit watch are those loops
            notify.retry_failed()
        except Exception as e:
            log.warning("status periodic retry of refused threads failed: %s", e)

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
            self._issue(
                f"unmapped:{slug}",
                f"🟠 Salla sent a delivery status the sync does not know: “{slug}”",
                st.message(
                    f"Order #{row.get('reference_id') or oid} moved to the Salla status “{slug}”. This status "
                    "has no matching stage in HubSpot yet, so the order's stage was not changed. Every order "
                    "that gets this status is listed on the Delivery Status Exceptions tab.",
                    ["Reply in this thread with the HubSpot stage this status should use (for example "
                     "Processed, Shipped or Delivered).",
                     "The integration team adds it. This thread confirms when it is live, and new updates "
                     "then apply by themselves."],
                    [self._tab_link(self.cfg.spreadsheet_id, EXCEPTIONS_TAB, "Delivery Status Exceptions tab")]))
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
            self._issue(
                f"zid:{oid}",
                f"🟠 Order #{row.get('reference_id') or oid} is not in HubSpot yet: an old Zid order uses its number",
                st.message(
                    f"Salla order {oid} got the status “{slug}”, but the only HubSpot order with that number is "
                    "an imported Zid order, so the Salla order has not been created yet. Nothing was changed on "
                    "the Zid order.",
                    "Nothing to do: the sync moves the Zid order to its own number and creates the Salla "
                    "order. This thread confirms when it is in HubSpot.",
                    [st.hubspot("order", zid, "Zid order in HubSpot")]),
                meta={"zid_hs_id": str(zid)})
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
            if getattr(self.cfg, "alerts_enabled", True):
                try:
                    import notify
                    notify.issue_resolve(f"status-missing:{oid}",
                                         f"✅ Resolved: order #{row.get('reference_id') or oid} is in HubSpot now "
                                         f"({st.hubspot('order', hs_id, 'open it')}) and its stage is “{slug}”.")
                except Exception as e:
                    log.debug("status-missing resolve failed: %s", e)
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
