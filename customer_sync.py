#!/usr/bin/env python3
"""Customer Sync (v2.7, sub-product B).

Creates or updates a HubSpot contact for every Salla `customer.created`
event, from the `Customer Queue` tab the thin-capture Make scenario appends
to. Replicates the blueprint's behavior -- including its property choices and
its auto-merge of duplicates (operator decision 2026-08-10) -- with the
engine's guarantees the Make version lacked: a durable queue, retries, an
append-only ledger, a merge audit trail, and Slack visibility.

Blueprint parity notes (deliberate, not accidents):
  * `incorrect_email` <- the Salla email. Store-generated addresses are often
    fake, so the real email property is left for verified sources.
  * `lifecyclestage` = lead on create.
  * Duplicate search = phone OR code+phone OR main_phone_number OR
    salla_customer_id, five most recent.
  * 2+ hits -> merge the two most recent (most recent is primary), then
    update. Additions over the blueprint: every merge lands in
    mirror/contact_merges.csv and posts a Slack info line, and
    config.customer_auto_merge=false turns merging into a needs-human alert
    without touching the rest of the pipeline.

Row contract (A:I): A received_at | B customer_id | C phone (code+mobile)
  | D "customer.created" | E state | F attempts | G source | H payload JSON
  | I note for a non-final outcome (v2.11; H keeps the payload until done)

v2.11 (2026-09-26): the payload is read in three steps, first success wins:
plain JSON, a salvage of the capture's text template (a quote or line break
in an address used to break it), then one Salla lookup. A row is only parked
as held when all three fail, or after cfg.realtime_max_attempts errors; the
cursor walks past held rows and tools/drain_held_customers.py drains them.
Rows from the daily sweep, and rows drained by a tool, never auto-merge.

Usage:
    python3 customer_sync.py --config config.live.json --live
    python3 customer_sync.py --config config.live.json --once   # dry run
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from backfill import (Config, HubSpot, GoogleIO, RelayClient, consent_status,
                      now_str, setup_logging)
from customer_payload import API_FIELDS, clean_text, from_api, phone_of, salvage_template
from realtime_base import RealtimeConsumer, TabLock

log = logging.getLogger("backfill")

LEDGER = Path("mirror/customers.csv")
MERGES = Path("mirror/contact_merges.csv")
# Rows whose source is one of these never auto-merge: a sweep or a drain can
# touch many customers at once, and HubSpot merges cannot be undone.
NO_MERGE_SOURCES = ("sweep", "drain")


class CustomerLedger:
    """salla_customer_id -> hubspot contact id, append-only."""

    def __init__(self, path=LEDGER):
        self.path = Path(path)
        self.map = {}
        if self.path.exists():
            try:
                with open(self.path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        self.map[str(row["salla_customer_id"])] = \
                            str(row["contact_id"])
            except OSError as e:
                log.warning("customer ledger unreadable: %s", e)

    def get(self, cid):
        return self.map.get(str(cid))

    def record(self, cid, contact_id, action):
        new = not self.path.exists()
        self.path.parent.mkdir(exist_ok=True)
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "salla_customer_id", "contact_id", "action"])
            w.writerow([now_str(), cid, contact_id, action])
        self.map[str(cid)] = str(contact_id)


class CustomerSync(RealtimeConsumer):
    name = "customers"
    # [v2.11] column H is the capture payload: keep it until the row is done
    payload_in_h = True
    # [v2.11] an error that repeats settles as held (drainable), not error-final
    exhausted_state = "held"
    trimmable = ("done", "superseded")
    trim_days_attr = "customer_trim_days"
    trim_hour_offset = 1

    def __init__(self, cfg, hs, gio, live=True, relay=None):
        super().__init__(cfg, hs, gio, tab=cfg.customer_queue_tab, live=live)
        self.ledger = CustomerLedger()
        self.auto_merge = bool(getattr(cfg, "customer_auto_merge", True))
        self.relay = relay
        self.lookup_timeout = float(getattr(cfg, "customer_lookup_timeout_s", 20.0))
        self.no_merge = False          # tools set this; see NO_MERGE_SOURCES

    # -- payload ----------------------------------------------------------------

    @staticmethod
    def payload(row):
        try:
            return json.loads(row.get("note") or "{}")
        except json.JSONDecodeError:
            return {}

    def read_payload(self, row):
        """[v2.11] (payload, path, reason). path is "json", "salvaged" or
        "looked up" on success; "lookup_failed" when Salla could not be read
        (retryable); None when there is nothing to work with."""
        raw = str(row.get("note") or "").strip()
        cid = str(row.get("order_id") or "").strip()
        if raw.startswith("{"):
            try:
                c = json.loads(raw)
                if isinstance(c, dict) and str(c.get("id") or "").strip():
                    return c, "json", ""
            except json.JSONDecodeError:
                pass
            c = salvage_template(raw, cid=cid or None,
                                 phone=row.get("reference_id") or None)
            if c:
                return c, "salvaged", ""
        if not cid:
            return None, None, "row has no customer id"
        if self.relay is None:
            return None, None, "payload unreadable and no Salla lookup configured"
        try:
            env = self.relay.get_path_once(f"customers/{cid}?{API_FIELDS}",
                                           timeout=self.lookup_timeout)
        except Exception as e:
            return None, "lookup_failed", f"payload unreadable; Salla lookup failed: {e}"[:170]
        data = env.get("data") if env.get("status") == 200 else None
        if isinstance(data, dict) and str(data.get("id") or "") == cid:
            return from_api(data), "looked up", ""
        return None, "lookup_failed", (f"payload unreadable; Salla returned no customer "
                                       f"{cid} (status {env.get('status')})")

    @staticmethod
    def props_from(c, phone):
        """The blueprint's property set, verbatim where it matters."""
        p = {"firstname": clean_text(c.get("first_name")),
             "lastname": clean_text(c.get("last_name")),
             "city": clean_text(c.get("city")),
             "gender": clean_text(c.get("gender")),
             "hs_language": clean_text(c.get("lang")),
             "phone": phone, "mobilephone": phone,
             "main_phone_number": phone,
             "salla_customer_id": str(c.get("id") or ""),
             "incorrect_email": clean_text(c.get("email")),
             "customer_location": clean_text(c.get("location"), sep=", ")}
        # [v2.10] Salla `is_notifications_enabled` -> salla_consent_status, on
        # create and on update alike. Only when the capture forwarded the flag:
        # an absent key leaves whatever the property already holds.
        consent = consent_status(c)
        if consent is not None:
            p["salla_consent_status"] = consent
        dob = birth_date(c.get("birthday"))
        if dob:
            p["date_of_birth"] = dob
        return {k: v for k, v in p.items() if v != ""}

    # -- search (blueprint parity) ------------------------------------------------

    def _search_contacts(self, cid, mobile, phone):
        groups = []
        if mobile:
            groups.append({"filters": [{"propertyName": "phone",
                                        "operator": "EQ", "value": mobile}]})
        if phone:
            groups.append({"filters": [{"propertyName": "phone",
                                        "operator": "EQ", "value": phone}]})
            groups.append({"filters": [{"propertyName": "main_phone_number",
                                        "operator": "EQ", "value": phone}]})
        groups.append({"filters": [{"propertyName": "salla_customer_id",
                                    "operator": "EQ", "value": str(cid)}]})
        d = self.hs.search("/crm/v3/objects/contacts/search", {
            "filterGroups": groups,
            "sorts": [{"propertyName": "createdate",
                       "direction": "DESCENDING"}],
            "properties": ["hs_object_id", "firstname", "lastname", "phone",
                           "salla_customer_id", "createdate"],
            "limit": 5}, "customer dup search")
        return d.get("results") or []

    # -- merge -------------------------------------------------------------------

    def _audit_merge(self, primary, secondary, cid, ok, detail=""):
        new = not MERGES.exists()
        MERGES.parent.mkdir(exist_ok=True)
        with open(MERGES, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "primary_id", "merged_id",
                            "salla_customer_id", "result", "detail"])
            w.writerow([now_str(),
                        primary.get("id"), secondary.get("id"), cid,
                        "merged" if ok else "failed", detail[:160]])

    def _merge(self, primary, secondary, cid):
        status, data = self.hs._write(
            "POST", "/crm/v3/objects/contacts/merge",
            {"primaryObjectId": str(primary.get("id")),
             "objectIdToMerge": str(secondary.get("id"))},
            "contact merge")
        ok = status in (200, 201)
        self._audit_merge(primary, secondary, cid, ok,
                          "" if ok else json.dumps(data)[:150])
        try:
            import notify
            p1 = primary.get("properties") or {}
            p2 = secondary.get("properties") or {}
            notify.send_alert(
                "🔗 Duplicate contacts merged automatically",
                f"Two contacts matched the same customer (Salla id {cid}) "
                f"and were merged, per the standing auto-merge policy.\n\n"
                f"• Kept:   {p1.get('firstname','')} {p1.get('lastname','')} "
                f"(HubSpot {primary.get('id')})\n"
                f"• Merged: {p2.get('firstname','')} {p2.get('lastname','')} "
                f"(HubSpot {secondary.get('id')})\n\n"
                f"Audit trail: mirror/contact_merges.csv. If this pairing "
                f"looks wrong, HubSpot merges are NOT reversible -- flag it "
                f"today.")
        except Exception as e:
            log.warning("merge notify failed: %s", e)
        return ok

    # -- core ---------------------------------------------------------------------

    def handle_row(self, row):
        cid = row["order_id"]          # generic entity-id column = customer id
        c, path, why = self.read_payload(row)
        if c is None:
            if path == "lookup_failed":
                # retried on the slow cadence; after the cap _settle parks it
                return "error", why
            log.error("CUSTOMER row %s (%s): %s -- held", row.get("row"), cid, why)
            self._held_alert(cid, why)
            return "held", why
        # [v2.12] every path logs its marker, "CUSTOMER payload json <cid>"
        # included: the daily digest counts these lines per path
        log.info("CUSTOMER payload %s %s", path, cid)
        tag = "" if path == "json" else f" ({path})"
        phone = row["reference_id"] or phone_of(c)
        mobile = str(c.get("mobile") or "")

        known = self.ledger.get(cid)
        if known:
            return "superseded", f"already synced -> contact {known}"

        hits = self._search_contacts(cid, mobile, phone)
        props = self.props_from(c, phone)

        if not hits:
            status, data = self.hs._write(
                "POST", "/crm/v3/objects/contacts",
                {"properties": {**props, "lifecyclestage": "lead",
                                "consent_date": datetime.now().strftime(
                                    "%Y-%m-%d")}},
                "contact create")
            if status not in (200, 201):
                return "error", f"create failed HTTP {status}: {_err(data)}"
            contact_id = str(data.get("id"))
            if self.live:
                self.ledger.record(cid, contact_id, "created")
            log.info("CUSTOMER created %s -> contact %s", cid, contact_id)
            return "done", f"created contact {contact_id}{tag}"

        if len(hits) >= 2:
            may_merge = (self.auto_merge and not self.no_merge
                         and str(row.get("source") or "") not in NO_MERGE_SOURCES)
            if not self.live:
                log.info("DRY RUN %s %s <- %s (customer %s)",
                         "would merge" if may_merge else "would NOT merge (alert)",
                         hits[0].get("id"), hits[1].get("id"), cid)
            elif may_merge:
                if not self._merge(hits[0], hits[1], cid):
                    return "error", "merge failed"
            else:
                self._alert("🖐 Duplicate contacts need a human",
                            f"Salla customer {cid} matches {len(hits)} contacts "
                            f"and this row may not auto-merge (auto-merge "
                            f"{'on' if self.auto_merge else 'off'}, source "
                            f"{row.get('source') or 'unknown'}). Most recent two: "
                            f"{hits[0].get('id')} / {hits[1].get('id')}. The "
                            f"most recent one was updated.")

        target = str(hits[0].get("id"))
        status, data = self.hs._write(
            "PATCH", f"/crm/v3/objects/contacts/{target}",
            {"properties": props}, "contact update")
        if status not in (200, 201):
            return "error", f"update failed HTTP {status}: {_err(data)}"
        if self.live:
            self.ledger.record(cid, target, "updated")
        log.info("CUSTOMER updated %s -> contact %s", cid, target)
        return "done", f"updated contact {target}{tag}"

    # -- v2.11 alerts -------------------------------------------------------------

    def _alert(self, subject, body):
        """Alerts only from a live run with alerts enabled; never raises."""
        if not self.live or not getattr(self.cfg, "alerts_enabled", True):
            return
        try:
            import notify
            notify.send_alert(subject, body)
        except Exception as e:
            log.warning("customer alert failed: %s", e)

    def _held_alert(self, cid, why):
        self._alert(
            "🖐 Customer row parked (held)",
            f"Salla customer {cid}: {why}. No contact was created from this "
            f"row; its payload is kept in the Customer Queue. Once the cause "
            f"is fixed, run tools/drain_held_customers.py (dry run first).")

    def on_exhausted(self, row, note):
        self._held_alert(row.get("order_id"), note)


def _err(data):
    """HubSpot's own error text, short, for the queue note."""
    if isinstance(data, dict):
        msg = data.get("message") or data.get("raw") or ""
        if msg:
            return str(msg)[:120]
    return json.dumps(data)[:120] if data else ""


def birth_date(value):
    """[v2.11] YYYY-MM-DD from any birthday shape Salla or the capture sends
    (text, the {"date": ...} struct, or its JSON text), or "" when it is not a
    real date (HubSpot rejects impossible dates and the row would retry)."""
    if isinstance(value, dict):
        value = value.get("date") or ""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", str(value or ""))
    if not m:
        return ""
    try:
        d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return ""
    if not 1900 <= d.year <= datetime.now().year:
        return ""
    return d.strftime("%Y-%m-%d")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--live", action="store_true",
                    help="write to HubSpot/sheets (omit for dry-run)")
    ap.add_argument("--once", action="store_true", help="one poll cycle")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="customer_sync.log")
    cfg = Config.load(args.config)
    if not getattr(cfg, "customer_sync_enabled", True):
        sys.exit("customer_sync_enabled=false in config -- not starting.")
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    TabLock("customer_sync").acquire()
    hs = HubSpot(cfg, token, live=args.live)
    gio = GoogleIO(cfg, enabled=True)
    secret = (os.environ.get("RELAY_SECRET") or "").strip()
    relay = RelayClient(cfg, secret) if secret and cfg.relay_url else None
    if relay is None:
        log.warning("CUSTOMERS no Salla lookup (RELAY_SECRET or relay_url "
                    "missing): unreadable payloads will be held")
    sync = CustomerSync(cfg, hs, gio, live=args.live, relay=relay)
    sync.run(once=args.once)


if __name__ == "__main__":
    main()
