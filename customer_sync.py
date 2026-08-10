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

Row contract (A:H): A received_at | B customer_id | C phone (code+mobile)
  | D "customer.created" | E state | F attempts | G source | H payload JSON

Usage:
    python3 customer_sync.py --live
    python3 customer_sync.py --once --dry
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from backfill import Config, HubSpot, GoogleIO, now_str, setup_logging
from realtime_base import RealtimeConsumer, TabLock

log = logging.getLogger("backfill")

LEDGER = Path("mirror/customers.csv")
MERGES = Path("mirror/contact_merges.csv")


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

    def __init__(self, cfg, hs, gio, live=True):
        super().__init__(cfg, hs, gio, tab=cfg.customer_queue_tab, live=live)
        self.ledger = CustomerLedger()
        self.auto_merge = bool(getattr(cfg, "customer_auto_merge", True))

    # -- payload ----------------------------------------------------------------

    @staticmethod
    def payload(row):
        try:
            return json.loads(row.get("note") or "{}")
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def props_from(c, phone):
        """The blueprint's property set, verbatim where it matters."""
        p = {"firstname": c.get("first_name") or "",
             "lastname": c.get("last_name") or "",
             "city": c.get("city") or "",
             "gender": c.get("gender") or "",
             "hs_language": c.get("lang") or "",
             "phone": phone, "mobilephone": phone,
             "main_phone_number": phone,
             "salla_customer_id": str(c.get("id") or ""),
             "incorrect_email": c.get("email") or "",
             "customer_location": c.get("location") or ""}
        bday = str(c.get("birthday") or "")
        if bday:
            # blueprint: substring(...) of the {date:...} struct; capture side
            # normalizes to ISO already, keep defensive slice for raw structs
            p["date_of_birth"] = bday[9:19] if "date" in bday else bday[:10]
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
        c = self.payload(row)
        cid = row["order_id"]          # generic entity-id column = customer id
        phone = row["reference_id"] or (
            f"{c.get('mobile_code', '')}{c.get('mobile', '')}")
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
                return "error", f"create failed HTTP {status}"
            contact_id = str(data.get("id"))
            self.ledger.record(cid, contact_id, "created")
            log.info("CUSTOMER created %s -> contact %s", cid, contact_id)
            return "done", f"created contact {contact_id}"

        if len(hits) >= 2:
            if self.auto_merge:
                if not self._merge(hits[0], hits[1], cid):
                    return "error", "merge failed"
            else:
                try:
                    import notify
                    notify.send_alert(
                        "🖐 Duplicate contacts need a human",
                        f"Salla customer {cid} matches {len(hits)} contacts "
                        f"(auto-merge is OFF). Most recent two: "
                        f"{hits[0].get('id')} / {hits[1].get('id')}.")
                except Exception:
                    pass

        target = str(hits[0].get("id"))
        status, _ = self.hs._write(
            "PATCH", f"/crm/v3/objects/contacts/{target}",
            {"properties": props}, "contact update")
        if status not in (200, 201):
            return "error", f"update failed HTTP {status}"
        self.ledger.record(cid, target, "updated")
        log.info("CUSTOMER updated %s -> contact %s", cid, target)
        return "done", f"updated contact {target}"


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
    sync = CustomerSync(cfg, hs, gio, live=args.live)
    sync.run(once=args.once)


if __name__ == "__main__":
    main()
