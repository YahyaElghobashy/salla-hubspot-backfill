#!/usr/bin/env python3
"""Re-hydrate contacts the customer sync created from an unreadable payload.

On 2026-09-22 22:53 Riyadh the capture scenario's payload template was
edited and, for a window of minutes, wrote a string the engine could not
parse. customer_sync.payload() returned {} for those rows, so every contact
created in the window carries the phone and nothing else: no name, no city,
no salla_customer_id, no email. The engine log still names each pair:

    CUSTOMER created <salla_customer_id> -> contact <hubspot_contact_id>

This tool takes those pairs, fetches each customer through the relay
(Merchant API, customers/{id}), rebuilds the exact property set
customer_sync.props_from() would have written, and PATCHes it onto the
existing contact. Nothing is created, nothing is merged, and a contact that
already carries a salla_customer_id is left alone.

    python3 tools/repair_customer_contacts.py --since "2026-09-22 22:54:30"          # dry run
    python3 tools/repair_customer_contacts.py --since "2026-09-22 22:54:30" --apply

Ledger: mirror/customer_contact_repairs.csv (old values kept).
"""

import argparse
import csv
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backfill import Config, HubSpot, RelayClient, apply_portal_config, now_str, setup_logging
from customer_payload import API_FIELDS, from_api, phone_of
from customer_sync import CustomerSync
from realtime_base import trim_lock_active

log = logging.getLogger("backfill")

LEDGER = Path("mirror/customer_contact_repairs.csv")
LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ .*CUSTOMER created (\d+) -> contact (\d+)")
CHECK_PROPS = "firstname,lastname,city,salla_customer_id,incorrect_email,salla_consent_status,phone"


def pairs_from_log(path, since, until):
    out = []
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = LINE.match(line)
            if not m:
                continue
            ts, cid, contact = m.groups()
            if ts < since or (until and ts > until):
                continue
            out.append((ts, cid, contact))
    return out


def pairs_from_contacts(contacts, ledger_path, log_path):
    """[v2.11] Explicit contacts -> (ts, salla_customer_id, contact). The
    engine's customer ledger is read first (it survives log rotation); the
    log's "CUSTOMER created <sid> -> contact <id>" lines second."""
    want = {str(c).strip() for c in contacts if str(c).strip()}
    found = {}
    lp = Path(ledger_path)
    if lp.exists():
        with open(lp, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if str(row.get("contact_id")) in want:
                    found.setdefault(str(row["contact_id"]),
                                     (row.get("ts", ""), str(row["salla_customer_id"])))
    missing = want - set(found)
    if missing and Path(log_path).exists():
        for ts, sid, contact in pairs_from_log(log_path, "", ""):
            if contact in missing:
                found.setdefault(contact, (ts, sid))
    for c in sorted(want - set(found)):
        log.error("  contact %s: no Salla id in the ledger or the log -- skipped", c)
    return [(ts, sid, contact) for contact, (ts, sid) in sorted(found.items())]


def ledger_write(rows):
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "contact_id", "salla_customer_id", "old_firstname",
                        "old_salla_customer_id", "props_written"])
        for r in rows:
            w.writerow([now_str()] + r)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--log", default="customer_sync.log")
    ap.add_argument("--since", default="", help='"YYYY-MM-DD HH:MM:SS" (engine local time)')
    ap.add_argument("--contacts", default="", help="explicit contact ids, comma-separated (v2.11)")
    ap.add_argument("--customers-ledger", default="mirror/customers.csv")
    ap.add_argument("--until", default="", help="optional upper bound, same format")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    if not args.since and not args.contacts:
        ap.error("give --since (log window) or --contacts")

    setup_logging(args.verbose, logfile="repair_customer_contacts.log")
    if trim_lock_active():
        sys.exit("a queue trim is running (mirror/trim.lock): try again later")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not tok or not secret:
        sys.exit("HUBSPOT_ACCESS_TOKEN / RELAY_SECRET missing")
    hs = HubSpot(cfg, tok, live=args.apply)
    relay = RelayClient(cfg, secret)

    if args.contacts:
        pairs = pairs_from_contacts(args.contacts.split(","), args.customers_ledger, args.log)
    else:
        pairs = pairs_from_log(args.log, args.since, args.until)
    log.info("%s -- %d contact(s) to check", "APPLY" if args.apply else "DRY RUN", len(pairs))
    written = []
    for ts, cid, contact in pairs:
        st, cur = hs._req("GET", f"/crm/v3/objects/contacts/{contact}?properties={CHECK_PROPS}",
                          what="read contact")
        if st != 200:
            log.error("  %s contact %s: HTTP %s -- skipped", ts, contact, st)
            continue
        p = cur.get("properties") or {}
        if (p.get("salla_customer_id") or "").strip():
            log.info("  contact %s already carries salla_customer_id %s -- untouched",
                     contact, p.get("salla_customer_id"))
            continue
        try:
            raw = relay.get_path(f"customers/{cid}?{API_FIELDS}").get("data") or {}
        except Exception as e:
            log.error("  customer %s: relay failed (%s) -- skipped", cid, e)
            continue
        if str(raw.get("id") or "") != str(cid):
            log.error("  customer %s: relay returned id %r -- skipped", cid, raw.get("id"))
            continue
        c = from_api(raw)                   # [v2.11] includes the consent flag
        phone = phone_of(c)
        props = CustomerSync.props_from(c, phone)
        keep_phone = (p.get("phone") or "").strip()
        if keep_phone and props.get("phone") and props["phone"] != keep_phone:
            log.warning("  contact %s: phone on record %s differs from Salla %s -- keeping record phone",
                        contact, keep_phone, props["phone"])
            for k in ("phone", "mobilephone", "main_phone_number"):
                props.pop(k, None)
        log.info("  %s contact %s <- customer %s: %s",
                 "PATCH" if args.apply else "WOULD PATCH", contact, cid, sorted(props))
        if not args.apply:
            continue
        st, _ = hs._write("PATCH", f"/crm/v3/objects/contacts/{contact}", {"properties": props},
                          "repair contact")
        if st not in (200, 201):
            log.error("        PATCH failed HTTP %s", st)
            continue
        st, back = hs._req("GET", f"/crm/v3/objects/contacts/{contact}?properties={CHECK_PROPS}",
                           what="readback")
        q = (back.get("properties") or {}) if st == 200 else {}
        ok = q.get("salla_customer_id") == str(cid)
        log.info("        readback salla_customer_id=%s firstname=%s -> %s",
                 q.get("salla_customer_id"), "set" if q.get("firstname") else "EMPTY",
                 "OK" if ok else "MISMATCH")
        if ok:
            written.append([contact, cid, p.get("firstname") or "", p.get("salla_customer_id") or "",
                            ";".join(sorted(props))])
    if written:
        ledger_write(written)
        log.info("ledger: %s (+%d)", LEDGER, len(written))
    log.info("done: %d repaired", len(written))


if __name__ == "__main__":
    main()
