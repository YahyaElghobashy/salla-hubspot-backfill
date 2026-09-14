#!/usr/bin/env python3
"""Gift address refresh (v2.9, sub-product of the gift-orders feature).

A "buy as gift" order is created before the receiver has confirmed their
delivery address: the buyer checks out, Salla sends the receiver a
confirmation link, and the address lands on the order minutes-to-days later.
Order creation is forward-only by design, so nothing else in the engine ever
re-reads an order -- this module is the one component that does, closing the
lifecycle:

  awaiting confirmation  (gift_address_incomplete=true, no gift_address_state)
      -> confirmed            receiver confirmed: flag clears, receiver +
                              shipping properties re-baked from fresh Salla
      -> expired_unconfirmed  the link lapsed unanswered: honest terminal
                              state + ONE aggregated Slack chase alert

Each run is one bounded cycle (the systemd timer provides the cadence):

  1. HubSpot search for the working set: is_gift_order=true AND
     gift_address_incomplete=true AND gift_address_state is empty. Terminal
     orders leave the set by property, so the search converges to zero.
  2. Hydrate up to gift_refresh_batch orders through the relay (batched).
  3. Classify each against the FRESH Salla payload and patch accordingly.

Invariants:
  - Never-regress: the search never returns flag=false orders, so once an
    address is confirmed nothing can flip it back.
  - PATCH before ledger: the HubSpot property is the idempotency key (a
    cleared order exits the search); mirror/gift_refreshed.csv is audit +
    alert dedupe, never correctness-critical.
  - A payload is trusted as "confirmed" only when it is a full order (id
    matches, status/amounts present); degraded payloads retry next cycle.
  - An order missing from the relay response is NEVER treated as confirmed;
    after gift_hydrate_fail_max consecutive misses it parks as unfetchable.
  - All local writes (ledger, state, alerts) gate on this module's own live
    flag -- HubSpot's dry-run fake 200 must not poison the mirror.

Usage:
    python3 gift_refresh.py --config config.live.json --once --live   # timer entrypoint
    python3 gift_refresh.py --config config.live.json --once --dry    # rehearsal, no writes
STOP.gift halts the next run; gift_refresh.lock prevents overlap.
"""

import argparse
import csv
import json
import logging
import os
import socket
import sys
from datetime import datetime, timedelta
from pathlib import Path

from backfill import (Config, HubSpot, RelayClient, RelayError, dig,
                      gift_address_unconfirmed, gift_props,
                      install_dns_cache, now_str)
from notify import send_alert

log = logging.getLogger("backfill")

STOP_FILE = Path("STOP.gift")
LOCK_FILE = Path("gift_refresh.lock")
LEDGER = Path("mirror/gift_refreshed.csv")
STATE = Path("mirror/gift_refresh_state.json")

TERMINAL = {"cleared", "expired", "not_gift", "unfetchable"}

SEARCH_PROPS = ["salla_order_id", "salla_order_reference", "hs_createdate",
                "gift_confirmation_expiry", "gift_confirmation_url",
                "gift_receiver_name"]


class GiftLedger:
    """Append-only outcome ledger, one row per terminal decision.

    Doubles as the alert-once store: an order with an `expired` row never
    re-alerts, across restarts (an in-memory cooldown would resend)."""

    def __init__(self, path=LEDGER):
        self.path = Path(path)
        self.outcome = {}
        if self.path.exists():
            try:
                with open(self.path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        self.outcome[str(row["order_id"])] = row.get("outcome") or ""
            except OSError as e:
                log.warning("gift ledger unreadable: %s", e)

    def record(self, order_id, hs_order_id, outcome, note=""):
        new = not self.path.exists()
        self.path.parent.mkdir(exist_ok=True)
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "order_id", "hs_order_id", "outcome", "note"])
            w.writerow([now_str(), order_id, hs_order_id, outcome,
                        str(note)[:200]])
        self.outcome[str(order_id)] = outcome


class GiftState:
    """mirror/gift_refresh_state.json: digest metrics + retry counters.

    Runs are oneshot processes, so consecutive-miss and patch-failure counters
    must survive between cycles; they live here, not in memory."""

    def __init__(self, path=STATE):
        self.path = Path(path)
        self.data = {"miss": {}, "patch_fail": {}, "patch_alerted": []}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
                if isinstance(loaded, dict):
                    self.data.update(loaded)
            except (OSError, ValueError) as e:
                log.warning("gift state unreadable, starting fresh: %s", e)

    def save(self, **metrics):
        self.data.update(metrics)
        self.data["ts"] = now_str()
        self.path.parent.mkdir(exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1))
        tmp.replace(self.path)

    def bump(self, bucket, order_id):
        m = self.data.setdefault(bucket, {})
        m[str(order_id)] = int(m.get(str(order_id), 0)) + 1
        return m[str(order_id)]

    def clear(self, bucket, order_id):
        self.data.setdefault(bucket, {}).pop(str(order_id), None)


def _hs_order_url(cfg, hs_id):
    """Same convention the audit log uses (record_url_base + /id)."""
    base = str(getattr(cfg, "record_url_base", "") or "").rstrip("/")
    return f"{base}/{hs_id}" if base and hs_id else ""


def _full_payload(payload, salla_id):
    """A payload is trusted only when it is a whole order, not a fragment."""
    return (isinstance(payload, dict)
            and str(payload.get("id")) == str(salla_id)
            and (isinstance(payload.get("status"), dict)
                 or isinstance(payload.get("amounts"), dict)))


def _date_only(v):
    v = str(v or "")[:10]
    try:
        return datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        return None


def _expiry_of(payload, hs_props):
    """Fresh Salla expiry first, HubSpot's stored copy as fallback."""
    return (_date_only(dig(payload, "gift.expiry_date"))
            or _date_only(hs_props.get("gift_confirmation_expiry")))


def _created_of(hs_props):
    return _date_only(str(hs_props.get("hs_createdate") or "")[:10])


def _confirm_patch(payload):
    """The one PATCH that closes a confirmed order. Property set derives from
    gift_props() (single source of truth for create AND refresh; its
    empty-filter means a refresh can never blank an existing value), plus the
    shipping fields re-sourced from the receiver-confirmed address. Only the
    three hs_shipping_* fields the engine already writes are touched; the
    hs_billing_* buyer fields never are."""
    props = dict(gift_props(payload))
    props["gift_address_incomplete"] = "false"
    props["gift_address_state"] = "confirmed"
    addr = dig(payload, "shipping.address", None)
    if not isinstance(addr, dict):
        ship = dig(payload, "shipments", None)
        addr = (ship[0].get("address") if isinstance(ship, list) and ship
                and isinstance(ship[0], dict)
                and isinstance(ship[0].get("address"), dict) else None)
        if not isinstance(addr, dict):
            addr = None
    if addr:
        city = str(addr.get("city") or "").strip()
        country = str(addr.get("country") or "").strip()
        # live-verified shape (order 861883089): shipping.address has no phone;
        # the delivery contact is shipping.receiver.phone
        phone = (str(addr.get("phone") or addr.get("mobile") or "").strip()
                 or str(dig(payload, "shipping.receiver.phone") or "").strip()
                 or props.get("gift_receiver_phone", ""))
        if city:
            props["hs_shipping_address_city"] = city
        if country:
            props["hs_shipping_address_country"] = country
        if phone:
            props["hs_shipping_address_phone"] = phone
    elif props.get("gift_receiver_phone"):
        props["hs_shipping_address_phone"] = props["gift_receiver_phone"]
    return {k: v for k, v in props.items() if v != ""}


def run_cycle(cfg, hs, relay, ledger, state, live, log_shipping=False):
    """One bounded refresh cycle. Returns the metrics dict it saved."""
    today = datetime.now().date()
    stale_cutoff = today - timedelta(days=int(cfg.gift_stale_terminal_days))

    data = hs.search("/crm/v3/objects/orders/search", {
        "filterGroups": [{"filters": [
            {"propertyName": "is_gift_order", "operator": "EQ", "value": "true"},
            {"propertyName": "gift_address_incomplete", "operator": "EQ",
             "value": "true"},
            {"propertyName": "gift_address_state",
             "operator": "NOT_HAS_PROPERTY"},
        ]}],
        "sorts": [{"propertyName": "hs_createdate", "direction": "ASCENDING"}],
        "properties": SEARCH_PROPS,
        "limit": int(cfg.gift_refresh_search_limit),
    }, "gift working set")
    results = data.get("results") or []
    searched = len(results)

    # index-lag guard: a just-patched order can linger in the search page
    eligible = [r for r in results
                if ledger.outcome.get(
                    str((r.get("properties") or {}).get("salla_order_id") or ""))
                not in TERMINAL]
    # recheck rotation: an order checked last cycle and still pending must not
    # crowd never-checked orders out of the batch (oldest-first sort would pin
    # sticky pendings to the front of every page and starve the backlog)
    checked = state.data.get("checked", {})
    eligible.sort(key=lambda r: checked.get(
        str((r.get("properties") or {}).get("salla_order_id") or ""), ""))
    batch = eligible[:int(cfg.gift_refresh_batch)]
    by_salla = {str((r.get("properties") or {}).get("salla_order_id") or ""): r
                for r in batch if (r.get("properties") or {}).get("salla_order_id")}

    payloads = {}
    if by_salla:
        try:
            payloads = relay.fetch_orders(list(by_salla))
        except RelayError as e:
            # abort cleanly: nothing was patched, nothing needs unwinding
            log.warning("GIFT cycle aborted, relay unavailable: %s", e)
            if live:
                state.save(pending=searched, cycle_aborted=str(e)[:200])
            return state.data

    cleared = expired = pending = parked = failed = 0
    newly_expired = []
    cycle_ts = now_str()

    for salla_id, row in by_salla.items():
        state.data.setdefault("checked", {})[salla_id] = cycle_ts
        hs_id = row.get("id")
        hsp = row.get("properties") or {}
        payload = payloads.get(salla_id)

        if payload is None:
            n = state.bump("miss", salla_id)
            if n >= int(cfg.gift_hydrate_fail_max):
                if live:
                    ledger.record(salla_id, hs_id, "unfetchable",
                                  f"missing from relay {n} cycles")
                parked += 1
                log.warning("GIFT %s parked unfetchable after %d misses",
                            salla_id, n)
            else:
                pending += 1
            continue
        state.clear("miss", salla_id)

        if log_shipping:
            log.info("GIFT %s shipping subtree: %s", salla_id,
                     json.dumps(payload.get("shipping") or payload.get("shipments")
                                or {}, ensure_ascii=False)[:600])

        if not _full_payload(payload, salla_id):
            # degraded fetch: a fragment must never confirm, expire, or park
            pending += 1
            log.warning("GIFT %s payload not a full order -- retrying next "
                        "cycle", salla_id)
            continue

        if not gift_props(payload):
            if live:
                ledger.record(salla_id, hs_id, "not_gift",
                              "hydration shows no gift shape; manual review")
            parked += 1
            log.warning("GIFT %s parked not_gift: hydrated payload has no "
                        "gift shape", salla_id)
            continue

        # the same evidence rule creation uses: truthy/False trusted, absent
        # key claims confirmed only with an actual shipping address
        if not gift_address_unconfirmed(payload):
            status, resp = hs.update_order(hs_id, _confirm_patch(payload),
                                           f"gift confirm {salla_id}")
            if status in (200, 201):
                state.clear("patch_fail", salla_id)
                if live:
                    ledger.record(salla_id, hs_id, "cleared",
                                  "address confirmed in Salla")
                cleared += 1
            else:
                failed += 1
                _note_patch_failure(cfg, state, salla_id, status, resp, live)
            continue

        expiry = _expiry_of(payload, hsp)
        created = _created_of(hsp)
        lapsed = ((expiry is not None and expiry < today)
                  or (expiry is None and created is not None
                      and created < stale_cutoff))
        if lapsed:
            status, resp = hs.update_order(
                hs_id, {"gift_address_state": "expired_unconfirmed"},
                f"gift expire {salla_id}")
            if status in (200, 201):
                state.clear("patch_fail", salla_id)
                if ledger.outcome.get(salla_id) != "expired":
                    newly_expired.append({
                        "salla_id": salla_id, "hs_id": hs_id,
                        "ref": hsp.get("salla_order_reference") or salla_id,
                        "name": hsp.get("gift_receiver_name") or "",
                        "expiry": str(expiry or ""),
                        "url": hsp.get("gift_confirmation_url")
                        or dig(payload, "urls.gift_confirmation"),
                    })
                if live:
                    ledger.record(salla_id, hs_id, "expired",
                                  f"link lapsed {expiry or 'no expiry, aged out'}")
                expired += 1
            else:
                failed += 1
                _note_patch_failure(cfg, state, salla_id, status, resp, live)
            continue

        pending += 1  # not confirmed, not lapsed: stays in watch, no write

    if newly_expired and live:
        _chase_alert(cfg, newly_expired)

    remaining = searched - cleared - expired - parked
    oldest = None
    for r in eligible:
        d = _created_of(r.get("properties") or {})
        if d and (oldest is None or d < oldest):
            oldest = d
    # the checked map only matters for orders still in watch
    for oid, out in ledger.outcome.items():
        if out in TERMINAL:
            state.data.get("checked", {}).pop(oid, None)
    metrics = dict(
        pending=max(remaining, 0),
        oldest_days=(today - oldest).days if oldest else 0,
        cleared_total=int(state.data.get("cleared_total", 0)) + cleared,
        expired_total=int(state.data.get("expired_total", 0)) + expired,
        parked_total=int(state.data.get("parked_total", 0)) + parked,
        enabled=bool(cfg.gift_refresh_enabled),
        cycle_aborted="")
    if live:
        state.save(**metrics)
    log.info("GIFT cycle: searched=%d eligible=%d hydrated=%d cleared=%d "
             "expired=%d pending=%d parked=%d failed=%d%s",
             searched, len(eligible), len(payloads), cleared, expired,
             pending, parked, failed, "" if live else "  [DRY RUN]")
    return metrics


def _note_patch_failure(cfg, state, salla_id, status, resp, live):
    n = state.bump("patch_fail", salla_id)
    log.error("GIFT %s patch failed (%s, attempt %d): %s",
              salla_id, status, n, json.dumps(resp)[:200])
    alerted = state.data.setdefault("patch_alerted", [])
    if (n >= int(cfg.gift_hydrate_fail_max) and salla_id not in alerted
            and live):
        alerted.append(salla_id)
        send_alert(
            f"🎁🔴 Gift refresh cannot update order {salla_id} "
            f"({n} consecutive HubSpot rejections) — needs a human look.",
            f"HubSpot keeps rejecting the gift-address PATCH for Salla order "
            f"{salla_id} (last HTTP {status}). The order stays in the refresh "
            f"working set and will keep retrying; fix the property values or "
            f"the portal schema, then the next cycle clears it.")


def _chase_alert(cfg, items):
    """One aggregated alert per cycle: headline carries only the count, the
    detail thread lists the orders (no phone numbers in the headline)."""
    n = len(items)
    subject = (f"🎁🟡 {n} gift order{'s' if n != 1 else ''}' address "
               f"confirmation expired unanswered — the receivers never "
               f"confirmed where to ship. Support should nudge them.")
    lines = []
    for it in items:
        hs_url = _hs_order_url(cfg, it["hs_id"])
        head = f"RID{it['ref']}"
        if hs_url:
            head = f"<{hs_url}|{head}>"
        lines.append(f"• {head} — receiver {it['name'] or 'unnamed'} — link "
                     f"expired {it['expiry'] or '(aged out, no expiry set)'}"
                     + (f" — confirmation link: {it['url']}" if it["url"] else ""))
    body = ("These gift orders are marked `expired_unconfirmed` in HubSpot "
            "(filter on Gift address state). The confirmation link can be "
            "resent, or the receiver contacted via the Gift receiver phone "
            "property on the order:\n" + "\n".join(lines))
    send_alert(subject, body)


def main():
    ap = argparse.ArgumentParser(description="Gift address refresh (v2.9)")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--once", action="store_true", help="run one cycle")
    ap.add_argument("--live", action="store_true", help="write for real")
    ap.add_argument("--dry", action="store_true",
                    help="rehearse: search + hydrate, zero writes anywhere")
    ap.add_argument("--log-shipping", action="store_true",
                    help="log each hydrated order's shipping subtree")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    fmt = "%(asctime)s %(levelname)-7s [gift] %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt,
                        handlers=[logging.FileHandler("gift_refresh.log",
                                                      encoding="utf-8")])
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(console)
    socket.setdefaulttimeout(180)
    install_dns_cache()

    from queue_drain import load_dotenv
    load_dotenv()

    if STOP_FILE.exists():
        log.warning("STOP.gift present -- not running")
        return
    if not (args.dry or args.live):
        sys.exit("Pick a mode: --dry (rehearsal) or --live.")

    cfg = Config.load(args.config)
    if not cfg.gift_refresh_enabled and args.live:
        log.info("gift_refresh_enabled is false -- nothing to do")
        return

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        # a missing secret idles the loop with a warning; it must never make
        # the timer unit look like a crash loop
        log.warning("HUBSPOT_ACCESS_TOKEN / RELAY_SECRET missing -- idle")
        return

    lock = open(LOCK_FILE, "w")
    try:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        log.warning("fcntl unavailable: no lock protection on this OS")
    except OSError:
        sys.exit("Another gift_refresh instance holds gift_refresh.lock.")
    lock.write(f"{os.getpid()}\n")
    lock.flush()

    live = args.live and not args.dry
    hs = HubSpot(cfg, token, live=live)
    relay = RelayClient(cfg, secret)
    ledger = GiftLedger()
    state = GiftState()
    run_cycle(cfg, hs, relay, ledger, state, live=live,
              log_shipping=args.log_shipping or args.dry)


if __name__ == "__main__":
    main()
