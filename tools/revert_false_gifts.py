#!/usr/bin/env python3
"""Revert orders falsely flagged as gifts (2026-09 receiver-block incident).

Salla began attaching `receiver` to NORMAL orders as the delivery contact,
and receiver-alone detection marked 77 of them as gifts. Detection is fixed
in gift_props; this one-off clears the wrongly written gift properties on
orders whose LIVE Salla payload proves they are not gifts.

Evidence-gated per order: the order is re-fetched through the relay and
reverted only when the fixed detector returns {} for the fresh payload. An
order that cannot be fetched, or that the detector still calls a gift, is
skipped and reported, never cleared on suspicion.

Resumable: each revert appends a `reclassified_not_gift` row to
mirror/gift_refreshed.csv, and already-reclassified ids are skipped, so an
interrupted run just re-runs.

Usage:
    python3 tools/revert_false_gifts.py --config config.live.json --dry
    python3 tools/revert_false_gifts.py --config config.live.json --live
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backfill import Config, HubSpot, RelayClient, gift_props  # noqa: E402
from gift_refresh import GiftLedger  # noqa: E402

log = logging.getLogger("backfill")

CLEAR_PROPS = {
    "is_gift_order": "false",
    "gift_receiver_name": "",
    "gift_receiver_phone": "",
    "gift_receiver_email": "",
    "gift_receiver_salla_notified": "",
    "gift_message": "",
    "gift_card_image_url": "",
    "gift_confirmation_url": "",
    "gift_confirmation_expiry": "",
    "gift_deliver_at": "",
    "gift_address_incomplete": "",
    "gift_address_state": "",
}


def suspects(hs):
    """Every order still flagged gift with no confirmation link (a real
    buy-as-gift order always carries one)."""
    rows, after = [], None
    while True:
        body = {"filterGroups": [{"filters": [
            {"propertyName": "is_gift_order", "operator": "EQ",
             "value": "true"},
            {"propertyName": "gift_confirmation_url",
             "operator": "NOT_HAS_PROPERTY"},
        ]}], "properties": ["salla_order_id", "salla_order_reference"],
            "limit": 100}
        if after:
            body["after"] = after
        data = hs.search("/crm/v3/objects/orders/search", body,
                         "false-gift suspects")
        rows += data.get("results") or []
        after = ((data.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()
    if not (args.dry or args.live):
        sys.exit("Pick --dry or --live.")

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s [revert] %(message)s")
    from queue_drain import load_dotenv
    load_dotenv()
    cfg = Config.load(args.config)
    hs = HubSpot(cfg, os.environ["HUBSPOT_ACCESS_TOKEN"],
                 live=args.live and not args.dry)
    relay = RelayClient(cfg, os.environ["RELAY_SECRET"])
    ledger = GiftLedger()

    cands = suspects(hs)
    log.info("suspects still flagged: %d", len(cands))
    todo = {}
    for r in cands:
        sid = str((r.get("properties") or {}).get("salla_order_id") or "")
        if sid and ledger.outcome.get(sid) != "reclassified_not_gift":
            todo[sid] = r["id"]
    log.info("to verify against Salla: %d", len(todo))

    payloads = relay.fetch_orders(list(todo)) if todo else {}
    reverted = still_gift = unfetched = 0
    for sid, hs_id in todo.items():
        p = payloads.get(sid)
        if not isinstance(p, dict):
            unfetched += 1
            log.warning("%s not fetched -- left untouched", sid)
            continue
        if gift_props(p):
            still_gift += 1
            log.warning("%s live payload IS a gift -- left untouched", sid)
            continue
        status, resp = hs.update_order(hs_id, CLEAR_PROPS,
                                       f"revert false gift {sid}")
        if status in (200, 201):
            if args.live and not args.dry:
                ledger.record(sid, hs_id, "reclassified_not_gift",
                              "receiver-block false positive reverted")
            reverted += 1
        else:
            log.error("%s PATCH failed (%s): %s", sid, status,
                      json.dumps(resp)[:200])
    log.info("REVERT done: reverted=%d still_gift=%d unfetched=%d%s",
             reverted, still_gift, unfetched,
             "  [DRY RUN]" if not (args.live and not args.dry) else "")


if __name__ == "__main__":
    main()
