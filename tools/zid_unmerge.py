#!/usr/bin/env python3
"""Separate the Zid orders that were merged into Salla orders (v2.12).

On 2026-09-16 tools/zed_create_missing.py created the Zid orders the import
had missed. For 52 of them the Zid order number was already held by a
genuine Salla order (the numbers overlap), so the create resolved to that
Salla order: the Zid line item was added to it and its salla_store was set
to "Zid". Each such Salla order now shows a product the customer never
bought in that order, and is counted as a Zid order.

For each one this:
  1. rebuilds the Zid order as its own record from the import corpus
     (mirror/zed/*.jsonl.gz), through the engine's own order property
     builder, labelled like every other imported order (store Zid, name
     "| Zid |") and keyed Z<number>, its delivery date the Zid order date,
     linked to the Zid customer's contact when one matches by phone (never
     by customer id: Zid and Salla customer ids share a number space too);
  2. moves the Zid line item(s) from the Salla order to it (association
     archived and created, the item's salla_order_id set to Z<number>);
  3. moves any warranty minted on a Zid item: its key re-keyed to
     Z<number>, re-associated to the Zid order, and voided (wrong_order)
     when the Zid order is older than the warranty backfill scope;
  4. puts the Salla order's salla_store back to "Salla".

Dry run by default. Every old value goes to mirror/zid_unmerge.csv first.
Idempotent: an order already separated has no Zid item left and is skipped.

    venv/bin/python3 tools/zid_unmerge.py --config config.live.json          # dry
    venv/bin/python3 tools/zid_unmerge.py --config config.live.json --apply
"""

import argparse
import csv
import glob
import gzip
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import zid_rekey as zr
from backfill import (ASSOC_ORDER_CONTACT, ZID_ITEM_KEY, Config, HubSpot, apply_portal_config, dig,
                      now_str, setup_logging)

log = logging.getLogger("backfill")
LEDGER = Path("mirror/zid_unmerge.csv")
WARRANTY = "2-252148104"


def merged_orders(hs):
    out, after = [], None
    while True:
        body = {"filterGroups": [{"filters": [
            {"propertyName": "salla_store", "operator": "EQ", "value": "Zid"},
            {"propertyName": "hs_source_store", "operator": "EQ", "value": "Salla"}]}],
            "properties": ["salla_order_id", "hs_order_name", "last_salla_sync_status"], "limit": 100}
        if after:
            body["after"] = after
        d = hs.search("/crm/v3/objects/orders/search", body, "unmerge list")
        out += d.get("results") or []
        after = dig(d, "paging.next.after")
        if not after:
            return out


def corpus(ids, pattern="mirror/zed/20*.jsonl.gz"):
    want, found = set(ids), {}
    for p in sorted(glob.glob(pattern)):
        with gzip.open(p, "rt") as f:
            for line in f:
                if not any(w in line[:80] for w in want - set(found)):
                    continue
                try:
                    o = json.loads(line)
                except ValueError:
                    continue
                if str(o.get("id")) in want:
                    found[str(o.get("id"))] = o
    return found


def zid_props(hs, o, tz):
    """The property set the engine's create path would send for this order,
    captured without writing, then labelled and keyed as an imported order."""
    captured = {}

    class Capture(HubSpot):
        def _write(self, method, path, body, what):
            captured.update(body)
            return 200, {"id": "CAPTURED"}

    cap = object.__new__(Capture)
    cap.__dict__.update({k: v for k, v in hs.__dict__.items() if k not in ("_write", "_req")})
    cap._req = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("capture must not call HubSpot"))
    Capture.create_order(cap, o, None, tz)
    p = dict(captured.get("properties") or {})
    n = str(o.get("id"))
    p.update({"salla_store": "Zid", "hs_source_store": "Zid",
              "salla_order_id": zr.zid_key(n), "salla_order_reference": zr.zid_key(n),
              "hs_external_order_id": zr.zid_key(n),
              "hs_order_name": str(p.get("hs_order_name", "")).replace("| Salla |", "| Zid |"),
              "last_salla_sync_status": str(dig(o, "status.slug") or "synced").lower()})
    day = str(dig(o, "date.date") or "")[:10]
    if day and str(dig(o, "status.slug") or "").lower() in ("delivered", "completed"):
        p["delivery_date"] = day
    return {k: v for k, v in p.items() if v not in (None, "")}


def contact_by_phone(hs, o):
    code, mobile = dig(o, "customer.mobile_code"), dig(o, "customer.mobile")
    if not str(mobile or "").strip():
        return None
    d = hs.search("/crm/v3/objects/contacts/search", {
        "filterGroups": hs._phone_filter_groups(code, mobile),
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "properties": ["hs_object_id"], "limit": 1}, "unmerge contact")
    res = d.get("results") or []
    return res[0]["id"] if res else None


def plan(hs, cfg, s, o):
    sid, n = str(s["id"]), str(dig(s, "properties.salla_order_id"))
    st, a = hs._req("GET", f"/crm/v4/objects/orders/{sid}/associations/line_items?limit=500", what="unmerge LIs")
    li = [str(x["toObjectId"]) for x in (a or {}).get("results") or []]
    st, b = hs._req("POST", "/crm/v3/objects/line_items/batch/read",
                    {"properties": ["salla_order_item_id", "salla_order_id"], "inputs": [{"id": x} for x in li]},
                    what="unmerge LI read") if li else (200, {})
    zitems = [{"id": r["id"], "key": dig(r, "properties.salla_order_item_id"), "old": dig(r, "properties.salla_order_id")}
              for r in (b or {}).get("results") or []
              if ZID_ITEM_KEY.match(str(dig(r, "properties.salla_order_item_id") or ""))]
    st, wa = hs._req("GET", f"/crm/v4/objects/orders/{sid}/associations/{WARRANTY}?limit=500", what="unmerge W")
    wids = [str(x["toObjectId"]) for x in (wa or {}).get("results") or []]
    wz = []
    if wids:
        st, wb = hs._req("POST", f"/crm/v3/objects/{WARRANTY}/batch/read",
                         {"properties": ["warranty_key", "hs_pipeline_stage", "origin"],
                          "inputs": [{"id": x} for x in wids]}, what="unmerge W read")
        for w in (wb or {}).get("results") or []:
            key = str(dig(w, "properties.warranty_key") or "")
            parts = key.split(":")
            if len(parts) == 3 and ZID_ITEM_KEY.match(parts[1]):
                order_day = str(dig(o, "date.date") or "")[:10]
                in_scope = bool(order_day) and date.fromisoformat(order_day) >= zr.BACKFILL_CUTOFF
                props = {"warranty_key": ":".join([zr.zid_key(n)] + parts[1:])}
                if not in_scope:
                    props.update({"hs_pipeline_stage": zr.W_STAGE["voided"], "void_reason": zr.VOID_OPTION["value"]})
                wz.append({"id": str(w["id"]), "key": key, "stage": dig(w, "properties.hs_pipeline_stage"),
                           "props": props, "action": "rekey" if in_scope else "void"})
    return {"salla_hs": sid, "n": n, "zid_items": zitems, "warranties": wz,
            "props": zid_props(hs, o, cfg.salla_timezone_default), "contact": contact_by_phone(hs, o)}


def ledger(rows):
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "salla_order_id", "salla_hs_id", "object", "object_id", "field", "old", "new", "note"])
        for r in rows:
            w.writerow([now_str()] + r)


def write(hs, method, path, body, what):
    st, d = hs._write(method, path, body, what)
    if st not in (200, 201, 202, 204):
        raise RuntimeError(f"{what}: HTTP {st} {json.dumps(d)[:200]}")
    return d


def apply(hs, p):
    n, s = p["n"], p["salla_hs"]
    body = {"properties": p["props"]}
    if p["contact"]:
        body["associations"] = [{"to": {"id": str(p["contact"])}, "types": [
            {"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": ASSOC_ORDER_CONTACT}]}]
    z = str(write(hs, "POST", "/crm/v3/objects/orders", body, "unmerge create zid order").get("id"))
    ledger([[n, s, "order", z, "created", "", zr.zid_key(n), f"Zid order rebuilt from corpus; contact {p['contact'] or 'none'}"]])
    if p["zid_items"]:
        write(hs, "POST", "/crm/v4/associations/orders/line_items/batch/archive",
              {"inputs": [{"from": {"id": s}, "to": [{"id": it["id"]} for it in p["zid_items"]]}]}, "unmerge detach")
        write(hs, "POST", "/crm/v4/associations/orders/line_items/batch/create",
              {"inputs": [{"from": {"id": z}, "to": {"id": it["id"]},
                           "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 513}]}
                          for it in p["zid_items"]]}, "unmerge attach")
        write(hs, "POST", "/crm/v3/objects/line_items/batch/update",
              {"inputs": [{"id": it["id"], "properties": {"salla_order_id": zr.zid_key(n)}} for it in p["zid_items"]]},
              "unmerge item key")
        ledger([[n, s, "line_item", it["id"], "order", s, z, f"moved {it['key']} (salla_order_id {it['old']} -> {zr.zid_key(n)})"]
                for it in p["zid_items"]])
    if p["warranties"]:
        if any(w["action"] == "void" for w in p["warranties"]):
            zr.ZidRekey(hs, live=True)._ensure_void_option()
        write(hs, "POST", f"/crm/v3/objects/{WARRANTY}/batch/update",
              {"inputs": [{"id": w["id"], "properties": w["props"]} for w in p["warranties"]]}, "unmerge warranties")
        write(hs, "POST", f"/crm/v4/associations/{WARRANTY}/orders/batch/archive",
              {"inputs": [{"from": {"id": w["id"]}, "to": [{"id": s}]} for w in p["warranties"]]}, "unmerge W detach")
        write(hs, "POST", f"/crm/v4/associations/{WARRANTY}/orders/batch/create",
              {"inputs": [{"from": {"id": w["id"]}, "to": {"id": z},
                           "types": [{"associationCategory": "USER_DEFINED", "associationTypeId": zr.ASSOC_WARRANTY_ORDER}]}
                          for w in p["warranties"]]}, "unmerge W attach")
        ledger([[n, s, "warranty", w["id"], "warranty_key", w["key"], w["props"]["warranty_key"],
                 f"{w['action']}; moved to Zid order {z}; stage was {w['stage']}"] for w in p["warranties"]])
    write(hs, "PATCH", f"/crm/v3/objects/orders/{s}", {"properties": {"salla_store": "Salla"}}, "unmerge label")
    ledger([[n, s, "order", s, "salla_store", "Zid", "Salla", "label restored"]])
    return z


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="zid_unmerge.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    hs = HubSpot(cfg, os.environ["HUBSPOT_ACCESS_TOKEN"], live=args.apply)
    rows = merged_orders(hs)
    if args.limit:
        rows = rows[:args.limit]
    src = corpus([str(dig(r, "properties.salla_order_id")) for r in rows])
    log.info("%s -- %d merged Salla order(s), %d found in the corpus", "APPLY" if args.apply else "DRY RUN",
             len(rows), len(src))
    done = failed = skipped = 0
    for s in rows:
        n = str(dig(s, "properties.salla_order_id"))
        if n not in src:
            log.error("  %s: not in the corpus -- skipped", n)
            skipped += 1
            continue
        try:
            p = plan(hs, cfg, s, src[n])
        except Exception as e:
            log.error("  %s: plan failed: %s", n, e)
            failed += 1
            continue
        if not p["zid_items"]:
            log.info("  %s: no Zid item left on HS %s -- already separated, label only", n, s["id"])
        taken = hs.orders_by_salla_id(zr.zid_key(n))
        if taken != (None, None):
            log.error("  %s: %s already exists as HS %s -- skipped", n, zr.zid_key(n), taken)
            skipped += 1
            continue
        log.info("  %s: HS %s -> new Zid order %s (%s, total %s, stage %s), move %d item(s) %s, warranties %s, contact %s",
                 n, s["id"], zr.zid_key(n), p["props"].get("hs_order_name", "")[:40], p["props"].get("hs_total_price"),
                 str(p["props"].get("hs_pipeline_stage"))[:12], len(p["zid_items"]), [i["key"] for i in p["zid_items"]],
                 [w["action"] for w in p["warranties"]] or "none", p["contact"] or "none")
        if not args.apply:
            continue
        try:
            z = apply(hs, p)
            done += 1
            log.info("     done: Zid order HS %s", z)
        except Exception as e:
            failed += 1
            log.error("     FAILED %s: %s", n, e)
    log.info("done: %d separated, %d skipped, %d failed", done, skipped, failed)


if __name__ == "__main__":
    main()
