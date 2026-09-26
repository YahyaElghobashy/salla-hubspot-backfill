#!/usr/bin/env python3
"""Free a Salla order number held by an imported Zid order (v2.12).

The Zid import stored each Zid order NUMBER in orders.salla_order_id, which
is unique. A Salla order whose id equals one cannot be created until the Zid
record moves to its own key. This module does that move, once per number,
and records every old value so it can be undone by hand:

  order        salla_order_id, salla_order_reference, hs_external_order_id
               n -> Z<n>; last_salla_sync_status partial/failed -> synced
               (stops the Auto-Repair workflow posting the Zid order to Make)
  Zid items    line items keyed Z<n>-k: salla_order_id n -> Z<n>, so joins
               by salla_order_id never mix Zid items into the Salla order
  Salla items  line items with Salla keys that earlier repairs attached to
               the Zid order: detached (association 513 archived); the
               engine's create path reuses them by their unique key
  warranties   Salla-keyed records: detached from the Zid order (the Salla
               order's own mint re-attaches them by key);
               Zid-keyed records: key prefix n -> Z<n>; a record the live
               engine minted from a Salla status event is kept and re-dated
               from the Zid order's own date when that order is inside the
               warranty backfill scope, and voided (void_reason wrong_order)
               when it is not
  ledgers      the created-ledger row for n is revoked; the collision
               worklist entry is marked resolved
  stage        the Zid order's stage is restored from its property history
               ONLY with restore_stages=True, once the warranty engine
               ignores Zid orders (a stage change into Delivered/Completed
               mints warranties)

Used by the engine (Config.zid_auto_rekey) when a new collision is met, and
by the CLI for the historical ones. Dry run by default: the plan is printed
and nothing is written. Ledger: mirror/zid_rekeys.csv.

    python3 zid_rekey.py --config config.live.json --from-scan            # dry
    python3 zid_rekey.py --config config.live.json --from-scan --apply --requeue
    python3 zid_rekey.py --config config.live.json --order 4938528 --apply
    python3 zid_rekey.py --config config.live.json --from-scan --verify
"""

import argparse
import csv
import json
import logging
import os
import sys
import threading
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from backfill import (ZID_ITEM_KEY, Config, CreatedLedger, GoogleIO, HubSpot,
                      apply_portal_config, dig, is_zid_order, now_str, setup_logging)

log = logging.getLogger("backfill")

WARRANTY_OBJ = "2-252148104"
W_STAGE = {"active": "5913821399", "expiring": "5913821400", "expired": "5913821401",
           "voided": "5913821402"}
VOID_OPTION = {"label": "Minted on the wrong order (Zid number collision)",
               "value": "wrong_order"}
BACKFILL_CUTOFF = date(2024, 3, 1)
EXPIRING_DAYS = 30
DELIVERED_LIKE = {"3725360f-519b-4b18-a593-494d60a29c9f", "5656450292"}   # Delivered, Completed
ASSOC_ORDER_LI = 513
ASSOC_WARRANTY_ORDER = 113
LEDGER_LOCK = threading.Lock()


def zid_key(n):
    return f"Z{n}"


def add_months(iso, months):
    y, m, d = int(iso[:4]), int(iso[5:7]), int(iso[8:10])
    t = (m - 1) + int(months)
    y += t // 12
    m = t % 12 + 1
    leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
    last = 29 if (m == 2 and leap) else (28 if m == 2 else 30 if m in (4, 6, 9, 11) else 31)
    return "%04d-%02d-%02d" % (y, m, min(d, last))


def warranty_stage(end_iso, today=None):
    today = today or date.today()
    e = date.fromisoformat(end_iso[:10])
    if e < today:
        return W_STAGE["expired"]
    return W_STAGE["expiring"] if (e - today).days <= EXPIRING_DAYS else W_STAGE["active"]


class ZidRekey:
    def __init__(self, hs, mirror_dir="mirror", live=False, today=None):
        self.hs, self.live = hs, live
        self.mirror = Path(mirror_dir)
        self.ledger = self.mirror / "zid_rekeys.csv"
        self.today = today or date.today()

    # -- reads -----------------------------------------------------------------

    def _assoc_ids(self, from_obj, from_id, to_obj):
        ids, after = [], None
        while True:
            path = f"/crm/v4/objects/{from_obj}/{from_id}/associations/{to_obj}?limit=500"
            if after:
                path += f"&after={after}"
            st, d = self.hs._req("GET", path, what=f"zid assoc {to_obj}")
            if st != 200:
                raise RuntimeError(f"association read {from_obj} {from_id} -> {to_obj}: HTTP {st}")
            ids += [str(r.get("toObjectId")) for r in d.get("results") or []]
            after = dig(d, "paging.next.after")
            if not after:
                return ids

    def _batch_read(self, obj, ids, props):
        out = {}
        for i in range(0, len(ids), 100):
            st, d = self.hs._req("POST", f"/crm/v3/objects/{obj}/batch/read",
                                 {"properties": props, "inputs": [{"id": x} for x in ids[i:i + 100]]},
                                 what=f"zid read {obj}")
            if st not in (200, 207):
                raise RuntimeError(f"batch read {obj}: HTTP {st}")
            for r in d.get("results") or []:
                out[str(r["id"])] = r.get("properties") or {}
        return out

    def plan(self, zid_hs_id, n):
        """Everything the move would change, read from HubSpot. Raises when
        the record is not the Zid order holding n."""
        n, zid_hs_id = str(n), str(zid_hs_id)
        st, o = self.hs._req("GET", f"/crm/v3/objects/orders/{zid_hs_id}?properties=salla_order_id,"
                             "salla_order_reference,hs_external_order_id,hs_source_store,salla_store,"
                             "last_salla_sync_status,hs_pipeline_stage,hs_external_created_date,"
                             "hs_order_name&propertiesWithHistory=hs_pipeline_stage", what="zid order")
        if st != 200:
            raise RuntimeError(f"order {zid_hs_id}: HTTP {st}")
        p = o.get("properties") or {}
        if not is_zid_order(p):
            raise RuntimeError(f"order {zid_hs_id} is not a Zid-import order (hs_source_store="
                               f"{p.get('hs_source_store')!r}); nothing to move")
        if str(p.get("salla_order_id")) != n:
            raise RuntimeError(f"order {zid_hs_id} holds salla_order_id {p.get('salla_order_id')!r}, "
                               f"not {n}")
        taken = self.hs.orders_by_salla_id(zid_key(n))
        if taken != (None, None):
            raise RuntimeError(f"{zid_key(n)} is already held by HS {taken}")
        hist = dig(o, "propertiesWithHistory.hs_pipeline_stage") or []
        original_stage = str(hist[-1].get("value")) if hist else str(p.get("hs_pipeline_stage") or "")
        plan = {"n": n, "zid_hs_id": zid_hs_id, "name": p.get("hs_order_name", ""),
                "order_props": {"salla_order_id": zid_key(n)},
                "order_old": {"salla_order_id": n},
                "stage_current": str(p.get("hs_pipeline_stage") or ""),
                "stage_original": original_stage, "stage_changes": len(hist) - 1 if hist else 0,
                "zid_items": [], "salla_items": [], "warranties": []}
        for k in ("salla_order_reference", "hs_external_order_id"):
            if str(p.get(k) or "") == n:
                plan["order_props"][k] = zid_key(n)
                plan["order_old"][k] = n
        if str(p.get("last_salla_sync_status") or "") in ("partial", "failed"):
            plan["order_props"]["last_salla_sync_status"] = "synced"
            plan["order_old"]["last_salla_sync_status"] = p.get("last_salla_sync_status")
        # line items
        li_ids = self._assoc_ids("orders", zid_hs_id, "line_items")
        for lid, q in self._batch_read("line_items", li_ids, ["salla_order_item_id", "salla_order_id"]).items():
            key = str(q.get("salla_order_item_id") or "")
            if ZID_ITEM_KEY.match(key):
                plan["zid_items"].append({"id": lid, "key": key, "old": q.get("salla_order_id")})
            else:
                plan["salla_items"].append({"id": lid, "key": key})
        # warranties
        order_day = str(p.get("hs_external_created_date") or "")[:10]
        in_scope = (bool(order_day) and date.fromisoformat(order_day) >= BACKFILL_CUTOFF
                    and original_stage in DELIVERED_LIKE)
        w_ids = self._assoc_ids("orders", zid_hs_id, WARRANTY_OBJ)
        for wid, q in self._batch_read(WARRANTY_OBJ, w_ids, ["warranty_key", "origin", "hs_pipeline_stage",
                                                             "warranty_start_date", "warranty_end_date",
                                                             "warranty_months_snapshot"]).items():
            key = str(q.get("warranty_key") or "")
            parts = key.split(":")
            item_key = parts[1] if len(parts) == 3 else ""
            w = {"id": wid, "key": key, "origin": q.get("origin"), "stage": q.get("hs_pipeline_stage"),
                 "props": {}, "action": ""}
            if parts and parts[0] == n and item_key and not ZID_ITEM_KEY.match(item_key):
                w["action"] = "detach"                    # the Salla order's own record
            elif parts and parts[0] == n:
                w["props"]["warranty_key"] = ":".join([zid_key(n)] + parts[1:])
                if str(q.get("origin") or "").startswith("backfill"):
                    w["action"] = "rekey"
                elif in_scope and q.get("warranty_months_snapshot"):
                    start = order_day
                    end = add_months(start, float(q["warranty_months_snapshot"]))
                    w["props"].update({"warranty_start_date": start, "warranty_end_date": end,
                                       "hs_pipeline_stage": warranty_stage(end, self.today)})
                    w["action"] = "redate"
                else:
                    w["props"].update({"hs_pipeline_stage": W_STAGE["voided"],
                                       "void_reason": VOID_OPTION["value"]})
                    w["action"] = "void"
            else:
                w["action"] = "leave"                     # not keyed on this number
            plan["warranties"].append(w)
        return plan

    # -- writes ----------------------------------------------------------------

    def _ledger(self, rows):
        with LEDGER_LOCK:
            self.mirror.mkdir(parents=True, exist_ok=True)
            new = not self.ledger.exists()
            with open(self.ledger, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["ts", "salla_order_id", "zid_hs_id", "object", "object_id",
                                "field", "old", "new", "note"])
                for r in rows:
                    w.writerow([now_str()] + r)

    def _write(self, method, path, body, what):
        st, d = self.hs._write(method, path, body, what)
        if st not in (200, 201, 202, 204):
            raise RuntimeError(f"{what}: HTTP {st} {json.dumps(d)[:200]}")
        return d

    def _ensure_void_option(self):
        st, d = self.hs._req("GET", f"/crm/v3/properties/{WARRANTY_OBJ}/void_reason", what="void_reason")
        opts = d.get("options") or [] if st == 200 else []
        if any(o.get("value") == VOID_OPTION["value"] for o in opts):
            return
        opts.append({"label": VOID_OPTION["label"], "value": VOID_OPTION["value"],
                     "displayOrder": len(opts), "hidden": False})
        self._write("PATCH", f"/crm/v3/properties/{WARRANTY_OBJ}/void_reason", {"options": opts},
                    "void_reason option")
        log.info("void_reason option %s added", VOID_OPTION["value"])

    def apply(self, plan, restore_stages=False):
        """Execute a plan in a safe order; returns the ledger rows written."""
        n, z = plan["n"], plan["zid_hs_id"]
        rows = []
        # 1. the order itself (frees the number)
        self._write("PATCH", f"/crm/v3/objects/orders/{z}", {"properties": plan["order_props"]},
                    "zid order rekey")
        for k, v in plan["order_props"].items():
            rows.append([n, z, "order", z, k, plan["order_old"].get(k, ""), v, "rekey"])
        # 2. Zid line items follow their order
        if plan["zid_items"]:
            self._write("POST", "/crm/v3/objects/line_items/batch/update",
                        {"inputs": [{"id": it["id"], "properties": {"salla_order_id": zid_key(n)}}
                                    for it in plan["zid_items"]]}, "zid items rekey")
            rows += [[n, z, "line_item", it["id"], "salla_order_id", it["old"], zid_key(n), it["key"]]
                     for it in plan["zid_items"]]
        # 3. Salla line items leave the Zid order (the engine reuses them by key)
        if plan["salla_items"]:
            self._write("POST", "/crm/v4/associations/orders/line_items/batch/archive",
                        {"inputs": [{"from": {"id": z}, "to": {"id": it["id"]}}
                                    for it in plan["salla_items"]]}, "salla items detach")
            rows += [[n, z, "assoc_order_line_item", it["id"], "association", z, "", f"detached {it['key']}"]
                     for it in plan["salla_items"]]
        # 4. warranties
        detach = [w for w in plan["warranties"] if w["action"] == "detach"]
        if detach:
            self._write("POST", f"/crm/v4/associations/{WARRANTY_OBJ}/orders/batch/archive",
                        {"inputs": [{"from": {"id": w["id"]}, "to": {"id": z}} for w in detach]},
                        "warranties detach")
            rows += [[n, z, "assoc_warranty_order", w["id"], "association", z, "", f"detached {w['key']}"]
                     for w in detach]
        upd = [w for w in plan["warranties"] if w["action"] in ("rekey", "redate", "void")]
        if any(w["action"] == "void" for w in upd):
            self._ensure_void_option()
        if upd:
            self._write("POST", f"/crm/v3/objects/{WARRANTY_OBJ}/batch/update",
                        {"inputs": [{"id": w["id"], "properties": w["props"]} for w in upd]},
                        "warranties update")
            for w in upd:
                for k, v in w["props"].items():
                    old = w["key"] if k == "warranty_key" else w.get("stage") if k == "hs_pipeline_stage" else ""
                    rows.append([n, z, "warranty", w["id"], k, old, v, w["action"]])
        # 5. stage, only when the caller says the warranty engine is safe
        if restore_stages and plan["stage_original"] and plan["stage_current"] != plan["stage_original"]:
            self._write("PATCH", f"/crm/v3/objects/orders/{z}",
                        {"properties": {"hs_pipeline_stage": plan["stage_original"]}}, "zid stage restore")
            rows.append([n, z, "order", z, "hs_pipeline_stage", plan["stage_current"],
                         plan["stage_original"], "restored from history"])
        # 6. local ledgers
        led = CreatedLedger(str(self.mirror))
        if led.get(n):
            rows.append([n, z, "created_ledger", n, "hubspot_order_id", led.get(n), "", "revoked"])
            led.revoke(n)
        self._mark_resolved(n)
        self._ledger(rows)
        return rows

    def _mark_resolved(self, n):
        path = self.mirror / "zid_collisions.json"
        try:
            seen = json.loads(path.read_text()) if path.exists() else {}
        except Exception:
            seen = {}
        if n in seen and not seen[n].get("resolved"):
            seen[n]["resolved"] = now_str()
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(seen, indent=1, sort_keys=True))
            tmp.replace(path)

    def rekey(self, zid_hs_id, n, restore_stages=False):
        """Plan and, when live, apply. Returns the plan (with 'applied')."""
        plan = self.plan(zid_hs_id, n)
        plan["applied"] = False
        describe(plan, log.info)
        if self.live:
            self.apply(plan, restore_stages=restore_stages)
            plan["applied"] = True
            log.info("ZID REKEY done: %s -> %s freed for salla order %s", zid_hs_id, zid_key(n), n)
        return plan


def describe(plan, out=print):
    n, z = plan["n"], plan["zid_hs_id"]
    acts = {}
    for w in plan["warranties"]:
        acts[w["action"]] = acts.get(w["action"], 0) + 1
    out(f"ZID REKEY plan salla {n}: HS {z} {plan['name'][:40]!r}: order {sorted(plan['order_props'])}, "
        f"{len(plan['zid_items'])} Zid item(s) rekeyed, {len(plan['salla_items'])} Salla item(s) detached, "
        f"warranties {acts or 'none'}, stage {plan['stage_current'][:12]} (original "
        f"{plan['stage_original'][:12]}, {plan['stage_changes']} change(s))")


# -- CLI ----------------------------------------------------------------------

def pairs_from_impact(path="mirror/zid_impact.csv"):
    out = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["kind"] in ("zid_holder", "idspace_zid_holder"):
                for n in str(r["salla_order_id"]).split(";"):
                    if n.strip():
                        out[n.strip()] = str(r["hs_order_id"])
    return out


def verify(hs, pairs):
    ok = 0
    for n, z in sorted(pairs.items()):
        salla, zid = hs.orders_by_salla_id(n)
        st, o = hs._req("GET", f"/crm/v3/objects/orders/{z}?properties=salla_order_id", what="verify")
        held = dig(o, "properties.salla_order_id") if st == 200 else "?"
        good = bool(salla) and not zid and held == zid_key(n)
        ok += good
        log.info("  %s %-12s salla order %s | Zid HS %s now holds %s", "OK  " if good else "WAIT", n,
                 salla or "missing", z, held)
    log.info("verified %d / %d", ok, len(pairs))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--from-scan", action="store_true", help="every holder in mirror/zid_impact.csv")
    ap.add_argument("--order", default="", help="one Salla order id (its Zid holder is found by search)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--restore-stages", action="store_true",
                    help="also restore each Zid order's stage from its history (warranty engine must skip Zid orders first)")
    ap.add_argument("--requeue", action="store_true", help="after a live re-key, queue the Salla order for the live engine")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose, logfile="zid_rekey.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    tok = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not tok:
        sys.exit("HUBSPOT_ACCESS_TOKEN missing")
    hs = HubSpot(cfg, tok, live=args.apply)
    if args.from_scan:
        pairs = pairs_from_impact()
    elif args.order:
        salla, zid = hs.orders_by_salla_id(args.order)
        if not zid:
            sys.exit(f"no Zid order holds {args.order} (salla order: {salla})")
        pairs = {args.order: zid}
    else:
        ap.error("give --from-scan or --order")
    if args.verify:
        verify(hs, pairs)
        return
    rk = ZidRekey(hs, live=args.apply)
    log.info("%s -- %d number(s)", "APPLY" if args.apply else "DRY RUN", len(pairs))
    done, failed = [], []
    for n, z in sorted(pairs.items(), key=lambda kv: int(kv[0])):
        try:
            rk.rekey(z, n, restore_stages=args.restore_stages)
            done.append(n)
        except Exception as e:
            failed.append(n)
            log.error("  %s: %s", n, e)
    log.info("done: %d re-keyed, %d failed%s", len(done), len(failed), f" {failed}" if failed else "")
    if args.apply and args.requeue and done:
        gio = GoogleIO(cfg, enabled=True)
        rows = [[now_str(), n, "", "requeue", "queued", 0, "zid_rekey",
                 f"Salla order after Zid re-key (Zid HS {pairs[n]})"] for n in done]
        gio.queue_append_rows(cfg.queue_spreadsheet_id, rows)
        log.info("queued %d order(s) on the Live Queue", len(rows))


if __name__ == "__main__":
    main()
