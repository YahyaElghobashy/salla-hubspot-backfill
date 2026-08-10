#!/usr/bin/env python3
"""Auto-resolver for catalog-deleted blockers (v2.6) -- a backfill sub-engine.

The problem it owns: historical orders reference products the store has since
DELETED from Salla. Their payloads carry product=null, so the id-based catalog
gate can never pass them; only the line item's SKU survives as identity. v2.5
taught the gate to resolve such items against "legacy catalog records" (products
carrying a namespaced hs_sku, salla_product_id empty). This module decides WHEN
such a record deserves to exist, creates it, announces it, and hands the engine
the released orders for immediate resync.

Rules of engagement, deliberately narrow:
  * BACKFILL holds only. Live-sync holds are recent orders whose products are
    unlikely to be legitimately deleted -- those stay a human decision.
  * Null-product items only. An item with a real product id that fails the
    gate is a genuine catalog-approval decision; the resolver never touches it.
  * A SKU earns a record only above cfg.legacy_auto_threshold distinct held
    orders AND cfg.legacy_auto_min_consistency dominant-name share. Below the
    consistency bar it posts a needs-human Slack note (once) instead -- volume
    without a stable identity is precisely the case that should not be
    automated.
  * Records are created in the LEGACY SKU NAMESPACE (cfg.legacy_sku_prefix,
    default "LGCY-"): hs_sku = "LGCY-C41" for item sku "C41". Live Salla
    listings will never mint a SKU with this prefix, so today's records cannot
    collide with any SKU the store creates later. salla_product_id stays empty
    for the same reason on the id axis.

State lives in mirror/legacy_blockers.json and survives restarts; every
creation also lands in mirror/legacy_created.csv as a plain audit trail.
"""

import csv
import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path

log = logging.getLogger("backfill")

STATE_FILE = Path("mirror/legacy_blockers.json")
AUDIT_FILE = Path("mirror/legacy_created.csv")


class LegacyResolver:
    def __init__(self, cfg, hs):
        self.cfg, self.hs = cfg, hs
        self.prefix = str(getattr(cfg, "legacy_sku_prefix", "LGCY-"))
        self.threshold = int(getattr(cfg, "legacy_auto_threshold", 300))
        self.min_consistency = float(
            getattr(cfg, "legacy_auto_min_consistency", 0.9))
        self._lock = threading.Lock()
        self._state = self._load()

    # -- state -----------------------------------------------------------------

    def _load(self):
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"skus": {}}

    def _save(self):
        STATE_FILE.parent.mkdir(exist_ok=True)
        tmp = STATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False),
                       encoding="utf-8")
        tmp.replace(STATE_FILE)   # atomic: a crash never truncates the ledger

    # -- observation (called from route_held, backfill mode only) --------------

    def observe(self, order_id, item):
        """Record one null-product held item. Cheap, lock-guarded, persisted."""
        sku = str(item.get("sku") or "").strip()
        if not sku:
            return
        name = (item.get("name") or "").strip()
        price = item.get("_legacy_price") or ""
        with self._lock:
            e = self._state["skus"].setdefault(sku, {
                "names": {}, "prices": {}, "order_ids": [],
                "resolved": None, "flagged_inconsistent": False})
            if e["resolved"]:
                # record already exists; keep collecting ids for resync
                if order_id not in e["order_ids"]:
                    e["order_ids"].append(order_id)
                self._save()
                return
            if name:
                e["names"][name] = e["names"].get(name, 0) + 1
            if price:
                e["prices"][str(price)] = e["prices"].get(str(price), 0) + 1
            if order_id not in e["order_ids"]:
                e["order_ids"].append(order_id)
            self._save()

    # -- decision --------------------------------------------------------------

    def _dominant(self, counter):
        if not counter:
            return "", 0.0
        total = sum(counter.values())
        top = max(counter.items(), key=lambda kv: kv[1])
        return top[0], (top[1] / total if total else 0.0)

    def due(self):
        """SKUs past the threshold, split into auto-creatable vs needs-human."""
        ready, needs_human = [], []
        with self._lock:
            for sku, e in self._state["skus"].items():
                if e["resolved"] or len(e["order_ids"]) < self.threshold:
                    continue
                name, share = self._dominant(e["names"])
                if share >= self.min_consistency and name:
                    ready.append(sku)
                elif not e["flagged_inconsistent"]:
                    needs_human.append(sku)
        return ready, needs_human

    # -- resolution ------------------------------------------------------------

    def resolve(self, sku):
        """Create + approve the legacy record for `sku`. Returns (hs_id, count)
        or (None, 0) on failure. Idempotent: searches the namespace first."""
        with self._lock:
            e = self._state["skus"].get(sku)
            if not e or e["resolved"]:
                return None, 0
            name, _ = self._dominant(e["names"])
            price, _ = self._dominant(e["prices"])
            count = len(e["order_ids"])
        namespaced = f"{self.prefix}{sku}"
        # someone (or a previous crash-interrupted run) may have created it
        if self.hs.gate_search_product_by_sku(namespaced) > 0:
            hs_id = "existing"
        else:
            props = {"name": name, "hs_sku": namespaced,
                     "catalog_approval_status": "approved",
                     "description": ("Legacy catalog record: product deleted "
                                     "in Salla; created automatically for "
                                     "historical order sync. Original SKU "
                                     f"{sku}.")}
            if price:
                try:
                    props["price"] = str(float(price))
                except ValueError:
                    pass
            hs_id = self.hs.create_product(props, "legacy product")   # [M400]
            if not hs_id:
                log.error("legacy resolve failed for sku %s", sku)
                return None, 0
        with self._lock:
            e = self._state["skus"][sku]
            e["resolved"] = {"hs_id": hs_id, "at": datetime.now().isoformat(
                timespec="seconds"), "name": name, "price": price}
            self._save()
        self._audit(sku, namespaced, hs_id, name, price, count)
        log.info("LEGACY RESOLVED sku=%s -> %s (%s) frees ~%d held order(s)",
                 sku, namespaced, hs_id, count)
        return hs_id, count

    def flag_inconsistent(self, sku):
        with self._lock:
            e = self._state["skus"].get(sku)
            if e:
                e["flagged_inconsistent"] = True
                self._save()
            name, share = self._dominant(e["names"]) if e else ("", 0)
            return name, share, len(e["order_ids"]) if e else 0

    def _audit(self, sku, namespaced, hs_id, name, price, count):
        AUDIT_FILE.parent.mkdir(exist_ok=True)
        new = not AUDIT_FILE.exists()
        with open(AUDIT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "item_sku", "hs_sku", "hubspot_id", "name",
                            "price", "held_orders_at_creation"])
            w.writerow([datetime.now().isoformat(timespec="seconds"), sku,
                        namespaced, hs_id, name, price, count])

    # -- resync hand-off -------------------------------------------------------

    def pending_resync(self, batch=200):
        """Order ids released by resolved SKUs, not yet resynced. The engine
        fetches and reprocesses them (dedup makes repeats free), then calls
        mark_resynced."""
        with self._lock:
            out = []
            for e in self._state["skus"].values():
                if e["resolved"]:
                    out.extend(e["order_ids"])
                if len(out) >= batch:
                    break
            return list(dict.fromkeys(out))[:batch]

    def mark_resynced(self, order_ids):
        done = set(str(o) for o in order_ids)
        with self._lock:
            for e in self._state["skus"].values():
                if e["resolved"]:
                    e["order_ids"] = [o for o in e["order_ids"]
                                      if str(o) not in done]
            self._save()

    # -- announcements ---------------------------------------------------------

    def announce_created(self, sku, hs_id, count, send_alert):
        namespaced = f"{self.prefix}{sku}"
        with self._lock:
            meta = (self._state["skus"].get(sku) or {}).get("resolved") or {}
        subject = (f"🧩 Legacy product created — releasing ~{count:,} held "
                   f"orders")
        body = (
            f"The backfill kept hitting orders blocked by a product the store "
            f"has deleted from Salla.\n\n"
            f"• Item: {meta.get('name', '?')}\n"
            f"• Original SKU: {sku}  →  legacy record {namespaced} "
            f"(HubSpot {hs_id})\n"
            f"• Held orders it unblocks: ~{count:,}\n"
            f"• Price on record: {meta.get('price') or 'from each order'} SAR\n\n"
            f"The legacy record lives in its own SKU namespace ({self.prefix}…) "
            f"and carries no Salla product id, so it can never conflict with "
            f"products the store creates later.\n\n"
            f"Resync of the released orders starts now at full backfill speed; "
            f"the historical sweep resumes automatically once they are in.")
        try:
            send_alert(subject, body)
        except Exception as exc:  # announcement must never break the engine
            log.warning("legacy announce failed: %s", exc)

    def announce_needs_human(self, sku, send_alert):
        name, share, count = self.flag_inconsistent(sku)
        subject = (f"🖐 Held-order blocker needs a human — SKU {sku} "
                   f"({count:,} orders)")
        body = (
            f"{count:,} backfill orders are held on deleted-product SKU {sku}, "
            f"but its item names disagree across orders (top name only "
            f"{share:.0%} consistent: “{name[:60]}”).\n\n"
            f"Auto-creation is deliberately blocked below "
            f"{self.min_consistency:.0%} consistency — volume without a stable "
            f"identity should not be automated. Review the blocker matrix and "
            f"create the record manually if appropriate.")
        try:
            send_alert(subject, body)
        except Exception as exc:
            log.warning("legacy needs-human announce failed: %s", exc)
