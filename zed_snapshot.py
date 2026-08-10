#!/usr/bin/env python3
"""HubSpot snapshot + SnapshotHubSpot: turn 3.37M searches into zero.

The planner runs the real `Engine.process_order` over ~974k Zid orders. Every
one of its reads resolves against a bounded set -- 57 products, 48 bundle
templates, ~137k orders, ~958k contacts -- so all of it can be dumped once and
answered from RAM/sqlite afterwards. That single decision is what makes the
import arithmetically possible: per-order searching would cost ~3.37M search
calls at an account-wide 4.6/s ceiling (weeks of wall clock, and it would
starve live sync of the shared budget).

Dumping uses the LIST endpoints, not search: search caps at 10,000 results per
query, list paginates without limit. ~9,584 calls for contacts, ~1,375 for
orders, a handful for the catalog.

`SnapshotHubSpot` subclasses the real `HubSpot` and overrides only the read
methods, returning byte-identical response shapes. Writes still go through the
parent (and are themselves intercepted by the plan recorder), so nothing about
the engine's behaviour changes.
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

from backfill import Config, HubSpot, apply_portal_config, setup_logging
import zed_normalize as zn

log = logging.getLogger("backfill")

SNAP = Path("mirror/snapshot")
CONTACTS_DB = SNAP / "contacts.sqlite"


# ---------------------------------------------------------------------------
# dumping
# ---------------------------------------------------------------------------

def list_all(hs, obj, properties, page=100, label=""):
    """Paginate a list endpoint to exhaustion. Yields result dicts."""
    after, n, t0 = None, 0, time.time()
    while True:
        qs = f"limit={page}&properties={','.join(properties)}"
        if after:
            qs += f"&after={after}"
        status, data = hs._req("GET", f"/crm/v3/objects/{obj}?{qs}",
                               what=f"list {obj}")
        if status != 200:
            raise RuntimeError(f"list {obj} failed HTTP {status}: "
                               f"{json.dumps(data)[:200]}")
        for r in data.get("results", []):
            yield r
            n += 1
        after = ((data.get("paging") or {}).get("next") or {}).get("after")
        if n and n % 5000 == 0:
            log.info("  %s %s: %d rows (%.0fs)", label or obj, obj, n,
                     time.time() - t0)
        if not after:
            break


def dump_catalog(hs):
    """Products, bundle templates and components -- small, kept as JSON."""
    SNAP.mkdir(parents=True, exist_ok=True)
    out = {}
    prods = list(list_all(hs, "products",
                          ["hs_sku", "name", "salla_product_id",
                           "catalog_approval_status", "price"]))
    out["products"] = prods
    log.info("products: %d", len(prods))

    from backfill import OBJ_BUNDLE_TEMPLATE, OBJ_COMPONENT
    for key, otype, props in (
            ("templates", OBJ_BUNDLE_TEMPLATE,
             ["bundle_template_key", "template_status",
              "active_component_count", "bundle_sku", "bundle_name"]),
            ("components", OBJ_COMPONENT,
             ["bundle_template_key", "component_status", "component_code",
              "component_hubspot_product_id", "quantity_in_bundle",
              "component_sku", "component_name"])):
        if not otype:
            out[key] = []
            continue
        rows = list(list_all(hs, otype, props))
        out[key] = rows
        log.info("%s: %d", key, len(rows))

    (SNAP / "catalog.json").write_text(json.dumps(out, ensure_ascii=False))
    return out


def dump_orders(hs):
    """salla_order_id -> hubspot id, for the dedup layer."""
    SNAP.mkdir(parents=True, exist_ok=True)
    idx = {}
    for r in list_all(hs, "orders", ["salla_order_id"], label="orders"):
        sid = (r.get("properties") or {}).get("salla_order_id")
        if sid:
            idx[str(sid)] = r["id"]
    (SNAP / "orders.json").write_text(json.dumps(idx))
    log.info("orders indexed: %d", len(idx))
    return idx


def dump_contacts(hs):
    """Every contact, indexed by EVERY normalised variant of its three phone
    properties. Engine-created contacts store '966...'; the ~817k non-Salla
    contacts have unknown formatting, so a single-key index would miss them.

    Ties resolve to the most recently created contact, matching
    search_contact_by_phone's `createdate DESCENDING` sort.
    """
    SNAP.mkdir(parents=True, exist_ok=True)
    if CONTACTS_DB.exists():
        CONTACTS_DB.unlink()
    db = sqlite3.connect(CONTACTS_DB)
    db.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        CREATE TABLE contact(id TEXT PRIMARY KEY, createdate TEXT,
                             salla_customer_id TEXT);
        CREATE TABLE phone(key TEXT, id TEXT, createdate TEXT);
    """)
    n = 0
    batch_c, batch_p = [], []
    for r in list_all(hs, "contacts",
                      ["phone", "mobilephone", "main_phone_number",
                       "salla_customer_id", "createdate"], label="contacts"):
        p = r.get("properties") or {}
        cid, cdate = r["id"], str(p.get("createdate") or "")
        batch_c.append((cid, cdate, str(p.get("salla_customer_id") or "")))
        keys = set()
        for f in ("phone", "mobilephone", "main_phone_number"):
            k = zn.zed_phone_key(p.get(f))
            if k:
                keys.add(k)
        for k in keys:
            batch_p.append((k, cid, cdate))
        n += 1
        if len(batch_c) >= 5000:
            db.executemany("INSERT OR REPLACE INTO contact VALUES (?,?,?)", batch_c)
            db.executemany("INSERT INTO phone VALUES (?,?,?)", batch_p)
            db.commit()
            batch_c, batch_p = [], []
    if batch_c:
        db.executemany("INSERT OR REPLACE INTO contact VALUES (?,?,?)", batch_c)
        db.executemany("INSERT INTO phone VALUES (?,?,?)", batch_p)
    db.execute("CREATE INDEX idx_phone_key ON phone(key)")
    db.commit()
    dup = db.execute(
        "SELECT COUNT(*) FROM (SELECT key FROM phone GROUP BY key "
        "HAVING COUNT(DISTINCT id) > 1)").fetchone()[0]
    total_keys = db.execute("SELECT COUNT(DISTINCT key) FROM phone").fetchone()[0]
    db.close()
    log.info("contacts: %d indexed, %d distinct phone keys, "
             "%d keys map to >1 contact (pre-existing duplicates)",
             n, total_keys, dup)
    return {"contacts": n, "phone_keys": total_keys, "duplicate_keys": dup}


class ContactIndex:
    """Read side of contacts.sqlite. Most-recent-createdate wins."""

    def __init__(self, path=CONTACTS_DB):
        self.db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    def lookup(self, e164):
        if not e164:
            return None
        row = self.db.execute(
            "SELECT id FROM phone WHERE key=? ORDER BY createdate DESC LIMIT 1",
            (e164,)).fetchone()
        return row[0] if row else None

    def count(self):
        return self.db.execute("SELECT COUNT(*) FROM contact").fetchone()[0]


# ---------------------------------------------------------------------------
# the offline HubSpot
# ---------------------------------------------------------------------------

class SnapshotHubSpot(HubSpot):
    """Answers every read the engine performs from local snapshots.

    Response shapes are byte-compatible with the live client's, because the
    engine reads `.get("total")` / `["results"][0]["properties"][...]` and any
    divergence would be a silent behaviour change rather than an error.
    """

    def __init__(self, cfg, token, live, snap_dir=SNAP):
        super().__init__(cfg, token, live)
        snap_dir = Path(snap_dir)
        cat = json.loads((snap_dir / "catalog.json").read_text())
        self.orders_idx = json.loads((snap_dir / "orders.json").read_text())
        self.contacts = ContactIndex(snap_dir / "contacts.sqlite")

        # products, indexed the two ways the gate asks for them
        self.by_salla_pid, self.by_sku = {}, {}
        for p in cat["products"]:
            pr = p.get("properties") or {}
            rec = {"id": p["id"], "properties": pr}
            sid = str(pr.get("salla_product_id") or "").strip()
            sku = str(pr.get("hs_sku") or "").strip()
            approved = (pr.get("catalog_approval_status") or "") == "approved"
            if sid:
                self.by_salla_pid.setdefault(sid, []).append((approved, rec))
            if sku:
                self.by_sku.setdefault(sku, []).append((approved, rec))

        self.tpl_by_key, self.comps_by_key = {}, {}
        for t in cat.get("templates", []):
            pr = t.get("properties") or {}
            k = str(pr.get("bundle_template_key") or "").strip()
            if k:
                self.tpl_by_key.setdefault(k, []).append({"id": t["id"],
                                                          "properties": pr})
        for c in cat.get("components", []):
            pr = c.get("properties") or {}
            k = str(pr.get("bundle_template_key") or "").strip()
            if k and (pr.get("component_status") or "") == "active":
                self.comps_by_key.setdefault(k, []).append({"id": c["id"],
                                                            "properties": pr})

    # -- shape helpers -------------------------------------------------------

    @staticmethod
    def _body(rows):
        return {"total": len(rows), "results": rows}

    # -- order dedup ---------------------------------------------------------

    def dedup_order_exists(self, salla_order_id):
        return str(salla_order_id) in self.orders_idx

    def find_order_by_salla_id(self, salla_order_id):
        return self.orders_idx.get(str(salla_order_id))

    def order_line_item_count(self, order_id):
        # never consulted by the planner: process_order does not call it
        return -1

    # -- contacts ------------------------------------------------------------

    def search_contact_by_phone(self, mobile_code, mobile):
        cid = self.contacts.lookup(zn.zed_phone_key(f"{mobile_code}{mobile}"))
        return (cid, 1) if cid else (None, 0)

    def search_contact_retry(self, order):
        c = order.get("customer") or {}
        return self.contacts.lookup(
            zn.zed_phone_key(f"{c.get('mobile_code','')}{c.get('mobile','')}"))

    # -- catalog gate --------------------------------------------------------

    def gate_search_product_approved(self, salla_product_id):
        rows = self.by_salla_pid.get(str(salla_product_id or "0"), [])
        return sum(1 for ok, _ in rows if ok)

    def gate_search_product_by_sku(self, skus):
        if isinstance(skus, str):
            skus = [skus]
        n = 0
        for s in skus:
            n += sum(1 for ok, _ in self.by_sku.get(str(s), []) if ok)
        return n

    def gate_search_template(self, salla_product_id, eligible_only):
        rows = self.tpl_by_key.get(str(salla_product_id or ""), [])
        if not eligible_only:
            return len(rows)
        n = 0
        for t in rows:
            pr = t["properties"]
            try:
                acc = int(float(pr.get("active_component_count") or 0))
            except ValueError:
                acc = 0
            if (pr.get("template_status") or "") == "active" and acc > 0:
                n += 1
        return n

    # -- item routing --------------------------------------------------------

    def item_search_product(self, salla_product_id):
        rows = [r for _, r in self.by_salla_pid.get(str(salla_product_id), [])]
        return self._body(rows[:1])

    def item_search_product_by_sku(self, skus):
        if isinstance(skus, str):
            skus = [skus]
        rows = []
        for s in skus:
            rows += [r for ok, r in self.by_sku.get(str(s), []) if ok]
        return self._body(rows[:2])

    def item_search_template(self, salla_product_id, eligible_only):
        rows = self.tpl_by_key.get(str(salla_product_id or ""), [])
        if eligible_only:
            keep = []
            for t in rows:
                pr = t["properties"]
                try:
                    acc = int(float(pr.get("active_component_count") or 0))
                except ValueError:
                    acc = 0
                if (pr.get("template_status") or "") == "active" and acc > 0:
                    keep.append(t)
            rows = keep
        return self._body(rows[:1])

    def search_active_components(self, template_key):
        return self.comps_by_key.get(str(template_key), [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--what", default="all",
                    choices=("all", "catalog", "orders", "contacts"))
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="zed_snapshot.log")
    cfg = Config.load(args.config)
    # populates OBJ_BUNDLE_TEMPLATE / OBJ_BUNDLE / OBJ_COMPONENT and the assoc
    # ids; only backfill.main() calls this, so a standalone tool must too or
    # the custom-object dumps silently come back empty
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")
    hs = HubSpot(cfg, token, live=False)   # reads only; live flag gates writes

    t0 = time.time()
    if args.what in ("all", "catalog"):
        dump_catalog(hs)
    if args.what in ("all", "orders"):
        dump_orders(hs)
    if args.what in ("all", "contacts"):
        dump_contacts(hs)
    log.info("snapshot complete in %.1f min -> %s", (time.time() - t0) / 60, SNAP)


if __name__ == "__main__":
    main()
