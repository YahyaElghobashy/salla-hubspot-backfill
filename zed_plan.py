#!/usr/bin/env python3
"""Run the REAL engine offline and record what it would write.

This is the heart of the import. `Engine.process_order` is not reimplemented,
reimagined or ported: it is executed verbatim, with two shims underneath it.

  * reads  -> SnapshotHubSpot answers from local dumps (zero network)
  * writes -> PlanRecorder appends to a JSONL plan and hands back a synthetic
              id like "§4711"

The synthetic ids are the trick that makes this work. They flow back through
the unmodified engine, so when `route_create` associates an order to a line
item it emits a body already wired with `§`-references. **The dependency graph
therefore builds itself out of the engine's own call ordering** -- nothing here
models which object depends on which, because the engine already knows.

What that buys: the catalog gate, the four-way item router, the ~40-key order
property map and the whole bundle machinery all behave exactly as they do in
production, because they ARE production. A held order writes nothing here for
the same reason it writes nothing live: `route_held` performs no HubSpot
writes at all, so it cannot leak into the plan.

Output per month:
  mirror/zed_plans/YYYY-MM.plan.jsonl   ordered write operations
  mirror/zed_plans/YYYY-MM.summary.json counts + the reconciliation chain

Usage:
    python3 zed_plan.py --month 2020-06
    python3 zed_plan.py --all
"""

import argparse
import gzip
import json
import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path

import backfill
from backfill import Config, GoogleIO, LocalMirror, apply_portal_config, setup_logging
from zed_snapshot import SnapshotHubSpot

log = logging.getLogger("backfill")

PLANS = Path("mirror/zed_plans")
NORM = Path("mirror/zed")


class PlanRecorder:
    """Mixin over HubSpot that turns every write into a plan line.

    Mixed in ahead of SnapshotHubSpot so `_write` is intercepted while all the
    reads still resolve from the snapshot.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.plan = []
        self._sym = 0

    def _next_sym(self):
        self._sym += 1
        return f"§{self._sym}"

    def _write(self, method, path, body, what):
        """Record instead of sending. Returns the shape callers expect:
        (status, data) with an id, so `create_order`'s (id, was_fresh) and
        `create_line_item`'s data["id"] both work untouched."""
        sym = self._next_sym()
        self.plan.append({"op": method, "path": path, "body": body,
                          "what": what, "sym": sym})
        return 201, {"id": sym}


class PlanHubSpot(PlanRecorder, SnapshotHubSpot):
    """Reads from the snapshot, writes to the plan."""


class _NoCursor:
    """The engine expects a cursor; the planner has no page loop.
    Same stub queue_drain.py uses."""
    data = {"status": "zed-plan"}
    status = "zed-plan"


class ZedPlanEngine(backfill.Engine):
    def __init__(self, cfg, hs, gio, mirror):
        # relay is never touched below process_order (verified: the only
        # self.relay uses are in run() and a _rates_report log line), but the
        # constructor stores it, so a stub with the one attribute
        # _rates_report reads is enough.
        class _RelayStub:
            class _gap:
                rate = 1.0
        super().__init__(cfg, _NoCursor(), _RelayStub(), hs, gio, mirror,
                         live=True, workers=1)
        self.is_live_sync = False
        self.legacy = None
        self.health = None

    def process_order(self, order):
        """Skip the per-order archive/Drive write the base class does first.

        The base writes archive/order_RID*.json for every order; at 974k orders
        that is a million files for no benefit -- the source of truth is the
        Zid export, which we already have.
        """
        oid = str(order.get("id"))
        items = order.get("items", []) or []
        audit_row = -1        # no Sheets: audit_update no-ops on row < 0
        self.mirror.audit_event("arrived_append", audit_row,
                                {0: oid, 9: str(len(items))})
        unverified = self.gate_unverified_items(order)
        if unverified:
            self.route_held(order, audit_row, unverified)
        else:
            self.route_create(order, audit_row)


def month_files(month=None):
    if not NORM.exists():
        return []
    files = sorted(NORM.glob("*.jsonl.gz"))
    if month:
        files = [f for f in files if f.name.startswith(month)]
    return files


def plan_month(path, cfg, token):
    month = path.name.split(".")[0]
    hs = PlanHubSpot(cfg, token, live=True)
    gio = GoogleIO(cfg, enabled=False)      # never touch Sheets/Drive
    mirror = LocalMirror(f"mirror/zed_mirror/{month}")
    eng = ZedPlanEngine(cfg, hs, gio, mirror)

    PLANS.mkdir(parents=True, exist_ok=True)
    plan_path = PLANS / f"{month}.plan.jsonl"
    t0 = time.time()
    n_orders = skipped = 0
    ops = Counter()
    held_skus = Counter()

    with gzip.open(path, "rt", encoding="utf-8") as f, \
            open(plan_path, "w", encoding="utf-8") as out:
        for line in f:
            order = json.loads(line)
            oid = str(order.get("id"))
            # dedup against what HubSpot already holds: the snapshot is the
            # same check run() does live, just from RAM
            if hs.dedup_order_exists(oid):
                skipped += 1
                continue
            hs.plan.clear()          # symbol counter is separate (_sym)
            eng.process_order(order)
            for rec in hs.plan:
                rec["order_id"] = oid
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                ops[rec["what"].split()[0]] += 1
            n_orders += 1

    st = eng.stats
    summary = {
        "month": month,
        "orders_in_file": n_orders + skipped,
        "already_in_hubspot": skipped,
        "planned": n_orders,
        "created": st.created, "held": st.held, "errors": st.errors,
        "held_note": "held orders write nothing; they need catalog approval",
        "plan_operations": dict(ops),
        "plan_lines": sum(ops.values()),
        "seconds": round(time.time() - t0, 1),
    }
    (PLANS / f"{month}.summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1))
    log.info("%s: %d planned, %d already in HubSpot, created=%d held=%d "
             "errors=%d, %d plan ops in %.1fs", month, n_orders, skipped,
             st.created, st.held, st.errors, summary["plan_lines"],
             summary["seconds"])
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--month")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose, logfile="zed_plan.log")
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    token = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not token:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN first.")

    files = month_files(args.month)
    if not files:
        sys.exit(f"no normalised months found in {NORM} "
                 f"(run zed_normalize first)")
    if not args.all and len(files) > 1:
        sys.exit(f"{len(files)} months present; pass --month or --all")

    totals = Counter()
    for p in files:
        s = plan_month(p, cfg, token)
        for k in ("planned", "created", "held", "errors", "already_in_hubspot"):
            totals[k] += s[k]
    log.info("TOTAL planned=%d created=%d held=%d errors=%d skipped=%d",
             totals["planned"], totals["created"], totals["held"],
             totals["errors"], totals["already_in_hubspot"])


if __name__ == "__main__":
    main()
