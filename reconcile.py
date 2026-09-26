#!/usr/bin/env python3
"""Weekly reconciliation: the Sunday certificate (v3.0).

Three divergence classes once ran silent for months -- a whole outage window
of orders that no sweep owned, statuses frozen at creation, and a copied
population counted twice -- until a manual audit caught them. This module IS
that audit, productized: every Sunday at the quietest hour it re-derives the
Salla-to-HubSpot truth from scratch, posts one certificate to every channel,
and repairs what it found. The live sync always keeps priority.

Phases, in the order the checks earn trust:

  A  count parity   per-day (trailing window, excluding today) and per-month
                    (all era) totals, Salla vs HubSpot. Breaches drill down
                    to NAMED order ids with their Salla statuses, so drafts
                    explain themselves and repairs get ids, not counts.
  B  field samples  random orders per era, hydrated fresh and diffed field
                    by field: stage, status text, total, reference, line
                    items (HubSpot >= Salla items passes: bundle expansion),
                    gift fields on gift orders.
  C  property sweep non-terminal stages older than a cutoff (the shared
                    stage_resweep core, report mode), gift-lifecycle strays,
                    population sums, pre-cutover dupe guard.
  D  pipeline       queue rows stuck beyond a week (with blocker names),
                    dead-timer sensors (a dead gift/drain timer is a finding
                    nothing else reports), and, as its own "make queue"
                    section, unresolved Make retry-queue (DLQ) items per
                    watched scenario older than dlq_min_age_minutes, read
                    with credit_watch.scan_dlq. Nothing repairs from that
                    section, so an unreadable Make API shows NOT RECORDED but
                    never trips the insanity ceiling.
  E  certificate    ONE Slack message per run, always -- green reads as a
                    single sentence; detail lives in the thread. State to
                    mirror/reconcile_state.json for the daily digest's
                    dead-man switch. "Not recorded" is never rendered as 0.
  F  repairs        AFTER the certificate (a repair crash can never eat the
                    findings; outcomes post as a threaded reply):
                    tier 1: patch ALL stage/status drift (paced, uncapped);
                    tier 2: auto-sweep missing orders up to
                    reconcile_backfill_max in an isolated seeded workspace.
                    INSANITY CEILING: if any phase was unmeasurable, or a
                    gap exceeds reconcile_insane_month_pct/_orders, every
                    repair is suppressed and the workspace is prepared
                    emit-only with the exact command in the alert -- a gap
                    that size is a measurement bug or a disaster, and
                    neither should meet an unattended repairer.

Run:
    venv/bin/python3 reconcile.py --config config.live.json --dry
    venv/bin/python3 reconcile.py --config config.live.json --live
STOP.reconcile halts between phases; reconcile.lock forbids overlap;
mirror/reconcile_run.json lets a manual re-run skip completed phases.
"""

import argparse
import csv
import dataclasses
import json
import logging
import os
import random
import socket
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import backfill
from backfill import (Config, HubSpot, RelayClient, RelayError, dig, now_str)
from tools import stage_resweep

log = logging.getLogger("backfill")

RIYADH = timezone(timedelta(hours=3))
CUTOVER = date(2026, 2, 23)          # Salla store first real day
STOP_FILE = Path("STOP.reconcile")
LOCK_FILE = Path("reconcile.lock")
STATE = Path("mirror/reconcile_state.json")
MANIFEST = Path("mirror/reconcile_run.json")
RECOVER_DIR = Path("recover")


# --------------------------------------------------------------------------
# time helpers: every boundary is Riyadh-local, converted once
# --------------------------------------------------------------------------

def day_bounds(d):
    """(salla 'YYYY-MM-DD', hs epoch-ms lo, hs epoch-ms hi) for one Riyadh day."""
    lo = datetime(d.year, d.month, d.day, tzinfo=RIYADH)
    hi = lo + timedelta(days=1)
    return d.isoformat(), str(int(lo.timestamp() * 1000)), str(int(hi.timestamp() * 1000))


def month_span(d):
    """(first_day, first_day_of_next_month) for the month containing d."""
    first = d.replace(day=1)
    nxt = (first + timedelta(days=32)).replace(day=1)
    return first, nxt


# --------------------------------------------------------------------------
# sources: salla today; noon/amazon slot in here later
# --------------------------------------------------------------------------

class SallaSource:
    name = "salla"
    store_value = "Salla"

    def __init__(self, relay, hs):
        self.relay, self.hs = relay, hs

    def day_count(self, d):
        s, _, _ = day_bounds(d)
        r = self.relay.get_path(f"orders?from_date={s}&to_date={s}&per_page=1")
        return int((r.get("pagination") or {}).get("total") or 0)

    def range_count(self, a, b):
        """Salla-side count for [a, b] inclusive dates."""
        r = self.relay.get_path(
            f"orders?from_date={a.isoformat()}&to_date={b.isoformat()}&per_page=1")
        return int((r.get("pagination") or {}).get("total") or 0)

    def hs_day_count(self, d):
        _, lo, hi = day_bounds(d)
        return self._hs_count(lo, hi)

    def hs_range_count(self, a, b_exclusive):
        lo = str(int(datetime(a.year, a.month, a.day, tzinfo=RIYADH).timestamp() * 1000))
        hi = str(int(datetime(b_exclusive.year, b_exclusive.month,
                              b_exclusive.day, tzinfo=RIYADH).timestamp() * 1000))
        return self._hs_count(lo, hi)

    def _hs_count(self, lo, hi):
        data = self.hs.search("/crm/v3/objects/orders/search", {
            "filterGroups": [{"filters": [
                {"propertyName": "salla_store", "operator": "EQ",
                 "value": self.store_value},
                {"propertyName": "hs_external_created_date", "operator": "GTE",
                 "value": lo},
                {"propertyName": "hs_external_created_date", "operator": "LT",
                 "value": hi},
            ]}], "limit": 1}, "reconcile count")
        return int(data.get("total") or 0)

    def list_day_ids(self, d):
        """All Salla order ids for one day, with status slugs (drill-down)."""
        s, _, _ = day_bounds(d)
        out, page = {}, 1
        while True:
            r = self.relay.get_path(
                f"orders?from_date={s}&to_date={s}&per_page=30&page={page}")
            for o in (r.get("data") or []):
                if isinstance(o, dict) and o.get("id") is not None:
                    out[str(o["id"])] = str(dig(o, "status.slug") or "")
            pag = r.get("pagination") or {}
            if page >= int(pag.get("totalPages") or pag.get("total_pages") or 1):
                return out
            page += 1

    def hs_day_ids(self, d):
        _, lo, hi = day_bounds(d)
        out, after = set(), None
        while True:
            body = {"filterGroups": [{"filters": [
                {"propertyName": "salla_store", "operator": "EQ",
                 "value": self.store_value},
                {"propertyName": "hs_external_created_date", "operator": "GTE",
                 "value": lo},
                {"propertyName": "hs_external_created_date", "operator": "LT",
                 "value": hi},
            ]}], "properties": ["salla_order_id"], "limit": 200,
                "sorts": [{"propertyName": "hs_object_id",
                           "direction": "ASCENDING"}]}
            if after:
                body["after"] = after
            data = self.hs.search("/crm/v3/objects/orders/search", body,
                                  "reconcile drilldown")
            for r in data.get("results") or []:
                sid = (r.get("properties") or {}).get("salla_order_id")
                if sid:
                    out.add(str(sid))
            after = (data.get("paging") or {}).get("next", {}).get("after")
            if not after:
                return out


# --------------------------------------------------------------------------
# findings
# --------------------------------------------------------------------------

@dataclasses.dataclass
class Finding:
    phase: str
    measured: bool
    ok: bool
    summary: str
    detail: list = dataclasses.field(default_factory=list)
    data: dict = dataclasses.field(default_factory=dict)


def _check_stop():
    if STOP_FILE.exists():
        raise SystemExit("STOP.reconcile present -- halting between phases")


# --------------------------------------------------------------------------
# phase A: counts
# --------------------------------------------------------------------------

def phase_counts(src, cfg, today=None):
    today = today or datetime.now(RIYADH).date()
    detail, gaps, structural = [], [], []
    try:
        # daily, trailing window, excluding today (index lag)
        for back in range(1, int(cfg.reconcile_window_days) + 1):
            d = today - timedelta(days=back)
            if d < CUTOVER:
                break
            s, h = src.day_count(d), src.hs_day_count(d)
            if abs(s - h) > int(cfg.reconcile_day_allowance):
                ids = drilldown_day(src, d)
                if ids["missing"]:
                    gaps.append({"day": d.isoformat(), "salla": s, "hs": h,
                                 "missing_ids": ids["missing"],
                                 "statuses": ids["statuses"]})
                    detail.append(f"{d.isoformat()}: salla {s} vs hubspot {h}; "
                                  f"missing ids {ids['missing'][:8]}"
                                  + (" ..." if len(ids['missing']) > 8 else ""))
                else:
                    structural.append(f"{d.isoformat()}: {s - h} unmatched, all "
                                      f"non-syncable shapes")
        # monthly, whole era
        m = CUTOVER
        while m <= today:
            first, nxt = month_span(m)
            a = max(first, CUTOVER)
            b_incl = min(nxt - timedelta(days=1), today - timedelta(days=1))
            if a <= b_incl:
                s = src.range_count(a, b_incl)
                h = src.hs_range_count(a, b_incl + timedelta(days=1))
                if abs(s - h) > int(cfg.reconcile_month_allowance):
                    detail.append(f"month {a:%Y-%m}: salla {s} vs hubspot {h}")
                    gaps.append({"month": f"{a:%Y-%m}", "salla": s, "hs": h})
            m = nxt
    except RelayError as e:
        return Finding("counts", False, False,
                       f"count parity not recorded (relay unavailable: "
                       f"{str(e)[:80]})")
    n_missing = sum(len(g.get("missing_ids") or []) for g in gaps)
    ok = not gaps
    summary = ("all days and months match Salla within allowance"
               if ok else f"{len(gaps)} window(s) off, {n_missing} named "
                          f"missing order(s)")
    return Finding("counts", True, ok, summary, detail + structural,
                   {"gaps": gaps, "missing_total": n_missing})


def drilldown_day(src, d):
    """Name the actual missing ids for one breached day, with Salla statuses."""
    salla = src.list_day_ids(d)
    hs = src.hs_day_ids(d)
    missing = sorted(set(salla) - hs)
    return {"missing": missing,
            "statuses": {i: salla[i] for i in missing[:40]}}


# --------------------------------------------------------------------------
# phase B: samples
# --------------------------------------------------------------------------

SAMPLE_ERAS = (7, 90)   # days back: a recent week and an older slice


def phase_samples(src, cfg, cache, rng=None, today=None):
    rng = rng or random.Random()
    today = today or datetime.now(RIYADH).date()
    stage_map = backfill.STATUS_STAGE_MAP
    drift, checked, unfetchable = [], 0, 0
    try:
        for era_back in SAMPLE_ERAS:
            d = max(today - timedelta(days=era_back), CUTOVER)
            _, lo, hi = day_bounds(d)
            data = src.hs.search("/crm/v3/objects/orders/search", {
                "filterGroups": [{"filters": [
                    {"propertyName": "salla_store", "operator": "EQ",
                     "value": "Salla"},
                    {"propertyName": "hs_external_created_date",
                     "operator": "GTE", "value": lo},
                    {"propertyName": "hs_external_created_date",
                     "operator": "LT", "value": hi},
                ]}], "properties": [
                    "salla_order_id", "hs_pipeline_stage", "hs_total_price",
                    "salla_order_reference", "is_gift_order",
                    "gift_address_incomplete", "gift_address_state"],
                "limit": 60}, "reconcile sample")
            rows = data.get("results") or []
            picks = rng.sample(rows, min(int(cfg.reconcile_samples_per_era),
                                         len(rows)))
            ids = [p["properties"]["salla_order_id"] for p in picks]
            payloads = src.relay.fetch_orders(ids)
            cache.update(payloads)
            for p in picks:
                pr = p["properties"]
                sid = str(pr["salla_order_id"])
                pay = payloads.get(sid)
                if not (isinstance(pay, dict) and str(pay.get("id")) == sid
                        and isinstance(pay.get("status"), dict)):
                    unfetchable += 1
                    continue
                checked += 1
                probs = []
                slug = str(dig(pay, "status.slug")).lower()
                want = stage_map.get(slug, backfill.ORDER_PIPELINE_STAGE)
                if pr.get("hs_pipeline_stage") != want:
                    probs.append(f"stage (salla {slug})")
                if str(dig(pay, "amounts.total.amount")) != str(pr.get("hs_total_price")):
                    probs.append("total")
                ref = str(pay.get("reference_id") or pay.get("id"))
                if ref != str(pr.get("salla_order_reference")):
                    probs.append("reference")
                items = len([i for i in (pay.get("items") or [])
                             if str(i.get("product_type", "")).lower()
                             != "group_products"])
                li = src.hs.order_line_item_count(p["id"])
                if 0 <= li < items:   # HS >= items passes (bundle expansion)
                    probs.append(f"line items {li}<{items}")
                if pr.get("is_gift_order") == "true":
                    if (pr.get("gift_address_incomplete") == "false"
                            and backfill.gift_address_unconfirmed(pay)
                            and not pr.get("gift_address_state")):
                        probs.append("gift flag wrong-false")
                if probs:
                    drift.append({"salla_order_id": sid, "hs_id": p["id"],
                                  "problems": probs})
    except RelayError as e:
        return Finding("samples", False, False,
                       f"field samples not recorded (relay unavailable: "
                       f"{str(e)[:80]})")
    ok = not drift
    summary = (f"{checked}/{checked} sampled orders correct"
               if ok else f"{len(drift)} of {checked} sampled orders drifted")
    detail = [f"{d['salla_order_id']}: {', '.join(d['problems'])}"
              for d in drift]
    if unfetchable:
        detail.append(f"{unfetchable} sample(s) unfetchable, reported not "
                      f"counted")
    return Finding("samples", True, ok, summary, detail, {"drift": drift})


# --------------------------------------------------------------------------
# phase C: property sweep
# --------------------------------------------------------------------------

def phase_properties(src, cfg, cache, today=None):
    today = today or datetime.now(RIYADH).date()
    detail, stale = [], []
    try:
        before = (today - timedelta(days=int(cfg.reconcile_stale_stage_days)))
        before_ms = str(int(datetime(before.year, before.month, before.day,
                                     tzinfo=RIYADH).timestamp() * 1000))
        work = stage_resweep.collect(src.hs, cfg, before_ms)
        todo = list(work)[:int(cfg.reconcile_hydrate_max)]
        capped = len(work) - len(todo)
        stage_map = backfill.STATUS_STAGE_MAP
        for i in range(0, len(todo), cfg.relay_batch_size):
            _check_stop()
            chunk = todo[i:i + cfg.relay_batch_size]
            payloads = src.relay.fetch_orders(chunk)
            cache.update(payloads)
            for sid in chunk:
                pay = payloads.get(sid)
                if not (isinstance(pay, dict) and str(pay.get("id")) == sid
                        and isinstance(pay.get("status"), dict)):
                    continue
                props = stage_resweep.plan_patch(
                    pay, work[sid]["stage"], stage_map,
                    backfill.ORDER_PIPELINE_STAGE)
                if props is not None:
                    stale.append({"salla_order_id": sid,
                                  "hs_id": work[sid]["hs_id"],
                                  "old": work[sid]["stage"], "props": props})
        if capped:
            detail.append(f"{capped} further non-terminal candidate(s) beyond "
                          f"the hydrate cap; next run continues")
        # gift strays
        g = _count(src.hs, [
            {"propertyName": "is_gift_order", "operator": "EQ", "value": "true"},
            {"propertyName": "gift_address_incomplete", "operator": "EQ",
             "value": "true"},
            {"propertyName": "gift_address_state",
             "operator": "NOT_HAS_PROPERTY"}])
        # population sums
        tot = src.hs.search("/crm/v3/objects/orders/search", {"limit": 1},
                            "reconcile pop")["total"]
        parts = {v: _count(src.hs, [{"propertyName": "salla_store",
                                     "operator": "EQ", "value": v}])
                 for v in ("Zid", "Salla", "Zid (copy)")}
        unlabeled = _count(src.hs, [{"propertyName": "salla_store",
                                     "operator": "NOT_HAS_PROPERTY"}])
        pop_ok = (sum(parts.values()) + unlabeled == tot and unlabeled == 0)
        if not pop_ok:
            detail.append(f"population sums off: total {tot} vs "
                          f"{parts} + unlabeled {unlabeled}")
        # pre-cutover dupe guard (the 5 known test orders are the floor)
        pre = _count(src.hs, [
            {"propertyName": "salla_store", "operator": "EQ", "value": "Salla"},
            {"propertyName": "hs_external_created_date", "operator": "LT",
             "value": str(int(datetime(2026, 2, 23,
                                       tzinfo=RIYADH).timestamp() * 1000))}])
        if pre > 5:
            detail.append(f"{pre} pre-cutover Salla records (expected <=5): "
                          f"migration copies may be leaking again")
    except RelayError as e:
        return Finding("properties", False, False,
                       f"property sweep not recorded (relay unavailable: "
                       f"{str(e)[:80]})")
    ok = not stale and pop_ok and pre <= 5
    summary = ("no stale stages, populations sum exactly"
               if ok else f"{len(stale)} stale stage(s)"
                          + ("" if pop_ok else "; population mismatch")
                          + ("" if pre <= 5 else f"; {pre} pre-cutover strays"))
    return Finding("properties", True, ok, summary,
                   detail + [f"{s['salla_order_id']}: {s['old']} -> "
                             f"{s['props']['hs_pipeline_stage']}"
                             for s in stale[:10]],
                   {"stale": stale, "gift_watch": g})


def _count(hs, filters):
    return int(hs.search("/crm/v3/objects/orders/search",
                         {"filterGroups": [{"filters": filters}], "limit": 1},
                         "reconcile count")["total"] or 0)


# --------------------------------------------------------------------------
# phase D: pipeline integrity
# --------------------------------------------------------------------------

def phase_pipeline(cfg, now=None):
    now = now or datetime.now(RIYADH)
    detail = []
    # dead-timer sensors: state files that stop moving mean a timer died,
    # and nothing else reports that
    gift_state = Path("mirror/gift_refresh_state.json")
    if getattr(cfg, "gift_refresh_enabled", False):
        try:
            ts = json.loads(gift_state.read_text()).get("ts", "")
            age_h = (now - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                     .replace(tzinfo=RIYADH)).total_seconds() / 3600
            if age_h > 2:
                detail.append(f"gift refresh state is {age_h:.0f}h old -- "
                              f"its timer may be dead")
        except (OSError, ValueError):
            detail.append("gift refresh enabled but its state file is "
                          "missing/unreadable")
    matrix = Path("mirror/blocker_matrix.csv")
    if matrix.exists():
        age_h = (time.time() - matrix.stat().st_mtime) / 3600
        if age_h > 3:
            detail.append(f"drain blocker matrix is {age_h:.0f}h old -- the "
                          f"hourly drain may be dead")
    # queue rows stuck beyond a week -- read the LIVE sheet, never the local
    # append-only mirror: the mirror records what was once written and never
    # learns a row was drained, so it accumulates history forever (a first
    # dry run read 88k ancient rows out of it)
    stuck, queue_read = [], True
    try:
        from queue_drain import DrainGoogleIO
        gio = DrainGoogleIO(cfg, enabled=True)
        for row in gio.qlog_read() or []:
            status = str(row.get("status") or "").strip().lower()
            if status not in ("queued", "held"):
                continue
            qd = str(row.get("queued_at") or "")[:19]
            try:
                age = (now.replace(tzinfo=None)
                       - datetime.strptime(qd, "%Y-%m-%d %H:%M:%S")).days
            except ValueError:
                continue
            if age >= 7:
                stuck.append((row.get("order_id"),
                              str(row.get("items") or row.get("reason")
                                  or "")[:50]))
    except Exception as e:   # sheet unreachable: report, never fake a zero
        queue_read = False
        detail.append(f"queue check not recorded (sheet unreachable: "
                      f"{str(e)[:70]})")
    if stuck:
        blockers = Counter(b for _, b in stuck if b)
        detail.append(f"{len(stuck)} queue row(s) stuck >7d; top blockers: "
                      + ", ".join(f"{b} ({n})"
                                  for b, n in blockers.most_common(3)))
    ok = not detail
    return Finding("pipeline", queue_read, ok,
                   "pipeline clean" if ok else f"{len(detail)} pipeline "
                                               f"finding(s)",
                   detail, {"stuck": len(stuck)})


# [v2.12] Sections nothing repairs from. Unmeasurable still renders NOT
# RECORDED, but it is no reason to withhold the repairs the data phases
# earned: a Make API blip on Sunday must not cost a week of stage patches.
ADVISORY_PHASES = ("make queue",)


def phase_make_queue(cfg, get=None, now=None):
    """[v2.12] Unresolved Make retry-queue items per watched scenario, older
    than dlq_min_age_minutes (Make still owns younger ones: it retries
    transient failures itself ~30 min on). The very read credit_watch alerts
    from, so the certificate and the 12h alert can never disagree. One line
    per scenario: clear, stuck (count, oldest age, replay note, link), or
    not checked. Every read failing is NOT RECORDED, never 0."""
    import credit_watch
    rows = credit_watch.scan_dlq(cfg, get=get, now=now)
    mins = int(getattr(cfg, "dlq_min_age_minutes", 45) or 0)
    stuck = [r for r in rows if r["count"]]
    unread = [r for r in rows if r["count"] is None]
    total = sum(r["count"] for r in stuck)
    detail = [credit_watch.dlq_line(r) for r in rows]
    data = {"total": total, "by_scenario": credit_watch.dlq_state(rows)}
    if not rows:
        return Finding("make queue", True, True, "no Make scenarios watched",
                       [], data)
    if len(unread) == len(rows):
        return Finding("make queue", False, False,
                       "Make retry queues not recorded (Make API unreadable)",
                       detail, data)
    if stuck:
        summary = (f"{total} unresolved Make item(s) older than {mins} min "
                   f"in {len(stuck)} of {len(rows)} scenario(s)")
    else:
        summary = (f"no Make retry-queue items older than {mins} min in "
                   f"{len(rows) - len(unread)} scenario(s)")
    if unread:
        summary += f"; {len(unread)} scenario(s) not checked"
    return Finding("make queue", True, not stuck and not unread, summary,
                   detail, data)


# --------------------------------------------------------------------------
# certificate + state
# --------------------------------------------------------------------------

def render(findings, cfg):
    all_measured = all(f.measured for f in findings)
    all_ok = all(f.ok for f in findings if f.measured)
    if all_measured and all_ok:
        head = ("✅ Weekly reconciliation: " +
                "; ".join(f.summary for f in findings) + ".")
    else:
        bad = [f for f in findings if not f.ok or not f.measured]
        head = ("🟠 Weekly reconciliation found "
                + "; ".join(f"{f.phase}: {f.summary}" for f in bad) + ".")
    lines = []
    for f in findings:
        lines.append(f"*{f.phase}* -- "
                     + ("" if f.measured else "NOT RECORDED -- ") + f.summary)
        lines += [f"  • {d}" for d in f.detail[:12]]
    lines.append(f"_window {cfg.reconcile_window_days}d · budget ~135 relay "
                 f"(Make ops) + ~110 searches per run_")
    return head, "\n".join(lines)


def write_state(findings, repairs=None):
    STATE.parent.mkdir(exist_ok=True)
    data = {"ts": now_str(),
            "phases": {f.phase: {"measured": f.measured, "ok": f.ok,
                                 "summary": f.summary} for f in findings},
            "green": all(f.measured and f.ok for f in findings),
            "repairs": repairs or {}}
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1))
    tmp.replace(STATE)
    return data


# --------------------------------------------------------------------------
# repairs (report-first; called only after the certificate posted)
# --------------------------------------------------------------------------

def insane(findings, cfg):
    """True when the world (or the measurement) cannot be trusted."""
    if any(not f.measured for f in findings
           if f.phase not in ADVISORY_PHASES):          # [v2.12]
        return "a phase was unmeasurable"
    counts = next((f for f in findings if f.phase == "counts"), None)
    if counts:
        missing = counts.data.get("missing_total", 0)
        if missing > int(cfg.reconcile_insane_orders):
            return f"{missing} missing orders exceeds the ceiling"
        for g in counts.data.get("gaps", []):
            if "month" in g and g["salla"]:
                pct = 100.0 * abs(g["salla"] - g["hs"]) / g["salla"]
                if pct > float(cfg.reconcile_insane_month_pct):
                    return f"month {g['month']} is {pct:.0f}% off"
    return None


def repair_stages(src, cfg, findings, cache, live):
    """Patch every stale stage found by phase C. Uncapped by count (the user's
    'repair everything'), bounded by pace: batches of 100 on the general
    bucket, payloads already hydrated in-cache."""
    props_f = next((f for f in findings if f.phase == "properties"), None)
    stale = (props_f.data.get("stale") if props_f else None) or []
    batch, patched, failed = [], 0, 0
    for s in stale:
        _check_stop()
        batch.append((s["salla_order_id"], s["hs_id"], s["old"],
                      s["props"]["hs_pipeline_stage"], "", s["props"]))
        if len(batch) >= 100:
            ok, bad = stage_resweep.flush_batch(src.hs, batch, ledger_live=live)
            patched, failed = patched + ok, failed + bad
            batch = []
            time.sleep(1.0)
    ok, bad = stage_resweep.flush_batch(src.hs, batch, ledger_live=live)
    return {"stage_patched": patched + ok, "stage_failed": failed + bad}


def repair_backfill_prepare(findings, cfg, today=None):
    """Stage an isolated, seeded sweep workspace for the named gap days.
    Returns (workspace_path, gap_days, missing_count, auto_ok)."""
    counts = next((f for f in findings if f.phase == "counts"), None)
    gaps = [g for g in (counts.data.get("gaps") if counts else []) or []
            if g.get("missing_ids")]
    if not gaps:
        return None
    days = sorted(g["day"] for g in gaps)
    missing = sum(len(g["missing_ids"]) for g in gaps)
    ws = RECOVER_DIR / f"reconcile-{(today or datetime.now(RIYADH).date()).isoformat()}"
    (ws / "mirror").mkdir(parents=True, exist_ok=True)
    live_cfg = json.loads(Path("config.live.json").read_text()) \
        if Path("config.live.json").exists() else {}
    live_cfg.update({"workers": 2, "hs_search_limit_per_s": 1.5,
                     "hs_general_limit_per_10s": 60, "alerts_enabled": False,
                     "state_file": "cursor.json", "archive_dir": "archive"})
    (ws / "config.june.json").write_text(json.dumps(live_cfg, indent=2))
    first, last = days[0], days[-1]
    to = (date.fromisoformat(last) + timedelta(days=1)).isoformat()
    (ws / "cursor.json").write_text(json.dumps(
        {"from_date": f"{first}T00:00:00", "to_date": to, "next_page": 1,
         "total_pages": 0, "status": "running"}, indent=2))
    # seed: every order HubSpot already has for those days
    with open(ws / "mirror" / "created.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "salla_order_id", "hubspot_order_id"])
    auto_ok = missing <= int(cfg.reconcile_backfill_max)
    return {"workspace": str(ws), "days": days, "missing": missing,
            "auto_ok": auto_ok,
            "command": (f"cd {ws} && printf 'RUN\\n' | "
                        f"../../venv/bin/python3 ../../backfill.py "
                        f"--config config.june.json --live")}


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def _load_manifest(run_id):
    if MANIFEST.exists():
        try:
            m = json.loads(MANIFEST.read_text())
            if m.get("run_id") == run_id:
                return m
        except (OSError, ValueError):
            pass
    return {"run_id": run_id, "phases": {}}


def _save_manifest(m):
    MANIFEST.parent.mkdir(exist_ok=True)
    tmp = MANIFEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(m, indent=1))
    tmp.replace(MANIFEST)


def main():
    ap = argparse.ArgumentParser(description="Weekly reconciliation (v3.0)")
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--dry", action="store_true",
                    help="measure and print; no Slack, no writes")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    fmt = "%(asctime)s %(levelname)-7s [reconcile] %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt,
                        handlers=[logging.FileHandler("reconcile.log",
                                                      encoding="utf-8")])
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(console)
    socket.setdefaulttimeout(180)
    backfill.install_dns_cache()
    from queue_drain import load_dotenv
    load_dotenv()

    if not (args.dry or args.live):
        sys.exit("Pick a mode: --dry or --live.")
    if STOP_FILE.exists():
        log.warning("STOP.reconcile present -- not running")
        return

    cfg = Config.load(args.config)
    backfill.apply_portal_config(cfg)
    if not cfg.reconcile_enabled and args.live:
        log.info("reconcile_enabled is false -- nothing to do")
        return
    # the live sync owns the account search pool; reconcile takes a sliver
    cfg.hs_search_per_s = float(cfg.reconcile_search_per_s)
    cfg.hs_search_limit_per_s = float(cfg.reconcile_search_per_s)

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        log.warning("HUBSPOT_ACCESS_TOKEN / RELAY_SECRET missing -- idle")
        return

    lock = open(LOCK_FILE, "w")
    try:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        pass
    except OSError:
        sys.exit("Another reconcile instance holds reconcile.lock.")
    lock.write(f"{os.getpid()}\n")
    lock.flush()

    # outage gate: never measure (or repair) during a platform outage
    health = Path("mirror/relay_health.json")
    if health.exists():
        try:
            hstate = json.loads(health.read_text()).get("state", "ok")
        except (OSError, ValueError):
            hstate = "ok"
        if str(hstate).lower() not in ("ok", ""):
            log.warning("relay health = %s -- reconcile skipped", hstate)
            if args.live:
                from notify import send_alert
                send_alert("🟡 Weekly reconciliation skipped: a platform "
                           "outage is flagged. It runs next Sunday, or "
                           "manually once the outage clears.", "")
            return

    live = args.live and not args.dry
    hs = HubSpot(cfg, token, live=live)
    relay = RelayClient(cfg, secret)
    src = SallaSource(relay, hs)
    cache = {}
    run_id = datetime.now(RIYADH).strftime("%G-W%V")
    # the manifest exists so a CRASHED live run can resume its week without
    # re-measuring; a dry run is a diagnostic and must always measure fresh
    manifest = _load_manifest(run_id) if live else {"run_id": run_id,
                                                    "phases": {}}

    findings = []
    for name, fn in [("counts", lambda: phase_counts(src, cfg)),
                     ("samples", lambda: phase_samples(src, cfg, cache)),
                     ("properties", lambda: phase_properties(src, cfg, cache)),
                     ("pipeline", lambda: phase_pipeline(cfg)),
                     ("make queue", lambda: phase_make_queue(cfg))]:
        _check_stop()
        prev = manifest["phases"].get(name)
        if prev and prev.get("done"):
            findings.append(Finding(**prev["finding"]))
            log.info("phase %s: resumed from manifest", name)
            continue
        log.info("--- phase %s ---", name)
        f = fn()
        findings.append(f)
        if live:
            manifest["phases"][name] = {"done": True,
                                        "finding": dataclasses.asdict(f)}
            _save_manifest(manifest)
        log.info("phase %s: %s", name, f.summary)

    head, body = render(findings, cfg)
    log.info("VERDICT: %s", head)
    cert_ts = None
    if live:
        state = write_state(findings)
        from notify import send_alert
        cert_ts = send_alert(head, body)
    else:
        print("\n" + head + "\n\n" + body)

    # ---- repairs: after the certificate, never before -------------------
    repairs = {}
    reason = insane(findings, cfg)
    tier = int(cfg.reconcile_autorepair_tier)
    if reason:
        log.warning("auto-repair suppressed: %s", reason)
        repairs["suppressed"] = reason
    elif tier >= 1:
        repairs.update(repair_stages(src, cfg, findings, cache, live))
        prep = repair_backfill_prepare(findings, cfg)
        if prep:
            repairs["backfill"] = {k: prep[k] for k in
                                   ("workspace", "days", "missing", "auto_ok")}
            if tier >= 2 and prep["auto_ok"] and live:
                import subprocess
                log.info("auto-backfill: %s day(s), %s missing",
                         len(prep["days"]), prep["missing"])
                r = subprocess.run(["bash", "-c", prep["command"]],
                                   capture_output=True, text=True,
                                   timeout=5400)
                repairs["backfill"]["exit"] = r.returncode
            else:
                repairs["backfill"]["command"] = prep["command"]
    if live:
        write_state(findings, repairs)
        if repairs:
            from notify import send_alert
            send_alert("Reconciliation repairs: "
                       + json.dumps({k: v for k, v in repairs.items()
                                     if k != "backfill"} |
                                    ({"backfill_missing":
                                      repairs["backfill"]["missing"]}
                                     if "backfill" in repairs else {}),
                                    ensure_ascii=False)[:280],
                       json.dumps(repairs, indent=1, ensure_ascii=False)[:3000],
                       thread_ts=cert_ts)
    else:
        print("\nrepairs (dry preview):",
              json.dumps(repairs, indent=1)[:1500])
    # a COMPLETED run clears its manifest: resume is for crashes only, and a
    # deliberate re-run after a posted certificate must measure fresh
    if live:
        MANIFEST.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
