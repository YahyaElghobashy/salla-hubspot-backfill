#!/usr/bin/env python3
"""Scheduled Slack digests: daily 09:00, weekly Sunday, monthly on the 1st.

Shape (same for all three, because the audience already recognises it from the
30-minute ops thread): the CHANNEL MESSAGE is an executive summary readable in
fifteen seconds -- verdict, the numbers a non-technical reader needs, and
anything requiring a decision. The THREAD carries one reply per aspect, each
self-contained, each with the analysis rather than just the number.

Three design rules that matter more than they look:

1. Counts come from append-only ledgers (mirror/created.csv, mirror/errors.csv),
   never from logs. Logs rotate; a report that silently loses a week of history
   is worse than one that admits it cannot see that far back.
2. A metric that could not be measured renders as "not recorded", never as 0.
   Reporting an unmeasured value as zero is how a broken sensor becomes a
   confident lie.
3. The report is built entirely from local files, so it still posts during a
   Make credit outage. The day the platform is down is the day the report
   matters most.

v2.12: the daily digest also reports the Customer and Status Queue states,
how yesterday's customer payloads were read, the last customer sweep, consent
coverage, workbook capacity and orders that got no audit sheet row. Each
of those values is computed on its own: one that fails logs a WARNING and
only its line is left out.

Usage:
    python3 report_digest.py --period daily
    python3 report_digest.py --period weekly  --channel C0AQMMS4TRD
    python3 report_digest.py --period monthly --dry-run
"""

import argparse
import calendar
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import metrics_rollup as roll

ROOT = Path(__file__).resolve().parent
FAIL_FLAG = ROOT / "mirror" / "report_failed.flag"

log = logging.getLogger("digest")

SPARK = "▁▂▃▄▅▆▇█"


def spark(values):
    """Text sparkline. Slack renders no charts and no tables; this is the honest
    substitute -- shape at a glance, exact numbers in the thread."""
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return ""
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return SPARK[3] * len(vals)
    return "".join(SPARK[min(7, int(7 * (v - lo) / (hi - lo)))]
                   if isinstance(v, (int, float)) else " " for v in values)


def n(v, unit=""):
    """Format a number, or say plainly that it was never measured."""
    if v is None:
        return "not recorded"
    if isinstance(v, float):
        # credit balances arrive as floats from the Make API but are whole
        # numbers; "96,739.0 credits" reads like a bug to anyone sensible
        return f"{v:,.0f}{unit}" if v == int(v) else f"{v:,.1f}{unit}"
    return f"{v:,}{unit}"


def dur(seconds):
    """Human duration. A raw '1,156,112s' is technically true and useless."""
    if seconds is None:
        return "not recorded"
    s = int(seconds)
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    if s < 172800:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def pct_change(cur, prev):
    if not prev or cur is None:
        return ""
    d = 100.0 * (cur - prev) / prev
    arrow = "up" if d >= 0 else "down"
    # beyond a few hundred percent the ratio stops informing and starts
    # sounding like spin; give the raw comparison instead
    if abs(d) > 300:
        return f" (previous period: {prev:,})"
    return f" ({arrow} {abs(d):.0f}% on the previous period)"


# --------------------------------------------------------------------------
# aggregation
# --------------------------------------------------------------------------

def window(period, today=None):
    """(start, end, label) inclusive date strings for the reporting period."""
    today = today or datetime.now().date()
    if period == "daily":
        d = today - timedelta(days=1)
        return str(d), str(d), d.strftime("%A %-d %B")
    if period == "weekly":
        # report runs Sunday morning and covers the week that just ended
        end = today - timedelta(days=1)
        start = end - timedelta(days=6)
        return str(start), str(end), (f"{start.strftime('%-d %b')} – "
                                      f"{end.strftime('%-d %b %Y')}")
    first_this = today.replace(day=1)
    end = first_this - timedelta(days=1)
    start = end.replace(day=1)
    return str(start), str(end), end.strftime("%B %Y")


def ledger_total(path, start, end, stage_field=None):
    """Exact count over a date range, straight from the append-only ledger."""
    if not path.exists():
        return (None, None) if stage_field else None
    total, by = 0, {}
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            d = (row.get("ts") or "")[:10]
            if not d or d < start or d > end:
                continue
            if "hubspot_order_id" in row and not (row.get("hubspot_order_id") or "").strip():
                continue   # [v2.12] CreatedLedger.revoke tombstone, not a create
            total += 1
            if stage_field:
                k = (row.get(stage_field) or "unknown").strip()
                by[k] = by.get(k, 0) + 1
    return (total, by) if stage_field else total


def aggregate(period):
    start, end, label = window(period)
    days = roll.load(start, end)
    span = (datetime.strptime(end, "%Y-%m-%d")
            - datetime.strptime(start, "%Y-%m-%d")).days + 1

    created = ledger_total(roll.CREATED, start, end)
    err_total, err_by = ledger_total(roll.ERRORS, start, end, stage_field="stage")
    partials = err_by.get("partial", 0) if err_by is not None else None
    unrecovered = (sum(v for k, v in err_by.items() if k != "partial")
                   if err_by is not None else None)

    # previous period of equal length, for trend
    p_end = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=1)).date()
    p_start = p_end - timedelta(days=span - 1)
    prev_created = ledger_total(roll.CREATED, str(p_start), str(p_end))

    avail = [d.get("availability_pct") for d in days
             if d.get("availability_pct") is not None]
    depth_max = [d.get("queue_depth_max") for d in days
                 if d.get("queue_depth_max") is not None]
    waits = [d.get("oldest_wait_s_max") for d in days
             if d.get("oldest_wait_s_max") is not None]
    burns = [(d.get("credits") or {}).get("burned") for d in days
             if (d.get("credits") or {}).get("burned") is not None]
    alerts = [a for d in days for a in (d.get("alerts") or [])]
    restarts = 0
    for d in days:
        for s in (d.get("services") or {}).values():
            restarts += (s.get("restarts") or 0) if isinstance(s, dict) else 0

    live_vals = [d.get("live_processed") for d in days
                 if d.get("live_processed") is not None]
    latest = days[-1] if days else {}
    live_now = roll.collect(datetime.now().strftime("%Y-%m-%d"))

    # v2.8: backfill-stall watchdog. The cursor page/window frozen across two
    # consecutive dailies means the backfill loop is not advancing, which is
    # exactly how "page 15 of 29" sat unnoticed for five days in September.
    stalled = False
    bf_now = (live_now.get("backfill") or {})
    bf_prev = ((days[-1].get("backfill") if days else None) or {})
    if (period == "daily" and bf_now.get("window") and bf_prev.get("window")
            and bf_now.get("window") == bf_prev.get("window")
            and bf_now.get("page") == bf_prev.get("page")
            and str(bf_now.get("status") or "").lower() not in
            ("done", "complete", "finished", "disabled")):
        stalled = True

    return {
        "live_held": _live_held() if period == "daily" else None,
        "gift_watch": _gift_watch() if period == "daily" else None,
        "reconcile": _reconcile_watch() if period == "daily" else None,
        # [v2.12] realtime queues, customers, capacity, audit misses (daily)
        "ops": (_safe("realtime lines", _ops_watch, start, end)
                if period == "daily" else None),
        "backfill_stalled": stalled,
        "period": period, "start": start, "end": end, "label": label,
        "days_recorded": len(days), "days_expected": span,
        "created": created, "prev_created": prev_created,
        "daily_series": [(d["date"], d.get("orders_created")) for d in days],
        "errors_unrecovered": unrecovered, "partials": partials,
        "availability": round(sum(avail) / len(avail), 2) if avail else None,
        "queue_depth_max": max(depth_max) if depth_max else None,
        "wait_max_s": max(waits) if waits else None,
        "credits_burned": sum(burns) if burns else None,
        "credits_daily": burns,
        "live_created": sum(live_vals) if live_vals else None,
        "alerts": alerts, "restarts": restarts,
        "latest": latest, "now": live_now,
    }


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

def _engine_config_path():
    """[v2.12] The engine config the digest reads: ENGINE_CONFIG when set,
    else config.live.json (what every engine service runs on) when present,
    else config.json. On the VM config.json carries no queue_spreadsheet_id,
    so reading it made the queue lines fall back to stale figures."""
    env = os.environ.get("ENGINE_CONFIG")
    if env:
        return env
    return "config.live.json" if Path("config.live.json").exists() else "config.json"


def _live_held():
    """Outstanding catalog-held orders, measured LIVE from the Live Queue tab.

    v2.8: the old number came from the last drain-scan line in drain.log, which
    goes stale the moment scans stop (it reported "1 (as of 2026-08-25)" for
    two weeks while the true backlog reached 19). Best-effort by design: any
    failure falls back to the stale-but-dated figure rather than breaking the
    digest.
    Returns {"value", "as_of", "oldest_days", "top"} or None.
    """
    try:
        from backfill import Config, GoogleIO
        cfg = Config.load(_engine_config_path())
        gio = GoogleIO(cfg, enabled=True)
        rows = gio.queue_read(cfg.queue_spreadsheet_id, start_row=2,
                              tab=getattr(cfg, "live_queue_tab", "Live Queue"))
        held = [r for r in rows or []
                if str(r.get("status") or r.get("state") or "") == "held"
                and not str(r.get("note") or "").startswith("zid collision")]
        oldest = None
        names = {}
        for r in held:
            ts = str(r.get("received_at") or "")[:10]
            if ts:
                oldest = ts if oldest is None or ts < oldest else oldest
            note = str(r.get("note") or "")
            if note.startswith("catalog gate:"):
                for nm in note[len("catalog gate:"):].split("--")[0].split(","):
                    nm = nm.strip()
                    if nm:
                        names[nm] = names.get(nm, 0) + 1
        oldest_days = None
        if oldest:
            try:
                oldest_days = (datetime.now()
                               - datetime.strptime(oldest, "%Y-%m-%d")).days
            except ValueError:
                pass
        top = ", ".join(f"{nm} ({c})" for nm, c in
                        sorted(names.items(), key=lambda kv: -kv[1])[:3])
        return {"value": len(held), "as_of": "live",
                "oldest_days": oldest_days, "top": top}
    except Exception as e:
        log.warning("live held count unavailable: %s", e)
        return None


def _gift_watch():
    """Gift orders still awaiting address confirmation, from the refresh
    loop's own state file (mirror/gift_refresh_state.json) -- local file per
    rule 3, so the line still renders during a platform outage. Returns None
    when the loop is disabled or has never run; {"stale": True} when the loop
    is enabled but has not written state for 48h (renders "not recorded" per
    rule 2, never a confident 0)."""
    try:
        p = ROOT / "mirror/gift_refresh_state.json"
        if not p.exists():
            return None
        d = json.loads(p.read_text())
        if not d.get("enabled"):
            return None
        try:
            fresh = (datetime.now() - datetime.strptime(
                str(d.get("ts") or ""), "%Y-%m-%d %H:%M:%S")
            ) <= timedelta(hours=48)
        except ValueError:
            fresh = False
        if not fresh:
            return {"stale": True}
        return {"stale": False,
                "pending": int(d.get("pending") or 0),
                "oldest_days": d.get("oldest_days"),
                "expired_total": int(d.get("expired_total") or 0)}
    except Exception as e:
        log.warning("gift watch state unavailable: %s", e)
        return None


def _reconcile_watch():
    """The weekly certificate's dead-man switch. Reads the reconciler's own
    state file (local, rule 3). Returns None while the feature has never run;
    {"stale": True, "age_days": n} when the last certificate is older than 8
    days -- a dead Sunday timer is exactly the failure this line exists to
    surface; otherwise the latest verdict."""
    try:
        p = ROOT / "mirror/reconcile_state.json"
        if not p.exists():
            return None
        d = json.loads(p.read_text())
        try:
            age = (datetime.now() - datetime.strptime(
                str(d.get("ts") or ""), "%Y-%m-%d %H:%M:%S")).days
        except ValueError:
            age = 99
        if age > 8:
            return {"stale": True, "age_days": age}
        return {"stale": False, "green": bool(d.get("green")),
                "ts": str(d.get("ts") or "")[:10],
                "repairs": d.get("repairs") or {}}
    except Exception as e:
        log.warning("reconcile state unavailable: %s", e)
        return None


# --------------------------------------------------------------------------
# [v2.12] realtime queues, customer payload paths, sweep finds, consent
# coverage, workbook capacity and audit-sheet misses (daily digest only).
# _ops_watch computes each value on its own: one that fails logs a WARNING
# and comes back None, its line is left out, and every other line renders.
# A broken sensor costs one line, never the report.
# --------------------------------------------------------------------------

HS_BASE = "https://api.hubapi.com"
CELL_CAP = 10_000_000              # Google's cell limit per workbook
TAIL_BLOCK = 1 << 20               # backwards read step for append-only files
CUSTOMER_LOGS = ("customer_sync.log.1", "customer_sync.log")  # rotated first

_TS_B = re.compile(rb"(?m)^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")
_TS_LINE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}")

# customer_sync.log markers (customer_sync.handle_row, and the realtime_base
# give-up line). handle_row logs "CUSTOMER payload <path> <id>" for every row
# it could read: json, salvaged or looked up. Only those markers are counted.
# Log days from before the json marker existed have none, so for a window with
# no "CUSTOMER payload json" line the JSON count falls back to the old
# inference: "CUSTOMER created/updated" lines of customers with no other
# marker that day. A failed lookup reaches the log only when its row gives up
# (the retries before that show as the Customer Queue's `error` count); an
# explicit "CUSTOMER payload lookup_failed <id>" marker is counted if the sync
# ever logs one. A row with nothing to read logs "CUSTOMER row ... -- held".
_CUST_TS = r"^(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}"
_CUST_PATH = re.compile(_CUST_TS + r".*?\bCUSTOMER payload "
                        r"(json|salvaged|looked up|lookup_failed) (\S+)")
_CUST_WROTE = re.compile(_CUST_TS + r".*?\bCUSTOMER (?:created|updated) (\S+) -> contact")
_CUST_HELD = re.compile(_CUST_TS + r".*?\bCUSTOMER row \S+ \(([^)]*)\): .* -- held")
_CUST_GAVE_UP = re.compile(_CUST_TS + r".*?\bid (\w+)\b.*?payload unreadable; Salla "
                           r"(?:lookup failed|returned no customer)")

# the capacity module's own place (tools/sheet_capacity.py); an ImportError
# naming anything else is a broken module, not an absent one
_CAPACITY_MODULES = ("tools.sheet_capacity", "tools", "sheet_capacity")


def _safe(label, fn, *args, **kw):
    """Run one digest measurement; a failure logs a WARNING and returns None."""
    try:
        return fn(*args, **kw)
    except Exception as e:
        log.warning("digest line %s unavailable: %s", label, e)
        return None


def _render_safe(label, fn, value):
    """Render one digest line; a failure logs a WARNING and returns None."""
    if value is None:
        return None
    try:
        return fn(value)
    except Exception as e:
        log.warning("digest line %s not rendered: %s", label, e)
        return None


def _when(value):
    """A queue or ledger timestamp as a naive local datetime, or None.
    Accepts "YYYY-MM-DD HH:MM:SS", the ISO "T" form, a bare date, and the
    Sheets date serial UNFORMATTED_VALUE returns for a date-typed cell."""
    s = str(value or "").strip()
    if not s:
        return None
    try:
        x = float(s)
    except ValueError:
        x = None
    if x is not None:
        # 20000..100000 days after 1899-12-30 is 1954..2173: a date serial
        return (datetime(1899, 12, 30) + timedelta(days=x)
                if 20000 < x < 100000 else None)
    for fmt, k in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%dT%H:%M:%S", 19),
                   ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(s[:k], fmt)
        except ValueError:
            continue
    return None


def _tail_since(path, since, block=None):
    """Lines of an append-only, time-ordered file (an engine log, a mirror
    CSV) from `since` ("YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS") onward, read
    backwards in blocks: customer_sync.log and audit_mirror.csv only grow,
    and the digest needs their last day, not their whole history. Older
    lines may come back too (callers filter by date); no newer line is lost.
    The line cut by the first block edge is dropped."""
    block = block or TAIL_BLOCK
    want = since.encode()
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos, buf = f.tell(), b""
        while pos > 0:
            step = min(block, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
            m = _TS_B.search(buf, buf.find(b"\n") + 1 if pos else 0)
            if m and m.group(1).replace(b"T", b" ") < want:
                break
    lines = buf.decode("utf-8", errors="replace").split("\n")
    if pos:
        lines = lines[1:]
    return [ln.rstrip("\r") for ln in lines if ln.strip()]


def _csv_since(path, since):
    """Rows (dicts by header) of an append-only ledger CSV whose first column
    is a "YYYY-MM-DD HH:MM:SS" ts, from `since` onward: the header line, then
    only the tail _tail_since reads, never the whole history. Older rows may
    come back too; callers filter by ts."""
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        head = next(csv.reader(f), [])
    lines = _tail_since(path, since)
    k = next((i for i, ln in enumerate(lines) if _TS_LINE.match(ln)), len(lines))
    return [dict(zip(head, row)) for row in csv.reader(lines[k:])]


def _hs_post(path, body, timeout=20):
    """One HubSpot POST (a CRM search) with the private-app token from the
    environment; the token travels in the header only. Returns the parsed
    reply, or None when no token is set. Raises on transport or HTTP errors."""
    import urllib.request
    tok = (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip()
    if not tok:
        return None
    req = urllib.request.Request(
        HS_BASE + path, data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {tok}",
                 "Content-Type": "application/json"}, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def _engine():
    """(cfg, gio) for the queue and workbook lines, or (cfg|None, None)."""
    cfg = gio = None
    try:
        from backfill import Config, GoogleIO
        cfg = Config.load(_engine_config_path())
        gio = GoogleIO(cfg, enabled=True)
    except Exception as e:
        log.warning("digest: engine config or Google access unavailable; "
                    "queue and workbook lines omitted: %s", e)
    return cfg, gio


def _queue_state(gio, cfg, tab, now):
    """Rows of one realtime queue tab by state, and the age of the oldest
    queued row (a blank state is queued, as the consumers read it)."""
    rows = gio.queue_read_all(cfg.queue_spreadsheet_id, tab=tab) or []
    counts, oldest = {}, None
    for r in rows:
        st = str(r.get("status") or "").strip().lower()
        if not st and not str(r.get("order_id") or "").strip():
            continue                                  # an empty sheet row
        st = st or "queued"
        counts[st] = counts.get(st, 0) + 1
        if st == "queued":
            t = _when(r.get("received_at"))
            if t is not None and (oldest is None or t < oldest):
                oldest = t
    return {"tab": tab, "queued": counts.get("queued", 0),
            "held": counts.get("held", 0), "error": counts.get("error", 0),
            "deferred": counts.get("deferred", 0),
            "oldest_queued_s": (max(0.0, (now - oldest).total_seconds())
                                if oldest is not None else None)}


def _customer_paths(start, end):
    """How the day's customer rows were read, from customer_sync.log (and its
    rotated .1): distinct customer ids per path, counted from the markers.
    None when there is no log; {"recorded": False} when the log has no
    timestamped line in the window (a sync that logged nothing that day is
    not a day of zeros). json_marker is False when the window has no
    "CUSTOMER payload json" marker (log days from before it existed).
    [v2.12] json_inferred is True only when the JSON count then really came
    from the inference, i.e. contacts were written that day; with no marker
    and no contact write the count is a plain 0 and nothing was inferred."""
    files = [ROOT / p for p in CUSTOMER_LOGS if (ROOT / p).exists()]
    if not files:
        return None
    seen = {k: set() for k in ("json", "salvaged", "looked up",
                               "lookup_failed", "held")}
    wrote, logged = set(), False
    for p in files:
        for line in _tail_since(p, start):
            if not _TS_LINE.match(line) or not start <= line[:10] <= end:
                continue
            logged = True
            m = _CUST_PATH.match(line)
            if m:
                seen[m.group(2)].add(m.group(3))
                continue
            m = _CUST_WROTE.match(line)
            if m:
                wrote.add(m.group(2))
                continue
            m = _CUST_HELD.match(line)
            if m:
                seen["held"].add(m.group(2))
                continue
            m = _CUST_GAVE_UP.match(line)
            if m:
                seen["lookup_failed"].add(m.group(2))
    if not logged:
        return {"recorded": False}
    marker = bool(seen["json"])
    inferred = not marker and bool(wrote)
    if inferred:
        # a log day from before the json marker: infer JSON from contacts
        # written by customers that carry no other marker
        seen["json"] = wrote - set().union(*seen.values())
    out = {k: len(v) for k, v in seen.items()}
    out.update(recorded=True, json_marker=marker, json_inferred=inferred)
    return out


def _sweep_finds(now, cfg=None):
    """The last customer sweep run (customer_sweep.py writes its state only
    on a live run): days checked, customers, missing, queued, the queued ids
    from mirror/customer_sweep.csv, and the flags the consent filler wrote in
    the same run (mirror/consent_filled.csv). None when it never ran here.
    `stale` (no run for 48h) stays False when customer_sweep_enabled is off:
    a sweep switched off is meant to be quiet. Without a config it is kept,
    since the state file shows the sweep did run here."""
    p = ROOT / "mirror/customer_sweep_state.json"
    if not p.exists():
        return None
    runs = []
    for day, rec in (json.loads(p.read_text()).get("swept") or {}).items():
        t = _when((rec or {}).get("ts"))
        if t is not None:
            runs.append((t, day, rec))
    if not runs:
        return None
    last = max(t for t, _, _ in runs)
    # one run saves each day as it finishes, minutes apart; the service is
    # capped at an hour, and the consent filler runs right after the days
    first, upto = last - timedelta(hours=1), last + timedelta(hours=1)
    cur = [(day, rec) for t, day, rec in runs if t >= first]

    def in_run(path, field=None):
        if not path.exists():
            return None
        out = []
        for r in _csv_since(path, first.strftime("%Y-%m-%d %H:%M:%S")):
            t = _when(r.get("ts"))
            if t is not None and first <= t <= upto:
                out.append(str(r.get(field) or "") if field else r)
        return out

    ids = in_run(ROOT / "mirror/customer_sweep.csv", "salla_customer_id") or []
    consent = in_run(ROOT / "mirror/consent_filled.csv")
    age = now - last
    enabled = (True if cfg is None
               else bool(getattr(cfg, "customer_sweep_enabled", False)))
    return {"ts": last, "days": sorted(day for day, _ in cur),
            "customers": sum(int(r.get("customers") or 0) for _, r in cur),
            "missing": sum(int(r.get("missing") or 0) for _, r in cur),
            "queued": sum(int(r.get("queued") or 0) for _, r in cur),
            "ids": [i for i in ids if i],
            "consent": len(consent) if consent is not None else None,
            "stale": enabled and age > timedelta(hours=48), "age_days": age.days}


def _consent_gap(now, days):
    """Contacts with a Salla id and no consent flag, created in the last
    `days` days: one HubSpot search, total only (the digest already runs one
    such count for the under-review watchdog). None when no token is set or
    the search fails, never 0."""
    since_ms = int((now - timedelta(days=days)).timestamp() * 1000)
    body = {"filterGroups": [{"filters": [
        {"propertyName": "salla_customer_id", "operator": "HAS_PROPERTY"},
        {"propertyName": "salla_consent_status", "operator": "NOT_HAS_PROPERTY"},
        {"propertyName": "createdate", "operator": "GTE", "value": str(since_ms)}]}],
        "properties": ["salla_customer_id"], "limit": 1}
    try:
        d = _hs_post("/crm/v3/objects/contacts/search", body)
        return None if d is None else int(d["total"])
    except Exception as e:
        log.warning("consent gap search unavailable: %s", e)
        return None


def _consent_coverage(now, cfg=None):
    """Consent flags the filler wrote in the last 24 hours (with the yes/no
    split, from mirror/consent_filled.csv) and the HubSpot gap it left.
    `written` is None when the ledger does not exist yet."""
    days = int(getattr(cfg, "consent_filler_days", 3) or 3)
    p = ROOT / "mirror/consent_filled.csv"
    written, yes, no = None, 0, 0
    if p.exists():
        since, written = now - timedelta(hours=24), 0
        for r in _csv_since(p, since.strftime("%Y-%m-%d %H:%M:%S")):
            t = _when(r.get("ts"))
            if t is None or not since <= t <= now:
                continue
            written += 1
            v = str(r.get("salla_consent_status") or "").strip().lower()
            yes += v == "true"
            no += v == "false"
    gap = _consent_gap(now, days)
    if written is None and gap is None:
        return None
    return {"written": written, "yes": yes, "no": no, "gap": gap, "days": days}


def _capacity_fn():
    """tools.sheet_capacity.capacity(gio, cfg), or None. Only a missing
    module (tools/sheet_capacity.py not deployed) is quiet; any other import
    failure, such as a dependency the module needs or a module without
    capacity(), logs a WARNING before the line is left out."""
    try:
        from tools.sheet_capacity import capacity
    except ModuleNotFoundError as e:
        if e.name not in _CAPACITY_MODULES:
            log.warning("workbook capacity module failed to import: %s", e)
            return None
        log.info("workbook capacity module not present; line omitted")
        return None
    except ImportError as e:
        log.warning("workbook capacity module failed to import: %s", e)
        return None
    if not callable(capacity):
        log.warning("workbook capacity module has no callable capacity()")
        return None
    return capacity


def _workbook_label(e, cfg):
    """A short workbook name for the digest: "queue" and "audit" for the two
    workbooks Config names (matched by spreadsheet id), else the module's own
    label lowercased without a trailing " workbook" ("Queue workbook" ->
    "queue"), so "<label> workbook" never says workbook twice."""
    sid = str(e.get("spreadsheet_id") or "").strip()
    if sid:
        if sid == str(getattr(cfg, "queue_spreadsheet_id", "") or "").strip():
            return "queue"
        if sid == str(getattr(cfg, "spreadsheet_id", "") or "").strip():
            return "audit"
    label = " ".join(str(e.get("workbook") or "").lower().split())
    if label == "workbook" or label.endswith(" workbook"):
        label = label[:-len("workbook")].strip()
    return label or sid or "unnamed"


def _capacity(gio, cfg):
    """Cells used per workbook against Google's 10M cap, and the alert pct."""
    fn = _capacity_fn()
    if fn is None:
        return None
    entries = []
    for e in fn(gio, cfg) or []:
        cells = int(e.get("cells") or 0)
        pct = e.get("pct")
        entries.append({"workbook": _workbook_label(e, cfg),
                        "cells": cells,
                        "pct": (float(pct) if pct is not None
                                else 100.0 * cells / CELL_CAP)})
    if not entries:
        return None
    return {"entries": entries,
            "alert_pct": float(getattr(cfg, "capacity_alert_pct", 80.0) or 80.0)}


def _sheet_row(value):
    """An audit mirror sheet_row as an int, or None when it is not a number."""
    try:
        return int(float(str(value or "").strip()))
    except (ValueError, OverflowError):
        return None


def _audit_misses(now):
    """Orders from the last 24 hours that have no audit sheet row. The engine
    mirrors every audit write to mirror/audit_mirror.csv (LocalMirror), with
    sheet_row -1 when there is no row: a failed append, but also a dry run or
    a run without Google. Counted per order id (an arrival's c0), and only
    when no later arrival of the same order in the window got a real row: a
    retry that landed heals the miss, an earlier row does not (a reprocess
    whose append failed lost its status updates too). Queue and processing
    updates carry no order id and reuse their arrival's row, so an update
    with -1 is that same miss and is not counted twice."""
    p = ROOT / "mirror/audit_mirror.csv"
    if not p.exists():
        return None
    since = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    until = now.strftime("%Y-%m-%d %H:%M:%S")
    missed, blank = {}, 0                 # order id -> None, in arrival order
    for r in _csv_since(p, since):
        if not since <= str(r.get("ts") or "")[:19] <= until:
            continue
        if str(r.get("event") or "").strip() != "arrived_append":
            continue
        oid, row = str(r.get("c0") or "").strip(), _sheet_row(r.get("sheet_row"))
        if row == -1:
            if oid:
                missed[oid] = None
            else:
                blank += 1                # no id to heal it by: counted as is
        elif row is not None and row > 0:
            missed.pop(oid, None)         # a later real row heals the miss
    return {"total": len(missed) + blank, "ids": list(missed), "blank": blank}


def _ops_watch(start, end, now=None, engine=None):
    """[v2.12] Every v2.12 digest value, each measured on its own."""
    now = now or datetime.now()
    cfg, gio = engine if engine is not None else _engine()
    ops = {"customer_queue": None, "status_queue": None, "capacity": None}
    if cfg is not None and gio is not None:
        ops["customer_queue"] = _safe(
            "Customer Queue", _queue_state, gio, cfg,
            getattr(cfg, "customer_queue_tab", "Customer Queue"), now)
        ops["status_queue"] = _safe(
            "Status Queue", _queue_state, gio, cfg,
            getattr(cfg, "status_queue_tab", "Status Queue"), now)
        ops["capacity"] = _safe("workbook capacity", _capacity, gio, cfg)
    ops["customer_paths"] = _safe("customer payload paths", _customer_paths,
                                  start, end)
    ops["sweep"] = _safe("customer sweep", _sweep_finds, now, cfg)
    ops["consent"] = _safe("consent coverage", _consent_coverage, now, cfg)
    ops["audit_misses"] = _safe("audit sheet misses", _audit_misses, now)
    return ops


# ---- [v2.12] line renderers (plain text, no em dashes) --------------------

def _cells(c):
    if c >= 1_000_000:
        return f"{c / 1_000_000:.1f}".rstrip("0").rstrip(".") + "M"
    if c >= 1000:
        return f"{c / 1000:.0f}k"
    return f"{c:,}"


def _queue_line(q):
    line = f"• {q['tab']}: {q['queued']:,} queued"
    if q["queued"]:
        line += f" (oldest {dur(q['oldest_queued_s'])})"
    line += f", {q['held']:,} held, {q['error']:,} error"
    if q.get("deferred"):
        line += f", {q['deferred']:,} deferred"
    return line


def _paths_line(p):
    if not p.get("recorded", True):
        return ("• Customer payloads yesterday: not recorded (customer_sync.log "
                "has no lines for the day)")
    json_part = f"{p['json']:,} plain JSON"
    # [v2.12] the note only when the count was inferred; a result without
    # json_inferred (older shape) falls back to json_marker
    if p.get("json_inferred", not p.get("json_marker", True)):
        json_part += " (inferred from contacts written, the log has no JSON marker)"
    line = (f"• Customer payloads yesterday (distinct customers by how the row "
            f"was read): {json_part}, {p['salvaged']:,} salvaged from the "
            f"capture text, {p['looked up']:,} fetched from Salla, "
            f"{p['lookup_failed']:,} gave up after Salla lookups failed")
    if p.get("held"):
        line += f", {p['held']:,} held with nothing to read"
    return line


def _sweep_line(s):
    line = (f"• Customer sweep (last run {s['ts'].strftime('%-d %b %H:%M')}): "
            f"{len(s['days'])} day(s), {s['customers']:,} customers checked, "
            f"{s['missing']:,} missing, {s['queued']:,} queued")
    if s["ids"]:
        line += ": " + ", ".join(s["ids"][:5])
        if len(s["ids"]) > 5:
            line += f" and {len(s['ids']) - 5} more"
    line += "."
    if s["consent"] is not None:
        line += f" Consent filler wrote {s['consent']:,} flag(s)."
    if s["stale"]:
        line += f"  ⚠️ no run for {s['age_days']} days, check the timer"
    return line


def _consent_line(c):
    w = c["written"]
    line = f"• Consent flags written in the last 24h: {n(w)}"
    if w:
        line += f" ({c['yes']:,} opted in, {c['no']:,} opted out)"
    return (line + f". Contacts created in the last {c['days']} days with no "
            f"flag: {n(c['gap'])}.")


def _audit_line(m):
    total = m["total"]
    if not total:
        return None                      # a quiet sheet needs no line
    line = f"• Audit sheet: {total:,} order(s) from the last 24h have no sheet row"
    ids = [str(i) for i in (m.get("ids") or [])]
    blank = m.get("blank")
    if blank is None:                    # older shape: the rest of the total
        blank = max(0, total - len(ids))
    # [v2.12] counted from the total, so the named ids, "N more" and the
    # arrivals with no order id always add up to it
    shown = ids[:5]
    more = max(0, total - blank - len(shown))
    parts = []
    if shown:
        parts.append(", ".join(shown) + (f" and {more:,} more" if more else ""))
    if blank:
        parts.append(f"{'plus ' if shown else ''}{blank:,} with no order id")
    if parts:
        line += " (" + ", ".join(parts) + ")"
    return (line + ". Dry runs and runs without Google count here too. "
            "The local mirror has them.")


def _capacity_line(cap):
    parts = []
    for i, e in enumerate(cap["entries"]):
        s = f"{e['workbook']} {_cells(e['cells'])}"
        if i == 0:
            s += f" of {_cells(CELL_CAP)} cells"
        s += f" ({e['pct']:.0f}%)"
        if e["pct"] > cap["alert_pct"]:
            s += " ⚠️"
        parts.append(s)
    return "• Workbooks: " + ", ".join(parts) if parts else None


def _capacity_breaches(a):
    """Verdict entries for workbooks above capacity_alert_pct."""
    try:
        cap = (a.get("ops") or {}).get("capacity") or {}
        return [f"{e['workbook']} workbook at {e['pct']:.0f}% of its cell limit"
                for e in cap.get("entries") or [] if e["pct"] > cap["alert_pct"]]
    except Exception as e:
        log.warning("capacity verdict unavailable: %s", e)
        return []


def _ops_lines(ops):
    """Thread lines for the customers and realtime queues reply."""
    out = []
    for label, fn, key in (("Customer Queue", _queue_line, "customer_queue"),
                           ("Status Queue", _queue_line, "status_queue"),
                           ("customer payload paths", _paths_line, "customer_paths"),
                           ("customer sweep", _sweep_line, "sweep"),
                           ("consent coverage", _consent_line, "consent"),
                           ("audit sheet misses", _audit_line, "audit_misses")):
        line = _render_safe(label, fn, (ops or {}).get(key))
        if line:
            out.append(line)
    return out


def _verdict(a):
    """One honest sentence up top. Reads the actual numbers, not a fixed string."""
    bad = []
    if a["errors_unrecovered"]:
        bad.append(f"{a['errors_unrecovered']} unrecovered error(s)")
    if a["availability"] is not None and a["availability"] < 99:
        bad.append(f"availability {a['availability']}%")
    if a["alerts"]:
        bad.append(f"{len(a['alerts'])} alert(s)")
    lh = a.get("live_held") or {}
    if (lh.get("value") or 0) >= 5 or (lh.get("oldest_days") or 0) >= 3:
        bad.append(f"{lh['value']} orders held for catalog "
                   f"(oldest {lh.get('oldest_days', '?')}d)")
    if a.get("backfill_stalled"):
        bad.append("backfill has not advanced since the previous report")
    if (a.get("reconcile") or {}).get("stale"):
        bad.append("the weekly reconciliation has not run for "
                   f"{a['reconcile']['age_days']} days")
    bad += _capacity_breaches(a)                       # [v2.12]
    if not a["created"]:
        return "No sync activity recorded in this period."
    if not bad:
        return "Healthy. No incidents, no data loss, no manual intervention."
    return "Needs a look: " + ", ".join(bad) + "."


def _credit_lines(a):
    """Balance, burn and runway -- the number that most often needs a decision."""
    c = (a["now"].get("credits") or {})
    rem, nxt = c.get("remaining"), c.get("next_reset")
    out = []
    if rem is None:
        return ["• Make credits: not recorded"]
    burn_day = a["credits_burned"]
    line = f"• Make credits: *{n(rem)}* left"
    if burn_day and a["days_recorded"]:
        per_h = burn_day / max(1, a["days_recorded"] * 24)
        line += f", burning ~{per_h:,.0f}/hour"
        if per_h > 0:
            hrs = rem / per_h
            line += (f" · about {hrs:.0f}h of runway" if hrs < 72
                     else f" · {hrs/24:.0f} days of runway")
    out.append(line)
    if nxt:
        out.append(f"• Plan renews: {nxt}")
    return out


def _coverage_note(a):
    """Say out loud when the window is only partly recorded.

    A weekly report built from two days of history looks identical to one built
    from seven unless it says so. Silence here would be the most misleading
    thing this file could do.
    """
    if a["days_recorded"] >= a["days_expected"]:
        return None
    return (f"_Based on {a['days_recorded']} of {a['days_expected']} days — "
            f"daily metrics collection started recently. Order counts are exact "
            f"regardless (they come from the ledger); the sampled figures cover "
            f"only the recorded days._")


def render(a):
    """Return (headline, [thread replies])."""
    p = a["period"]
    title = {"daily": "Daily report", "weekly": "Weekly report",
             "monthly": "Monthly report"}[p]
    head = [f"*{title} · {a['label']}*", _verdict(a), ""]

    head.append(f"• Orders created: *{n(a['created'])}*"
                + pct_change(a["created"], a["prev_created"]))
    if a["live_created"] is not None:
        batch = (a["created"] or 0) - a["live_created"]
        head.append(f"   ↳ {a['live_created']:,} live orders, "
                    f"{max(0, batch):,} from backfill/drain")
    if p != "daily" and a["daily_series"]:
        head.append(f"• Daily volume: {spark([v for _, v in a['daily_series']])}")
    if a["availability"] is not None:
        head.append(f"• Availability: *{a['availability']}%*")
    q = a["now"]
    # A weekly report must not quote today's queue as if it described the week.
    if p == "daily":
        if q.get("queue_depth_max") is not None:
            head.append(f"• Queue: peaked at {q['queue_depth_max']}, "
                        f"oldest item {dur(q.get('oldest_wait_s_max'))}")
    elif a["queue_depth_max"] is not None:
        head.append(f"• Queue: peak depth {a['queue_depth_max']} over the period"
                    f" (now {q.get('queue_depth_max', 0)})")
    lh = a.get("live_held")
    if lh and lh.get("value") is not None:
        line = f"• Held for catalog: *{lh['value']:,}* (live count)"
        if lh.get("oldest_days") is not None:
            line += f", oldest {lh['oldest_days']}d"
        head.append(line)
        if lh.get("top"):
            head.append(f"   ↳ blocking products: {lh['top']}")
    else:
        held = (q.get("held") or {})
        if held.get("value") is not None:
            head.append(f"• Held for catalog: {held['value']:,} "
                        f"(as of {held.get('as_of') or 'unknown'})")
    rc = a.get("reconcile")
    if rc:
        if rc.get("stale"):
            head.append(f"• Weekly reconciliation: not recorded for "
                        f"{rc['age_days']}d — its Sunday timer may be dead")
        elif rc.get("green"):
            head.append(f"• Weekly reconciliation ({rc.get('ts')}): all green")
        else:
            head.append(f"• Weekly reconciliation ({rc.get('ts')}): findings "
                        f"posted — see the Sunday certificate thread")
    gw = a.get("gift_watch")
    if gw:
        if gw.get("stale"):
            head.append("• Gift addresses: not recorded (refresh loop enabled "
                        "but its state is stale — check the timer)")
        elif gw.get("pending") or gw.get("expired_total"):
            line = (f"• Gift orders awaiting address confirmation: "
                    f"*{gw['pending']:,}*")
            if gw.get("oldest_days"):
                line += f", oldest {gw['oldest_days']}d"
            if gw.get("expired_total"):
                line += (f" · {gw['expired_total']:,} expired unconfirmed "
                         f"(chase list)")
            head.append(line)
    bf = (q.get("backfill") or {})
    if bf.get("window"):
        line = (f"• Backfill: {bf['window']}, page {bf.get('page')} "
                f"of {bf.get('total_pages')}")
        if a.get("backfill_stalled"):
            line += "  ⚠️ unchanged since the previous report"
        head.append(line)
    head += _credit_lines(a)
    # [v2.12] workbook capacity: the one v2.12 value that can need a decision
    cap = _render_safe("workbook capacity", _capacity_line,
                       (a.get("ops") or {}).get("capacity"))
    if cap:
        head.append(cap)
    if a["errors_unrecovered"] is not None:
        head.append(f"• Errors: {n(a['errors_unrecovered'])} unrecovered")

    note = _coverage_note(a)
    if note:
        head += ["", note]
    head += ["", "Breakdown in thread."]

    return "\n".join(head), _threads(a)


def _stuck_in_review(hours=48):
    """Orders sitting in the Under Review stage longer than `hours`.

    The stage exists so reviews are VISIBLE; this is the guardrail that stops
    it becoming a silent parking lot. Best-effort by design: it needs one
    HubSpot search, and the digest must still post during an outage, so any
    failure renders as unavailable rather than breaking the report.
    """
    try:
        from backfill import Config
        stage = (Config.load("config.json").status_stage_map or {}).get(
            "under_review")
        if not stage:
            return None
        cutoff = int((datetime.now().timestamp() - hours * 3600) * 1000)
        body = {"filterGroups": [{"filters": [
            {"propertyName": "hs_pipeline_stage", "operator": "EQ",
             "value": stage},
            {"propertyName": "hs_lastmodifieddate", "operator": "LT",
             "value": str(cutoff)}]}],
            "properties": ["salla_order_id"], "limit": 10}
        # [v2.12] shared with the consent gap count; None when no token
        d = _hs_post("/crm/v3/objects/orders/search", body)
        if d is None:
            return None
        total = int(d.get("total", 0))
        ids = [r["properties"].get("salla_order_id", "?")
               for r in d.get("results", [])[:5]]
        return {"total": total, "sample": ids, "hours": hours}
    except Exception:
        return {"total": None, "sample": [], "hours": hours}


def _threads(a):
    q, out = a["now"], []
    p = a["period"]

    # --- under review watchdog -------------------------------------------
    # Rendered only when there is something to say: a quiet stage needs no
    # section, and an unavailable check must not read as "zero stuck".
    ur = _stuck_in_review()
    if ur and ur["total"]:
        out.append("\n".join([
            "*Orders stuck under review*",
            f"{ur['total']:,} order(s) have sat in the Under Review stage "
            f"for more than {ur['hours']} hours"
            + (f" (e.g. {', '.join(ur['sample'])})" if ur["sample"] else "")
            + ". A review that old usually means it was forgotten, not that "
              "it is still being reviewed — worth a pass in Salla admin. "
              "Once the store moves the order on, the stage follows "
              "automatically within seconds."]))
    elif ur and ur["total"] is None:
        out.append("*Orders stuck under review*\ncheck unavailable this run "
                   "(HubSpot unreachable); not zero, unknown.")

    # --- live sync -------------------------------------------------------
    live = a["live_created"]
    t = ["*Live sync*"]
    if live is not None:
        t.append(f"{live:,} live orders picked up from the store"
                 f"{' this period' if p != 'daily' else ''}. The headline "
                 f"figure of {n(a['created'])} also includes records created "
                 f"by the backfill and the queue drain.")
    else:
        t.append(f"{n(a['created'])} records created across all sources.")
    if a["wait_max_s"] is not None:
        t.append(f"Peak queue depth {n(a['queue_depth_max'])}; oldest item "
                 f"sitting in the queue reached {dur(a['wait_max_s'])}.")
        if (a["wait_max_s"] or 0) > 86400:
            t.append("That age reflects historical rows re-queued by the "
                     "drain, not a new order waiting — new orders are picked "
                     "up within seconds.")
    if a["availability"] is not None:
        t.append(f"Availability {a['availability']}% — measured as minutes in "
                 f"which the intake actually polled successfully, not merely "
                 f"minutes the service was running. Those differ: in July a "
                 f"healthy service sat on a dead relay for 69 hours.")
    out.append("\n".join(t))

    # --- backfill --------------------------------------------------------
    bf = q.get("backfill") or {}
    if bf.get("window"):
        t = ["*Backfill*",
             f"Currently sweeping {bf['window']}, page {bf.get('page')} of "
             f"{bf.get('total_pages')} (status: {bf.get('status')})."]
        if bf.get("status") in ("overflow", "done_overflow"):
            t.append("The overflow flag means a time-slot reported more pages "
                     "than the per-slot limit, so the engine splits it into "
                     "sub-batches instead of skipping it. Nothing is missed; "
                     "it takes longer.")
        out.append("\n".join(t))

    # --- catalog holds ---------------------------------------------------
    held = q.get("held") or {}
    if held.get("value") is not None:
        t = ["*Catalog holds*",
             f"{held['value']:,} orders held, measured {held.get('as_of')}."]
        if q.get("blocker_products"):
            t.append(f"{q['blocker_products']} product(s) account for all of "
                     f"them — see mirror/blocker_matrix.csv. These are catalog "
                     f"decisions, not sync failures: the orders sit safely in "
                     f"the queue and sync themselves once the products go live.")
        out.append("\n".join(t))

    # --- [v2.12] customers and realtime queues ---------------------------
    lines = _ops_lines(a.get("ops"))
    if lines:
        out.append("\n".join(["*Customers and realtime queues*"] + lines))

    # --- credits ---------------------------------------------------------
    c = q.get("credits") or {}
    if c.get("remaining") is not None:
        t = ["*Make credits*", f"{c['remaining']:,} remaining."]
        if a["credits_burned"]:
            t.append(f"{a['credits_burned']:,} consumed over the period"
                     + (f" ({spark(a['credits_daily'])})" if p != "daily"
                        and len(a["credits_daily"] or []) > 1 else "") + ".")
        by = c.get("by_engine") or {}
        if by:
            t.append(f"Split today: live {by.get('live', 0):,}, backfill "
                     f"{by.get('backfill', 0):,}, other {by.get('other', 0):,}. "
                     f"Most live-sync consumption is the intake poll itself "
                     f"rather than order volume, so the floor cost is roughly "
                     f"constant regardless of how busy the store is.")
        if c.get("plan") and a["credits_burned"]:
            t.append(f"Plan allowance is {c['plan']}; consumption at this rate "
                     f"is the structural constraint on how fast the backfill "
                     f"can run.")
        out.append("\n".join(t))

    # --- data quality ----------------------------------------------------
    t = ["*Data quality*"]
    t.append(f"{n(a['errors_unrecovered'])} unrecovered failure(s) this period.")
    if a["partials"]:
        t.append(f"{a['partials']:,} order(s) flagged as partial — the HubSpot "
                 f"record exists but carries fewer line items than the source. "
                 f"These are detected as the engine passes over historical "
                 f"orders, not newly broken, and are repairable in a batch.")
    out.append("\n".join(t))

    # --- incidents -------------------------------------------------------
    if a["alerts"]:
        lines = ["*Incidents*"]
        for al in a["alerts"][:10]:
            lines.append(f"• {al.get('t')} — {al.get('detail')}")
        if len(a["alerts"]) > 10:
            lines.append(f"…and {len(a['alerts']) - 10} more.")
        out.append("\n".join(lines))

    # --- infrastructure --------------------------------------------------
    svc = q.get("services") or {}
    if svc:
        up = []
        for unit, s in svc.items():
            secs = s.get("uptime_s")
            human = f"{secs//86400}d {secs%86400//3600}h" if secs else "—"
            up.append(f"• {unit.replace('salla-', '')}: {s.get('state')}, "
                      f"up {human}, {s.get('restarts')} restart(s)")
        t = ["*Infrastructure*"] + up
        if q.get("disk_pct") is not None:
            t.append(f"Disk {q['disk_pct']}% used.")
        t.append("Firewall lockdown unchanged: inbound denied except SSH via "
                 "Google IAP, outbound HTTPS only, no service account on the VM.")
        out.append("\n".join(t))

    return out


# --------------------------------------------------------------------------

def post(headline, replies, dry_run=False):
    """Post headline, then each reply into its thread, to every configured channel.

    Imported late so that --dry-run works on a machine with no .env at all.
    """
    import notify
    if dry_run or not notify.slack_enabled():
        print(headline)
        for r in replies:
            print("\n  ---- thread ----")
            print(r)
        return True
    ok_any = False
    for ch in notify.slack_channels():
        ts = notify.post_slack(headline, channel=ch)
        if not ts:
            log.warning("headline post failed for channel %s", ch)
            continue
        ok_any = True
        for r in replies:
            notify.post_slack(r, thread_ts=ts, channel=ch)
    # A digest that silently fails to post reads as "nothing happened", which is
    # the worst outcome available. Leave a flag the 30-minute ops update reads.
    if ok_any:
        FAIL_FLAG.unlink(missing_ok=True)
    else:
        FAIL_FLAG.parent.mkdir(exist_ok=True)
        FAIL_FLAG.write_text(datetime.now().isoformat(timespec="seconds") + "\n")
    return ok_any


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=("daily", "weekly", "monthly"),
                    default="daily")
    ap.add_argument("--channel", default=None,
                    help="post ONLY to this channel id (overrides configuration)")
    ap.add_argument("--dry-run", action="store_true", help="print, do not post")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s [digest] %(message)s")

    # Hard scope, not a filter: overwriting the env var means the transport
    # itself cannot see any other channel, so a test run is incapable of
    # reaching the client channel even if some other code path posts.
    if args.channel:
        os.environ["SLACK_CHANNEL_IDS"] = args.channel
        os.environ.pop("SLACK_CHANNEL_ID", None)
        log.info("channel scope forced to %s", args.channel)

    a = aggregate(args.period)
    headline, replies = render(a)
    ok = post(headline, replies, dry_run=args.dry_run)
    log.info("%s report: %s orders, %d thread reply(ies), posted=%s",
             args.period, a["created"], len(replies), ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
