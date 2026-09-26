#!/usr/bin/env python3
"""Make.com credit watcher (v2.2) -- the alarm that fires BEFORE the wall.

The 2026-07-23 and 2026-07-29 outages both had the same shape: credits ran out,
every scenario silently paused, and the first human signal was orders not
syncing. relay_health.py detects that after the fact; this module exists so the
fact never happens -- it polls the real balance and escalates while there is
still time to top up.

What it watches (one read-only GET /organizations/{id} per tick):
  * unusedCenticredits  -> exact credits remaining (centicredits / 100)
  * centicreditsExtra   -> exact extra credits purchased this cycle, so a refill
                           is detected as a precise delta, not a guess
  * lastReset/nextReset -> billing-cycle boundaries -> renewal notice
  * GET /organizations/{id}/usage -> Make's own 30-day daily ledger, so
    today/this week/this month are real figures rather than a total
    accumulated since this watcher happened to start
  * GET /scenarios/consumptions -> cycle-to-date spend per scenario, grouped
    by ROLE (fetch relay / live intake / other). Deliberately not "backfill vs
    live": both engines call the same fetch-relay scenario, so splitting it
    between them would be invented (see scenario_breakdown)
  * GET /dlqs?scenarioId=...&status=unresolved -> retry-queue items Make has
    given up on, per watched scenario (v2.12: Config.make_dlq_watch, six
    scenarios by default, items Make is still retrying skipped, a
    dlq_min_age_minutes floor on top, paged). scan_dlq is shared with
    reconcile.py so the Sunday certificate and this alert count the same way

What it says (via notify.py -- Slack + email, never through Make itself):
  * threshold alerts as the balance crosses 50k / 10k / 1k (configurable)
  * "out of credits" when the balance hits zero -- and it KEEPS polling while
    out, then sends a comforting all-clear the moment credits return
  * "topped up +N" on any refill, with before/after balances
  * "new billing month" when the cycle renews, with the fresh allowance

Runs standalone (`python credit_watch.py`) so it stays awake even when both
engines are stopped or crashed -- a watcher that dies with the thing it watches
is not a watcher. Single-instance via flock; STOP.credits stops it gracefully.
State persists in mirror/credit_state.json (also read by the dashboard).
"""

import argparse
import fcntl
import json
import logging
import math
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from backfill import Config, now_str  # noqa: E402
import notify  # noqa: E402

log = logging.getLogger("backfill")

MAKE_API = "https://eu1.make.com/api/v2"
# Cloudflare 403s urllib's default UA ("error code: 1010") on every endpoint,
# which is indistinguishable from bad auth. Verified 2026-07-27.
UA = "SallaHubSpotSync/2.2 (+credit-watch)"

STATE_FILE = Path("mirror/credit_state.json")
LOCK_FILE = Path("credit_watch.lock")
STOP_FILE = Path("STOP.credits")


def _get(path):
    token = os.environ.get("MAKE_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("MAKE_API_TOKEN missing from environment")
    req = urllib.request.Request(
        MAKE_API + path,
        headers={"Authorization": f"Token {token}", "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _safe_err(e):
    """Exception text fit for a log line. The token travels in a header and
    no known error echoes it, but scrub it anyway before truncating, so a
    library that ever did could not leak half of it."""
    msg = f"{type(e).__name__}: {e}"
    tok = os.environ.get("MAKE_API_TOKEN", "").strip()
    if tok:
        msg = msg.replace(tok, "***")
    return msg[:200]


# ------------- Make retry queue (DLQ) scan (v2.12) -------------
#
# Shared by CreditWatch.check_relay_dlq (the 12h alert) and reconcile.py (the
# Sunday certificate), so the two can never disagree about what counts.

DLQ_PAGE = 100          # Make's pg[limit] ceiling
DLQ_MAX_ITEMS = 500     # per scenario per scan; a capped count is a floor
DLQ_MIN_AGE_DEFAULT = 45
# [v2.12] Make retries an incomplete execution on its own backoff (1, 10, 10,
# 30, 30, 180 and 180 minutes, about 7.4 h in all). While it does, the item's
# derived status is scheduled or inprogress, so those are Make's, not lost
# work. GET /dlqs asks for status=unresolved; this is the client-side twin in
# case the API ignores the parameter.
DLQ_NOT_STUCK = ("resolved", "scheduled", "inprogress")
DLQ_FAIL_ALERT_S = 6 * 3600     # continuous read failure before it alerts
DLQ_COOLDOWN_S = 12 * 3600      # one retry-queue alert per 12h, any kind
# [v2.12] A failure streak is only continuous while failed ticks follow each
# other. A gap of more than this many poll intervals since the last failed
# tick (the watcher was stopped, the VM was down) starts a new streak.
DLQ_FAIL_GAP_POLLS = 3
POLL_S_DEFAULT = 300.0          # main()'s --interval default
# [v2.12] dlq_min_age_minutes ceiling: a longer floor would hide lost work
# for good, and a huge one overflows timedelta in scan_dlq.
DLQ_MIN_AGE_MAX = 7 * 24 * 60

# [v2.12] Replay notes go to Slack as written: plain sentences, no dashes.
# Replaying is NOT safe everywhere, so each scenario says what a replay does.
REPLAY_ORDER = ("Replaying re-sends the order to the engine. Safe: the "
                "engine skips orders it already has.")
# The backfill relay is the synchronous fetch relay both engines call and
# wait on; by the time an item sits here the engine has retried that fetch.
REPLAY_FETCH = ("Replaying does nothing useful: the engine already retried "
                "the fetch itself. Dismiss it.")
REPLAY_CAPTURE = ("Replaying adds the event to the queue sheet again. Safe: "
                  "the consumer skips the twin.")
REPLAY_DIRECT = ("Replaying writes straight to HubSpot. Check the contact "
                 "first.")
REPLAY_UNKNOWN = "Check what this scenario writes before replaying."

# The fixed part of the default watch. The two order relays are read from
# their own Config ids (make_intake_scenario_id, make_backfill_scenario_id)
# so a re-created relay is watched without editing this list. Ids per
# blueprints/baseline-2026-09-26/README.md.
#
# stores_incomplete: 6892982 and 6893541 have "Store incomplete executions"
# OFF today, so Make never parks their failures and there is nothing to read.
# Plan step 2 turns storage on for both; when it does, the flag must be
# flipped to true in config (Config.make_dlq_watch, which replaces this list).
DEFAULT_DLQ_WATCH = (
    {"id": "6892982", "label": "customer capture",
     "replay_note": REPLAY_CAPTURE, "stores_incomplete": False},
    {"id": "6893541", "label": "status capture",
     "replay_note": REPLAY_CAPTURE, "stores_incomplete": False},
    {"id": "5563154", "label": "customer updated",
     "replay_note": REPLAY_DIRECT},
    {"id": "5780791", "label": "abandoned cart",
     "replay_note": REPLAY_DIRECT},
)

# dlq_watch_list and dlq_min_age run every tick; a bad config value is worth
# one WARNING per process, not one every five minutes.
_CONFIG_WARNED = set()


def _config_warning(msg, *args):
    text = msg % args
    if text in _CONFIG_WARNED:
        return
    _CONFIG_WARNED.add(text)
    log.warning("%s", text)


def _flag(v, default=True):
    """A config boolean that may arrive as a JSON bool or a string."""
    if v is None:
        return default
    if isinstance(v, str):
        s = v.strip().lower()
        if not s:
            return default
        return s not in ("false", "0", "no", "off")
    return bool(v)


def dlq_min_age(cfg):
    """[v2.12] Config.dlq_min_age_minutes as minutes, between 0 and
    DLQ_MIN_AGE_MAX (7 days). A secondary floor: status=unresolved already
    leaves Make's own retries alone. Unset means 45; a value that is not a
    number (an int too large for a float included) logs one WARNING and
    means 45; a value over 7 days logs one WARNING and means 7 days."""
    raw = getattr(cfg, "dlq_min_age_minutes", DLQ_MIN_AGE_DEFAULT)
    if raw is None:
        return float(DLQ_MIN_AGE_DEFAULT)
    try:
        if isinstance(raw, bool):
            raise TypeError("a bool is not a number of minutes")
        mins = float(raw)
        if not math.isfinite(mins):
            raise ValueError("not finite")
    except (TypeError, ValueError, OverflowError):
        _config_warning("config dlq_min_age_minutes=%.60r is not a number; "
                        "using %d", raw, DLQ_MIN_AGE_DEFAULT)
        return float(DLQ_MIN_AGE_DEFAULT)
    if mins > DLQ_MIN_AGE_MAX:
        _config_warning("config dlq_min_age_minutes=%.60r is over 7 days; "
                        "using %d", raw, DLQ_MIN_AGE_MAX)
        return float(DLQ_MIN_AGE_MAX)
    return max(0.0, mins)


def _watch_entries(raw):
    out, seen = [], set()
    for e in raw:
        if not isinstance(e, dict):
            e = {"id": e}
        sid = str(e.get("id") or "").strip()
        if not sid or sid in seen:
            continue
        if not (sid.isascii() and sid.isdigit()):
            _config_warning("config make_dlq_watch: scenario id %r is not a "
                            "number; skipped", sid[:40])
            continue
        seen.add(sid)
        out.append({"id": sid,
                    "label": str(e.get("label") or f"scenario {sid}"),
                    "replay_note": str(e.get("replay_note") or REPLAY_UNKNOWN),
                    "stores_incomplete": _flag(e.get("stores_incomplete"))})
    return out


def dlq_watch_list(cfg):
    """[v2.12] The watched scenarios as [{id, label, replay_note,
    stores_incomplete}].

    Config.make_dlq_watch overrides the default: a list of {id, label,
    replay_note, stores_incomplete} entries (a {label: id} mapping is
    accepted too). Unset or empty means the two order relays plus
    DEFAULT_DLQ_WATCH. Any other type, or a list with no usable id, logs a
    WARNING and means the default. Blank ids are skipped, ids that are not
    numbers are skipped with a WARNING, and a scenario listed twice is
    watched once."""
    raw = getattr(cfg, "make_dlq_watch", None)
    if isinstance(raw, dict):
        raw = [{"id": v, "label": k} for k, v in raw.items()]
    elif raw is not None and not isinstance(raw, (list, tuple)):
        _config_warning("config make_dlq_watch is a %s, not a list of "
                        "{id, label, replay_note}; watching the defaults",
                        type(raw).__name__)
        raw = None
    if raw:
        out = _watch_entries(raw)
        if out:
            return out
        _config_warning("config make_dlq_watch names no usable scenario id; "
                        "watching the defaults")
    return _watch_entries(
        [{"id": getattr(cfg, "make_intake_scenario_id", ""),
          "label": "live intake", "replay_note": REPLAY_ORDER},
         {"id": getattr(cfg, "make_backfill_scenario_id", ""),
          "label": "backfill relay", "replay_note": REPLAY_FETCH},
         *DEFAULT_DLQ_WATCH])


def _parse_make_ts(s):
    """Make's ISO timestamp ('2026-09-13T08:15:22.123Z') as aware UTC."""
    s = str(s or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _dlq_open(x):
    """True for an item Make has given up on: not resolved, and not
    scheduled for (or in the middle of) one of Make's own retries."""
    if x.get("resolved"):
        return False
    status = re.sub(r"[^a-z]", "", str(x.get("status") or "").lower())
    return status not in DLQ_NOT_STUCK


def _dlq_items(get, sid):
    """Retry-queue records for one scenario, newest first, paged. Returns
    (items, capped); capped means the list is a floor, not the whole queue.

    Asks Make for status=unresolved only; scan_dlq filters again in case the
    API ignores that. Items are de-duplicated by id across pages. A page that
    adds no new id ends the scan (an API that ignored pg[offset] would hand
    back page one forever) and counts as capped. Otherwise stops on a short
    page or at DLQ_MAX_ITEMS."""
    items, seen, offset = [], set(), 0
    while True:
        d = get(f"/dlqs?scenarioId={sid}&status=unresolved"
                f"&pg[sortBy]=created&pg[sortDir]=desc"
                f"&pg[limit]={DLQ_PAGE}&pg[offset]={offset}") or {}
        raw = d.get("dlqs") or []
        fresh = []
        for x in raw:
            if not isinstance(x, dict):
                continue
            key = x.get("id")
            if key is not None:
                key = str(key)
                if key in seen:
                    continue
                seen.add(key)
            fresh.append(x)
        items.extend(fresh)
        if len(raw) < DLQ_PAGE:
            return items, False
        if len(items) >= DLQ_MAX_ITEMS:
            return items[:DLQ_MAX_ITEMS], True
        if not fresh:
            log.info("make retry queue for scenario %s: the page at offset %d "
                     "added no new items; stopped paging at %d read",
                     sid, offset, len(items))
            return items, True
        offset += DLQ_PAGE


def scenario_link(cfg, sid):
    team = str(getattr(cfg, "make_team_id", "") or "")
    if not team:
        return f"scenario {sid}"
    return f"https://eu1.make.com/{team}/scenarios/{sid}"


def scan_dlq(cfg, get=None, now=None, quiet=False):
    """[v2.12] Retry-queue items Make has given up on, per watched scenario.

    Make parks a failed execution in the scenario's retry queue (the API calls
    it a DLQ) and retries it on its own backoff (1, 10, 10, 30, 30, 180, 180
    min, about 7.4 h). While it does, the item is scheduled or inprogress;
    once it succeeds it is resolved. Only the rest (Make's "unresolved") is
    lost work, so that is all that counts. Config.dlq_min_age_minutes is a
    secondary floor on top: anything younger never counts.

    Returns one dict per watched scenario: id, label, replay_note,
    stores_incomplete, link, count, capped, records, oldest (ISO),
    oldest_age_min and error. count is None, never a fake 0, when the read
    failed (error set), when the scan was capped and found nothing unresolved
    on the pages it read (not measured), or when the scenario does not store
    incomplete executions (not read at all). Read-only, never raises. A failed
    read logs WARNING with the scenario label (never the token); quiet=True
    drops that to DEBUG for a caller that logs state changes itself."""
    get = get or _get
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    min_age = timedelta(minutes=dlq_min_age(cfg))
    out = []
    for w in dlq_watch_list(cfg):
        row = dict(w, link=scenario_link(cfg, w["id"]), count=None,
                   capped=False, records=0, oldest="", oldest_age_min=None,
                   error="")
        out.append(row)
        if not w["stores_incomplete"]:
            continue            # Make parks nothing here: nothing to read
        try:
            items, row["capped"] = _dlq_items(get, w["id"])
        except Exception as e:
            row["error"] = _safe_err(e)
            (log.debug if quiet else log.warning)(
                "make retry queue read failed for %s: %s",
                w["label"], row["error"])
            continue
        row["records"] = len(items)
        n, oldest = 0, None
        for x in items:
            if not _dlq_open(x):
                continue
            ts = _parse_make_ts(x.get("created"))
            # an unreadable timestamp cannot prove the item is young: count it
            if ts is not None and now - ts < min_age:
                continue
            n += 1
            if ts is not None and (oldest is None or ts < oldest):
                oldest = ts
        if row["capped"] and not n:
            # more pages exist and the ones read hold nothing that counts:
            # that is not "clear", it is not measured
            continue
        row["count"] = n
        if oldest is not None:
            row["oldest"] = oldest.isoformat()
            row["oldest_age_min"] = int((now - oldest).total_seconds() // 60)
    return out


def fmt_age(minutes):
    if minutes is None:
        return "unknown"
    m = int(minutes)
    if m < 120:
        return f"{m} min"
    if m < 48 * 60:
        return f"{m // 60} hours"
    return f"{m // 1440} days"


def dlq_line(row):
    """One plain line per scenario, shared by the alert and the certificate."""
    if not row.get("stores_incomplete", True):
        return (f"{row['label']}: not watchable (incomplete executions are "
                f"not stored)")
    if row["count"] is None:
        if row.get("capped") and not row.get("error"):
            return (f"{row['label']}: not measured ({row.get('records', 0)}+ "
                    f"records, none unresolved on the pages read)")
        return f"{row['label']}: not checked (the Make API read failed)"
    if not row["count"]:
        return f"{row['label']}: clear"
    n = f"{row['count']}{'+' if row['capped'] else ''}"
    # capped: newest first, so the oldest read is not the oldest there is
    seen = "oldest read" if row["capped"] else "oldest"
    return (f"{row['label']}: {n} stuck, {seen} "
            f"{fmt_age(row['oldest_age_min'])} old. {row['replay_note']} "
            f"{row['link']}")


def dlq_state(rows):
    """Per-scenario counts for credit_state.json (relay_dlq_by_scenario)."""
    return {r["id"]: {"label": r["label"], "count": r["count"],
                      "capped": r["capped"], "oldest": r["oldest"],
                      "watchable": bool(r.get("stores_incomplete", True)),
                      "measured": r["count"] is not None}
            for r in rows}


class CreditWatch:
    def __init__(self, cfg, dry_run=False, poll_s=POLL_S_DEFAULT):
        self.cfg = cfg
        self.dry_run = dry_run
        # [v2.12] seconds between ticks: a longer gap between two failed
        # retry-queue reads breaks their streak (_track_read_failures)
        self.poll_s = poll_s
        self.state = self._load()

    # ------------- state -------------

    def _load(self):
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {"fired": {}, "daily": {}, "snap": {}, "out": False}

    def _save(self):
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.state))
            tmp.replace(STATE_FILE)
        except Exception as e:
            log.warning("credit state save failed: %s", e)

    # ------------- data -------------

    def read_org(self):
        org_id = str(getattr(self.cfg, "make_org_id", "") or "")
        if not org_id:
            raise RuntimeError("make_org_id missing from config")
        d = _get(f"/organizations/{org_id}")
        org = d.get("organization", d)
        return {
            "remaining": int(org.get("unusedCenticredits") or 0) / 100.0,
            "consumed": int(org.get("centicreditsConsumed") or 0) / 100.0,
            "extra": int(org.get("centicreditsExtra") or 0) / 100.0,
            "plan": int((org.get("license") or {}).get("operations") or 0),
            "last_reset": str(org.get("lastReset") or ""),
            "next_reset": str(org.get("nextReset") or ""),
            "auto_purchase_on": bool(org.get("autoPurchasingActivated")),
            "auto_purchase_running": bool(org.get("autoPurchaseInProgress")),
        }

    def read_consumptions(self):
        team_id = str(getattr(self.cfg, "make_team_id", "") or "")
        if not team_id:
            return {}
        d = _get(f"/scenarios/consumptions?teamId={team_id}")
        return {str(r.get("scenarioId")): int(r.get("operations") or 0)
                for r in (d.get("scenarioConsumptions") or [])}

    def read_usage(self):
        """Make's own 30-day daily ledger -- the authoritative per-day spend.

        This replaces deriving daily totals from our own polling deltas, which
        could only ever cover the period since the watcher started (and so made
        'today', 'this week' and 'this month' identical on day one).
        """
        org_id = str(getattr(self.cfg, "make_org_id", "") or "")
        if not org_id:
            return {}
        d = _get(f"/organizations/{org_id}/usage?organizationTimezone=true")
        rows = d.get("usage") or d.get("data") or []
        out = {}
        for r in rows:
            day = str(r.get("date") or "")[:10]
            if not day:
                continue
            cc = r.get("centicredits")
            out[day] = (int(cc) / 100.0) if cc is not None else float(
                r.get("operations") or 0)
        return out

    def read_scenario_names(self):
        """{scenarioId: name} for the team. Cached ~1h -- names rarely change
        and the Core plan's API budget is only 60 req/min."""
        team_id = str(getattr(self.cfg, "make_team_id", "") or "")
        if not team_id:
            return {}
        cached = self.state.get("names") or {}
        if cached and (time.time() - float(self.state.get("names_ts", 0))) < 3600:
            return cached
        d = _get(f"/scenarios?teamId={team_id}")
        names = {str(s.get("id")): str(s.get("name") or "")
                 for s in (d.get("scenarios") or [])}
        if names:
            self.state["names"] = names
            self.state["names_ts"] = time.time()
        return names or cached

    def scenario_breakdown(self, cons):
        """Cycle-to-date credits per scenario, grouped by ROLE.

        Deliberately not labelled "backfill vs live": the fetch relay is a
        single Make scenario that BOTH engines call (identical relay_url), so
        attributing it to either engine would be a fabrication. Roles are what
        the API can actually prove.

        The "other" bucket is the biggest share of the bill and used to be a
        black box, so it also returns the named scenarios behind it -- enough
        of them, largest first, to cover ~80% of that spend (Pareto).
        """
        fetch = str(getattr(self.cfg, "make_backfill_scenario_id", "") or "")
        intake = str(getattr(self.cfg, "make_intake_scenario_id", "") or "")
        out = {"fetch_relay": 0, "live_intake": 0, "other": 0}
        others = []
        try:
            names = self.read_scenario_names()
        except Exception as e:
            log.debug("scenario names unavailable: %s", e)
            names = {}
        for sid, ops in cons.items():
            ops = int(ops)
            if sid == fetch:
                out["fetch_relay"] += ops
            elif sid == intake:
                out["live_intake"] += ops
            else:
                out["other"] += ops
                if ops > 0:
                    others.append({"id": sid,
                                   "name": names.get(sid) or f"scenario {sid}",
                                   "credits": ops})
        others.sort(key=lambda x: -x["credits"])
        total = out["other"] or 1
        top, run = [], 0
        for s in others:
            top.append(dict(s, pct=round(s["credits"] / total * 100, 1)))
            run += s["credits"]
            if run / total >= 0.80 or len(top) >= 8:
                break
        out["other_top"] = top
        out["other_count"] = len(others)
        out["other_covered_pct"] = round(run / total * 100, 1) if others else 0
        return out

    def _attribute(self, cons):
        """Fold per-scenario consumption deltas into today's engine buckets."""
        bf = str(getattr(self.cfg, "make_backfill_scenario_id", "") or "")
        lv = str(getattr(self.cfg, "make_intake_scenario_id", "") or "")
        snap = self.state.get("snap", {})
        today = datetime.now().strftime("%Y-%m-%d")
        day = self.state.setdefault("daily", {}).setdefault(
            today, {"backfill": 0, "live": 0, "other": 0, "total": 0})
        for sid, ops in cons.items():
            prev = int(snap.get(sid, ops))  # first sighting -> delta 0
            delta = ops - prev
            if delta < 0:      # cycle reset zeroed the counters
                delta = ops
            if delta:
                key = "backfill" if sid == bf else ("live" if sid == lv else "other")
                day[key] += delta
                day["total"] += delta
        self.state["snap"] = {k: v for k, v in cons.items()}
        # retain ~62 days so "this month" always has a full window
        for k in sorted(self.state["daily"])[:-62]:
            del self.state["daily"][k]

    def burn_per_hour(self):
        """Average burn over the last 2 calendar days, in credits/hour."""
        daily = self.state.get("daily", {})
        days = sorted(daily)[-2:]
        if not days:
            return 0.0
        total = sum(daily[d]["total"] for d in days)
        # first day may be partial; assume the span is (n-1)*24 + hours-so-far
        hours = (len(days) - 1) * 24 + max(1, datetime.now().hour)
        return total / max(1.0, hours)

    # ------------- messages -------------

    @staticmethod
    def _fmt(n):
        return f"{n:,.0f}"

    def _eta_line(self, remaining):
        rate = self.burn_per_hour()
        if rate <= 0:
            return ""
        hours = remaining / rate
        if hours > 72:
            return f"⏳ At the current pace this lasts roughly *{hours/24:.0f} more days*."
        if hours > 1.5:
            return f"⏳ At the current pace this runs out in roughly *{hours:.0f} hours*."
        return "⏳ At the current pace this runs out in *under an hour*."

    def _alert(self, subject, body):
        if self.dry_run:
            log.info("DRY RUN alert: %s", subject)
            return
        if not getattr(self.cfg, "alerts_enabled", True):
            log.warning("ALERT (suppressed): %s", subject)
            return
        try:
            notify.send_alert(subject, body)
        except Exception as e:
            log.warning("credit alert dispatch failed: %s", e)
        log.error("ALERT: %s", subject)

    # ------------- retry-queue watch (v2.8, widened v2.12) -------------

    def check_relay_dlq(self, get=None, now=None):
        """Incomplete executions parked in a Make retry queue = work that DIED
        before it landed. Three order events sat unnoticed for weeks in
        September (Salla 502s during capture); each stays lost until someone
        replays it.

        [v2.12] Watches every scenario in Config.make_dlq_watch (six by
        default, not just the two order relays) and counts only items Make
        has given up on: status=unresolved, never one Make still has
        scheduled or in progress on its own backoff (1, 10, 10, 30, 30, 180,
        180 min, about 7.4 h), with dlq_min_age_minutes as a floor on top.
        Scenarios that do not store incomplete executions are not read.
        Replaying is not safe everywhere, so each alert line carries that
        scenario's replay note.

        Alert at most every 12h (dlq_alerted_at) while anything is parked, or
        once a queue has been unreadable for 6h straight. State keys:
        relay_dlq (the total of the queues measured this tick; kept as it
        was, or None, when no queue could be measured, and None when no
        queue is watched at all, never a fake 0), relay_dlq_partial (True
        only while some watched queue was not measured, so relay_dlq counts
        part of the watch), dlq_alerted_at, relay_dlq_by_scenario, and
        dlq_read_fail (consecutive failed ticks per scenario)."""
        rows = scan_dlq(self.cfg, get=get, now=now, quiet=True)
        watch = [r for r in rows if r["stores_incomplete"]]
        found = [r for r in watch if r["count"]]
        unread = [r for r in watch if r["count"] is None]
        long_fail = self._track_read_failures(watch)
        if not watch:
            # [v2.12] nothing is watched, so nothing was measured: not a 0
            self.state["relay_dlq"] = None
        elif len(unread) == len(watch):
            # nothing measured: the last known total stands (None if none)
            self.state.setdefault("relay_dlq", None)
        else:
            self.state["relay_dlq"] = sum(r["count"] for r in found)
        if unread:
            self.state["relay_dlq_partial"] = True
        else:
            self.state.pop("relay_dlq_partial", None)
        self.state["relay_dlq_by_scenario"] = dlq_state(rows)
        if not found and not long_fail:
            # re-arm only when every queue was actually measured: a failed
            # read proves nothing, and must not reset the cooldown into a
            # re-alert
            if not unread:
                self.state.pop("dlq_alerted_at", None)
            return
        last = float(self.state.get("dlq_alerted_at") or 0)
        if time.time() - last < DLQ_COOLDOWN_S:
            return
        self.state["dlq_alerted_at"] = time.time()

        long_ids = {r["id"] for r, _, _ in long_fail}
        errs = [r for r in unread if r["error"] and r["id"] not in long_ids]
        floors = [r for r in unread if not r["error"]]
        fail_lines = [f"• {r['label']}: {ticks} checks in a row failed over "
                      f"{fmt_age(secs / 60)}. {r['link']} Last error: "
                      + " ".join(r["error"].split())
                      for r, secs, ticks in long_fail]
        token_hint = ("Check that the Make API token in .env is still valid "
                      "and can read incomplete executions.")
        if not found:
            hours = int(max(secs for _, secs, _ in long_fail) // 3600)
            n = len(long_fail)
            subject = (f"🟠 A Make retry queue has not been readable for "
                       f"{hours} hours." if n == 1 else
                       f"🟠 {n} Make retry queues have not been readable for "
                       f"{hours} hours.")
            self._alert(
                subject,
                "The watcher cannot see whether these scenarios have failed "
                "runs waiting, so a stuck run would go unnoticed. "
                + token_hint + "\n" + "\n".join(fail_lines))
            return

        total = sum(r["count"] for r in found)
        lines = ["• " + dlq_line(r) for r in found]
        if errs:
            lines.append("Not checked this time: "
                         + ", ".join(r["label"] for r in errs) + ".")
        if floors:
            lines.append("Not measured this time (more records than one scan "
                         "reads): "
                         + ", ".join(r["label"] for r in floors) + ".")
        if fail_lines:
            lines.append(f"Not readable for {DLQ_FAIL_ALERT_S // 3600} hours "
                         f"or more. " + token_hint)
            lines += fail_lines
        subject = (f"🟠 {total} failed Make run is stuck in the retry queue."
                   if total == 1 else
                   f"🟠 {total} failed Make runs are stuck in the retry queue.")
        self._alert(
            subject,
            "Make has not been able to finish these runs on its own. Each "
            "one stays undone until it is replayed in Make: open "
            "the scenario, then Incomplete executions, then Retry. Read the "
            "note on each line before you replay.\n"
            + "\n".join(lines))

    def _streak(self, p, t):
        """(ticks, since) carrying on the failure streak in state entry `p`,
        or None when this failed tick starts a new one: no entry, an entry
        that cannot be read, or more than DLQ_FAIL_GAP_POLLS poll intervals
        since the last failed tick. An entry written before `last` was kept
        is judged by `since`, a lower bound for its last failure."""
        if p is None:
            return None
        try:
            last = float(p.get("last") if p.get("last") is not None
                         else p.get("since"))
            ticks = int(p.get("ticks") or 0) + 1
            since = float(p.get("since") if p.get("since") is not None else last)
        except (TypeError, ValueError, OverflowError):
            return None
        gap = DLQ_FAIL_GAP_POLLS * float(self.poll_s)
        if not (math.isfinite(last) and math.isfinite(since)) or t - last > gap:
            return None
        return ticks, since

    def _track_read_failures(self, rows):
        """[v2.12] Consecutive failed reads per watched scenario, in state
        under dlq_read_fail as {id: {label, ticks, since, last}}. last is the
        time of the latest failed tick: when more than DLQ_FAIL_GAP_POLLS
        poll intervals separate it from this one (the watcher was stopped),
        the streak starts again at 1 instead of counting the gap as failing.
        Logs WARNING once when a queue starts failing (a restarted streak
        included) and once when it reads again, never on every tick. Returns
        (row, seconds failing, ticks) for each queue that has failed
        continuously for DLQ_FAIL_ALERT_S or longer."""
        prev = self.state.get("dlq_read_fail")
        prev = prev if isinstance(prev, dict) else {}
        t = time.time()
        cur, long_fail = {}, []
        for r in rows:
            p = prev.get(r["id"])
            p = p if isinstance(p, dict) else None
            if r["error"]:
                streak = self._streak(p, t)
                if streak is None:
                    ticks, since = 1, t
                    log.warning("make retry queue for %s cannot be read: %s",
                                r["label"], r["error"])
                else:
                    ticks, since = streak
                cur[r["id"]] = {"label": r["label"], "ticks": ticks,
                                "since": since, "last": t}
                if t - since >= DLQ_FAIL_ALERT_S:
                    long_fail.append((r, t - since, ticks))
            elif p is not None:
                log.warning("make retry queue for %s can be read again after "
                            "%s failed check(s)", r["label"], p.get("ticks"))
        self.state["dlq_read_fail"] = cur
        return long_fail

    # ------------- the tick -------------

    def tick(self):
        org = self.read_org()
        try:
            cons = self.read_consumptions()
            self._attribute(cons)
            self.state["breakdown"] = self.scenario_breakdown(cons)
        except Exception as e:
            log.debug("consumption attribution skipped: %s", e)
        try:
            usage = self.read_usage()
            if usage:
                self.state["usage_daily"] = usage
        except Exception as e:
            log.debug("usage history skipped: %s", e)
        try:
            self.check_relay_dlq()
        except Exception as e:
            log.warning("retry-queue watch skipped: %s", _safe_err(e))

        prev = self.state.get("org") or {}
        remaining = org["remaining"]
        label = getattr(self.cfg, "store_label", "the store")

        # -- 1. billing cycle renewed ------------------------------------
        if prev.get("last_reset") and org["last_reset"] != prev["last_reset"]:
            self.state["fired"] = {}          # thresholds re-arm each cycle
            self.state["out"] = False
            self._alert(
                f"🔄 New Make.com billing month — "
                f"{self._fmt(org['plan'])} fresh credits",
                "\n".join([
                    "📋 *A new billing cycle just started.*", "",
                    f"✅  The {label} Make.com account now has its full monthly "
                    f"allowance of *{self._fmt(org['plan'])} credits*.",
                    f"📊  Last cycle ended with "
                    f"*{self._fmt(prev.get('consumed', 0))} credits consumed*"
                    + (f" (of which {self._fmt(prev.get('extra', 0))} were "
                       f"extra purchases)." if prev.get("extra") else "."),
                    "",
                    "No action needed — this is just the monthly reset notice.",
                ]))

        # -- 2. refill detected (exact, via the extra-credits counter) ---
        elif prev and org["extra"] > prev.get("extra", 0) + 0.5:
            bought = org["extra"] - prev.get("extra", 0)
            self._alert(
                f"💳 Credits topped up — +{self._fmt(bought)} added",
                "\n".join([
                    "📋 *A credit purchase just landed.*", "",
                    f"➕  *{self._fmt(bought)} credits* were added "
                    f"({'auto-purchase' if org['auto_purchase_on'] else 'manual top-up'}).",
                    f"📊  Balance went from *{self._fmt(prev.get('remaining', 0))}* "
                    f"to *{self._fmt(remaining)}* credits.",
                    self._eta_line(remaining), "",
                    "Everything that was waiting resumes automatically.",
                ]))
            # re-arm any thresholds the refill climbed back above
            self.state["fired"] = {
                t: ts for t, ts in self.state.get("fired", {}).items()
                if float(t) >= remaining}

        # -- 3. out of credits / back online -----------------------------
        if remaining <= 0 and not self.state.get("out"):
            self.state["out"] = True
            self.state["out_since"] = time.time()
            self._alert(
                "🔴 Make.com is OUT of credits — order syncing is paused",
                "\n".join([
                    "📋 *The credit balance just hit zero.*", "",
                    "⛔  Make.com has paused every automation, so orders have "
                    "stopped flowing into HubSpot.",
                    "🛟  *Nothing is lost* — new orders are caught in a safety "
                    "buffer and will sync themselves once credits return.",
                    "",
                    "🛠️ *What to do*", "",
                    f"1️⃣  Top up credits for the {label} Make.com account.",
                    "2️⃣  That's it — everything resumes and catches up on its own.",
                    "",
                    "⏳ I'll keep checking every few minutes and post here the "
                    "moment we're back.",
                ]))
        elif remaining > 0 and self.state.get("out"):
            mins = (time.time() - float(self.state.get("out_since", time.time()))) / 60
            self.state["out"] = False
            self._alert(
                "🟢 Credits are back — order syncing has resumed",
                "\n".join([
                    "📋 *Good news — the balance is positive again.*", "",
                    f"💰  Current balance: *{self._fmt(remaining)} credits*.",
                    f"⏱️  We were out for about *{int(mins)} minutes*.",
                    "✅  Everything that queued up is processing automatically. "
                    "Nothing was lost, nothing needs re-running.",
                    "",
                    "You can relax — it healed itself, exactly as designed. 🧘",
                ]))

        # -- 4. threshold crossings (descending, once per cycle each) ----
        thresholds = sorted(
            (int(t) for t in getattr(self.cfg, "credit_alert_thresholds", None)
             or (50000, 10000, 1000)), reverse=True)
        sev = {0: "🟡", 1: "🟠", 2: "🔴"}
        for i, t in enumerate(thresholds):
            key = str(t)
            # No previous reading counts as "was above": a watcher that starts
            # while the balance is ALREADY under a threshold must say so
            # immediately, not stay quiet until the next crossing.
            was_above = prev.get("remaining", float("inf")) > t
            if remaining <= t and was_above and key not in self.state.get("fired", {}):
                self.state.setdefault("fired", {})[key] = int(time.time())
                icon = sev.get(i, "🔴")
                urgency = ("Plenty of runway left, but worth planning the top-up."
                           if i == 0 else
                           "Getting tight — top up soon to stay ahead of it."
                           if i == 1 else
                           "*Critical* — top up now to avoid an interruption.")
                self._alert(
                    f"{icon} Make.com credits below {self._fmt(t)} — "
                    f"{self._fmt(remaining)} left",
                    "\n".join([
                        "📋 *Credit balance check-in.*", "",
                        f"💰  Remaining: *{self._fmt(remaining)}* credits.",
                        f"📊  Consumed this cycle: {self._fmt(org['consumed'])}"
                        + (f" (incl. {self._fmt(org['extra'])} purchased extra)."
                           if org["extra"] else "."),
                        self._eta_line(remaining),
                        f"🗓️  Cycle renews: {org['next_reset'][:10] or 'unknown'}.",
                        "",
                        urgency,
                    ]))
                break  # one alert per tick, highest applicable

        self.state["org"] = org
        self.state["ts"] = int(time.time())
        self._save()
        log.info("CREDITS remaining=%s consumed=%s extra=%s out=%s",
                 self._fmt(remaining), self._fmt(org["consumed"]),
                 self._fmt(org["extra"]), self.state.get("out"))
        return org


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--interval", type=float, default=300.0,
                    help="seconds between checks (default 300; min 60)")
    ap.add_argument("--once", action="store_true", help="single check, then exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="log alerts instead of sending them")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [credits] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler("credit_watch.log")])

    # run.py-style .env loading, so the same file feeds every component
    envp = Path(".env")
    if envp.exists():
        for line in envp.read_text().splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    cfg = Config.load(args.config)
    interval = max(60.0, args.interval)
    watch = CreditWatch(cfg, dry_run=args.dry_run, poll_s=interval)

    if args.once:
        org = watch.tick()
        print(json.dumps({"remaining": org["remaining"],
                          "consumed": org["consumed"],
                          "extra": org["extra"],
                          "next_reset": org["next_reset"]}, indent=2))
        return

    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("another credit_watch instance holds the lock; exiting")

    log.info("credit watch started interval=%ss alerting=%s",
             int(interval), notify.channels_summary())
    while not STOP_FILE.exists():
        try:
            watch.tick()
        except Exception as e:
            # the watcher must never die of a transient error -- being awake
            # during the outage IS its job
            log.warning("credit tick failed (will retry): %s", e)
        time.sleep(interval)
    log.info("STOP.credits present: exiting cleanly")


if __name__ == "__main__":
    main()
