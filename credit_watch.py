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
  * a second GET /scenarios/consumptions attributes burn to backfill vs live

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
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta
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


class CreditWatch:
    def __init__(self, cfg, dry_run=False):
        self.cfg = cfg
        self.dry_run = dry_run
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

    # ------------- the tick -------------

    def tick(self):
        org = self.read_org()
        try:
            self._attribute(self.read_consumptions())
        except Exception as e:
            log.debug("consumption attribution skipped: %s", e)

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
    watch = CreditWatch(cfg, dry_run=args.dry_run)

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

    interval = max(60.0, args.interval)
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
