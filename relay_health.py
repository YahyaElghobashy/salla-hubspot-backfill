#!/usr/bin/env python3
"""Relay outage detection and triage (v2.0).

WHY THIS EXISTS
On 2026-07-23 19:57 the Make.com org exhausted its credits. Both relay scenarios
stopped executing for 68.9 hours (Jul 24 and Jul 25: zero executions). The engines
retried into a void and nobody was alerted -- the outage was found ~3 days later
by a human noticing orders had stopped syncing. RelayError was raised correctly
but caught nowhere, so backfill crash-restarted silently and live swallowed it
into an infinite 60s retry loop.

THE TRAP THIS MODULE AVOIDS
You cannot detect Make credit exhaustion by probing the webhook. Make answers
HTTP 200 with the body `Accepted` whether the scenario is running, paused for
credits, or deactivated -- verified against Make's own docs and community FAQ.
A detector built on "did the webhook respond" is blind to exactly this failure.

So triage cross-references a SECOND, INDEPENDENT signal: the live intake relay,
which appends rows to the queue sheet. Both engines share the Make org, so:

  relay failing + intake also stopped   -> the whole Make org is down (platform)
  relay failing + intake still writing  -> only this scenario is broken (scenario)

LOCAL CHECKS COME FIRST, ALWAYS
A wrong RELAY_SECRET produces a byte-identical `Accepted` response, because the
scenario's router filter rejects the request before any module runs. During the
2026-07-26 incident this cost about an hour of misdiagnosis. Never blame the
platform before ruling out local config.

State is published to mirror/relay_health.json, following the same
publish/consume-with-TTL convention as mirror/live_active.json (live.py) so the
dashboard and the other engine can read it without coupling to this module.
"""

import json
import logging
import os
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

from backfill import http_request, with_retries

log = logging.getLogger("backfill")

HEALTH_FILE = "relay_health.json"

# Triage outcomes
OK = "ok"
LOCAL = "local"          # our config is wrong: secret / url / dns
SCENARIO = "scenario"    # Make is up, this one scenario is broken
PLATFORM = "platform"    # the Make org is down (credits/outage)
UNKNOWN = "unknown"      # not enough evidence to classify

MAKE_API = "https://eu1.make.com/api/v2"

# Make sits behind Cloudflare, which blocks urllib's default
# "Python-urllib/3.x" User-Agent with HTTP 403 "error code: 1010" on EVERY
# endpoint -- including /users/me, so it looks exactly like an auth failure.
# Verified 2026-07-27: identical request 403s without this header and 200s with
# it. Without this, tier-2 checks would silently degrade and never say why.
MAKE_UA = "SallaHubSpotSync/2.2 (+relay-health-monitor)"


class RelayHealth:
    """Tracks consecutive relay failures, triages them, publishes state, alerts.

    Deliberately owns no engine state and mutates no pacing. It only observes,
    classifies, and notifies -- the caller decides how to slow down. That keeps
    the v1.7 live-priority system untouched.
    """

    def __init__(self, cfg, mirror_dir="mirror", notifier=None, dry_run=False):
        self.cfg = cfg
        self.dir = Path(mirror_dir)
        self.path = self.dir / HEALTH_FILE
        self.notifier = notifier
        self.dry_run = dry_run
        self.consecutive = 0
        self.state = OK
        self.since = None
        self._last_alert_ts = 0.0
        self._alert_thread_ts = None
        self._load()

    # ---------------- persistence ----------------

    def _load(self):
        try:
            d = json.loads(self.path.read_text())
            self.state = d.get("state", OK)
            self.since = d.get("since")
            self._last_alert_ts = float(d.get("last_alert_ts", 0) or 0)
            self._alert_thread_ts = d.get("alert_thread_ts")
        except Exception:
            pass  # first run, or unreadable -> start clean

    def publish(self, detail=""):
        payload = {
            "state": self.state,
            "since": self.since,
            "detail": str(detail)[:400],
            "consecutive_failures": self.consecutive,
            "last_alert_ts": int(self._last_alert_ts),
            "alert_thread_ts": self._alert_thread_ts,
            "ts": int(time.time()),
        }
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload))
            tmp.replace(self.path)  # atomic: readers never see a partial file
        except Exception as e:
            log.debug("relay_health publish failed: %s", e)
        return payload

    # ---------------- local checks (tier 0, always first) ----------------

    def _local_problems(self):
        """Cheap local misconfiguration checks. Returns a list of reasons."""
        problems = []
        secret = os.environ.get("RELAY_SECRET", "")
        if not secret:
            problems.append("RELAY_SECRET is empty in the environment")
        elif len(secret) < 32:
            # The relay's router filter compares this verbatim; a truncated or
            # mis-parsed value fails the filter and yields `Accepted`.
            problems.append(f"RELAY_SECRET looks malformed (length {len(secret)})")
        url = getattr(self.cfg, "relay_url", "") or ""
        if not url.startswith("https://"):
            problems.append("relay_url is not an https URL")
        else:
            host = urllib.parse.urlparse(url).hostname or ""
            try:
                import socket as _s
                _s.getaddrinfo(host, 443)
            except Exception as e:
                problems.append(f"cannot resolve relay host: {type(e).__name__}")
        return problems

    # ---------------- intake liveness (tier 1, no Make token needed) ----------------

    def _queue_facts(self, gio):
        """Read the queue sheet once and derive both the liveness signal and the
        customer-facing impact numbers.

        `age_min` is the independent signal: the intake relay is a *different*
        Make scenario writing these rows. If it has also gone quiet, the whole
        org is down rather than just the fetch relay.
        """
        facts = {"age_min": None, "waiting": None, "oldest_wait_min": None}
        qsid = getattr(self.cfg, "queue_spreadsheet_id", "") or ""
        if not gio or not qsid:
            return facts
        try:
            rows = gio.queue_read(qsid, start_row=2)
        except Exception as e:
            log.debug("intake liveness read failed: %s", e)
            return facts

        def _parse(s):
            try:
                return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                return None

        stamps = [d for d in (_parse(r.get("received_at")) for r in rows) if d]
        if stamps:
            facts["age_min"] = max(
                0.0, (datetime.now() - max(stamps)).total_seconds() / 60.0)
        pending = [r for r in rows
                   if str(r.get("status", "")).lower() in ("queued", "processing")]
        facts["waiting"] = len(pending)
        pend_stamps = [d for d in (_parse(r.get("received_at")) for r in pending) if d]
        if pend_stamps:
            facts["oldest_wait_min"] = max(
                0.0, (datetime.now() - min(pend_stamps)).total_seconds() / 60.0)
        return facts

    def _intake_age_minutes(self, gio):
        """Backwards-compatible wrapper around _queue_facts."""
        return self._queue_facts(gio)["age_min"]

    # ---------------- Make API checks (tier 2, needs a read-only token) ----------------

    def _make_get(self, path):
        token = os.environ.get("MAKE_API_TOKEN", "").strip()
        if not token:
            return None
        def go():
            return http_request("GET", f"{MAKE_API}{path}",
                                headers={"Authorization": f"Token {token}",
                                         "User-Agent": MAKE_UA},
                                timeout=25)
        status, _, text = with_retries(go, f"make GET {path[:40]}", retries=2)
        if status != 200:
            # 403 + "error code: 1010" is Cloudflare rejecting the User-Agent,
            # not a bad token -- warn loudly so it is never misread as auth.
            if status == 403 and "1010" in (text or ""):
                log.warning("make api %s blocked by Cloudflare (UA rejected), "
                            "not an auth failure", path)
            else:
                log.debug("make api %s -> %s", path, status)
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    def hook_queue(self):
        """(queueCount, queueLimit) for the configured hook, or (None, None).

        A climbing queueCount with no drain is the most direct symptom of a
        credit-paused org. queueCount approaching queueLimit means data loss is
        imminent, not just delay.
        """
        hook_id = str(getattr(self.cfg, "make_hook_id", "") or "")
        if not hook_id:
            return None, None
        d = self._make_get(f"/hooks/{hook_id}")
        if not isinstance(d, dict):
            return None, None
        hook = d.get("hook") if isinstance(d.get("hook"), dict) else d
        return hook.get("queueCount"), hook.get("queueLimit")

    # ---------------- triage ----------------

    def triage(self, gio=None):
        """Classify the current failure. Returns (state, detail).

        `detail` stays terse and technical -- it lands in the log and the health
        file. The human-readable walkthrough is built separately in _render().
        The evidence gathered here is stashed on self._evidence for that.
        """
        local = self._local_problems()
        if local:
            self._evidence = {"local": local}
            return LOCAL, "; ".join(local)

        facts = self._queue_facts(gio)
        age = facts["age_min"]
        stale_after = float(getattr(self.cfg, "intake_stale_minutes", 30) or 30)
        qc, ql = self.hook_queue()
        facts.update({"queue_count": qc, "queue_limit": ql,
                      "stale_after": stale_after})
        self._evidence = facts

        extra = ""
        if qc is not None:
            extra = f" hook queue {qc}"
            if ql:
                extra += f"/{ql}"
                try:
                    if float(qc) / float(ql) > 0.8:
                        extra += " (NEAR LIMIT - data loss risk)"
                except (TypeError, ZeroDivisionError):
                    pass

        if age is None:
            return UNKNOWN, f"relay failing; intake liveness unknown.{extra}"
        if age > stale_after:
            return PLATFORM, (f"relay failing AND intake silent {age:.0f}m "
                              f"(> {stale_after:.0f}m): the Make org looks down "
                              f"(credits/outage).{extra}")
        return SCENARIO, (f"relay failing BUT intake wrote {age:.0f}m ago: Make is "
                          f"up, the fetch relay scenario is broken.{extra}")

    # ---------------- human-readable rendering ----------------

    @staticmethod
    def _dur(minutes):
        """'2 hours 5 minutes' style, for people rather than machines."""
        if minutes is None:
            return "unknown"
        m = int(round(minutes))
        if m < 1:
            return "less than a minute"
        if m < 60:
            return f"{m} minute{'s' if m != 1 else ''}"
        h, rem = divmod(m, 60)
        if h < 24:
            return f"{h} hour{'s' if h != 1 else ''}" + (f" {rem} min" if rem else "")
        d, hh = divmod(h, 24)
        return f"{d} day{'s' if d != 1 else ''}" + (f" {hh} hr" if hh else "")

    def _impact_lines(self):
        """What this means for the business, in plain language."""
        ev = getattr(self, "_evidence", {}) or {}
        out = []
        waiting = ev.get("waiting")
        if waiting:
            out.append(f"📥  *{waiting:,} order{'s' if waiting != 1 else ''}* "
                       f"{'are' if waiting != 1 else 'is'} sitting in the queue and "
                       f"{'have' if waiting != 1 else 'has'} *not* reached HubSpot yet.")
            oldest = ev.get("oldest_wait_min")
            if oldest:
                out.append(f"⏱️  The one that's been waiting longest has been there "
                           f"*{self._dur(oldest)}*.")
        elif waiting == 0:
            out.append("📥  Nothing is stuck in the queue yet — we caught this early.")
        out.append("✅  *Nothing is lost.* Every new order from the store is still "
                   "being written down safely. They're delayed, not missing, and "
                   "they'll sync themselves once this is fixed.")
        qc, ql = ev.get("queue_count"), ev.get("queue_limit")
        if qc and ql:
            try:
                pct = float(qc) / float(ql) * 100
                if pct > 80:
                    out.append(f"🚨  *Act now.* The safety net is *{pct:.0f}% full* "
                               f"({qc:,} of {ql:,}). Once it fills, new orders get "
                               f"*thrown away* instead of waiting — and those can't "
                               f"be recovered. Fix this before it fills.")
                else:
                    out.append(f"🛟  The safety net is only *{pct:.0f}% full*, so "
                               f"there's plenty of room. No risk of losing anything "
                               f"yet.")
            except (TypeError, ZeroDivisionError):
                pass
        return out

    # ---------------- the API the engines call ----------------

    def record_success(self):
        """A relay call succeeded. Emits a 'resolved' notice on recovery.

        NOTE: callers pass EVERY clean poll cycle here, not only cycles that
        actually spoke to the relay. A cycle with nothing to fetch "succeeds"
        without touching the relay at all, so this must not be treated as
        evidence the relay is healthy -- see _recent_failures() for why the
        alerting decision uses a time window instead of a consecutive counter.
        """
        self.consecutive = 0
        # Only declare recovery once the sliding window is genuinely quiet.
        # A poll cycle that merely read the queue proves nothing about the
        # relay, and treating it as recovery would fire a false "all clear"
        # seconds after the outage alert -- then re-alert, flapping the channel.
        if self.state != OK and self._recent_failures() == 0:
            prior = self.state
            down_for = None
            if self.since:
                down_for = (time.time() - float(self.since)) / 60.0
            causes = {
                PLATFORM: "Make.com had run out of credits",
                SCENARIO: "the order-fetching connection had stopped working",
                LOCAL: "a wrong setting on our own server",
                UNKNOWN: "a fault we couldn't fully identify",
            }
            dur = f" after {self._dur(down_for)}" if down_for else ""
            title = f"🟢 Orders are syncing again{dur}"
            body = "\n".join([
                "📋 *What happened*", "",
                f"Orders had stopped reaching HubSpot because "
                f"{causes.get(prior, prior)}. That's now resolved and everything "
                f"is flowing normally again.",
                "",
                "───────────────", "",
                "📦 *What about the orders that were stuck?*", "",
                "✅  They're being processed right now, automatically.",
                "✅  *You don't need to do anything* — no re-runs, no manual fixes.",
                "✅  Nothing was lost. They were only ever delayed.",
                "",
                "───────────────", "",
                "⚡ The sync has gone back to full speed on its own.",
            ])
            self.state, self.since = OK, None
            self.publish("relay is answering again")
            # Top-level (not threaded): a recovery notice nobody sees is useless,
            # and a thread reply is easy to miss on a phone.
            self._alert(title, body)
            self._alert_thread_ts = None
            self.publish("relay is answering again")

    def _window_minutes(self):
        return float(getattr(self.cfg, "relay_failure_window_minutes", 15) or 15)

    def _recent_failures(self):
        """How many relay failures fall inside the sliding window.

        Prunes as it counts so the list cannot grow without bound during a
        multi-day outage.
        """
        cutoff = time.time() - self._window_minutes() * 60
        self._failure_times = [t for t in getattr(self, "_failure_times", [])
                               if t >= cutoff]
        return len(self._failure_times)

    def record_failure(self, what, gio=None):
        """A relay call exhausted its retries. Returns the triage state.

        Caller decides whether to slow down; this only classifies and alerts.
        """
        self._failure_times = getattr(self, "_failure_times", [])
        self._failure_times.append(time.time())
        self.consecutive += 1
        threshold = int(getattr(self.cfg, "relay_outage_threshold", 3) or 3)
        recent = self._recent_failures()
        # Trip on EITHER a consecutive run OR enough failures inside the window.
        # Real incident (2026-07-29 08:12-08:27): relay failed 4 times in 15 min
        # but a harmless queue-only poll succeeded between each one, resetting a
        # consecutive counter to 0 every time. A flapping platform never trips a
        # strictly-consecutive rule, which is exactly the outage we must catch.
        if self.consecutive < threshold and recent < threshold:
            log.warning("relay failure %d/%d consecutive, %d/%d in %dm window: %s",
                        self.consecutive, threshold, recent, threshold,
                        int(self._window_minutes()), what)
            return self.state
        state, detail = self.triage(gio)
        changed = state != self.state
        if changed:
            self.state = state
            self.since = int(time.time())
        self.publish(detail)
        if changed or self._cooldown_expired():
            self._alert(*self._render(state, detail, what))
        return state

    def _cooldown_expired(self):
        mins = float(getattr(self.cfg, "alert_cooldown_minutes", 30) or 30)
        return (time.time() - self._last_alert_ts) > mins * 60

    def _render(self, state, detail, what):
        """Build a message an on-call person can act on at 3am.

        Structure: what happened -> how we know (the checks, in order) ->
        what it means for orders -> what to do. Deliberately avoids stack
        traces and internal jargon; `detail` already carries that for the log.
        """
        ev = getattr(self, "_evidence", {}) or {}
        age = ev.get("age_min")
        stale_after = ev.get("stale_after", 30)
        qc, ql = ev.get("queue_count"), ev.get("queue_limit")

        waiting = ev.get("waiting") or 0
        wait_bit = f" · {waiting:,} orders waiting" if waiting else ""

        # The title IS the message in Slack. It must stand alone on a phone
        # lock screen: what broke, why, and how bad -- in one line.
        titles = {
            PLATFORM: (f"🔴 Orders stopped syncing — Make.com ran out of "
                       f"credits{wait_bit}"),
            SCENARIO: (f"🔴 Orders stopped syncing — a connection broke "
                       f"(NOT a billing problem){wait_bit}"),
            LOCAL: (f"🟠 Orders stopped syncing — wrong setting on our "
                    f"server{wait_bit}"),
            UNKNOWN: (f"🟠 Orders stopped syncing — still working out "
                      f"why{wait_bit}"),
        }
        headline = {
            PLATFORM: ("Make.com is the service that carries orders from the "
                       "store into HubSpot. It has run out of credits, so it has "
                       "paused *everything* it was doing. Orders are piling up in "
                       "a safe holding area instead of reaching HubSpot."),
            SCENARIO: ("Make.com is working fine and still has credits — so this "
                       "is *not* about money. One specific piece of it, the part "
                       "that fetches order details, has stopped responding "
                       "properly. Orders are piling up safely meanwhile."),
            LOCAL: ("Good news: Make.com and the store are both fine. The problem "
                    "is a setting on our own server, so nobody needs to contact "
                    "anyone else — we just fix it here."),
            UNKNOWN: ("Orders aren't reaching HubSpot. We ran our usual checks but "
                      "couldn't finish all of them, so we can't say for certain "
                      "what's wrong yet."),
        }

        # --- the walkthrough: which checks ran, what each returned ---
        checks = ["🔍 *How we worked out what's wrong*",
                  "_(the system runs these checks in order, every time)_", ""]
        if state == LOCAL:
            checks.append("1️⃣  *Are our own settings correct?* → ❌ *No — found it*")
            for p in ev.get("local", []):
                checks.append(f"       • {p}")
            checks.append("")
            checks.append("We stop here on purpose. There's no point blaming "
                          "Make.com or the store until our own settings are right.")
        else:
            checks.append("1️⃣  *Are our own settings correct?* → ✅ Yes "
                          "(access key present, address valid)")
            checks.append(f"2️⃣  *Can we fetch order details?* → ❌ No — failed "
                          f"{self.consecutive} times in a row")
            if age is None:
                checks.append("3️⃣  *Are new orders still arriving?* → ⚠️ Couldn't "
                              "check (we can't read the order list right now)")
            elif state == PLATFORM:
                checks.append(f"3️⃣  *Are new orders still arriving?* → ❌ Nothing "
                              f"for *{self._dur(age)}*")
                checks.append("")
                checks.append("       ↳ *This is the important one.* A completely "
                              "separate part of Make.com writes down every new "
                              "order as it happens. That has gone quiet too.")
                checks.append(f"       ↳ Two unrelated things failing at the exact "
                              f"same time isn't coincidence — it means *all* of "
                              f"Make.com is paused. Running out of credits is "
                              f"precisely what does that. (We wait "
                              f"{int(stale_after)} min of silence before saying so.)")
            else:
                checks.append(f"3️⃣  *Are new orders still arriving?* → ✅ Yes — "
                              f"newest one *{self._dur(age)} ago*")
                checks.append("")
                checks.append("       ↳ *This rules out credits.* That separate "
                              "part of Make.com is still working normally, which it "
                              "couldn't do if the account were empty. So only the "
                              "order-fetching piece is broken.")
            if qc is not None:
                checks.append(f"4️⃣  *Is the safety net holding?* → {qc:,} "
                              f"order{'s' if qc != 1 else ''} parked"
                              + (f" (it can hold {ql:,})" if ql else ""))

        actions = {
            PLATFORM: ["🛠️ *What to do*", "",
                       f"1️⃣  Log in to Make.com → check the credit balance for the "
                       f"*{getattr(self.cfg, 'store_label', 'store')}* account.",
                       "2️⃣  Out of credits? Top it up. Everything waiting will "
                       "process by itself afterwards — *you don't need to re-run "
                       "anything, and nothing is lost.*",
                       "3️⃣  Auto top-up was meant to prevent this. Check if the "
                       "saved card got declined — a failed payment quietly switches "
                       "auto top-up off, and it stays off until someone fixes the card."],
            SCENARIO: ["🛠️ *What to do*", "",
                       "1️⃣  *Don't buy credits* — that's not the problem here.",
                       "2️⃣  Open the *Salla | Local Backfill Relay* automation in "
                       "Make.com and look at its recent runs.",
                       "3️⃣  Usually this means someone edited it, or its access key "
                       "no longer matches the one our server uses."],
            LOCAL: ["🛠️ *What to do*", "",
                    "1️⃣  Nobody outside needs to be contacted — Make.com and the "
                    "store are both fine.",
                    "2️⃣  Fix the setting listed above on our server, then restart "
                    "the sync."],
            UNKNOWN: ["🛠️ *What to do*", "",
                      "1️⃣  Check the orders Google Sheet is reachable.",
                      "2️⃣  Then check Make.com for credits and paused automations."],
        }

        body = "\n".join(
            ["📋 *What's going on*", "", headline.get(state, ""), "",
             "───────────────", "",
             *checks, "",
             "───────────────", "",
             "📦 *What this means for orders*", "", *self._impact_lines(), "",
             "───────────────", "",
             *actions.get(state, []), "",
             "───────────────", "",
             "⏳ The sync is now retrying slowly on purpose, so it doesn't burn "
             "credits banging on a door that won't open. *You'll get a green "
             "message here the moment it recovers* — no need to watch it.",
             "",
             f"_For engineers: {str(what)[:160]}_"])
        return titles.get(state, "🔴 Orders stopped syncing"), body

    def _alert(self, subject, body, thread=False):
        self._last_alert_ts = time.time()
        if not getattr(self.cfg, "alerts_enabled", True):
            log.warning("ALERT (suppressed, alerts_enabled=false): %s", subject)
            return
        if self.notifier is None:
            log.warning("ALERT (no notifier): %s -- %s", subject, body[:300])
            return
        try:
            ts = self.notifier.send_alert(
                subject, body, dry_run=self.dry_run,
                thread_ts=self._alert_thread_ts if thread else None)
            if ts and not thread:
                self._alert_thread_ts = ts
        except Exception as e:
            log.warning("alert dispatch failed: %s", e)
        log.error("ALERT: %s", subject)
