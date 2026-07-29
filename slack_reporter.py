#!/usr/bin/env python3
"""Slack ops reporter (v2.4) -- a quiet, threaded heartbeat instead of spam.

Shape (exactly as specified by ops):
  * Once per day, per channel: post ONE anchor message ("ops thread for
    <date>"). Every subsequent report that day lands as a THREADED REPLY under
    it, so the channel shows a single line per day no matter how often the
    fleet reports. Next day -> new anchor, new thread.
  * Every report_interval_minutes (default 30): a compact status reply --
    services up/down + uptime, orders synced today, queue depth, Make credits
    left with a burn-rate ETA, relay health. All read from LOCAL files and
    systemd; a report costs one Slack API call and nothing else.
  * Commands: post `!status`, `!credits`, `!queue`, `!uptime` or `!help` in
    the channel and the reporter answers in that message's thread within ~a
    minute. Commands need two extra read scopes (channels:history,
    groups:history); without them the reporter logs one warning and carries
    on reporting -- never a silent death.

Free-tier safety: Slack does not charge per message; the constraint is rate
limits (chat.postMessage ~1/s/channel, conversations.history Tier 3). One
report per 30 min plus a ~60s command poll sits orders of magnitude below
both. 429s are retried with Retry-After via the shared with_retries().

State (mirror/slack_report.json): {"date": "YYYY-MM-DD", "anchors": {channel:
ts}, "last_report": epoch, "seen": {channel: last_handled_msg_ts}}.
Runs as its own systemd service; STOP.reporter stops it gracefully.
"""

import argparse
import fcntl
import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from backfill import Config, http_request, with_retries  # noqa: E402
import notify  # noqa: E402

log = logging.getLogger("backfill")

STATE_FILE = Path("mirror/slack_report.json")
LOCK_FILE = Path("slack_reporter.lock")
STOP_FILE = Path("STOP.reporter")

SERVICES = ("salla-live-sync", "salla-credit-watch", "salla-slack-reporter")


# ---------------------------------------------------------------- fleet facts

def _tail_lines(path, n=400):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 65536))
            return f.read().decode("utf-8", "replace").splitlines()[-n:]
    except OSError:
        return []


def _svc(unit):
    """(state, uptime_str, n_restarts) from systemd; degrades off-VM."""
    try:
        out = subprocess.run(
            ["systemctl", "show", unit, "--property",
             "ActiveState,ActiveEnterTimestamp,NRestarts"],
            capture_output=True, text=True, timeout=5).stdout
        props = dict(l.split("=", 1) for l in out.splitlines() if "=" in l)
        state = props.get("ActiveState", "unknown")
        up = ""
        ts = props.get("ActiveEnterTimestamp", "")
        if ts:
            try:
                t0 = datetime.strptime(" ".join(ts.split()[1:3]),
                                       "%Y-%m-%d %H:%M:%S")
                mins = max(0, int((datetime.now() - t0).total_seconds() / 60))
                up = f"{mins//1440}d {mins%1440//60}h {mins%60}m" if mins >= 1440 \
                    else (f"{mins//60}h {mins%60}m" if mins >= 60 else f"{mins}m")
            except ValueError:
                pass
        return state, up, int(props.get("NRestarts", "0") or 0)
    except Exception:
        return "unknown", "", 0


def fleet_snapshot(cfg):
    """Everything a status line needs, from local files only."""
    snap = {"ts": datetime.now().strftime("%H:%M"), "services": {}}
    for u in SERVICES[:2]:
        snap["services"][u] = _svc(u)

    lines = _tail_lines("live.log")
    for ln in reversed(lines):
        m = re.search(r"QUEUE depth=(\d+).*processed_today=(\d+)", ln)
        if m:
            snap["queue"], snap["today"] = int(m.group(1)), int(m.group(2))
            snap["queue_ts"] = ln[:19]
            break
    created = [l for l in lines if "CREATED order" in l]
    if created:
        snap["last_create"] = created[-1][:19]
    snap["errors_recent"] = sum(1 for l in lines if " ERROR " in l)

    try:
        cs = json.loads(Path("mirror/credit_state.json").read_text())
        org = cs.get("org") or {}
        snap["credits"] = org.get("remaining")
        snap["credits_out"] = bool(cs.get("out"))
        daily = cs.get("daily") or {}
        today = datetime.now().strftime("%Y-%m-%d")
        burn = (daily.get(today) or {}).get("total") or 0
        hours = max(1, datetime.now().hour)
        snap["burn_per_h"] = burn / hours if burn else None
    except Exception:
        snap["credits"] = None

    try:
        rh = json.loads(Path("mirror/relay_health.json").read_text())
        snap["relay"] = rh.get("state", "ok")
    except Exception:
        snap["relay"] = "ok"
    return snap


def render_report(snap):
    """One compact, human line-set for the thread."""
    parts = []
    live_state, live_up, live_rst = snap["services"].get("salla-live-sync",
                                                         ("unknown", "", 0))
    ok = live_state == "active"
    icon = "🟢" if ok and snap.get("relay") == "ok" else "🟠"
    if snap.get("credits_out") or snap.get("relay") == "platform":
        icon = "🔴"
    parts.append(f"{icon} *{snap['ts']}* — sync "
                 + (f"up {live_up}" if ok else f"*{live_state}*")
                 + (f" · {live_rst} restarts" if live_rst else ""))
    if "today" in snap:
        parts.append(f"📦 {snap['today']:,} orders synced today · "
                     f"queue {snap.get('queue', '?')}")
    if snap.get("last_create"):
        parts.append(f"🕐 last order {snap['last_create'][11:]}")
    if snap.get("credits") is not None:
        c = snap["credits"]
        cicon = "💰" if c > 50000 else ("🟡" if c > 10000 else "🔴")
        line = f"{cicon} {c:,.0f} Make credits left"
        if snap.get("burn_per_h"):
            hrs = c / snap["burn_per_h"]
            line += (f" · ~{hrs/24:.0f}d at today's pace" if hrs > 72
                     else f" · ~{hrs:.0f}h at today's pace")
        parts.append(line)
    if snap.get("relay") not in ("ok", None):
        parts.append(f"⚠️ relay health: *{snap['relay']}* — see alert above")
    if snap.get("errors_recent"):
        parts.append(f"🧯 {snap['errors_recent']} error line(s) in the recent log")
    return "\n".join(parts)


# ---------------------------------------------------------------- slack layer

def _api(method, payload=None, params=None):
    """Slack Web API call with retry; returns parsed JSON (ok checked by caller)."""
    tok = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not tok:
        return {"ok": False, "error": "no_token"}
    if payload is not None:
        def go():
            return http_request(
                "POST", f"https://slack.com/api/{method}",
                headers={"Authorization": f"Bearer {tok}",
                         "Content-Type": "application/json; charset=utf-8"},
                body=json.dumps(payload), timeout=20)
    else:
        qs = urllib.parse.urlencode(params or {})
        def go():
            return http_request(
                "GET", f"https://slack.com/api/{method}?{qs}",
                headers={"Authorization": f"Bearer {tok}"}, timeout=20)
    status, _, text = with_retries(go, f"slack {method}", retries=3)
    try:
        return json.loads(text) if status == 200 else {"ok": False,
                                                       "error": f"http_{status}"}
    except json.JSONDecodeError:
        return {"ok": False, "error": "non_json"}


class Reporter:
    def __init__(self, cfg, dry_run=False):
        self.cfg = cfg
        self.dry = dry_run
        self.state = self._load()
        self.commands_ok = None   # tri-state: unknown / True / False

    def _load(self):
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {"date": "", "anchors": {}, "last_report": 0, "seen": {}}

    def _save(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        tmp.replace(STATE_FILE)

    # ---------------- anchors: one parent message per channel per day

    def _anchor(self, channel):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.state.get("date") != today:
            self.state["date"] = today
            self.state["anchors"] = {}
        ts = (self.state.get("anchors") or {}).get(channel)
        if ts:
            return ts
        pretty = datetime.now().strftime("%A, %B %-d")
        r = _api("chat.postMessage", {
            "channel": channel,
            "text": (f"📊 *Engine ops — {pretty}*\n"
                     f"Status updates land in this thread every "
                     f"{int(self._interval_min())} min. "
                     f"`!status` here any time for an instant one.")})
        if not r.get("ok"):
            log.warning("anchor post failed for %s: %s", channel, r.get("error"))
            return None
        self.state.setdefault("anchors", {})[channel] = r["ts"]
        self._save()
        return r["ts"]

    def _interval_min(self):
        return float(getattr(self.cfg, "report_interval_minutes", 30) or 30)

    # ---------------- the 30-min heartbeat

    def report_tick(self, force=False):
        due = time.time() - float(self.state.get("last_report", 0)) \
            >= self._interval_min() * 60 - 15
        if not (due or force):
            return False
        snap = fleet_snapshot(self.cfg)
        text = render_report(snap)
        if self.dry:
            log.info("DRY RUN report:\n%s", text)
            self.state["last_report"] = time.time()
            self._save()
            return True
        for ch in notify.slack_channels():
            ts = self._anchor(ch)
            if not ts:
                continue
            r = _api("chat.postMessage",
                     {"channel": ch, "text": text, "thread_ts": ts})
            if not r.get("ok"):
                log.warning("report to %s failed: %s", ch, r.get("error"))
        self.state["last_report"] = time.time()
        self._save()
        log.info("report posted")
        return True

    # ---------------- commands: !status etc, answered in-thread

    HELP = ("`!status` full status · `!credits` balance & burn · "
            "`!queue` queue depth · `!uptime` service uptime/restarts · "
            "`!help` this")

    def _answer(self, cmd):
        snap = fleet_snapshot(self.cfg)
        if cmd == "!credits":
            return "\n".join(l for l in render_report(snap).splitlines()
                             if "credit" in l.lower()) or "no credit data yet"
        if cmd == "!queue":
            return (f"queue depth *{snap.get('queue', '?')}* · "
                    f"{snap.get('today', 0):,} synced today "
                    f"(as of {snap.get('queue_ts', '?')[11:]})")
        if cmd == "!uptime":
            out = []
            for u in SERVICES[:2]:
                st, up, rst = _svc(u)
                out.append(f"`{u}` {st}" + (f" · up {up}" if up else "")
                           + f" · {rst} restart(s) since enable")
            return "\n".join(out)
        if cmd == "!help":
            return self.HELP
        return render_report(snap)   # !status and anything else

    def poll_commands(self):
        if self.commands_ok is False:
            return
        oldest = time.time() - 180
        for ch in notify.slack_channels():
            r = _api("conversations.history", params={
                "channel": ch, "oldest": f"{oldest:.6f}", "limit": 20})
            if not r.get("ok"):
                if r.get("error") in ("missing_scope", "not_in_channel"):
                    if self.commands_ok is not False:
                        log.warning(
                            "commands disabled: %s (add channels:history + "
                            "groups:history scopes and reinstall the app to "
                            "enable; reports are unaffected)", r.get("error"))
                    self.commands_ok = False
                    return
                continue
            self.commands_ok = True
            seen = self.state.setdefault("seen", {})
            last = float(seen.get(ch, 0) or 0)
            for msg in sorted(r.get("messages", []),
                              key=lambda m: float(m.get("ts", 0))):
                ts = float(msg.get("ts", 0))
                if ts <= last or msg.get("bot_id"):
                    continue
                txt = (msg.get("text") or "").strip().lower()
                if txt.startswith("!") and (cmd := txt.split()[0]) in (
                        "!status", "!credits", "!queue", "!uptime", "!help"):
                    ans = self._answer(cmd)
                    if not self.dry:
                        _api("chat.postMessage",
                             {"channel": ch, "text": ans,
                              "thread_ts": msg.get("ts")})
                    log.info("answered %s in %s", cmd, ch)
                seen[ch] = max(float(seen.get(ch, 0) or 0), ts)
            self._save()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--once", action="store_true",
                    help="post one report now and exit")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--poll-s", type=float, default=60.0,
                    help="command-poll cadence (default 60s)")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [reporter] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler("slack_reporter.log")])

    envp = Path(".env")
    if envp.exists():
        for line in envp.read_text().splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(),
                                      v.strip().strip('"').strip("'"))

    cfg = Config.load(args.config)
    rep = Reporter(cfg, dry_run=args.dry_run)

    if args.once:
        rep.report_tick(force=True)
        return

    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("another slack_reporter holds the lock; exiting")

    log.info("reporter started: report every %.0f min, command poll %.0fs, %s",
             float(getattr(cfg, "report_interval_minutes", 30) or 30),
             args.poll_s, notify.channels_summary())
    while not STOP_FILE.exists():
        try:
            rep.report_tick()
            rep.poll_commands()
        except Exception as e:
            log.warning("reporter tick failed (will retry): %s", e)
        time.sleep(max(20.0, args.poll_s))
    log.info("STOP.reporter present: exiting cleanly")


if __name__ == "__main__":
    main()
