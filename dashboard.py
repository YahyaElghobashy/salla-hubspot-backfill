#!/usr/bin/env python3
"""Backfill live dashboard. Read-only companion to backfill.py:
tails backfill.log (the engine's DEBUG file handler) plus cursor.json and
renders a live terminal UI. Zero interaction with the engine or any API.

v1.5: per-lane view of the concurrent engine (worker lanes, adaptive pacing
bars, throughput sparkline). Parses both the v1.5 log format
("ts LEVEL [lane] msg") and older logs without the lane token.

    ./venv/bin/python3 dashboard.py [--target 17043] [--baseline 0]
"""
import argparse
import json
import re
import time
from collections import Counter, deque
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

LOG = Path("backfill.log")
CURSOR = Path("cursor.json")
SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
BLOCKS = " ▁▂▃▄▅▆▇█"

LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ (\w+)\s+"
                  r"(?:\[([\w-]+)\] )?(.*)$")

PHASES = [
    (re.compile(r"HS POST /crm/v3/objects/orders/search"), "dedup search"),
    (re.compile(r"HS POST /crm/v3/objects/contacts/search"), "matching contact"),
    (re.compile(r"HS POST /crm/v3/objects/contacts "), "creating contact"),
    (re.compile(r"HS POST /crm/v3/objects/orders "), "creating order"),
    (re.compile(r"HS PATCH /crm/v3/objects/orders/"), "patching order"),
    (re.compile(r"HS POST /crm/v3/objects/line_items"), "creating line items"),
    (re.compile(r"HS PATCH /crm/v3/objects/line_items/"), "stamping product id"),
    (re.compile(r"HS POST /crm/v4/associations/"), "associating records"),
    (re.compile(r"HS POST /crm/v3/objects/2-\d+ "), "creating bundle"),
    (re.compile(r"HS POST /crm/v3/objects/2-\d+/search"), "catalog / bundle lookup"),
    (re.compile(r"HS POST /crm/v3/objects/products/search"), "catalog / bundle lookup"),
    (re.compile(r"PHASE drive upload"), "Drive upload"),
    (re.compile(r"PHASE sheet append"), "audit row append"),
    (re.compile(r"PHASE sheet update"), "audit row update"),
]

R_PAGE = re.compile(r"PAGE slot (\S+ \S+) -> (\S+ \S+) page (\d+)")
R_REPORT = re.compile(r"Slot reports totalPages=(\d+), page has (\d+) order")
R_BEGIN = re.compile(r"ORDER begin (\S+) ref (\S+) items=(\d+)")
R_CREATED = re.compile(r"CREATED order (\S+) -> HubSpot (\S+) \((\w+) contact (\S+)\)")
R_HELD = re.compile(r"HELD order (\S+)")
R_SKIP = re.compile(r"skip existing (\S+)")
R_SUMMARY = re.compile(r"RUN SUMMARY")
# v1.5 format: cur/ceil pairs + lanes; the old single-value format still matches
# via R_RATES_OLD.
R_RATES = re.compile(
    r"RATES hs_search=([\d.]+)/([\d.]+)/s hs_general=([\d.]+)/([\d.]+)/s "
    r"sheets=([\d.]+)/([\d.]+)/min drive=([\d.]+)/([\d.]+)/min "
    r"relay_gap=([\d.]+)s lanes=(\d+)/(\d+)")
R_RATES_OLD = re.compile(
    r"RATES (hs_search=\S+ hs_general=\S+ sheets=\S+ drive=\S+ relay_gap=\S+)")
R_ADAPT = re.compile(r"ADAPT (\S+) ([\d.]+)->([\d.]+)/s \((.*)\)")


class State:
    def __init__(self, target, baseline=0):
        self.target = target
        self.baseline = baseline
        self.t0 = time.time()
        self.last_line = time.time()
        self.engine = "waiting for engine…"
        self.slot = "-"
        self.page = 0
        self.page_total = 0
        self.page_orders = 0
        self.page_done = 0
        self.lanes = {}            # lane -> dict(id, ref, items, phase, since)
        self.counts = Counter()
        self.recent = deque(maxlen=8)
        self.done_ts = deque(maxlen=4000)
        self.last_result = ""
        self.rates = ""            # raw pacing tail (web UI compatibility)
        self.pacing = {}           # bucket -> (current, ceiling, unit)
        self.lanes_used = 0
        self.lanes_max = 0
        self.last_adapt = ""
        self.spin_i = 0

    # -- feed -----------------------------------------------------------------

    def feed(self, ts, level, lane, msg):
        self.last_line = time.time()
        lane = lane or "main"
        m = R_PAGE.search(msg)
        if m:
            self.engine = "RUNNING"
            self.slot = f"{m.group(1)[:16]} → {m.group(2)[11:16]}"
            self.page = int(m.group(3))
            self.page_done = 0
            self.lanes.clear()
            return
        m = R_REPORT.search(msg)
        if m:
            self.page_total, self.page_orders = int(m.group(1)), int(m.group(2))
            return
        m = R_BEGIN.search(msg)
        if m:
            self.lanes[lane] = {"id": m.group(1), "ref": m.group(2),
                                "items": m.group(3), "phase": "starting",
                                "since": time.time()}
            return
        m = R_CREATED.search(msg)
        if m:
            self.counts["created"] += 1
            self.page_done += 1
            self.done_ts.append(time.time())
            self.last_result = f"✓ {m.group(1)} → HS {m.group(2)} ({m.group(3)} contact)"
            self.recent.appendleft((ts, "✓", f"{lane}  {m.group(1)} → HS {m.group(2)} ({m.group(3)} contact)"))
            self.lanes.pop(lane, None)
            return
        m = R_HELD.search(msg)
        if m:
            self.counts["held"] += 1
            self.page_done += 1
            self.done_ts.append(time.time())
            self.last_result = f"◼ {m.group(1)} HELD (catalog)"
            self.recent.appendleft((ts, "◼", f"{lane}  HELD {m.group(1)} — unverified item"))
            self.lanes.pop(lane, None)
            return
        m = R_SKIP.search(msg)
        if m:
            self.counts["skipped"] += 1
            return
        m = R_RATES.search(msg)
        if m:
            g = m.groups()
            self.pacing = {
                "hs_search": (float(g[0]), float(g[1]), "/s"),
                "hs_general": (float(g[2]), float(g[3]), "/s"),
                "sheets": (float(g[4]), float(g[5]), "/min"),
                "drive": (float(g[6]), float(g[7]), "/min"),
            }
            self.relay_gap = float(g[8])
            self.lanes_used, self.lanes_max = int(g[9]), int(g[10])
            self.rates = msg[6:]
            return
        m = R_RATES_OLD.search(msg)
        if m:
            self.rates = m.group(1)
            return
        m = R_ADAPT.search(msg)
        if m:
            arrow = "▲" if float(m.group(3)) > float(m.group(2)) else "▼"
            self.last_adapt = f"{arrow} {m.group(1)} {m.group(2)}→{m.group(3)}/s ({m.group(4)[:28]})"
            return
        if level == "ERROR":
            self.counts["errors"] += 1
            self.recent.appendleft((ts, "✗", msg[:90]))
            return
        if R_SUMMARY.search(msg):
            self.engine = "STOPPED (summary in backfill.log)"
            self.lanes.clear()
            return
        if lane in self.lanes:
            for rx, name in PHASES:
                if rx.search(msg):
                    self.lanes[lane]["phase"] = name
                    return

    # -- derived --------------------------------------------------------------

    def processed(self):
        return self.baseline + self.counts["created"] + self.counts["held"]

    def rate_h(self):
        if len(self.done_ts) < 2:
            return 0.0
        recent = [t for t in self.done_ts if t > time.time() - 1800] or list(self.done_ts)
        if len(recent) < 2:
            return 0.0
        dt = recent[-1] - recent[0]
        if dt < 5.0:  # replayed/burst-parsed logs: too little wall-clock to rate
            return 0.0
        return (len(recent) - 1) / dt * 3600

    def spark_counts(self, minutes=40):
        now = time.time()
        buckets = [0] * minutes
        for t in self.done_ts:
            age = int((now - t) // 60)
            if 0 <= age < minutes:
                buckets[minutes - 1 - age] += 1
        return buckets

    def lanes_list(self):
        out = []
        for lane in sorted(self.lanes):
            d = self.lanes[lane]
            out.append({"lane": lane, "id": d["id"], "ref": d["ref"],
                        "items": d["items"], "phase": d["phase"],
                        "age_s": int(time.time() - d["since"])})
        return out

    # -- rendering ------------------------------------------------------------

    @staticmethod
    def _bar(cur, ceil, width=16):
        frac = 0.0 if ceil <= 0 else min(1.0, cur / ceil)
        filled = int(round(frac * width))
        color = "green" if frac < 0.7 else ("yellow" if frac < 0.92 else "red")
        return Text("█" * filled + "░" * (width - filled), style=color), frac

    def _sparkline(self, width=40):
        buckets = self.spark_counts(width)
        peak = max(buckets) or 1
        chars = "".join(BLOCKS[min(8, int(b / peak * 8 + (0.999 if b else 0)))]
                        for b in buckets)
        return chars, peak

    def render(self):
        self.spin_i = (self.spin_i + 1) % len(SPIN)
        rate = self.rate_h()
        done = self.processed()
        left = max(0, self.target - done)
        eta = ""
        if rate > 1 and self.engine == "RUNNING":
            fin = datetime.now() + timedelta(hours=left / rate)
            eta = fin.strftime("%a %d %b %H:%M")
        elapsed = str(timedelta(seconds=int(time.time() - self.t0)))

        try:
            cur = json.loads(CURSOR.read_text())
            window_txt = f"window {cur['from_date'][:16]} → {cur['to_date']}"
            cursor_line = (f"cursor {cur['from_date']}  page {cur['next_page']}"
                           f"  status {cur['status']}")
        except Exception:
            window_txt = "window —"
            cursor_line = "cursor unreadable"

        status_txt, status_style = self.engine, (
            "bold green" if self.engine == "RUNNING" else "bold yellow")
        stale = time.time() - self.last_line
        if self.engine == "RUNNING" and stale > 90:
            import subprocess
            alive = subprocess.run(["pgrep", "-f", "backfill.py --live"],
                                   capture_output=True).returncode == 0
            status_txt = (f"STALLED {int(stale)}s — engine alive but silent"
                          if alive else f"ENGINE DEAD — no process, silent {int(stale)}s")
            status_style = "bold red"

        head = Table.grid(expand=True)
        head.add_column(justify="left")
        head.add_column(justify="right")
        head.add_row(Text("Salla → HubSpot Backfill", style="bold cyan"),
                     Text(status_txt, style=status_style))
        head.add_row(Text(window_txt, style="dim"),
                     Text(f"elapsed {elapsed}   rate {rate:5.0f}/h   ETA {eta or '—'}",
                          style="dim"))

        # progress panel
        target = max(self.target, 1)  # --target 0: session-only mode, no div/0
        overall = ProgressBar(total=target, completed=min(done, target), width=None)
        pagebar = ProgressBar(total=max(self.page_orders, 1), completed=self.page_done, width=None)
        base_note = f"  (incl. {self.baseline:,} earlier)" if self.baseline else ""
        head_line = (f"window progress  {done:,} / {self.target:,}"
                     f"  ({done / self.target * 100:4.1f}%){base_note}"
                     if self.target > 0 else
                     f"orders processed this session  {done:,}{base_note}")
        prog = Group(
            Text(head_line, style="bold"),
            overall,
            Text(""),
            Text(f"slot {self.slot}    page {self.page}/{self.page_total or '?'}"),
            Text(f"page orders {self.page_done}/{self.page_orders}"),
            pagebar,
            Text(""),
            Text(cursor_line, style="dim"),
        )

        # lanes panel
        lanes_tbl = Table.grid(padding=(0, 1))
        lanes_tbl.add_column(style="bold cyan", no_wrap=True)   # lane
        lanes_tbl.add_column(style="bold white", no_wrap=True)  # order
        lanes_tbl.add_column(style="dim", no_wrap=True)         # ref/items
        lanes_tbl.add_column(overflow="ellipsis")               # phase
        lanes_tbl.add_column(justify="right", style="dim")      # age
        active = self.lanes_list()
        for d in active:
            lanes_tbl.add_row(
                d["lane"].replace("lane_", "L"),
                d["id"],
                f"ref {d['ref']} ×{d['items']}",
                Text(f"{SPIN[self.spin_i]} {d['phase']}", style="magenta"),
                f"{d['age_s']}s")
        if not active:
            lanes_tbl.add_row("—", "(between orders)", "", "", "")
        lanes_title = (f"lanes {self.lanes_used or len(active)}"
                       f"/{self.lanes_max or '?'}")
        lane_lines = [lanes_tbl]
        if self.last_result:
            lane_lines += [Text(""), Text(f"last: {self.last_result}", style="green")]

        # pacing panel
        pace = Table.grid(padding=(0, 1))
        pace.add_column(style="bold", no_wrap=True)
        pace.add_column(no_wrap=True)
        pace.add_column(justify="right", style="dim", no_wrap=True)
        if self.pacing:
            for name, (cur_v, ceil_v, unit) in self.pacing.items():
                bar, frac = self._bar(cur_v, ceil_v)
                pace.add_row(name, bar, f"{cur_v:.2f}/{ceil_v:.2f}{unit}")
            pace.add_row("relay gap", Text("·" * 16, style="dim"),
                         f"{getattr(self, 'relay_gap', 0):.2f}s")
        else:
            pace.add_row("pacing", Text(self.rates or "waiting for RATES…", style="dim"), "")
        pace_lines = [pace]
        if self.last_adapt:
            pace_lines += [Text(""), Text(self.last_adapt, style="cyan")]

        # session panel
        c = self.counts
        sess = Table.grid(padding=(0, 2))
        sess.add_column(style="bold")
        sess.add_column(justify="right")
        sess.add_row("created", f"[green]{c['created']:,}[/]")
        sess.add_row("held", f"[yellow]{c['held']:,}[/]")
        sess.add_row("dedup skipped", f"{c['skipped']:,}")
        sess.add_row("errors", f"[red]{c['errors']:,}[/]" if c["errors"] else "0")
        sess.add_row("credits est.", f"~{c['created'] * 2.3 + c['held'] * 2.3:,.0f}")

        # throughput sparkline
        chars, peak = self._sparkline(46)
        spark = Group(
            Text(chars, style="cyan"),
            Text(f"orders/min, last 46 min · peak {peak}/min · now {rate/60:.1f}/min",
                 style="dim"),
        )

        # recent feed
        recent = Table.grid(padding=(0, 1))
        recent.add_column(style="dim", no_wrap=True)
        recent.add_column(no_wrap=True)
        recent.add_column(overflow="ellipsis")
        for ts, icon, txt in self.recent:
            style = {"✓": "green", "◼": "yellow", "✗": "red"}[icon]
            recent.add_row(ts[11:], Text(icon, style=style), Text(txt))

        root = Layout()
        root.split_column(
            Layout(Panel(head, border_style="cyan"), size=4),
            Layout(name="mid", size=12),
            Layout(name="mid2", size=9),
            Layout(Panel(recent, title="recent", border_style="dim"), size=10),
        )
        root["mid"].split_row(
            Layout(Panel(prog, title="progress", border_style="blue")),
            Layout(Panel(Group(*lane_lines), title=lanes_title,
                         border_style="magenta")),
        )
        root["mid2"].split_row(
            Layout(Panel(Group(*pace_lines), title="adaptive pacing",
                         border_style="cyan")),
            Layout(Panel(sess, title="session", border_style="green"), size=30),
            Layout(Panel(spark, title="throughput", border_style="blue"), size=54),
        )
        return root


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", type=int, default=0,
                    help="orders expected in the window (0 = session-only view)")
    ap.add_argument("--baseline", type=int, default=0,
                    help="orders already resolved in the window by earlier sessions")
    ap.add_argument("--from-start", action="store_true",
                    help="replay the whole existing log instead of tailing from EOF")
    args = ap.parse_args()

    st = State(args.target, args.baseline)
    console = Console()
    f = None
    with Live(st.render(), console=console, refresh_per_second=6, screen=True) as live:
        while True:
            try:
                if f is None and LOG.exists():
                    f = open(LOG, encoding="utf-8", errors="replace")
                    if not args.from_start:
                        f.seek(0, 2)
                if f:
                    while True:
                        line = f.readline()
                        if not line:
                            break
                        m = LINE.match(line.rstrip("\n"))
                        if m:
                            st.feed(*m.groups())
                live.update(st.render())
                time.sleep(0.18)
            except KeyboardInterrupt:
                break


if __name__ == "__main__":
    main()
