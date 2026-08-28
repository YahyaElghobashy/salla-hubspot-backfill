#!/usr/bin/env python3
"""Live dashboard for the Zid emit: http://127.0.0.1:8899

Reads the same files the emitter writes -- month ledgers, plan summaries, the
emit log -- and computes state fresh on every request, so it can never show a
stale cache and never touches the API. stdlib only; kill it any time.
"""

import glob
import http.server
import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

WS = Path(os.path.expanduser("~/zed_local/ws"))
PLANS = WS / "mirror/zed_plans"
LEDGERS = WS / "mirror/zed_emitted"
LOG = Path("/tmp/emit_all.log")
PORT = 8899


def state():
    months = []
    total_plan = total_done = 0
    for f in sorted(PLANS.glob("*.summary.json")):
        m = f.name.split(".")[0]
        s = json.loads(f.read_text())
        planned = s.get("created", 0)
        led = LEDGERS / f"{m}.csv"
        done = created = 0
        last_ts = ""
        if led.exists():
            for line in open(led, encoding="utf-8"):
                if line.endswith(",done\n") or line.rstrip().endswith(",done"):
                    done += 1
                    last_ts = line.split(",", 1)[0]
                elif ",created" in line:
                    created += 1
        total_plan += planned
        total_done += done
        months.append({"month": m, "planned": planned, "done": done,
                       "dirty": max(0, created - done), "last": last_ts})

    # rate from the most recent 400 done-rows across ledgers
    stamps = []
    for f in sorted(LEDGERS.glob("*.csv"), reverse=True)[:2]:
        for line in open(f, encoding="utf-8"):
            if line.rstrip().endswith(",done"):
                try:
                    stamps.append(datetime.strptime(
                        line.split(",", 1)[0], "%Y-%m-%d %H:%M:%S"))
                except ValueError:
                    pass
    stamps = sorted(stamps)[-400:]
    rate = eta_h = None
    if len(stamps) > 50:
        span = (stamps[-1] - stamps[0]).total_seconds()
        if span > 0:
            rate = len(stamps) / span * 60
            age = (datetime.now() - stamps[-1]).total_seconds()
            if age < 600 and rate > 0:          # only claim ETA while flowing
                eta_h = (total_plan - total_done) / (rate * 60)

    tail, recoveries, errors = [], 0, 0
    if LOG.exists():
        lines = LOG.read_text(errors="replace").splitlines()
        recoveries = sum("lost-response recovery" in l for l in lines)
        errors = sum("exited nonzero" in l for l in lines)
        keep = [l for l in lines if any(k in l for k in
                ("EMITTED", "in plan", "recovery", "exited nonzero",
                 "RuntimeError", "yielding"))]
        tail = keep[-12:]

    running = subprocess.run(
        ["pgrep", "-f", "Python.app.*zed_emit"],
        capture_output=True).returncode == 0
    return {"months": months, "total_plan": total_plan,
            "total_done": total_done, "rate_per_min": rate, "eta_hours": eta_h,
            "running": running, "recoveries": recoveries,
            "loop_restarts": errors, "tail": tail,
            "as_of": datetime.now().strftime("%H:%M:%S")}


PAGE = """<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Zid Haul</title>
<style>
:root{--bg:#111318;--card:#1a1d24;--line:#2a2e38;--ink:#e8eaf0;--mut:#9aa3b2;
 --fill:#2fa98c;--track:#262a33;--good:#3fb27f;--warn:#d9a13b;--bad:#d96b5b}
*{box-sizing:border-box;margin:0}
body{background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,system-ui,sans-serif;padding:28px;max-width:980px;margin:0 auto}
h1{font-size:18px;font-weight:650;letter-spacing:.2px}
.sub{color:var(--mut);font-size:12px;margin-top:2px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}
.tile{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
.tile b{font-size:24px;font-variant-numeric:tabular-nums;display:block}
.tile span{color:var(--mut);font-size:11.5px;text-transform:uppercase;letter-spacing:.08em}
.big{height:14px;background:var(--track);border-radius:7px;overflow:hidden;margin:6px 0 26px}
.big i{display:block;height:100%;background:var(--fill);border-radius:7px 4px 4px 7px;transition:width .6s}
.chip{display:inline-flex;align-items:center;gap:6px;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:600}
.on{background:rgba(63,178,127,.15);color:var(--good)}
.off{background:rgba(217,107,91,.15);color:var(--bad)}
table{width:100%;border-collapse:collapse}
td{padding:4px 8px;font-variant-numeric:tabular-nums;font-size:12.5px}
td.m{color:var(--mut);width:70px}
td.n{text-align:right;width:130px;color:var(--mut)}
.bar{height:8px;background:var(--track);border-radius:4px;overflow:hidden}
.bar i{display:block;height:100%;background:var(--fill);border-radius:4px 2px 2px 4px}
.bar i.f{background:var(--good)}
pre{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 14px;font-size:11px;color:var(--mut);overflow-x:auto;line-height:1.6}
h2{font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.1em;margin:22px 0 8px}
</style></head><body>
<h1>Zid historical import <span id="chip" class="chip off">checking…</span></h1>
<div class="sub" id="sub"></div>
<h2>Overall</h2>
<div style="display:flex;justify-content:space-between;font-variant-numeric:tabular-nums">
 <span id="big-label"></span><span id="big-pct" style="color:var(--mut)"></span></div>
<div class="big"><i id="big" style="width:0%"></i></div>
<div class="grid">
 <div class="tile"><b id="t-done">–</b><span>orders in HubSpot</span></div>
 <div class="tile"><b id="t-rate">–</b><span>orders / min</span></div>
 <div class="tile"><b id="t-eta">–</b><span>est. remaining</span></div>
 <div class="tile"><b id="t-rec">–</b><span>recoveries · restarts</span></div>
</div>
<h2>Months</h2>
<table id="months"></table>
<h2>Recent activity</h2>
<pre id="tail">loading…</pre>
<script>
const $=id=>document.getElementById(id);
async function tick(){
 try{
  const d=await (await fetch('/state')).json();
  const pct=d.total_plan? (100*d.total_done/d.total_plan):0;
  $('chip').className='chip '+(d.running?'on':'off');
  $('chip').textContent=d.running?'\\u25CF running':'\\u25A0 stopped';
  $('sub').textContent='as of '+d.as_of+' · refreshes every 5s · ledgers are ground truth';
  $('big').style.width=pct.toFixed(2)+'%';
  $('big-label').textContent=d.total_done.toLocaleString()+' of '+d.total_plan.toLocaleString()+' orders';
  $('big-pct').textContent=pct.toFixed(1)+'%';
  $('t-done').textContent=d.total_done.toLocaleString();
  $('t-rate').textContent=d.rate_per_min? Math.round(d.rate_per_min).toLocaleString():'—';
  $('t-eta').textContent=d.eta_hours? (d.eta_hours<1? Math.round(d.eta_hours*60)+' min' : d.eta_hours.toFixed(1)+' h'):'—';
  $('t-rec').textContent=d.recoveries+' · '+d.loop_restarts;
  $('months').innerHTML=d.months.map(m=>{
   const p=m.planned? (100*m.done/m.planned):0;
   return `<tr><td class="m">${m.month}</td><td><div class="bar"><i class="${p>=100?'f':''}" style="width:${p}%"></i></div></td><td class="n">${m.done.toLocaleString()} / ${m.planned.toLocaleString()}${m.dirty?' · '+m.dirty+' repairing':''}</td></tr>`;
  }).join('');
  $('tail').textContent=d.tail.join('\\n')||'(quiet)';
 }catch(e){$('chip').className='chip off';$('chip').textContent='\\u25A0 viewer offline';}
}
tick();setInterval(tick,5000);
</script></body></html>"""


class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/state":
            body = json.dumps(state()).encode()
            ctype = "application/json"
        else:
            body = PAGE.encode()
            ctype = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    print(f"Zid haul dashboard: http://127.0.0.1:{PORT}")
    http.server.ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
