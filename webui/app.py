"""Flask app behind serve.py: setup wizard APIs, run control, live status.

Design rules:
  - the web layer NEVER bypasses engine guardrails: it shells out to run.py,
    which drives backfill.py with the same STOP file, dry-run default,
    dedup, rate limits, mirrors, and logging as the terminal path
  - secrets live only in .env (0600 where supported) and are never echoed
    back to the browser; APIs report set/unset booleans only
  - the dashboard is a read-only tail of backfill.log via the same parser
    the terminal TUI uses (dashboard.State)
"""
import json
import os
import re
import ssl
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import dashboard as dash  # log parser reuse
from run import load_dotenv

CTX = ssl.create_default_context()
ENV_FILE = ROOT / ".env"
CONFIG = ROOT / "config.json"
LOG = ROOT / "backfill.log"
LIVELOG = ROOT / "live.log"          # v1.6 live-sync service log
PIDFILE = ROOT / "engine.pid"
LIVEPID = ROOT / "live.pid"


def _env():
    load_dotenv(ENV_FILE)
    return os.environ


def _pid_alive(pidfile):
    if not pidfile.exists():
        return False
    try:
        pid = int(pidfile.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        return False


def engine_running():
    return _pid_alive(PIDFILE)


def live_running():
    return _pid_alive(LIVEPID)


def hs_get(path, token):
    req = urllib.request.Request("https://api.hubapi.com" + path,
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
        return json.loads(r.read())


class LogHistory:
    """v1.6: incremental parser over backfill.log. Extracts every completed
    run (RUN SUMMARY blocks) and a rolling window of human-readable events.
    Tolerates both log formats (with/without the [lane] token) and log files
    in the hundreds of MB: the file is scanned once, then only appended bytes
    are read on subsequent calls."""

    R_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ (\w+)\s+"
                        r"(?:\[([\w-]+)\] )?(.*)$")
    R_SUM = re.compile(r"RUN SUMMARY\s+duration=([\d.]+) min\s+live=(\w+)")
    R_COUNTS = re.compile(r"pages=(\d+) scanned=(\d+) skipped_existing=(\d+) "
                          r"created=(\d+) held=(\d+)")
    R_LI = re.compile(r"line items: standalone=(\d+) bundle_parent=(\d+) "
                      r"component=(\d+) needs_review=(\d+)")
    R_ERR = re.compile(r"errors=(\d+) \(see")
    R_CURSOR = re.compile(r'cursor: ({.*})')
    R_CREATED = re.compile(r"CREATED order (\S+) -> HubSpot (\S+) \((\w+) contact")
    R_HELD = re.compile(r"HELD order (\S+) \((\d+) unverified")
    R_SKIP = re.compile(r"skip existing (\S+)")
    R_PAGE = re.compile(r"PAGE slot (\S+ \S+) -> \S+ (\S+) page (\d+)")
    R_ADAPT = re.compile(r"ADAPT (\S+) ([\d.]+)->([\d.]+)/s \((.*)\)")
    R_WORKERS = re.compile(r"lanes=\d+/(\d+)")

    EVENT_CAP = 4000

    def __init__(self, path, source="backfill"):
        self.path = path
        self.source = source
        # Flask's dev server is multi-threaded and several endpoints poll this
        # concurrently. Without a lock two threads read the same byte range and
        # append the same run summaries twice -> the run list / lifetime totals
        # double. This serializes the read+append so each line is parsed once.
        self._lock = threading.Lock()
        self._offset = 0
        self._runs = []
        self._events = []
        self._pending = None  # run summary being assembled
        # lifetime line-counts: run summaries only exist for graceful exits,
        # so the true all-time totals come from counting the lines themselves
        self.total_created = 0
        self.total_held = 0
        self.first_ts = ""
        self.last_ts = ""

    def _push(self, ts, kind, text, raw=""):
        oid = ""
        m = re.search(r"[Oo]rder (\S+)", text)
        if m and m.group(1).isdigit():
            oid = m.group(1)
        self._events.append({"ts": ts, "kind": kind, "text": text, "raw": raw,
                             "source": self.source, "oid": oid})
        if len(self._events) > self.EVENT_CAP:
            del self._events[: len(self._events) - self.EVENT_CAP]

    def _feed(self, line):
        m = self.R_LINE.match(line)
        if not m:
            return
        ts, level, _lane, msg = m.groups()
        lane = _lane or ""
        if self._pending is not None:
            c = self.R_COUNTS.search(msg)
            if c:
                self._pending.update(pages=int(c.group(1)), scanned=int(c.group(2)),
                                     skipped=int(c.group(3)), created=int(c.group(4)),
                                     held=int(c.group(5)))
                return
            li = self.R_LI.search(msg)
            if li:
                self._pending["line_items"] = dict(
                    standalone=int(li.group(1)), bundle_parent=int(li.group(2)),
                    component=int(li.group(3)), needs_review=int(li.group(4)))
                return
            e = self.R_ERR.search(msg)
            if e:
                self._pending["errors"] = int(e.group(1))
                return
            cu = self.R_CURSOR.search(msg)
            if cu:
                try:
                    cur = json.loads(cu.group(1))
                    self._pending["window_from"] = cur.get("from_date", "")
                    self._pending["window_to"] = cur.get("to_date", "")
                    self._pending["cursor_status"] = cur.get("status", "")
                except json.JSONDecodeError:
                    pass
                r = self._pending
                dur = max(r["duration_min"], 0.01)
                r["rate_h"] = round((r["created"] + r["held"]) / dur * 60, 1)
                self._runs.append(r)
                self._push(r["end_ts"], "system",
                           f"Run finished: {r['created']:,} created, {r['held']} held, "
                           f"{r['errors']} errors in {r['duration_min']:.0f} min "
                           f"({'live' if r['live'] else 'dry run'})")
                self._pending = None
                return
            return
        s = self.R_SUM.search(msg)
        if s:
            self._pending = {"end_ts": ts, "duration_min": float(s.group(1)),
                             "live": s.group(2) == "True", "pages": 0, "scanned": 0,
                             "skipped": 0, "created": 0, "held": 0, "errors": 0,
                             "line_items": {}, "window_from": "", "window_to": "",
                             "cursor_status": "", "workers": self._last_workers}
            return
        m2 = self.R_CREATED.search(msg)
        if m2:
            self.total_created += 1
            if not self.first_ts:
                self.first_ts = ts
            self.last_ts = ts
            self._push(ts, "created",
                       f"Order {m2.group(1)} created in HubSpot ({m2.group(3)} contact)"
                       + (f" — {lane.replace('_', ' ')}" if lane.startswith("lane") else ""),
                       raw=line)
            return
        m2 = self.R_HELD.search(msg)
        if m2:
            self.total_held += 1
            self._push(ts, "held",
                       f"Order {m2.group(1)} held — {m2.group(2)} item(s) not yet "
                       f"approved in the catalog", raw=line)
            return
        m2 = self.R_SKIP.search(msg)
        if m2:
            self._push(ts, "skip", f"Order {m2.group(1)} already in HubSpot — skipped "
                       f"(deduplication)", raw=line)
            return
        m2 = self.R_PAGE.search(msg)
        if m2:
            self._push(ts, "system",
                       f"Scanning slot {m2.group(1)} → {m2.group(2)[:5]}, "
                       f"page {m2.group(3)}", raw=line)
            return
        m2 = self.R_ADAPT.search(msg)
        if m2:
            up = float(m2.group(3)) > float(m2.group(2))
            self._push(ts, "pacing",
                       f"Pacing {'increased' if up else 'reduced'}: {m2.group(1)} "
                       f"{m2.group(2)}→{m2.group(3)}/s ({m2.group(4)})", raw=line)
            return
        w = self.R_WORKERS.search(msg)
        if w:
            self._last_workers = int(w.group(1))
        if level == "ERROR":
            self._push(ts, "error", msg[:180], raw=line)
        elif level == "WARNING" and ("429" in msg or "STOP" in msg or "SIGINT" in msg
                                     or "Halting" in msg):
            self._push(ts, "system", msg[:180], raw=line)

    _last_workers = 0

    def _refresh(self):
        with self._lock:  # one thread reads+appends at a time (no double-count)
            if not self.path.exists():
                return
            size = self.path.stat().st_size
            if size < self._offset:  # rotated/truncated: rescan
                self._offset, self._runs, self._events, self._pending = 0, [], [], None
                self.total_created = self.total_held = 0
                self.first_ts = self.last_ts = ""
            if size == self._offset:
                return
            with open(self.path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(self._offset)
                for line in f:
                    if line.endswith("\n"):
                        self._feed(line.rstrip("\n"))
                self._offset = f.tell()

    def get(self):
        self._refresh()
        return list(self._runs)

    def events(self):
        self._refresh()
        return list(self._events)


def order_trace(oid, logs, tail_bytes=8_000_000):
    """v1.6: reconstruct the checks a single order went through, from the
    tail of the given log files. Uses the [lane] token to gather the window
    of lines between an order's 'ORDER begin' and its resolution, then
    classifies each into a human-readable check + result. Read-only."""
    import re as _re
    LINE = _re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ (\w+)\s+"
                       r"(?:\[([\w-]+)\] )?(.*)$")
    # phase classifiers: (regex on msg) -> (label, how to read the result)
    CHECKS = [
        (_re.compile(r"orders/search -> 200 (.*)"), "Deduplication check",
         lambda m: ("already in HubSpot — skipped"
                    if '"total":0' not in m.group(1) else "not found — will create")),
        (_re.compile(r"contacts/search -> 200 (.*)"), "Contact lookup by phone",
         lambda m: ("matched an existing contact"
                    if '"total":0' not in m.group(1) else "no match — will create")),
        (_re.compile(r"POST /crm/v3/objects/contacts -> 20\d"), "Contact created", None),
        (_re.compile(r"products/search -> 200 (.*)"), "Catalog / product check",
         lambda m: ("product found" if '"total":0' not in m.group(1) else "not in catalog")),
        (_re.compile(r"objects/2-\d+/search -> 200 (.*)"), "Bundle / component check",
         lambda m: ("match" if '"total":0' not in m.group(1) else "none")),
        (_re.compile(r"POST /crm/v3/objects/orders -> 20\d"), "Order created in HubSpot", None),
        (_re.compile(r"PATCH /crm/v3/objects/orders/"), "Order patched (stage / sync)", None),
        (_re.compile(r"POST /crm/v3/objects/line_items -> 20\d"), "Line item created", None),
        (_re.compile(r"PATCH /crm/v3/objects/line_items/"), "Line item stamped with product", None),
        (_re.compile(r"associations/.*batch/create -> 20\d"), "Records linked (association)", None),
        (_re.compile(r"PHASE drive upload"), "Order JSON archived to Drive", None),
        (_re.compile(r"PHASE sheet append"), "Audit row written", None),
        (_re.compile(r"PHASE sheet update"), "Audit row updated", None),
    ]
    header = {}
    checks = []
    for src_name, path in logs:
        if not path.exists():
            continue
        size = path.stat().st_size
        with open(path, "rb") as f:
            if size > tail_bytes:
                f.seek(size - tail_bytes)
                f.readline()  # drop the partial first line
            data = f.read().decode("utf-8", "replace")
        lane_oid = {}          # lane -> the order id it is currently processing
        capturing_lane = None
        for line in data.splitlines():
            m = LINE.match(line)
            if not m:
                continue
            ts, level, lane, msg = m.groups()
            lane = lane or "main"
            b = _re.search(r"ORDER begin (\S+) ref (\S+) items=(\d+)", msg)
            if b:
                lane_oid[lane] = b.group(1)
                if b.group(1) == oid:
                    header = {"source": src_name, "ts": ts, "ref": b.group(2),
                              "items": b.group(3)}
                    capturing_lane = lane
                    checks = []
                continue
            cr = _re.search(r"CREATED order (\S+) -> HubSpot (\S+) \((\w+) contact (\S+)\)", msg)
            if cr and cr.group(1) == oid:
                header["result"] = "created"
                header["hs_order_id"] = cr.group(2)
                header["contact_kind"] = cr.group(3)
                header["hs_contact_id"] = cr.group(4)
                checks.append({"ts": ts, "label": "Order fully synced", "ok": "ok",
                               "detail": f"HubSpot order {cr.group(2)}, "
                                         f"{cr.group(3)} contact {cr.group(4)}"})
                capturing_lane = None
                continue
            hd = _re.search(r"HELD order (\S+) \((\d+) unverified", msg)
            if hd and hd.group(1) == oid:
                header["result"] = "held"
                checks.append({"ts": ts, "label": "Held for catalog approval", "ok": "warn",
                               "detail": f"{hd.group(2)} item(s) not yet approved"})
                capturing_lane = None
                continue
            sk = _re.search(r"skip existing (\S+)", msg)
            if sk and sk.group(1) == oid:
                header.setdefault("source", src_name)
                header.setdefault("ts", ts)
                header["result"] = "deduplicated"
                checks.append({"ts": ts, "label": "Deduplication", "ok": "ok",
                               "detail": "already in HubSpot — skipped (no duplicate)"})
                continue
            if level == "ERROR" and oid in msg:
                header.setdefault("result", "error")
                checks.append({"ts": ts, "label": "Error", "ok": "err",
                               "detail": msg[:200]})
                continue
            # trace lines: only while capturing this order's lane window
            if capturing_lane and lane == capturing_lane:
                for rx, label, reader in CHECKS:
                    mm = rx.search(msg)
                    if mm:
                        detail = reader(mm) if reader else ""
                        checks.append({"ts": ts, "label": label,
                                       "ok": "ok", "detail": detail})
                        break
    return {"order_id": oid, "header": header, "checks": checks}


def create_app():
    app = Flask(__name__, static_folder=str(ROOT / "webui" / "static"))
    history = LogHistory(LOG, source="backfill")
    live_history = LogHistory(LIVELOG, source="live")

    @app.get("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    # ---- state -------------------------------------------------------------

    @app.get("/api/state")
    def state():
        env = _env()
        cfg = {}
        if CONFIG.exists():
            try:
                cfg = json.loads(CONFIG.read_text())
            except Exception:
                cfg = {"_error": "config.json unreadable"}
        cursor = {}
        cursor_file = cfg.get("state_file", "cursor.json")
        if (ROOT / cursor_file).exists():
            try:
                cursor = json.loads((ROOT / cursor_file).read_text())
            except Exception:
                pass
        return jsonify({
            "config_present": CONFIG.exists(),
            "config": cfg,
            "secrets": {
                "hubspot_token": bool(env.get("HUBSPOT_ACCESS_TOKEN")),
                "relay_secret": bool(env.get("RELAY_SECRET")),
            },
            "google_credentials_present": (ROOT / "credentials.json").exists(),
            "google_token_cached": (ROOT / "token.json").exists(),
            "cursor": cursor,
            "engine_running": engine_running(),
            "live_running": live_running(),
            "stop_file": (ROOT / "STOP").exists(),
            "stop_live_file": (ROOT / "STOP.live").exists(),
            "log_present": LOG.exists(),
            "live_log_present": LIVELOG.exists(),
        })

    # ---- wizard ------------------------------------------------------------

    @app.post("/api/secrets")
    def secrets():
        body = request.get_json(force=True)
        lines = []
        if ENV_FILE.exists():
            lines = [l for l in ENV_FILE.read_text().splitlines()
                     if not l.startswith(("HUBSPOT_ACCESS_TOKEN=", "RELAY_SECRET="))]
        tok = body.get("hubspot_token", "").strip()
        sec = body.get("relay_secret", "").strip()
        cur = _env()
        tok = tok or cur.get("HUBSPOT_ACCESS_TOKEN", "")
        sec = sec or cur.get("RELAY_SECRET", "")
        lines += [f"HUBSPOT_ACCESS_TOKEN={tok}", f"RELAY_SECRET={sec}"]
        ENV_FILE.write_text("\n".join(lines) + "\n")
        if os.name != "nt":
            os.chmod(ENV_FILE, 0o600)
        os.environ["HUBSPOT_ACCESS_TOKEN"] = tok
        os.environ["RELAY_SECRET"] = sec
        return jsonify({"ok": True})

    @app.post("/api/test/hubspot")
    def test_hubspot():
        tok = (request.get_json(force=True).get("token") or
               _env().get("HUBSPOT_ACCESS_TOKEN", ""))
        if not tok:
            return jsonify({"ok": False, "error": "no token provided"})
        try:
            info = hs_get("/account-info/v3/details", tok)
            return jsonify({"ok": True, "portal_id": info.get("portalId"),
                            "ui_domain": info.get("uiDomain")})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)[:200]})

    @app.post("/api/test/relay")
    def test_relay():
        body = request.get_json(force=True)
        url = body.get("relay_url", "").strip()
        secret = body.get("relay_secret") or _env().get("RELAY_SECRET", "")
        if not url or not secret:
            return jsonify({"ok": False, "error": "relay_url and secret required"})
        try:
            req = urllib.request.Request(url, data=json.dumps(
                {"secret": secret, "path": "orders/statuses"}).encode(),
                method="POST", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=90, context=CTX) as r:
                p = json.loads(r.read())
            if not p.get("ok"):
                return jsonify({"ok": False, "error": "relay answered but ok=false "
                                "(secret mismatch or Salla module error)"})
            statuses = [{"slug": s.get("slug"), "name": s.get("name")}
                        for s in (p.get("data") or {}).get("data", [])]
            return jsonify({"ok": True, "statuses": statuses})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)[:200]})

    @app.get("/api/hubspot/pipelines")
    def pipelines():
        tok = _env().get("HUBSPOT_ACCESS_TOKEN", "")
        if not tok:
            return jsonify({"ok": False, "error": "save the HubSpot token first"})
        try:
            data = hs_get("/crm/v3/pipelines/orders", tok)
            out = [{"id": p["id"], "label": p["label"],
                    "stages": [{"id": s["id"], "label": s["label"]}
                               for s in p.get("stages", [])]}
                   for p in data.get("results", [])]
            return jsonify({"ok": True, "pipelines": out})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)[:200]})

    @app.get("/api/hubspot/custom-objects")
    def custom_objects():
        tok = _env().get("HUBSPOT_ACCESS_TOKEN", "")
        if not tok:
            return jsonify({"ok": False, "error": "save the HubSpot token first"})
        try:
            data = hs_get("/crm-object-schemas/v3/schemas", tok)
            out = [{"objectTypeId": s.get("objectTypeId"), "name": s.get("name"),
                    "labels": (s.get("labels") or {}).get("plural")}
                   for s in data.get("results", [])]
            return jsonify({"ok": True, "schemas": out})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)[:200]})

    @app.get("/api/config")
    def get_config():
        if not CONFIG.exists():
            return jsonify({"present": False, "config": {}})
        return jsonify({"present": True, "config": json.loads(CONFIG.read_text())})

    @app.post("/api/config")
    def set_config():
        body = request.get_json(force=True)
        cfg = body.get("config", {})
        if not isinstance(cfg, dict) or not cfg:
            return jsonify({"ok": False, "error": "empty config"})
        CONFIG.write_text(json.dumps(cfg, indent=2) + "\n")
        cursor = body.get("cursor")
        if cursor:
            state_file = cfg.get("state_file", "cursor.json")
            if not (ROOT / state_file).exists() or body.get("overwrite_cursor"):
                (ROOT / state_file).write_text(json.dumps(cursor, indent=2))
        try:
            import backfill
            c = backfill.Config.load(str(CONFIG))
            getattr(backfill, "apply_portal_config", lambda _c: None)(c)
            valid = True
            err = ""
        except SystemExit as e:
            valid, err = False, str(e)
        return jsonify({"ok": True, "valid": valid, "validation_error": err})

    # ---- run control -------------------------------------------------------

    @app.post("/api/run")
    def run():
        body = request.get_json(force=True)
        mode = body.get("mode", "dry")
        if mode == "live-sync":
            # v1.6: the 24/7 queue consumer (live.py under the supervisor)
            if live_running():
                return jsonify({"ok": False, "error": "live sync already running"})
            if body.get("confirm") != "RUN":
                return jsonify({"ok": False, "error": 'type RUN in the confirmation box'})
            (ROOT / "STOP.live").unlink(missing_ok=True)
            cmd = [sys.executable, "-u", "run.py", "--mode", "live", "--live", "--yes"]
            if body.get("workers"):
                cmd += ["--workers", str(int(body["workers"]))]
            logf = open(ROOT / "supervisor.live.out", "a")
            subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True)
            return jsonify({"ok": True})
        if engine_running():
            return jsonify({"ok": False, "error": "engine already running"})
        if mode == "live" and body.get("confirm") != "RUN":
            return jsonify({"ok": False, "error": 'type RUN in the confirmation box'})
        (ROOT / "STOP").unlink(missing_ok=True)
        cmd = [sys.executable, "-u", "run.py",
               "--live" if mode == "live" else "--dry", "--yes"]
        if body.get("max_orders"):
            cmd += ["--max-orders", str(int(body["max_orders"]))]
        if body.get("no_google"):
            cmd.append("--no-google")
        if body.get("workers"):
            cmd += ["--workers", str(int(body["workers"]))]
        logf = open(ROOT / "supervisor.out", "a")
        subprocess.Popen(cmd, stdout=logf, stderr=logf,
                         start_new_session=True)  # survives the web UI closing
        return jsonify({"ok": True})

    @app.post("/api/stop")
    def stop():
        body = request.get_json(silent=True) or {}
        if body.get("scope") == "live":
            (ROOT / "STOP.live").touch()
            return jsonify({"ok": True, "note": "live sync finishes in-flight "
                            "orders, then halts; queued rows stay in the sheet"})
        (ROOT / "STOP").touch()
        return jsonify({"ok": True, "note": "engine finishes the current order, "
                        "then halts; cursor stays safe and resume is free"})

    # ---- live stream -------------------------------------------------------

    @app.get("/api/stream")
    def stream():
        src = LIVELOG if request.args.get("source") == "live" else LOG
        running_fn = live_running if request.args.get("source") == "live" else engine_running

        def gen():
            st = dash.State(target=0)
            f = None
            last_emit = 0.0
            while True:
                if f is None and src.exists():
                    f = open(src, encoding="utf-8", errors="replace")
                    f.seek(0, 2)
                if f:
                    while True:
                        line = f.readline()
                        if not line:
                            break
                        m = dash.LINE.match(line.rstrip("\n"))
                        if m:
                            st.feed(*m.groups())
                now = time.time()
                if now - last_emit >= 1.0:
                    last_emit = now
                    lanes = st.lanes_list()
                    payload = {
                        "engine_running": running_fn(),
                        "queue_depth": st.queue_depth,
                        "queue_age": st.queue_age,
                        "processed_today": st.processed_today,
                        # legacy fields (phase/current) mirror the busiest lane
                        "phase": lanes[0]["phase"] if lanes else "idle",
                        "slot": st.slot,
                        "page": st.page, "page_total": st.page_total,
                        "page_orders": st.page_orders, "page_done": st.page_done,
                        "counts": dict(st.counts), "rate_h": round(st.rate_h(), 1),
                        "current": ({"id": lanes[0]["id"], "ref": lanes[0]["ref"],
                                     "items": lanes[0]["items"]} if lanes else None),
                        "last_result": st.last_result,
                        "recent": list(st.recent)[:10],
                        "stale_s": int(now - st.last_line),
                        "rates": st.rates,
                        "lanes": lanes,
                        "lanes_max": st.lanes_max,
                        "pacing": {k: {"cur": v[0], "ceil": v[1], "unit": v[2]}
                                   for k, v in st.pacing.items()},
                        "relay_gap": getattr(st, "relay_gap", None),
                        "last_adapt": st.last_adapt,
                        "spark": st.spark_counts(40),
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                time.sleep(0.25)
        return Response(gen(), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/api/log")
    def logtail():
        n = min(int(request.args.get("lines", 200)), 2000)
        src = LIVELOG if request.args.get("source") == "live" else LOG
        if not src.exists():
            return jsonify({"lines": []})
        lines = src.read_text(encoding="utf-8", errors="replace").splitlines()[-n:]
        return jsonify({"lines": lines})

    # ---- run history + readable events (v1.6) -------------------------------

    @app.get("/api/runs")
    def runs():
        """Every completed run parsed from backfill.log RUN SUMMARY blocks,
        newest first, plus holistic totals across all history."""
        hist = history.get()
        totals = {
            "runs": len(hist),
            # lifetime truth = counted CREATED/HELD lines (covers sessions
            # that never printed a summary and the currently running one)
            "created": max(history.total_created, sum(r["created"] for r in hist)),
            "held": max(history.total_held, sum(r["held"] for r in hist)),
            "scanned": sum(r["scanned"] for r in hist),
            "skipped": sum(r["skipped"] for r in hist),
            "errors": sum(r["errors"] for r in hist),
            "hours": round(sum(r["duration_min"] for r in hist) / 60.0, 1),
            "best_rate_h": max((r["rate_h"] for r in hist), default=0),
            "first_ts": history.first_ts, "last_ts": history.last_ts,
        }
        return jsonify({"runs": hist[::-1], "totals": totals})

    @app.get("/api/events")
    def events():
        """Human-readable event feed merged across the backfill and live logs,
        filterable by kind (created|held|skip|error|pacing|system) and source
        (backfill|live). Each event carries source + oid for the UI blobs and
        per-order drill-down."""
        kind = request.args.get("kind", "")
        source = request.args.get("source", "")   # ''|backfill|live
        q = request.args.get("q", "").strip().lower()
        limit = min(int(request.args.get("limit", 300)), 1000)
        merged = list(history.events())
        if source != "backfill":
            merged += list(live_history.events())
        if source == "backfill":
            merged = list(history.events())
        merged.sort(key=lambda e: e["ts"])
        out = []
        for ev in reversed(merged):
            if kind and ev["kind"] != kind:
                continue
            if source and ev.get("source") != source:
                continue
            if q and q not in ev["text"].lower() and q not in ev.get("raw", "").lower():
                continue
            out.append(ev)
            if len(out) >= limit:
                break
        return jsonify({"events": out})

    @app.get("/api/order/<oid>")
    def order_detail(oid):
        """v1.6: the full check trace a single order went through, read from
        both logs. Powers the click-to-expand order detail."""
        if not oid.isdigit():
            return jsonify({"error": "bad order id"}), 400
        return jsonify(order_trace(oid, [("backfill", LOG), ("live", LIVELOG)]))

    @app.post("/api/window")
    def set_window():
        """v1.6: set the backfill window (cursor) from a date range without the
        full wizard. Refused while the engine is running so a live sweep is
        never disturbed mid-slot."""
        if engine_running():
            return jsonify({"ok": False, "error": "stop the backfill before "
                            "changing its window"})
        body = request.get_json(force=True)
        frm, to = body.get("from", "").strip(), body.get("to", "").strip()
        if not frm or not to:
            return jsonify({"ok": False, "error": "pick both a from and to date"})
        if len(frm) == 16:
            frm += ":00"
        try:
            cfg = json.loads(CONFIG.read_text())
        except Exception:
            return jsonify({"ok": False, "error": "config.json not saved yet"})
        state_file = cfg.get("state_file", "cursor.json")
        (ROOT / state_file).write_text(json.dumps(
            {"from_date": frm, "to_date": to, "next_page": 1,
             "total_pages": 1, "status": "running"}, indent=2))
        return jsonify({"ok": True, "note": f"window set {frm} → {to}; "
                        "press Start when ready"})

    @app.get("/api/errors")
    def errors():
        p = ROOT / "mirror" / "errors.csv"
        if not p.exists():
            return jsonify({"rows": []})
        rows = p.read_text(encoding="utf-8", errors="replace").splitlines()
        return jsonify({"rows": rows[1:][-100:]})

    return app
