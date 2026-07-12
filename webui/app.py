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
PIDFILE = ROOT / "engine.pid"


def _env():
    load_dotenv(ENV_FILE)
    return os.environ


def engine_running():
    if not PIDFILE.exists():
        return False
    try:
        pid = int(PIDFILE.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        return False


def hs_get(path, token):
    req = urllib.request.Request("https://api.hubapi.com" + path,
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
        return json.loads(r.read())


def create_app():
    app = Flask(__name__, static_folder=str(ROOT / "webui" / "static"))

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
            "stop_file": (ROOT / "STOP").exists(),
            "log_present": LOG.exists(),
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
            backfill.apply_portal_config(c)
            valid = True
            err = ""
        except SystemExit as e:
            valid, err = False, str(e)
        return jsonify({"ok": True, "valid": valid, "validation_error": err})

    # ---- run control -------------------------------------------------------

    @app.post("/api/run")
    def run():
        if engine_running():
            return jsonify({"ok": False, "error": "engine already running"})
        body = request.get_json(force=True)
        mode = body.get("mode", "dry")
        if mode == "live" and body.get("confirm") != "RUN":
            return jsonify({"ok": False, "error": 'type RUN in the confirmation box'})
        (ROOT / "STOP").unlink(missing_ok=True)
        cmd = [sys.executable, "-u", "run.py",
               "--live" if mode == "live" else "--dry", "--yes"]
        if body.get("max_orders"):
            cmd += ["--max-orders", str(int(body["max_orders"]))]
        if body.get("no_google"):
            cmd.append("--no-google")
        logf = open(ROOT / "supervisor.out", "a")
        subprocess.Popen(cmd, stdout=logf, stderr=logf,
                         start_new_session=True)  # survives the web UI closing
        return jsonify({"ok": True})

    @app.post("/api/stop")
    def stop():
        (ROOT / "STOP").touch()
        return jsonify({"ok": True, "note": "engine finishes the current order, "
                        "then halts; cursor stays safe and resume is free"})

    # ---- live stream -------------------------------------------------------

    @app.get("/api/stream")
    def stream():
        def gen():
            st = dash.State(target=0)
            f = None
            last_emit = 0.0
            while True:
                if f is None and LOG.exists():
                    f = open(LOG, encoding="utf-8", errors="replace")
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
                    payload = {
                        "engine_running": engine_running(),
                        "phase": st.phase, "slot": st.slot,
                        "page": st.page, "page_total": st.page_total,
                        "page_orders": st.page_orders, "page_done": st.page_done,
                        "counts": dict(st.counts), "rate_h": round(st.rate_h(), 1),
                        "current": st.cur, "last_result": st.last_result,
                        "recent": list(st.recent)[:10],
                        "stale_s": int(now - st.last_line),
                        "rates": st.rates,
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                time.sleep(0.25)
        return Response(gen(), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/api/log")
    def logtail():
        n = min(int(request.args.get("lines", 200)), 2000)
        if not LOG.exists():
            return jsonify({"lines": []})
        lines = LOG.read_text(encoding="utf-8", errors="replace").splitlines()[-n:]
        return jsonify({"lines": lines})

    @app.get("/api/errors")
    def errors():
        p = ROOT / "mirror" / "errors.csv"
        if not p.exists():
            return jsonify({"rows": []})
        rows = p.read_text(encoding="utf-8", errors="replace").splitlines()
        return jsonify({"rows": rows[1:][-100:]})

    return app
