#!/usr/bin/env python3
"""Cross-platform supervisor for the backfill engine.

Replaces the old macOS-only `caffeinate + run.sh` combo:
  - keeps the machine awake for the duration (macOS: caffeinate; Windows:
    SetThreadExecutionState; Linux: systemd-inhibit when available)
  - relaunches the engine after an abnormal termination (crash, SIGKILL),
    up to --max-restarts times
  - a graceful engine exit (STOP file honored, cursor done, or clean finish)
  ends the loop
  - loads secrets from .env if present (KEY=VALUE or export KEY="VALUE")

Usage:
    python run.py --dry               # dry run, one page, no writes
    python run.py --live              # full live run (asks for RUN unless --yes)
    python run.py --live --max-orders 2
"""
import argparse
import ctypes
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
os.chdir(HERE)
PIDFILE = Path("engine.pid")


def load_dotenv(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


class KeepAwake:
    """Best-effort sleep inhibitor per platform. No-op when unsupported."""

    def __init__(self):
        self.proc = None
        self.win = False

    def __enter__(self):
        system = platform.system()
        try:
            if system == "Darwin":
                self.proc = subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])
            elif system == "Windows":
                ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
                ctypes.windll.kernel32.SetThreadExecutionState(
                    ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
                self.win = True
            elif system == "Linux":
                self.proc = subprocess.Popen(
                    ["systemd-inhibit", "--what=idle:sleep", "--why=backfill run",
                     "sleep", "infinity"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"keep-awake unavailable ({e}); keep the machine awake manually")
        return self

    def __exit__(self, *_):
        if self.proc:
            self.proc.terminate()
        if self.win:
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)


def cursor_status(state_file):
    try:
        return json.loads(Path(state_file).read_text()).get("status", "?")
    except Exception:
        return "?"


def main():
    ap = argparse.ArgumentParser()
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry", action="store_true", help="dry run (reads only)")
    mode.add_argument("--live", action="store_true", help="live run (writes)")
    ap.add_argument("--max-orders", type=int, default=None)
    ap.add_argument("--max-pages", type=int, default=None)
    ap.add_argument("--no-google", action="store_true")
    ap.add_argument("--max-restarts", type=int, default=50)
    ap.add_argument("--yes", action="store_true",
                    help="skip the interactive RUN confirmation (use from the web UI)")
    args = ap.parse_args()

    load_dotenv()
    if not os.environ.get("HUBSPOT_ACCESS_TOKEN") or not os.environ.get("RELAY_SECRET"):
        sys.exit("HUBSPOT_ACCESS_TOKEN / RELAY_SECRET missing: set them in .env "
                 "or the environment (the setup wizard writes .env for you).")
    state_file = "cursor.json"
    try:
        state_file = json.loads(Path("config.json").read_text()).get("state_file", state_file)
    except Exception:
        sys.exit("config.json missing or invalid: run the setup wizard (python serve.py)")

    if args.live and not args.yes:
        confirm = input("LIVE RUN will write to your HubSpot portal. Type RUN to proceed: ")
        if confirm.strip() != "RUN":
            sys.exit("aborted")

    cmd = [sys.executable, "-u", "backfill.py"]
    if args.live:
        cmd.append("--live")
    if args.max_orders is not None:
        cmd += ["--max-orders", str(args.max_orders)]
    if args.max_pages is not None:
        cmd += ["--max-pages", str(args.max_pages)]
    if args.no_google:
        cmd.append("--no-google")

    with KeepAwake():
        for attempt in range(1, args.max_restarts + 1):
            if Path("STOP").exists():
                print("run.py: STOP file present, not (re)starting")
                break
            if cursor_status(state_file) in ("done", "done_overflow"):
                print("run.py: cursor is done, nothing to do")
                break
            print(f"run.py: launch attempt {attempt} at {time.strftime('%F %T')}")
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)
            PIDFILE.write_text(str(proc.pid))
            try:
                if args.live:
                    proc.stdin.write("RUN\n")
                    proc.stdin.flush()
            except Exception:
                pass
            code = proc.wait()
            PIDFILE.unlink(missing_ok=True)
            print(f"run.py: engine exited with code {code} at {time.strftime('%F %T')}")
            if code == 0 or args.dry:
                break
            print("run.py: abnormal exit, relaunching in 20s (dedup makes the rescan free)")
            time.sleep(20)


if __name__ == "__main__":
    main()
