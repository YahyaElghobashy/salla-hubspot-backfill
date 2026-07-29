#!/usr/bin/env bash
# backfill_chain — run the backfill over a list of month windows, in the order
# given, waiting for any running queue drain to finish first.
#
#   ./tools/backfill_chain.sh 2026-02 2026-01 2025-12 ... 2025-01
#
# Each window is swept by the UNMODIFIED engine (ascending inside the window,
# as designed); this script only decides which window comes next. If the
# current cursor already sits inside the first window it resumes rather than
# resets — dedup makes any overlap free, but resuming skips even the re-scan.
# A window is complete when the cursor reports done/done_overflow. Crashes are
# retried after 60s (credit outages surface here: relay_health alerts, the
# engine exits, we retry later without hammering).
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

wait_for_drain() {
  while pgrep -f "queue_drain.py" >/dev/null 2>&1; do
    echo "[chain] queue drain still running — waiting 120s"
    sleep 120
  done
}

month_end() {  # 2026-01 -> 2026-02-01
  python3 - "$1" <<'PY'
import sys, datetime
y, m = map(int, sys.argv[1].split("-"))
print(f"{y + m // 12}-{m % 12 + 1:02d}-01")
PY
}

cursor_field() { python3 -c "import json;print(json.load(open('cursor.json')).get('$1',''))" 2>/dev/null; }

for W in "$@"; do
  FROM="${W}-01T00:00:00"; TO=$(month_end "$W")
  CF=$(cursor_field from_date); CT=$(cursor_field to_date); CS=$(cursor_field status)
  if [ "$CT" = "$TO" ] && [ "$CS" != "done" ] && [ "$CS" != "done_overflow" ] && [[ "$CF" == "$W"* || "$CF" < "$TO" ]]; then
    echo "[chain] $W: resuming existing cursor at $CF ($CS)"
  else
    echo "[chain] $W: fresh window $FROM -> $TO"
    printf '{"from_date": "%s", "to_date": "%s", "next_page": 1, "total_pages": 1, "status": "running"}\n' "$FROM" "$TO" > cursor.json
  fi
  tries=0
  until CS=$(cursor_field status); [ "$CS" = "done" ] || [ "$CS" = "done_overflow" ]; do
    wait_for_drain
    rm -f STOP
    echo "[chain] $W: engine run $((++tries)) starting ($(date '+%F %T'))"
    ./venv/bin/python3 backfill.py --live --yes || true
    CS=$(cursor_field status)
    [ "$CS" = "done" ] || [ "$CS" = "done_overflow" ] && break
    if [ -e STOP ]; then echo "[chain] STOP present — chain paused; exiting"; exit 0; fi
    echo "[chain] $W: engine exited early (cursor=$CS) — retrying in 60s"
    sleep 60
  done
  echo "[chain] $W: COMPLETE ($(cursor_field status))"
done
echo "[chain] all windows complete"
