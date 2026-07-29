#!/usr/bin/env bash
# vmtop — one-glance terminal status for the deployed sync fleet.
#
#   ./tools/vmtop.sh          # single snapshot
#   watch -c -n 5 ./tools/vmtop.sh   # live view, refreshes every 5s
#
# Reads only local files + systemd; makes no API calls, so it is free to run
# as often as you like. For the full interactive TUI use `python dashboard.py`;
# for the web dashboard, tunnel: gcloud compute ssh <vm> -- -L 8377:localhost:8377
set -euo pipefail
cd "$(dirname "$0")/.."

B=$'\033[1m'; DIM=$'\033[2m'; R=$'\033[0m'
GRN=$'\033[32m'; YEL=$'\033[33m'; RED=$'\033[31m'; CYN=$'\033[36m'

svc() {  # svc <unit> -> colored state word
  local s
  s=$(systemctl is-active "$1" 2>/dev/null || true)
  case "$s" in
    active)   printf '%s' "${GRN}● running${R}";;
    inactive) printf '%s' "${DIM}○ stopped${R}";;
    failed)   printf '%s' "${RED}✗ FAILED${R}";;
    *)        printf '%s' "${DIM}○ ${s:-not installed}${R}";;
  esac
}

last_match() { grep -E "$2" "$1" 2>/dev/null | tail -1 || true; }

echo "${B}┌─ Salla → HubSpot fleet ── $(hostname) ── $(date '+%Y-%m-%d %H:%M:%S %Z') ─┐${R}"
echo
echo "${B}SERVICES${R}"
printf '  %-22s %b\n' "live sync"      "$(svc salla-live-sync)"
printf '  %-22s %b\n' "credit watcher" "$(svc salla-credit-watch)"
for f in STOP.live STOP.credits STOP STOP.drain; do
  [ -e "$f" ] && printf '  %-22s %b\n' "$f" "${YEL}present — holds the matching engine${R}"
done
echo

echo "${B}LIVE SYNC${R}"
q=$(last_match live.log 'QUEUE depth=')
if [ -n "$q" ]; then
  ts=${q:0:19}
  depth=$(sed -E 's/.*depth=([0-9]+).*/\1/' <<<"$q")
  today=$(sed -E 's/.*processed_today=([0-9]+).*/\1/' <<<"$q")
  col=$GRN; [ "$depth" -gt 50 ] && col=$YEL; [ "$depth" -gt 500 ] && col=$RED
  printf '  queue depth   %b%s%b   processed today  %s   %sas of %s%s\n' \
    "$col" "$depth" "$R" "$today" "$DIM" "$ts" "$R"
else
  printf '  %sno QUEUE line in live.log yet%s\n' "$DIM" "$R"
fi
c=$(last_match live.log 'CREATED order')
[ -n "$c" ] && printf '  last created  %s%s%s\n' "$DIM" "${c:0:19} ${c:44:60}" "$R"
h=$(last_match live.log 'outage alerting')
[ -n "$h" ] && printf '  alerting      %s\n' "$(sed -E 's/.*alerting: //' <<<"$h")"
echo

echo "${B}MAKE CREDITS${R}"
if [ -f mirror/credit_state.json ]; then
  python3 - <<'PY'
import json, time
d = json.load(open("mirror/credit_state.json"))
org = d.get("org") or {}
rem = org.get("remaining") or 0
age = int(time.time() - (d.get("ts") or 0))
col = "\033[32m" if rem > 50000 else ("\033[33m" if rem > 10000 else "\033[31m")
print(f"  remaining     {col}{rem:,.0f}\033[0m   consumed {org.get('consumed',0):,.0f}"
      f"   renews {str(org.get('next_reset',''))[:10]}   \033[2mchecked {age}s ago\033[0m")
if d.get("out"):
    print("  \033[31m⛔ OUT OF CREDITS — syncing paused, watcher polling for recovery\033[0m")
PY
else
  printf '  %scredit watcher has not written state yet%s\n' "$DIM" "$R"
fi
echo

echo "${B}HEALTH${R}"
if [ -f mirror/relay_health.json ]; then
  python3 - <<'PY'
import json, time
d = json.load(open("mirror/relay_health.json"))
s = d.get("state", "ok"); age = int(time.time() - (d.get("ts") or 0))
icon = {"ok": "\033[32m● ok\033[0m"}.get(s, f"\033[31m✗ {s}\033[0m")
print(f"  relay         {icon}   \033[2m{(d.get('detail') or '')[:70]}\033[0m")
PY
else
  printf '  relay         %b   %sno failures recorded since deploy%s\n' "${GRN}● ok${R}" "$DIM" "$R"
fi
led=$( (cat mirror/created.csv 2>/dev/null || true) | wc -l | tr -d " ")
printf '  ledger        %s orders all-time\n' "$(printf "%'d" "$led" 2>/dev/null || echo "$led")"
err=$(tail -n +2 mirror/errors.csv 2>/dev/null | wc -l | tr -d ' ')
ecol=$GRN; [ "${err:-0}" -gt 0 ] && ecol=$YEL
printf '  error ledger  %b%s rows%b\n' "$ecol" "${err:-0}" "$R"
df -h / | awk 'NR==2 {printf "  disk          %s used of %s (%s)\n", $3, $2, $5}'
echo
echo "${DIM}dashboard.py = full TUI · serve.py via SSH tunnel = web UI · journalctl -u salla-live-sync -f = raw${R}"
