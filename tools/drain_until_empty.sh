#!/usr/bin/env bash
# Run the hourly drain unit back to back until the held backlog is empty,
# stopping itself the moment anything looks wrong.
#
# Each pass is EXACTLY the normal hourly pass (salla-queue-drain.service:
# same --max-orders cap, same live-yield pacing, same timeout); only the idle
# time between passes disappears. Stop rules, checked after every pass:
#   - a pass left orders blocked, or reported errors
#   - any NEW HubSpot 429 in drain.log, live.log or status_relay.log
#   - any NEW "needs_review item" line (a line item built wrongly)
#   - the live sync's oldest waiting order went over 60 s during the pass
#   - no summary line (the pass died) or STOP.drain present
# A pass that creates nothing means the backlog is empty: done.
#
# Never stop the drain with systemctl stop or kill: the engine only handles
# SIGINT, so a hard stop can leave a half-built order. Touch STOP.drain
# instead; the pass finishes its in-flight orders and this loop exits.
#
# Run as root through a transient unit so it survives the SSH session:
#   sudo systemd-run --unit=drain-backtoback /bin/bash \
#     /home/integrations_claragroup_com/app/tools/drain_until_empty.sh 20
set -u
APP=/home/integrations_claragroup_com/app
MAX_PASSES=${1:-20}
cd "$APP" || exit 1
LOG=drain_backtoback.log
say() { echo "$(date '+%F %T') $*" | tee -a "$LOG"; }
n429() { local n; n=$(grep -c " 429" "$1" 2>/dev/null); echo "${n:-0}"; }
state() { systemctl show -p ActiveState --value salla-queue-drain.service; }

say "start: up to $MAX_PASSES passes"
for i in $(seq 1 "$MAX_PASSES"); do
  [ -e STOP.drain ] && { say "STOP.drain present -- exiting"; exit 0; }
  while [ "$(state)" = "activating" ] || [ "$(state)" = "active" ]; do
    sleep 20   # an hourly pass is already running; wait for it
  done
  d0=$(n429 drain.log); l0=$(n429 live.log); s0=$(n429 status_relay.log)
  r0=$(grep -c "needs_review item" drain.log)
  dline=$(($(wc -l < drain.log) + 1)); lline=$(($(wc -l < live.log) + 1))

  systemctl start salla-queue-drain.service   # oneshot: blocks until the pass ends

  summary=$(tail -n +"$dline" drain.log | grep -E "created=[0-9]+ blocked\(still queued\)=" | tail -1)
  say "pass $i: ${summary#*\[main\] }"
  [ -z "$summary" ] && { say "ABORT: pass wrote no summary"; exit 1; }
  created=$(sed -E 's/.*created=([0-9]+).*/\1/' <<<"$summary")
  blocked=$(sed -E 's/.*blocked\(still queued\)=([0-9]+).*/\1/' <<<"$summary")
  errors=$(sed -E 's/.*errors=([0-9]+).*/\1/' <<<"$summary")

  [ "$blocked" -gt 0 ] && { say "ABORT: $blocked order(s) still blocked"; exit 1; }
  [ "$errors" -gt 0 ] && { say "ABORT: $errors error(s)"; exit 1; }
  [ "$(n429 drain.log)" -gt "$d0" ] || [ "$(n429 live.log)" -gt "$l0" ] || \
    [ "$(n429 status_relay.log)" -gt "$s0" ] && { say "ABORT: new HubSpot 429s"; exit 1; }
  [ "$(grep -c 'needs_review item' drain.log)" -gt "$r0" ] && { say "ABORT: needs_review line items"; exit 1; }
  if tail -n +"$lline" live.log | grep -qE "oldest_age=([0-9]+m|[6-9][0-9]s|[0-9]{3,}s)"; then
    say "ABORT: live sync queue waited over 60s during the pass"; exit 1
  fi
  [ "$created" -eq 0 ] && { say "DONE: backlog empty"; exit 0; }
  sleep 30
done
say "reached $MAX_PASSES passes"
