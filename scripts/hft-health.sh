#!/usr/bin/env bash
# External health check for the HFT FIX server.
#
# WHY THIS EXISTS
# On 2026-09-23 the FIX server was dead for six hours and nothing noticed.
# unattended-upgrades restarted pm2-ubuntu.service at 06:05, whose ExecStart is
# `pm2 resurrect` -- which rebuilt every app from ~/.pm2/dump.pm2, a file dated
# 4 June, months before the credentials moved into the process environment. The
# binary correctly treats a missing credential as fatal, so it died on every
# respawn, 7,276 times. deploy.sh already detects exactly this, but it only runs
# during a deploy, and this was not a deploy.
#
# So: this runs OUTSIDE the pm2 tree, on a systemd timer, and judges health by
# what the process actually did -- never by `pm2 status`, which reports "online"
# while respawning a process that exits immediately.
set -uo pipefail

LIVE=/home/ubuntu/fix_server
ECO=/home/ubuntu/ecosystem.config.js
ENVF=/home/ubuntu/.fix_server.env
STATE=/home/ubuntu/.hft-health.state
HEALS=/home/ubuntu/.hft-health.heals
LOG=/home/ubuntu/logs/hft-health.log
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Self-heal budget: this fixes a stuck box, it does not paper over a broken one.
MAX_HEALS_PER_HOUR=2
QUIET_INBOUND_SEC=300     # no inbound FIX at all for this long = suspect
MIN_UPTIME_SEC=45         # younger than this, give it a chance to settle

ts(){ date -u +%Y-%m-%dT%H:%M:%SZ; }
log(){ printf '%s  %s\n' "$(ts)" "$*" | tee -a "$LOG"; }

# ── Checks ───────────────────────────────────────────────────────────────────
# Each sets REASON and returns 1 when unhealthy.
REASON=""

check_process(){
  local pid
  pid=$(pgrep -f "$LIVE/target/release/fix_server" | head -1 || true)
  if [ -z "$pid" ]; then REASON="no fix_server process"; return 1; fi
  PID=$pid
  local up
  up=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ' || echo 0)
  UPTIME_SEC=${up:-0}
  return 0
}

check_startup_fatal(){
  # startup_note() writes FATAL on a config or credential failure.
  if [ -f "$LIVE/startup.log" ] && tail -40 "$LIVE/startup.log" | grep -qi "FATAL"; then
    # only counts if it is from the CURRENT process generation
    local last_fatal last_ok
    last_fatal=$(grep -i FATAL "$LIVE/startup.log" | tail -1 | awk '{print $1}')
    last_ok=$(grep -i "startup complete" "$LIVE/startup.log" | tail -1 | awk '{print $1}')
    if [ -z "$last_ok" ] || [[ "$last_fatal" > "$last_ok" ]]; then
      REASON="startup FATAL: $(grep -i FATAL "$LIVE/startup.log" | tail -1 | sed 's/.*FATAL: //' | cut -c1-120)"
      return 1
    fi
  fi
  return 0
}

check_not_flapping(){
  local r1 r2
  r1=$(pm2 jlist 2>/dev/null | python3 -c 'import json,sys;p=[x for x in json.load(sys.stdin) if x["name"]=="fix-server"];print(p[0]["pm2_env"].get("restart_time",0) if p else -1)' 2>/dev/null || echo -1)
  sleep 8
  r2=$(pm2 jlist 2>/dev/null | python3 -c 'import json,sys;p=[x for x in json.load(sys.stdin) if x["name"]=="fix-server"];print(p[0]["pm2_env"].get("restart_time",0) if p else -1)' 2>/dev/null || echo -1)
  if [ "$r1" != "$r2" ]; then REASON="crash-looping (restart count $r1 -> $r2 in 8s)"; return 1; fi
  return 0
}

check_inbound_recent(){
  # A process that is up but not talking to the venue is not healthy.
  [ "${UPTIME_SEC:-0}" -lt "$MIN_UPTIME_SEC" ] && return 0
  local f=$LIVE/fix_commands.log
  [ -f "$f" ] || { REASON="no fix_commands.log"; return 1; }
  local age=$(( $(date +%s) - $(stat -c %Y "$f") ))
  if [ "$age" -gt "$QUIET_INBOUND_SEC" ]; then
    REASON="no inbound FIX traffic for ${age}s"; return 1
  fi
  return 0
}

check_deploy_unconfirmed(){
  # deploy.sh writes this when a release installed but serving could not be
  # confirmed (no Logon ACK). Advisory: the binary is on disk and may well be
  # fine -- the check itself has produced false negatives -- but "installed"
  # and "serving" are different claims and the difference must be visible
  # somewhere a human looks, not only in journald.
  local m="$LIVE/.deploy-unconfirmed"
  if [ -f "$m" ]; then
    log "WARN last deploy installed but serving was NOT CONFIRMED ($m, $(date -u -r "$m" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null)) -- verify logons, then: rm $m"
  fi
  return 0
}

check_expected_apps(){
  # On 2026-09-30 an apt-triggered `pm2 resurrect` silently dropped
  # fix-server-prod: it had been started with `pm2 start` but never `pm2 save`,
  # so the dump did not contain it. Production market-data capture was dead for
  # two days and nothing said so -- this check only watched fix-server.
  # An app that is supposed to be running and is ABSENT is the failure that
  # hides best, because there is no process to look unhealthy.
  local want app
  want=$("$PYTHON_BIN" -c '
import json,re,sys
try: src=open("/home/ubuntu/ecosystem.config.js").read()
except Exception: sys.exit(0)
print(" ".join(re.findall(r"name:\s*\"([^\"]+)\"", src)))
' 2>/dev/null || true)
  [ -z "${want:-}" ] && return 0
  local running
  running=$(pm2 jlist 2>/dev/null | "$PYTHON_BIN" -c '
import json,sys
try: print(" ".join(a["name"] for a in json.load(sys.stdin)))
except Exception: pass
' 2>/dev/null || true)
  for app in $want; do
    case " $running " in
      *" $app "*) ;;
      *) log "WARN pm2 app '"'"'$app'"'"' is in ecosystem.config.js but NOT running -- it may have been lost by a resurrect; check and run: pm2 save" ;;
    esac
  done
  # And the same drift check for the dump, per app.
  for app in $want; do
    if ! grep -q "$app" /home/ubuntu/.pm2/dump.pm2 2>/dev/null; then
      log "WARN pm2 app '"'"'$app'"'"' is running but NOT in the saved dump -- a daemon restart would lose it; run: pm2 save"
    fi
  done
  return 0
}

check_dump_drift(){
  # The 2026-09-23 root cause: the saved pm2 dump lacked env vars the ecosystem
  # file defines, so any daemon restart silently dropped them. Warn BEFORE it
  # bites -- this is advisory, it does not mark the box unhealthy.
  local want got
  want=$(grep -oE '^[A-Za-z_][A-Za-z0-9_]*' "$ENVF" 2>/dev/null | sort -u || true)
  [ -z "$want" ] && return 0
  for v in $want; do
    if ! grep -q "$v" /home/ubuntu/.pm2/dump.pm2 2>/dev/null; then
      log "WARN pm2 dump is missing $v -- a daemon restart would lose it; run: pm2 save"
    fi
  done
  return 0
}

check_books_two_sided(){
  # Is PRODUCTION actually capturing two-sided market data?
  #
  # Scoped to production deliberately. UAT exists to test connectivity only --
  # Crypto.com confirmed on 2026-09-27 that quoting is not expected there -- so
  # a one-sided UAT book is correct, and warning about it every minute trained
  # the eye to ignore this check entirely. A warning that is always on is not a
  # warning.
  #
  # Asks the production kdb+ (5002) rather than an HTTP endpoint: the
  # production instance has no order-management session, so by design it binds
  # no web server. This also measures the thing that actually matters for
  # Stage 2 -- that two-sided data is being PERSISTED, not merely held in a
  # book that no one reads.
  #
  # Advisory, never a heal: a venue that stops quoting is not something a
  # restart fixes.
  local q=/home/ubuntu/q/l64/q
  [ -x "$q" ] || return 0
  [ -f /home/ubuntu/fix_server_prod/config.toml ] || return 0   # no prod instance here
  local tmp out
  tmp=$(mktemp /tmp/hft-health-sides.XXXXXX.q) || return 0
  cat > "$tmp" <<'QEOF'
h: hopen `::5002;
show h "select sides: count distinct side, n: count i by symbol from orderbook_levels where time > .z.p - 00:05:00";
hclose h; exit 0;
QEOF
  out=$(timeout 20 "$q" "$tmp" 2>/dev/null)
  rm -f "$tmp"
  if [ -z "${out:-}" ]; then
    log "WARN production kdb+ (5002) did not answer -- cannot confirm capture"
    return 0
  fi
  # Rows look like: BTCUSD-PERP| 2     25325
  local any=0 bad=0 line sym sides
  while IFS= read -r line; do
    case "$line" in
      *"|"*) ;;
      *) continue ;;
    esac
    sym=$(printf '%s' "$line" | cut -d'|' -f1 | tr -d ' ')
    sides=$(printf '%s' "$line" | cut -d'|' -f2 | awk '{print $1}')
    case "$sym" in symbol|''|-*) continue ;; esac
    case "$sides" in ''|*[!0-9]*) continue ;; esac
    any=1
    if [ "$sides" -lt 2 ]; then
      bad=1
      log "WARN production capture for ${sym} is ONE-SIDED in the last 5 min (distinct sides=${sides})"
    fi
  done <<EOF
$out
EOF
  if [ "$any" = "0" ]; then
    log "WARN production kdb+ has no orderbook_levels rows in the last 5 min -- capture may have stopped"
  elif [ "$bad" = "0" ]; then
    : # all symbols two-sided; stay quiet
  fi
  return 0
}

heal_budget_ok(){
  local now cutoff n
  now=$(date +%s); cutoff=$(( now - 3600 ))
  touch "$HEALS"
  n=$(awk -v c="$cutoff" '$1 > c' "$HEALS" | wc -l)
  [ "$n" -lt "$MAX_HEALS_PER_HOUR" ]
}

heal(){
  log "HEAL attempting reload from $ECO (reason: $REASON)"
  date +%s >> "$HEALS"
  : > "$LIVE/startup.log" 2>/dev/null || true
  pm2 reload "$ECO" --only fix-server --update-env >>"$LOG" 2>&1 \
    || pm2 start "$ECO" --only fix-server --update-env >>"$LOG" 2>&1 \
    || { log "HEAL FAILED: pm2 could not start fix-server"; return 1; }
  sleep 20
  if grep -qi "FATAL" "$LIVE/startup.log" 2>/dev/null; then
    log "HEAL FAILED: still fatal -- $(grep -i FATAL "$LIVE/startup.log" | tail -1 | sed 's/.*FATAL: //' | cut -c1-120)"
    return 1
  fi
  # Persist, so the next apt-triggered resurrect does not repeat 2026-09-23.
  pm2 save >>"$LOG" 2>&1 || log "WARN pm2 save failed after heal"
  log "HEAL OK -- fix-server restarted and pm2 state saved"
  return 0
}

# ── Run ──────────────────────────────────────────────────────────────────────
PID=""; UPTIME_SEC=0
HEALTHY=1
for c in check_process check_startup_fatal check_not_flapping check_inbound_recent; do
  if ! $c; then HEALTHY=0; break; fi
done
check_dump_drift
check_deploy_unconfirmed
check_expected_apps
check_books_two_sided

PREV=$(cat "$STATE" 2>/dev/null | head -1 || echo unknown)

if [ "$HEALTHY" = 1 ]; then
  printf 'ok\n%s\n' "$(ts)" > "$STATE"
  [ "$PREV" != "ok" ] && log "RECOVERED -- fix-server healthy (pid $PID, up ${UPTIME_SEC}s)"
  exit 0
fi

printf 'unhealthy\n%s\n%s\n' "$(ts)" "$REASON" > "$STATE"
log "UNHEALTHY: $REASON"

if heal_budget_ok; then
  if heal; then
    printf 'ok\n%s\nrecovered by self-heal from: %s\n' "$(ts)" "$REASON" > "$STATE"
    exit 0
  fi
  log "ALERT fix-server is DOWN and self-heal did not fix it: $REASON"
  exit 1
else
  log "ALERT fix-server is DOWN; self-heal budget spent (>${MAX_HEALS_PER_HOUR}/h) -- NOT restarting again: $REASON"
  exit 1
fi
