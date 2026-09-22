#!/usr/bin/env bash
# rollback-drill.sh — UAT-only rehearsal that drives real deploy.sh and asserts
# rollback restores serving. Dual env is child-only; this parent never exports
# ROLLBACK_DRILL or HFT_DRILL_I_AM_ON_UAT.
set -euo pipefail
umask 077

HERE="$(cd "$(dirname "$0")" && pwd)"
CWD="${CWD:-/home/ubuntu/fix_server}"
DEPLOY="${DEPLOY:-$HERE/deploy.sh}"
LIB="${LIB:-$HERE/deploy-lib.sh}"
PARSER="${PARSER:-$HERE/rollback-drill-parse-sessions.py}"
FIXTURE="${FIXTURE:-$HERE/crash-loop-fix_server.sh}"
EXAMPLE_CONFIG="${EXAMPLE_CONFIG:-$HERE/config.example.toml}"
LOCK_PATH="${LOCK_PATH:-/home/ubuntu/rollback-drill.lock}"
RESULTS_DIR="${RESULTS_DIR:-/home/ubuntu/rollback-drill-results}"
HTTP_BASE="${HTTP_BASE:-http://127.0.0.1:8080}"
HTTP_ORDERBOOK_PATH="${HTTP_ORDERBOOK_PATH:-/api/orderbook/BTC_USDT}"
BOOK_SYMBOL="${BOOK_SYMBOL:-BTC_USDT}"
HTTP_GAP_S="${HTTP_GAP_S:-5}"
HTTP_BIND_WAIT_S="${HTTP_BIND_WAIT_S:-30}"
LOGON_GREP="${LOGON_GREP:-Logon ACK (35=A) received}"
BOOK_GREP="${BOOK_GREP:-📊 Order Book:}"
FATAL_GREP_ERE="${FATAL_GREP_ERE:-FATAL: (could not load config\\.toml|config validation failed)}"
POLLER_TIMER="${POLLER_TIMER:-hft-deploy-poll.timer}"
POLLER_SERVICE="${POLLER_SERVICE:-hft-deploy-poll.service}"
CRASH_LOOP_SHA256="${CRASH_LOOP_SHA256:-97d063bab8e2714dbf5220adc6f03b67a40c466ff1de00e9d43fa3cf8e8090c3}"
PM2_APP="${PM2_APP:-fix_server}"
SWAP_TIMEOUT_S="${SWAP_TIMEOUT_S:-180}"
TIMEOUT_KILL_AFTER_S="${TIMEOUT_KILL_AFTER_S:-10}"
SUITE_DEADLINE_S="${SUITE_DEADLINE_S:-900}"
BOOK_OBS_S="${BOOK_OBS_S:-15}"
BOOK_RETRY_SLEEP_S="${BOOK_RETRY_SLEEP_S:-15}"
LOGON_ASSERT_WAIT_S="${LOGON_ASSERT_WAIT_S:-60}"
CRASH_SAMPLE_GAP_S="${CRASH_SAMPLE_GAP_S:-10}"
MIN_SWAP_TIMEOUT_S="${MIN_SWAP_TIMEOUT_S:-110}"
PYTHON="${PYTHON:-/usr/bin/python3}"
KDB_PORT="${KDB_PORT:-5001}"
ROLLBACK_LOUD_EXIT="${ROLLBACK_LOUD_EXIT:-3}"
LOUD_FAIL_MARKER="${LOUD_FAIL_MARKER:-LOUD_FAIL_MISSING_PREV}"

# Slot 1 — fill with `hostname -s` and `hostname -f` of the UAT box.
# Empty is fail-closed (identity_allowlist_empty). Not read from env.
UAT_HOST_ALLOWLIST=()
UAT_COMPID_CONTAINS="${UAT_COMPID_CONTAINS:-UAT}"
UAT_FIX_HOST_ALLOWLIST=("fix-uat.crypto.com")
UAT_FIX_HOST_DENY=("vpce-07bb726a65816b33f-2nhnv7ig.vpce-svc-0493a3dfb1a2d0d6a.ap-northeast-1.vpce.amazonaws.com")
UAT_CREDENTIALS_REF_ALLOWLIST=("UAT")
CREDENTIALS_REF_DENY=("PRD3")
I3B_PATH="${I3B_PATH:-}"
# Slot 8 — live .prev path. Empty → $HOME/.fix_server.prev (default only if live has none).
PREV_PATH="${PREV_PATH:-}"
if [[ -z "$PREV_PATH" ]]; then
  PREV_PATH="${HOME}/.fix_server.prev"
fi

# Filled from this tree's health_check (live import was not on the builder).
CRASH_LOOP_LOG_GREP="${CRASH_LOOP_LOG_GREP:-crash-loop detected}"
NO_LOGON_WARNING_GREP="${NO_LOGON_WARNING_GREP:-WARNING: no Logon ACK}"

DRY_RUN=0
HELP=0
CASE_SET=()
LOCK_FD=""
DID_LOCK=0
DID_PAUSE=0
STASH=""
RUN_ID=""
JSONL=""
DRILL_LOG=""
T_LAST_GOOD=""
SUITE_T0=""
WORST_EXIT=0
INJECTED=0

print_help() {
  cat <<'EOF'
rollback-drill.sh — UAT-only rehearsal that drives real deploy.sh and asserts rollback restores serving.

USAGE
  rollback-drill.sh                      run cases 1-4 (operator default)
  rollback-drill.sh --all                alias of no-args
  rollback-drill.sh --dry-run            plan cases 1-4; no pause, no lock, no snapshot
  rollback-drill.sh --dry-run --case N   plan one case
  rollback-drill.sh --case N             rerun one case after a fail
  rollback-drill.sh --help

  optional on mutating runs: --timeout-s 180   (default 180)

CASES
  1  empty sessions default (sessions = []; HFT-042). Not a parse-fail. Not kdb FATAL.
  2  crash-loop wrapper (crash-loop-fix_server.sh). pm2 autorestart=false is a box blocker.
  3  unreachable host 127.0.0.1:1. deploy.sh must NOT auto-rollback; the drill restores.
  4  relocate .prev between snapshot and swap; deploy.sh loud-fail if live is silent.
     Skip if deploy.sh has no LOUD_FAIL_MISSING_PREV marker (PR3 not shipped).

EXITS
  0   pass (or Case 4 expected-skip)
  1   case assertion failed
  2   preflight / usage
  124 hang/timeout after emergency_restore (not a Case 4 pass)

UAT only. The script refuses non-UAT hosts. Do not export ROLLBACK_DRILL —
the drill sets it on the deploy.sh child; the poller unsets it.
Does not pause, lock, or snapshot under --dry-run.
EOF
}

say() {
  printf '%s\n' "$*"
  if [[ -n "${DRILL_LOG:-}" ]]; then
    printf '%s\n' "$*" >>"$DRILL_LOG"
  fi
}

err() {
  printf '%s\n' "$*" >&2
  if [[ -n "${DRILL_LOG:-}" ]]; then
    printf '%s\n' "$*" >>"$DRILL_LOG"
  fi
}

die2() {
  err "$*"
  exit 2
}

now_ms() {
  "$PYTHON" -c 'import time; print(int(time.monotonic()*1000))'
}

rfc3339() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

parse_args() {
  local saw_case=0 saw_all=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --help|-h)
        HELP=1
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --all)
        saw_all=1
        shift
        ;;
      --case)
        if [[ $# -lt 2 || ! "$2" =~ ^[1-4]$ ]]; then
          die2 "ERROR: --case needs N in {1,2,3,4}"
        fi
        saw_case=1
        CASE_SET+=("$2")
        shift 2
        ;;
      --case=*)
        local n=${1#--case=}
        if [[ ! "$n" =~ ^[1-4]$ ]]; then
          die2 "ERROR: --case needs N in {1,2,3,4}"
        fi
        saw_case=1
        CASE_SET+=("$n")
        shift
        ;;
      --timeout-s)
        if [[ $# -lt 2 || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
          die2 "ERROR: --timeout-s needs a positive integer (got ${2:-})"
        fi
        SWAP_TIMEOUT_S=$2
        shift 2
        ;;
      --timeout-s=*)
        local t=${1#--timeout-s=}
        if [[ ! "$t" =~ ^[1-9][0-9]*$ ]]; then
          die2 "ERROR: --timeout-s needs a positive integer (got ${t})"
        fi
        SWAP_TIMEOUT_S=$t
        shift
        ;;
      --staged|--staged=*)
        die2 "ERROR: unknown flag: $1"
        ;;
      -*)
        die2 "ERROR: unknown flag: $1"
        ;;
      *)
        die2 "ERROR: unknown flag: $1"
        ;;
    esac
  done
  if [[ "$saw_case" -eq 1 && "$saw_all" -eq 1 ]]; then
    die2 "ERROR: use no-args (or --all) or --case N, not both"
  fi
  if [[ "$HELP" -eq 1 ]]; then
    print_help
    exit 0
  fi
  if [[ "${#CASE_SET[@]}" -eq 0 ]]; then
    CASE_SET=(1 2 3 4)
  fi
  if [[ "$SWAP_TIMEOUT_S" -lt "$MIN_SWAP_TIMEOUT_S" ]]; then
    die2 "ERROR: --timeout-s needs a positive integer (got ${SWAP_TIMEOUT_S})"
  fi
}

case_in_set() {
  local n=$1 c
  for c in "${CASE_SET[@]}"; do
    if [[ "$c" == "$n" ]]; then
      return 0
    fi
  done
  return 1
}

sha256_file() {
  sha256sum "$1" | awk '{print $1}'
}

hashes_match_known_good() {
  [[ -f "$STASH/known-good.sha256" ]] || return 1
  local want_bin want_cfg got_bin got_cfg
  want_bin=$(awk 'NR==1{print $1}' "$STASH/known-good.sha256")
  want_cfg=$(awk 'NR==2{print $1}' "$STASH/known-good.sha256")
  [[ -f "${CWD}/fix_server" && -f "${CWD}/config.toml" ]] || return 1
  got_bin=$(sha256_file "${CWD}/fix_server")
  got_cfg=$(sha256_file "${CWD}/config.toml")
  [[ "$got_bin" == "$want_bin" && "$got_cfg" == "$want_cfg" ]]
}

pm2_jlist_app() {
  local field=${1:-}
  pm2 jlist 2>/dev/null | "$PYTHON" -c '
import json, sys
name = sys.argv[1]
field = sys.argv[2] if len(sys.argv) > 2 else ""
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if not isinstance(data, list):
    sys.exit(0)
for p in data:
    if p.get("name") == name:
        env = p.get("pm2_env") or {}
        if not field:
            print(json.dumps({
                "status": env.get("status"),
                "restart_time": env.get("restart_time"),
                "pm_id": p.get("pm_id"),
                "autorestart": env.get("autorestart"),
                "pm_out_log_path": env.get("pm_out_log_path"),
                "pm_err_log_path": env.get("pm_err_log_path"),
                "exec_interpreter": env.get("exec_interpreter"),
            }))
        else:
            v = env.get(field, p.get(field, ""))
            if v is None:
                v = ""
            print(v)
        break
' "$PM2_APP" "$field"
}

pm2_log_path() {
  local which=$1
  if [[ "$which" == "out" && -n "${PM2_OUT_LOG:-}" ]]; then
    printf '%s\n' "$PM2_OUT_LOG"
    return 0
  fi
  if [[ "$which" == "err" && -n "${PM2_ERR_LOG:-}" ]]; then
    printf '%s\n' "$PM2_ERR_LOG"
    return 0
  fi
  if [[ "$which" == "out" ]]; then
    pm2_jlist_app pm_out_log_path
  else
    pm2_jlist_app pm_err_log_path
  fi
}

take_log_cursor() {
  local dest=${1:-$STASH/.rollback-drill-reload.cursor}
  local out err startup os es ss mono
  out=$(pm2_log_path out)
  err=$(pm2_log_path err)
  startup="${CWD}/startup.log"
  os=0 es=0 ss=0
  [[ -n "$out" && -f "$out" ]] && os=$(stat -c %s "$out" 2>/dev/null || echo 0)
  [[ -n "$err" && -f "$err" ]] && es=$(stat -c %s "$err" 2>/dev/null || echo 0)
  [[ -f "$startup" ]] && ss=$(stat -c %s "$startup" 2>/dev/null || echo 0)
  mono=$(now_ms)
  mkdir -p "$(dirname "$dest")"
  printf 'out=%s err=%s startup=%s mono_ms=%s\n' "$os" "$es" "$ss" "$mono" >"$dest"
  chmod 0600 "$dest"
  if [[ -d "$CWD" ]]; then
    printf 'out=%s err=%s startup=%s mono_ms=%s\n' "$os" "$es" "$ss" "$mono" \
      >"${CWD}/.rollback-drill-reload.cursor" 2>/dev/null || true
    chmod 0600 "${CWD}/.rollback-drill-reload.cursor" 2>/dev/null || true
  fi
}

read_cursor() {
  local f=$STASH/.rollback-drill-reload.cursor
  if [[ -f "$f" ]]; then
    cat "$f"
  elif [[ -f "${CWD}/.rollback-drill-reload.cursor" ]]; then
    cat "${CWD}/.rollback-drill-reload.cursor"
  else
    echo "out=0 err=0 startup=0 mono_ms=0"
  fi
}

cursor_field() {
  local line=$1 key=$2
  local v
  if [[ "$line" == *"${key}="* ]]; then
    v=${line#*"${key}="}
    v=${v%% *}
    printf '%s\n' "${v:-0}"
    return 0
  fi
  # legacy two-integer "out err"
  if [[ "$key" == "out" ]]; then
    printf '%s\n' "${line%% *}"
  elif [[ "$key" == "err" ]]; then
    v=${line#* }
    printf '%s\n' "${v%% *}"
  else
    echo 0
  fi
}

_grep_after() {
  local file=$1 offset=$2 pattern=$3
  [[ -f "$file" ]] || return 1
  local sz
  sz=$(stat -c %s "$file" 2>/dev/null || echo 0)
  if [[ "$sz" -le "$offset" ]]; then
    return 1
  fi
  tail -c +"$((offset + 1))" "$file" | grep -F -q -- "$pattern"
}

http_get_orderbook() {
  curl -fsS --max-time 2 "${HTTP_BASE}${HTTP_ORDERBOOK_PATH}" 2>/dev/null || true
}

http_bid_ask_ok() {
  local body=$1
  [[ -n "$body" ]] || return 1
  printf '%s' "$body" | "$PYTHON" -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
bid = d.get("best_bid")
ask = d.get("best_ask")
try:
    ok = float(bid) > 0 and float(ask) > 0
except (TypeError, ValueError):
    sys.exit(1)
sys.exit(0 if ok else 1)
'
}

assert_serving_already_up() {
  local a b
  a=$(http_get_orderbook)
  if ! http_bid_ask_ok "$a"; then
    return 1
  fi
  sleep "$HTTP_GAP_S"
  b=$(http_get_orderbook)
  if ! http_bid_ask_ok "$b"; then
    return 1
  fi
  return 0
}

assert_logon() {
  local cursor=$1
  local waited=0
  local out err co ce
  out=$(pm2_log_path out)
  err=$(pm2_log_path err)
  co=$(cursor_field "$cursor" out)
  ce=$(cursor_field "$cursor" err)
  co=${co:-0}
  ce=${ce:-0}
  while [[ "$waited" -lt "$LOGON_ASSERT_WAIT_S" ]]; do
    if [[ -n "$out" ]] && _grep_after "$out" "$co" "$LOGON_GREP"; then
      return 0
    fi
    if [[ -n "$err" ]] && _grep_after "$err" "$ce" "$LOGON_GREP"; then
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  return 1
}

assert_book_flow() {
  local cursor=$1
  local out err co ce
  out=$(pm2_log_path out)
  err=$(pm2_log_path err)
  co=$(cursor_field "$cursor" out)
  ce=$(cursor_field "$cursor" err)
  local attempt=0
  while [[ "$attempt" -lt 2 ]]; do
    if [[ -n "$out" ]] && _grep_after "$out" "$co" "$BOOK_GREP"; then
      return 0
    fi
    if [[ -n "$err" ]] && _grep_after "$err" "$ce" "$BOOK_GREP"; then
      return 0
    fi
    # HTTP supplement when :8080 is bound
    local body
    body=$(http_get_orderbook)
    if http_bid_ask_ok "$body"; then
      return 0
    fi
    attempt=$((attempt + 1))
    if [[ "$attempt" -lt 2 ]]; then
      sleep "$BOOK_RETRY_SLEEP_S"
    fi
  done
  sleep "$BOOK_OBS_S"
  if [[ -n "$out" ]] && _grep_after "$out" "$co" "$BOOK_GREP"; then
    return 0
  fi
  return 1
}

assert_serving() {
  local cursor=$1
  local t1
  if ! assert_logon "$cursor"; then
    return 1
  fi
  local t_logon
  t_logon=$(now_ms)
  if ! assert_book_flow "$cursor"; then
    return 1
  fi
  t1=$(now_ms)
  SERVING_LOGON_MS=$t_logon
  SERVING_CONFIRMED_MS=$t1
  return 0
}

identity_from_config() {
  local path=$1
  "$PYTHON" "$PARSER" identity "$path"
}

identity_ok_text() {
  local json=$1
  printf '%s' "$json" | "$PYTHON" -c '
import json, sys, os
j = json.load(sys.stdin)
n = int(j.get("n_sessions") or 0)
if n == 0:
    sys.stderr.write("identity_leftover_case1\n")
    sys.exit(3)
comp = os.environ.get("UAT_COMPID_CONTAINS", "UAT")
for s in j.get("sender_comp_ids") or []:
    if comp not in s:
        sys.stderr.write("identity_compid\n")
        sys.exit(2)
allow = [x for x in os.environ.get("FIX_ALLOW", "").split("\n") if x]
deny = [x for x in os.environ.get("FIX_DENY", "").split("\n") if x]
cred_allow = [x for x in os.environ.get("CRED_ALLOW", "").split("\n") if x]
cred_deny = [x for x in os.environ.get("CRED_DENY", "").split("\n") if x]
for h in j.get("hosts") or []:
    if h in deny:
        sys.stderr.write("identity_fix_host_denied\n")
        sys.exit(2)
    if allow and h not in allow:
        sys.stderr.write("identity_fix_host\n")
        sys.exit(2)
for c in j.get("credentials_refs") or []:
    if c in cred_deny:
        sys.stderr.write("identity_fix_host_denied\n")
        sys.exit(2)
    if cred_allow and c not in cred_allow:
        sys.stderr.write("identity_fix_host\n")
        sys.exit(2)
sys.exit(0)
'
}

check_i1() {
  if [[ "${#UAT_HOST_ALLOWLIST[@]}" -eq 0 ]]; then
    err "ERROR: preflight: host $(hostname -s) is not the UAT box — rollback-drill is UAT only"
    return 1
  fi
  local hs hf h
  hs=$(hostname -s 2>/dev/null || true)
  hf=$(hostname -f 2>/dev/null || true)
  for h in "${UAT_HOST_ALLOWLIST[@]}"; do
    if [[ -n "$h" && ( "$hs" == "$h" || "$hf" == "$h" ) ]]; then
      return 0
    fi
  done
  err "ERROR: preflight: host ${hs} is not the UAT box — rollback-drill is UAT only"
  return 1
}

check_i2_i3() {
  local cfg=$1
  [[ -f "$cfg" ]] || return 1
  local json
  json=$(identity_from_config "$cfg")
  local rc=0
  FIX_ALLOW=$(printf '%s\n' "${UAT_FIX_HOST_ALLOWLIST[@]}")
  FIX_DENY=$(printf '%s\n' "${UAT_FIX_HOST_DENY[@]}")
  CRED_ALLOW=$(printf '%s\n' "${UAT_CREDENTIALS_REF_ALLOWLIST[@]}")
  CRED_DENY=$(printf '%s\n' "${CREDENTIALS_REF_DENY[@]}")
  export FIX_ALLOW FIX_DENY CRED_ALLOW CRED_DENY UAT_COMPID_CONTAINS
  set +e
  identity_ok_text "$json"
  rc=$?
  set -e
  if [[ "$rc" -eq 3 ]]; then
    err "ERROR: preflight: leftover empty sessions (identity_leftover_case1)"
    return 1
  fi
  if [[ "$rc" -ne 0 ]]; then
    err "ERROR: preflight: host $(hostname -s) is not the UAT box — rollback-drill is UAT only"
    return 1
  fi
  if [[ -n "$I3B_PATH" && -f "$I3B_PATH" ]]; then
    if ! grep -q . "$I3B_PATH"; then
      err "ERROR: preflight: host $(hostname -s) is not the UAT box — rollback-drill is UAT only"
      return 1
    fi
  fi
  return 0
}

tool_missing() {
  local t
  for t in bash flock mktemp curl ss systemctl pm2 sha256sum timeout grep mv install chmod id "$PYTHON"; do
    if ! command -v "$t" >/dev/null 2>&1; then
      if [[ "$t" == "$PYTHON" && -x "$PYTHON" ]]; then
        continue
      fi
      echo "$t"
      return 0
    fi
  done
  return 1
}

kdb_up() {
  ss -ltn 2>/dev/null | grep -F "127.0.0.1:${KDB_PORT}" >/dev/null 2>&1
}

poller_is_active() {
  systemctl is-active --quiet "$POLLER_TIMER" 2>/dev/null \
    || systemctl is-active --quiet "$POLLER_SERVICE" 2>/dev/null
}

pause_poller() {
  say "✅ rollback-drill: pausing ${POLLER_TIMER} and ${POLLER_SERVICE}"
  if ! systemctl stop "$POLLER_TIMER" "$POLLER_SERVICE" 2>/dev/null; then
    err "ERROR: preflight: ${POLLER_TIMER}/service still running — refuse to inject a fault while the poller can deploy"
    return 1
  fi
  DID_PAUSE=1
  return 0
}

resume_poller() {
  if [[ "$DID_PAUSE" -ne 1 ]]; then
    return 0
  fi
  if ! hashes_match_known_good; then
    err "ERROR: poller left stopped"
    return 1
  fi
  if systemctl start "$POLLER_TIMER" 2>/dev/null; then
    say "✅ rollback-drill: poller resumed"
    DID_PAUSE=0
    return 0
  fi
  err "ERROR: poller left stopped"
  return 1
}

lock_probe() {
  if [[ ! -e "$LOCK_PATH" ]]; then
    return 0
  fi
  local pid
  pid=$(tr -d ' \n' <"$LOCK_PATH" 2>/dev/null || true)
  if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
    say "⚠️  rollback-drill: removing stale lock (pid=${pid:-unknown} not running)"
    rm -f "$LOCK_PATH"
    return 0
  fi
  if [[ -d "/proc/$pid" ]] && grep -F -q rollback-drill "/proc/$pid/cmdline" 2>/dev/null; then
    err "ERROR: lock held by pid=${pid} file=${LOCK_PATH} — another drill is running"
    return 1
  fi
  say "⚠️  rollback-drill: removing stale lock (pid=${pid} not running)"
  rm -f "$LOCK_PATH"
  return 0
}

acquire_lock() {
  exec {LOCK_FD}<>"$LOCK_PATH"
  chmod 0600 "$LOCK_PATH"
  if ! flock -n "$LOCK_FD"; then
    local pid
    pid=$(tr -d ' \n' <"$LOCK_PATH" 2>/dev/null || echo unknown)
    err "ERROR: lock held by pid=${pid} file=${LOCK_PATH} — another drill is running"
    return 1
  fi
  printf '%s\n' "$$" >"$LOCK_PATH"
  DID_LOCK=1
  say "✅ rollback-drill: lock acquired file=${LOCK_PATH}"
  return 0
}

release_lock() {
  if [[ "$DID_LOCK" -eq 1 ]]; then
    if [[ -n "${LOCK_FD}" ]]; then
      flock -u "$LOCK_FD" 2>/dev/null || true
      exec {LOCK_FD}>&- 2>/dev/null || true
    fi
    rm -f "$LOCK_PATH"
    DID_LOCK=0
    say "✅ rollback-drill: lock released"
  fi
}

copy_known_good() {
  mkdir -p "$STASH/known-good"
  chmod 0700 "$STASH/known-good"
  cp -a "${CWD}/fix_server" "$STASH/known-good/fix_server"
  chmod 0755 "$STASH/known-good/fix_server"
  cp -a "${CWD}/config.toml" "$STASH/known-good/config.toml"
  chmod 0600 "$STASH/known-good/config.toml"
  {
    sha256_file "$STASH/known-good/fix_server"
    sha256_file "$STASH/known-good/config.toml"
  } >"$STASH/known-good.sha256"
  chmod 0600 "$STASH/known-good.sha256"
}

stage_dir() {
  mkdir -p "$STASH/staged"
  chmod 0700 "$STASH/staged"
  cp -a "$STASH/known-good/fix_server" "$STASH/staged/fix_server"
  cp -a "$STASH/known-good/config.toml" "$STASH/staged/config.toml"
  chmod 0755 "$STASH/staged/fix_server"
  chmod 0600 "$STASH/staged/config.toml"
}

manual_restore() {
  take_log_cursor "$STASH/.rollback-drill-reload.cursor"
  cp -a "$STASH/known-good/fix_server" "${CWD}/fix_server"
  cp -a "$STASH/known-good/config.toml" "${CWD}/config.toml"
  chmod 0755 "${CWD}/fix_server"
  chmod 0600 "${CWD}/config.toml"
  pm2 reload "$PM2_APP"
}

inverse_restore_prev() {
  local dest="$STASH/fix_server.prev"
  [[ -n "${STASH:-}" && -d "$dest" ]] || return 0
  if [[ -d "$dest/dir" ]]; then
    if [[ -e "${HOME}/.fix_server.prev" ]]; then
      echo "ERROR: inverse_restore dest exists" >&2
      return 1
    fi
    mv "$dest/dir" "${HOME}/.fix_server.prev"
    return 0
  fi
  local f
  for f in "$dest"/.fix_server.prev "$dest"/.fix_server.prev.tar "$dest"/.fix_server.prev.tgz "$dest"/.fix_server.prev.config; do
    [[ -e "$f" ]] || continue
    local base
    base=$(basename "$f")
    if [[ -e "${HOME}/${base}" ]]; then
      echo "ERROR: inverse_restore dest exists" >&2
      return 1
    fi
    mv "$f" "${HOME}/${base}"
  done
  return 0
}

emergency_restore() {
  inverse_restore_prev || true
  if [[ -d "$STASH/known-good" ]]; then
    say "✅ rollback-drill: emergency_restore of known-good bin+config pair"
    take_log_cursor "$STASH/.rollback-drill-reload.cursor" || true
    cp -a "$STASH/known-good/fix_server" "${CWD}/fix_server" 2>/dev/null || true
    cp -a "$STASH/known-good/config.toml" "${CWD}/config.toml" 2>/dev/null || true
    pm2 reload "$PM2_APP" 2>/dev/null || true
  fi
}

on_int() {
  emergency_restore || true
  resume_poller || true
  release_lock
  [[ -n "$STASH" && -d "$STASH" ]] && rm -rf "$STASH"
  exit 130
}

on_term() {
  emergency_restore || true
  resume_poller || true
  release_lock
  [[ -n "$STASH" && -d "$STASH" ]] && rm -rf "$STASH"
  exit 143
}

FINISHING=0
on_exit() {
  local rc=$?
  if [[ "$DRY_RUN" -eq 1 || "$FINISHING" -eq 1 ]]; then
    return 0
  fi
  FINISHING=1
  if [[ "$INJECTED" -eq 1 ]]; then
    emergency_restore || true
  fi
  if [[ "$DID_PAUSE" -eq 1 ]]; then
    resume_poller || true
  fi
  release_lock
  if [[ -n "$STASH" && -d "$STASH" ]]; then
    rm -rf "$STASH"
  fi
  return "$rc"
}

emit_suite_start() {
  local ts cases_json
  ts=$(rfc3339)
  cases_json=$("$PYTHON" -c 'import json,sys; print(json.dumps([int(x) for x in sys.argv[1:]]))' "${CASE_SET[@]}")
  "$PYTHON" -c 'import json,sys; print(json.dumps({"ts":sys.argv[1],"run_id":sys.argv[2],"event":"suite_start","cases":json.loads(sys.argv[3]),"timeout_s":int(sys.argv[4])}, separators=(",", ":")))' \
    "$ts" "$RUN_ID" "$cases_json" "$SWAP_TIMEOUT_S" >>"$JSONL"
}

emit_case_jsonl() {
  # env: JL_*
  "$PYTHON" - <<'PY' >>"$JSONL"
import json, os
def ni(k):
    v = os.environ.get(k, "")
    if v == "" or v == "null":
        return None
    try:
        return int(v)
    except ValueError:
        return v
def nb(k):
    return os.environ.get(k, "") == "true"
obj = {
    "ts": os.environ.get("JL_TS", ""),
    "run_id": os.environ.get("JL_RUN", ""),
    "host_short": os.environ.get("JL_HOST", ""),
    "case": ni("JL_CASE"),
    "name": os.environ.get("JL_NAME", ""),
    "result": os.environ.get("JL_RESULT", ""),
    "reason": os.environ.get("JL_REASON", ""),
    "downtime_ms": ni("JL_DOWN"),
    "serving_confirmed_ms": ni("JL_SERV"),
    "auto_rollback": nb("JL_AUTO"),
    "deploy_exit": ni("JL_DEXIT"),
    "fatal_seen": nb("JL_FATAL"),
    "logon_seen_after": nb("JL_LOGON"),
    "book_flow_seen_after": nb("JL_BOOK"),
    "known_good_bin_sha256": os.environ.get("JL_KG_BIN", ""),
    "known_good_cfg_sha256": os.environ.get("JL_KG_CFG", ""),
    "running_bin_sha256_after": os.environ.get("JL_RB_BIN", ""),
    "running_cfg_sha256_after": os.environ.get("JL_RB_CFG", ""),
    "notes": os.environ.get("JL_NOTES", ""),
    "swap_timeout_s": ni("JL_TMO"),
    "pm2_status_during": os.environ.get("JL_PM2", ""),
    "logon_cursor": os.environ.get("JL_CUR", ""),
    "logon_cursor_err": os.environ.get("JL_CURE", ""),
    "loud_fail_seen": nb("JL_LOUD"),
    "identity_ok": nb("JL_IDOK"),
}
print(json.dumps(obj, separators=(",", ":")))
PY
}

record_case() {
  local case_n=$1 name=$2 result=$3 reason=$4 auto=$5 dexit=$6 down=$7 serv=$8 notes=${9:-}
  local kg_bin kg_cfg rb_bin rb_cfg
  kg_bin=$(awk 'NR==1{print $1}' "$STASH/known-good.sha256" 2>/dev/null || true)
  kg_cfg=$(awk 'NR==2{print $1}' "$STASH/known-good.sha256" 2>/dev/null || true)
  rb_bin=""; rb_cfg=""
  [[ -f "${CWD}/fix_server" ]] && rb_bin=$(sha256_file "${CWD}/fix_server")
  [[ -f "${CWD}/config.toml" ]] && rb_cfg=$(sha256_file "${CWD}/config.toml")
  local cur
  cur=$(read_cursor)
  export JL_TS JL_RUN JL_HOST JL_CASE JL_NAME JL_RESULT JL_REASON JL_DOWN JL_SERV JL_AUTO JL_DEXIT
  export JL_FATAL JL_LOGON JL_BOOK JL_KG_BIN JL_KG_CFG JL_RB_BIN JL_RB_CFG JL_NOTES JL_TMO JL_PM2
  export JL_CUR JL_CURE JL_LOUD JL_IDOK
  JL_TS=$(rfc3339)
  JL_RUN=$RUN_ID
  JL_HOST=$(hostname -s 2>/dev/null || echo unknown)
  JL_CASE=$case_n
  JL_NAME=$name
  JL_RESULT=$result
  JL_REASON=$reason
  JL_DOWN=$down
  JL_SERV=$serv
  JL_AUTO=$auto
  JL_DEXIT=$dexit
  JL_FATAL=${JL_FATAL:-false}
  JL_LOGON=${JL_LOGON:-false}
  JL_BOOK=${JL_BOOK:-false}
  JL_KG_BIN=$kg_bin
  JL_KG_CFG=$kg_cfg
  JL_RB_BIN=$rb_bin
  JL_RB_CFG=$rb_cfg
  JL_NOTES=$notes
  JL_TMO=$SWAP_TIMEOUT_S
  JL_PM2=$(pm2_jlist_app status 2>/dev/null || true)
  JL_CUR=$(cursor_field "$cur" out)
  JL_CURE=$(cursor_field "$cur" err)
  JL_LOUD=${JL_LOUD:-false}
  JL_IDOK=${JL_IDOK:-true}
  emit_case_jsonl
}

redact_capture() {
  local f=$1
  [[ -f "$f" ]] || return 0
  "$PYTHON" - "$f" <<'PY'
import re, sys
p = sys.argv[1]
try:
    t = open(p, "r", encoding="utf-8", errors="replace").read()
except OSError:
    sys.exit(0)
t = re.sub(r"CRYPTOCOM_[A-Z0-9_]+=\S+", "CRYPTOCOM_*=***", t)
t = re.sub(r"(api_secret|api_key|password)\s*=\s*\S+", r"\1=***", t, flags=re.I)
open(p, "w", encoding="utf-8").write(t)
PY
}

invoke_deploy_staged() {
  local dir=$1
  local cap=$2
  mkdir -p "$(dirname "$cap")"
  local rc=0
  set +e
  timeout --kill-after="$TIMEOUT_KILL_AFTER_S" "$SWAP_TIMEOUT_S" \
    env ROLLBACK_DRILL=1 HFT_DRILL_I_AM_ON_UAT=1 CURSOR_FILE="$STASH/.rollback-drill-reload.cursor" \
        CWD="$CWD" PREV_PATH="$PREV_PATH" PM2_APP="$PM2_APP" \
    "$DEPLOY" --staged "$dir" >"${cap}.out" 2>"${cap}.err"
  rc=$?
  set -e
  redact_capture "${cap}.out"
  redact_capture "${cap}.err"
  if [[ -s "${cap}.out" ]]; then
    cat "${cap}.out"
  fi
  if [[ -s "${cap}.err" ]]; then
    cat "${cap}.err" >&2
  fi
  # Allowlist capture: keep FATAL/Logon/crash-loop/no-logon tokens plus grep -F of PR1 greps.
  {
    grep -E 'FATAL:|crash-loop|restart_time|Logon|WARNING:' "${cap}.out" "${cap}.err" 2>/dev/null || true
    if [[ -n "$CRASH_LOOP_LOG_GREP" ]]; then
      grep -F -- "$CRASH_LOOP_LOG_GREP" "${cap}.out" "${cap}.err" 2>/dev/null || true
    fi
    if [[ -n "$NO_LOGON_WARNING_GREP" ]]; then
      grep -F -- "$NO_LOGON_WARNING_GREP" "${cap}.out" "${cap}.err" 2>/dev/null || true
    fi
  } >"${cap}.allow" || true
  chmod 0600 "${cap}.out" "${cap}.err" "${cap}.allow" 2>/dev/null || true
  return "$rc"
}

loud_fail_shipped() {
  grep -F -q "$LOUD_FAIL_MARKER" "$DEPLOY" "$LIB" 2>/dev/null
}

prev_layout_line() {
  bash -c 'source "$1"; prev_layout' _ "$LIB"
}

fail_case() {
  local n=$1 reason=$2
  say "❌ rollback-drill: case ${n} FAIL reason=${reason}"
  err "ERROR: case ${n} failed; box should be on known-good — verify serving before anything else"
  WORST_EXIT=1
}

pass_case() {
  local n=$1 auto=$2 dexit=$3
  say "✅ rollback-drill: case ${n} PASS auto_rollback=${auto} deploy_exit=${dexit}"
}

print_dry_run() {
  say "⚠️  --dry-run: no pause, no lock, no snapshot. Plan only."
  say "rollback-drill plan"
  say "  host          UAT (asserted by box identity, not by exported env)"
  if [[ "${#CASE_SET[@]}" -eq 4 ]]; then
    say "  cases         1 2 3 4"
  else
    say "  case          ${CASE_SET[*]}"
  fi
  say "  deploy.sh     ${DEPLOY} (will not run)"
  say "  snapshot_prev always (deploy.sh path; not a drill flag)"
  say "  poller        ${POLLER_TIMER} + .service (will not pause)"
  say "  lock          ${LOCK_PATH} (will not take)"
  say "  timeout_s     ${SWAP_TIMEOUT_S}"
  say "  serving       NEW Logon in pm2 logs after reload cursor + 📊 Order Book: after that Logon"
  say "                + HTTP best_bid/best_ask > 0 when :8080 is bound"
  say "  secrets       env values and config.toml contents will not be logged"
  say "  case 4        skip if deploy.sh has no ${LOUD_FAIL_MARKER} marker"
  say "✅ dry-run complete"
}

preflight() {
  if [[ "$(id -u)" -eq 0 ]]; then
    die2 "ERROR: preflight: host $(hostname -s) is not the UAT box — rollback-drill is UAT only"
  fi
  if [[ ! -x "$DEPLOY" ]]; then
    die2 "ERROR: preflight: deploy.sh not found at ${DEPLOY}"
  fi
  if [[ ! -f "$LIB" || ! -f "$PARSER" || ! -f "$FIXTURE" || ! -f "$EXAMPLE_CONFIG" ]]; then
    die2 "ERROR: preflight: deploy.sh not found at ${DEPLOY}"
  fi
  if [[ "${#UAT_HOST_ALLOWLIST[@]}" -eq 0 ]]; then
    err "ERROR: preflight: host $(hostname -s) is not the UAT box — rollback-drill is UAT only"
    exit 2
  fi
  check_i1 || exit 2
  local id_cfg=$EXAMPLE_CONFIG
  if [[ -f "${CWD}/config.toml" ]]; then
    id_cfg="${CWD}/config.toml"
  fi
  check_i2_i3 "$id_cfg" || exit 2
  if case_in_set 2; then
    local got
    got=$(sha256_file "$FIXTURE")
    if [[ "$got" != "$CRASH_LOOP_SHA256" ]]; then
      die2 "ERROR: preflight: crash-loop fixture hash mismatch"
    fi
    if [[ -z "$CRASH_LOOP_LOG_GREP" ]]; then
      die2 "ERROR: preflight: CRASH_LOOP_LOG_GREP empty"
    fi
  fi
  if case_in_set 3 && [[ -z "$NO_LOGON_WARNING_GREP" ]]; then
    die2 "ERROR: preflight: NO_LOGON_WARNING_GREP empty"
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi
  local miss
  miss=$(tool_missing || true)
  if [[ -n "$miss" ]]; then
    die2 "ERROR: preflight: missing tool ${miss}"
  fi
  if ! kdb_up; then
    die2 "ERROR: preflight: kdb not listening on 127.0.0.1:${KDB_PORT}"
  fi
  if [[ ! -f "${CWD}/fix_server" || ! -f "${CWD}/config.toml" ]]; then
    die2 "ERROR: preflight: live pair missing under ${CWD}"
  fi
  if ! assert_serving_already_up; then
    die2 "ERROR: preflight: serving-before failed (HTTP best_bid/best_ask > 0 twice)"
  fi
  if case_in_set 2; then
    local ar
    ar=$(pm2_jlist_app autorestart)
    if [[ "$ar" == "False" || "$ar" == "false" ]]; then
      die2 "ERROR: preflight: pm2 autorestart=false for ${PM2_APP} — cannot claim 'online'; this is a box blocker for Case 2"
    fi
    local interp
    interp=$(pm2_jlist_app exec_interpreter)
    case "$interp" in
      none|bash|""|null) ;;
      *) die2 "ERROR: preflight: pm2 interpreter ${interp} not allowed for Case 2" ;;
    esac
  fi
  lock_probe || exit 2
  # poller stoppable? do not stop yet
  if ! command -v systemctl >/dev/null; then
    die2 "ERROR: preflight: ${POLLER_TIMER}/service still running — refuse to inject a fault while the poller can deploy"
  fi
}

layout_kind() {
  local line
  line=$(prev_layout_line)
  printf '%s\n' "${line%% *}"
}

run_case1() {
  local name=bad_config_schema
  say "✅ rollback-drill: start case=1 timeout_s=${SWAP_TIMEOUT_S}"
  say "✅ rollback-drill: case 1 inject sessions = [] (HFT-042 default; not a strip; not kdb FATAL)"
  local kind
  kind=$(layout_kind)
  if [[ "$kind" == "missing" ]]; then
    snapshot_or_note_prev || true
  fi
  kind=$(layout_kind)
  if [[ "$kind" == "missing" || "$kind" == "ambiguous" ]]; then
    record_case 1 "$name" fail "prev_${kind}" false "" "" "" "prev ${kind}"
    fail_case 1 "prev_${kind}"
    return 1
  fi
  stage_dir
  "$PYTHON" "$PARSER" rewrite-case1 "$STASH/known-good/config.toml" >"$STASH/staged/config.toml"
  chmod 0600 "$STASH/staged/config.toml"
  T_LAST_GOOD=$(now_ms)
  INJECTED=1
  local cap=$STASH/capture.1
  local dexit=0
  invoke_deploy_staged "$STASH/staged" "$cap" || dexit=$?
  if [[ "$dexit" -eq 124 ]]; then
    handle_timeout 1 "$name"
    return 1
  fi
  local auto=false
  if hashes_match_known_good; then
    auto=true
  fi
  local cursor
  cursor=$(read_cursor)
  SERVING_LOGON_MS="" SERVING_CONFIRMED_MS=""
  if ! assert_serving "$cursor"; then
    manual_restore || true
    record_case 1 "$name" fail serving_not_confirmed "$auto" "$dexit" "" "" ""
    fail_case 1 serving_not_confirmed
    return 1
  fi
  if [[ "$auto" != "true" ]]; then
    record_case 1 "$name" fail auto_rollback_missing false "$dexit" "" "" ""
    fail_case 1 auto_rollback_missing
    return 1
  fi
  local down=$((SERVING_LOGON_MS - T_LAST_GOOD))
  local serv=$((SERVING_CONFIRMED_MS - T_LAST_GOOD))
  JL_FATAL=true JL_LOGON=true JL_BOOK=true
  record_case 1 "$name" pass ok true "$dexit" "$down" "$serv" ""
  say "✅ serving confirmed: live Logon + advancing book (downtime_ms=${down} serving_confirmed_ms=${serv})"
  pass_case 1 true "$dexit"
  INJECTED=0
  return 0
}

run_case2() {
  local name=crash_loop_wrapper
  say "✅ rollback-drill: start case=2 timeout_s=${SWAP_TIMEOUT_S}"
  say "✅ rollback-drill: case 2 wrap with crash-loop-fix_server.sh"
  local kind
  kind=$(layout_kind)
  if [[ "$kind" == "missing" || "$kind" == "ambiguous" ]]; then
    record_case 2 "$name" fail "prev_${kind}" false "" "" "" ""
    fail_case 2 "prev_${kind}"
    return 1
  fi
  stage_dir
  cp -a "$FIXTURE" "$STASH/staged/fix_server"
  chmod 0755 "$STASH/staged/fix_server"
  T_LAST_GOOD=$(now_ms)
  INJECTED=1
  local cap=$STASH/capture.2
  local dexit=0
  invoke_deploy_staged "$STASH/staged" "$cap" || dexit=$?
  if [[ "$dexit" -eq 124 ]]; then
    handle_timeout 2 "$name"
    return 1
  fi
  if ! grep -F -q -- "$CRASH_LOOP_LOG_GREP" "$cap.allow" "$cap.err" "$cap.out" 2>/dev/null; then
    manual_restore || true
    record_case 2 "$name" fail auto_rollback_missing false "$dexit" "" "" "crash-loop grep missed"
    fail_case 2 auto_rollback_missing
    return 1
  fi
  local auto=false
  if hashes_match_known_good; then
    auto=true
  fi
  local cursor
  cursor=$(read_cursor)
  SERVING_LOGON_MS="" SERVING_CONFIRMED_MS=""
  if ! assert_serving "$cursor"; then
    manual_restore || true
    record_case 2 "$name" fail serving_not_confirmed "$auto" "$dexit" "" "" ""
    fail_case 2 serving_not_confirmed
    return 1
  fi
  if [[ "$auto" != "true" ]]; then
    record_case 2 "$name" fail auto_rollback_missing false "$dexit" "" "" ""
    fail_case 2 auto_rollback_missing
    return 1
  fi
  local down=$((SERVING_LOGON_MS - T_LAST_GOOD))
  local serv=$((SERVING_CONFIRMED_MS - T_LAST_GOOD))
  JL_LOGON=true JL_BOOK=true
  record_case 2 "$name" pass ok true "$dexit" "$down" "$serv" ""
  say "✅ serving confirmed: live Logon + advancing book (downtime_ms=${down} serving_confirmed_ms=${serv})"
  pass_case 2 true "$dexit"
  INJECTED=0
  return 0
}

run_case3() {
  local name=no_logon_manual_restore
  say "✅ rollback-drill: start case=3 timeout_s=${SWAP_TIMEOUT_S}"
  say "✅ rollback-drill: case 3 inject host=127.0.0.1:1 only (no other host rewrite)"
  say "⚠️  Case 3: deploy.sh will NOT auto-rollback (expected). Drill will restore the known-good pair after the assertion."
  local kind
  kind=$(layout_kind)
  if [[ "$kind" == "missing" || "$kind" == "ambiguous" ]]; then
    record_case 3 "$name" fail "prev_${kind}" false "" "" "" ""
    fail_case 3 "prev_${kind}"
    return 1
  fi
  stage_dir
  "$PYTHON" "$PARSER" rewrite-case3 "$STASH/known-good/config.toml" >"$STASH/staged/config.toml"
  chmod 0600 "$STASH/staged/config.toml"
  "$PYTHON" "$PARSER" --assert-case3 "$STASH/staged/config.toml"
  T_LAST_GOOD=$(now_ms)
  INJECTED=1
  local cap=$STASH/capture.3
  local dexit=0
  invoke_deploy_staged "$STASH/staged" "$cap" || dexit=$?
  if [[ "$dexit" -eq 124 ]]; then
    handle_timeout 3 "$name"
    return 1
  fi
  if hashes_match_known_good; then
    record_case 3 "$name" fail auto_rollback_unexpected true "$dexit" "" "" "auto-rollback on no Logon"
    fail_case 3 auto_rollback_unexpected
    return 1
  fi
  if ! grep -F -q -- "$NO_LOGON_WARNING_GREP" "$cap.allow" "$cap.err" "$cap.out" 2>/dev/null; then
    manual_restore || true
    record_case 3 "$name" fail logon_not_found false "$dexit" "" "" "no-logon warning missing"
    fail_case 3 logon_not_found
    return 1
  fi
  say "✅ Case 3 assertion: auto_rollback=false (expected — not a bug)"
  say "✅ rollback-drill: drill-owned restore of known-good bin+config pair"
  manual_restore
  local cursor
  cursor=$(read_cursor)
  SERVING_LOGON_MS="" SERVING_CONFIRMED_MS=""
  if ! assert_serving "$cursor"; then
    record_case 3 "$name" fail serving_not_confirmed false "$dexit" "" "" "after manual_restore"
    fail_case 3 serving_not_confirmed
    return 1
  fi
  local down=$((SERVING_LOGON_MS - T_LAST_GOOD))
  local serv=$((SERVING_CONFIRMED_MS - T_LAST_GOOD))
  JL_LOGON=true JL_BOOK=true
  record_case 3 "$name" pass ok false "$dexit" "$down" "$serv" ""
  say "✅ serving confirmed: live Logon + advancing book (downtime_ms=${down} serving_confirmed_ms=${serv})"
  pass_case 3 false "$dexit"
  INJECTED=0
  return 0
}

# Single-quoted Case 4 child. Paths arrive only as $1 (stash) and $2 (staged).
# shellcheck disable=SC2016
CASE4_CHILD='
set -euo pipefail
[[ "${ROLLBACK_DRILL:-}" == 1 && "${HFT_DRILL_I_AM_ON_UAT:-}" == 1 ]] || {
  echo "ERROR: --staged requires ROLLBACK_DRILL=1 and HFT_DRILL_I_AM_ON_UAT=1" >&2
  exit 2
}
source "$(dirname "$0")/deploy-lib.sh"
stash=$1
staged=$2
stash_real=$(realpath -e "$stash")
staged_real=$(realpath -e "$staged")
[[ "$staged_real" == "$stash_real"/* ]] || {
  echo "FATAL: staged dir is not under stash" >&2
  exit 2
}
snapshot_prev
dest="$stash/fix_server.prev"
mkdir -p -m 0700 "$dest"
layout=$(prev_layout)
layout=${layout%% *}
case "$layout" in
  dir) mv "${HOME}/.fix_server.prev" "$dest/dir" ;;
  tar) mv "${HOME}/.fix_server.prev.tar" "$dest/" ;;
  tgz) mv "${HOME}/.fix_server.prev.tgz" "$dest/" ;;
  siblings|binary_only)
    mv "${HOME}/.fix_server.prev" "$dest/"
    if [[ -e "${HOME}/.fix_server.prev.config" ]]; then
      mv "${HOME}/.fix_server.prev.config" "$dest/"
    fi
    ;;
  absent)
    echo "FATAL: relocate: no .prev after snapshot" >&2
    exit 2
    ;;
  ambiguous)
    echo "FATAL: relocate: prev_layout_ambiguous" >&2
    exit 2
    ;;
  *)
    echo "FATAL: relocate: unknown layout" >&2
    exit 2
    ;;
esac
if [[ -e "${HOME}/.fix_server.prev" || -e "${HOME}/.fix_server.prev.tar" || -e "${HOME}/.fix_server.prev.tgz" || -e "${HOME}/.fix_server.prev.config" ]]; then
  echo "FATAL: relocate left a .prev member behind" >&2
  exit 2
fi
swap_reload "$staged"
set +e
health_check
rollback
exit $?
'

run_case4() {
  local name=missing_prev_loud_fail
  say "✅ rollback-drill: start case=4 timeout_s=${SWAP_TIMEOUT_S}"
  if ! loud_fail_shipped; then
    say "⚠️  Case 4 skipped: loud-fail not shipped on this box (no ${LOUD_FAIL_MARKER} in deploy.sh). Not a failure."
    say "✅ rollback-drill: case 4 SKIP"
    record_case 4 "$name" skip loud_fail_not_yet_implemented false "" "" "" ""
    return 0
  fi
  say "✅ rollback-drill: case 4 relocate .prev pair between snapshot and swap (never delete)"
  stage_dir
  "$PYTHON" "$PARSER" rewrite-case1 "$STASH/known-good/config.toml" >"$STASH/staged/config.toml"
  chmod 0600 "$STASH/staged/config.toml"
  local staged_real stash_real
  stash_real=$(realpath -e "$STASH")
  staged_real=$(realpath -e "$STASH/staged")
  if [[ "$staged_real" != "$stash_real"/* ]]; then
    die2 "ERROR: staged dir is not under stash"
  fi
  INJECTED=1
  local cap=$STASH/case4.deploy-raw
  local dexit=0
  set +e
  timeout --signal=TERM --kill-after="$TIMEOUT_KILL_AFTER_S" "$SWAP_TIMEOUT_S" \
    env ROLLBACK_DRILL=1 HFT_DRILL_I_AM_ON_UAT=1 CWD="$CWD" PM2_APP="$PM2_APP" \
        CURSOR_FILE="$STASH/.rollback-drill-reload.cursor" PYTHON="$PYTHON" \
    bash -c "$CASE4_CHILD" "$0" "$STASH" "$STASH/staged" >"$cap" 2>&1
  dexit=$?
  set -e
  if [[ -n "${ROLLBACK_DRILL:-}" || -n "${HFT_DRILL_I_AM_ON_UAT:-}" ]]; then
    unset ROLLBACK_DRILL HFT_DRILL_I_AM_ON_UAT
    record_case 4 "$name" fail parent_env_leaked false "$dexit" "" "" ""
    fail_case 4 parent_env_leaked
    emergency_restore
    exit 1
  fi
  redact_capture "$cap"
  [[ -s "$cap" ]] && cat "$cap" >&2
  if [[ "$dexit" -eq 124 ]]; then
    handle_timeout 4 "$name"
    return 1
  fi
  local loud=false
  if grep -F -q 'missing path:' "$cap" 2>/dev/null; then
    loud=true
  fi
  if [[ "$dexit" -eq 0 ]]; then
    inverse_restore_prev || true
    manual_restore || true
    record_case 4 "$name" fail missing_prev_did_not_loud_fail false "$dexit" "" "" "loud-fail must be non-zero != 124"
    fail_case 4 missing_prev_did_not_loud_fail
    return 1
  fi
  if [[ "$loud" != "true" ]]; then
    inverse_restore_prev || true
    manual_restore || true
    record_case 4 "$name" fail missing_prev_did_not_loud_fail false "$dexit" "" "" ""
    say "❌ rollback-drill: case 4 FAIL reason=missing_prev_did_not_loud_fail"
    err "ERROR: case 4: deploy.sh did not emit FATAL missing path: — not a pass"
    WORST_EXIT=1
    return 1
  fi
  # Exit 3 + hashes==known-good + online = fail (silent restore)
  if [[ "$dexit" -eq "$ROLLBACK_LOUD_EXIT" ]] && hashes_match_known_good; then
    local st
    st=$(pm2_jlist_app status || true)
    if [[ "$st" == "online" ]]; then
      inverse_restore_prev || true
      record_case 4 "$name" fail missing_prev_did_not_loud_fail false "$dexit" "" "" "exit 3 but hashes==known-good and online"
      fail_case 4 missing_prev_did_not_loud_fail
      return 1
    fi
  fi
  say "✅ Case 4 assertion: deploy.sh loud-fail observed deploy_exit=${dexit} (!= 0 and != 124)"
  say "✅ rollback-drill: restoring relocated .prev pair + known-good serving pair"
  inverse_restore_prev
  manual_restore
  local cursor
  cursor=$(read_cursor)
  SERVING_LOGON_MS="" SERVING_CONFIRMED_MS=""
  if ! assert_serving "$cursor"; then
    record_case 4 "$name" fail serving_not_confirmed false "$dexit" "" "" ""
    fail_case 4 serving_not_confirmed
    return 1
  fi
  local down=$((SERVING_LOGON_MS - T_LAST_GOOD))
  local serv=$((SERVING_CONFIRMED_MS - T_LAST_GOOD))
  JL_LOUD=true JL_LOGON=true JL_BOOK=true
  record_case 4 "$name" pass ok false "$dexit" "$down" "$serv" ""
  say "✅ serving confirmed: live Logon + advancing book (downtime_ms=${down} serving_confirmed_ms=${serv})"
  pass_case 4 false "$dexit"
  INJECTED=0
  return 0
}

snapshot_or_note_prev() {
  # Cases 1-3 need a .prev pair. Create via snapshot_prev if live cwd is good.
  bash -c 'source "$1"; snapshot_prev' _ "$LIB" || true
}

handle_timeout() {
  local n=$1 name=$2
  say "❌ rollback-drill: timeout (${SWAP_TIMEOUT_S}s) — running emergency_restore"
  emergency_restore
  local cursor
  cursor=$(read_cursor)
  local ok=0
  if hashes_match_known_good && assert_serving "$cursor"; then
    ok=1
  fi
  INJECTED=0
  err "ERROR: case ${n} timed out (exit 124). This is NOT a Case 4 loud-fail pass."
  if [[ "$ok" -eq 1 ]]; then
    local down serv
    down=$(( ${SERVING_LOGON_MS:-0} - ${T_LAST_GOOD:-0} ))
    serv=$(( ${SERVING_CONFIRMED_MS:-0} - ${T_LAST_GOOD:-0} ))
    record_case "$n" "$name" timeout timeout false 124 "$down" "$serv" ""
    WORST_EXIT=124
  else
    record_case "$n" "$name" fail timeout false 124 "" "" "emergency_restore_failed"
    WORST_EXIT=1
  fi
}

print_summary() {
  say "========== rollback-drill =========="
  say "run_id  ${RUN_ID}"
  if [[ -f "$JSONL" ]]; then
    "$PYTHON" - "$JSONL" <<'PY'
import json, sys
path = sys.argv[1]
rows = []
with open(path, encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        if o.get("event") == "suite_start":
            continue
        rows.append(o)
p = s = f = t = 0
for o in rows:
    n = o.get("case")
    res = (o.get("result") or "").upper()
    if res == "PASS":
        p += 1
        print("case {n}  PASS  downtime_ms={d}  serving_confirmed_ms={c}  auto_rollback={a}  deploy_exit={e}".format(
            n=n, d=o.get("downtime_ms"), c=o.get("serving_confirmed_ms"),
            a=str(o.get("auto_rollback")).lower() if isinstance(o.get("auto_rollback"), bool) else o.get("auto_rollback"),
            e=o.get("deploy_exit")))
    elif res == "SKIP":
        s += 1
        print("case {n}  SKIP  reason={r}".format(n=n, r=o.get("reason")))
    elif res == "TIMEOUT":
        t += 1
        f += 1
        print("case {n}  FAIL  reason=timeout".format(n=n))
    else:
        f += 1
        print("case {n}  FAIL  reason={r}".format(n=n, r=o.get("reason")))
print("p=%d s=%d f=%d" % (p, s, f))
PY
  fi
}

run_suite() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    print_dry_run
  fi
  preflight
  if [[ "$DRY_RUN" -eq 1 ]]; then
    exit 0
  fi
  acquire_lock || exit 2
  if ! pause_poller; then
    release_lock
    exit 2
  fi
  mkdir -p "$RESULTS_DIR"
  chmod 0700 "$RESULTS_DIR"
  RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  JSONL="${RESULTS_DIR}/${RUN_ID}.jsonl"
  DRILL_LOG="${RESULTS_DIR}/${RUN_ID}.log"
  touch "$JSONL" "$DRILL_LOG"
  chmod 0600 "$JSONL" "$DRILL_LOG"
  STASH=$(mktemp -d /tmp/rollback-drill-stash-XXXXXX)
  chmod 0700 "$STASH"
  copy_known_good
  T_LAST_GOOD=$(now_ms)
  SUITE_T0=$(now_ms)
  emit_suite_start
  if [[ "${#CASE_SET[@]}" -eq 1 ]]; then
    say "✅ rollback-drill: start case=${CASE_SET[0]} timeout_s=${SWAP_TIMEOUT_S}"
  else
    local joined
    joined=$(IFS=,; echo "${CASE_SET[*]}")
    say "✅ rollback-drill: start suite run_id=${RUN_ID} timeout_s=${SWAP_TIMEOUT_S} cases=${joined}"
  fi
  say "jsonl ${JSONL}"
  say "✅ rollback-drill: secrets not logged (env values and config.toml contents omitted)"

  trap on_int INT
  trap on_term TERM
  trap on_exit EXIT

  local c rc
  for c in "${CASE_SET[@]}"; do
    local now
    now=$(now_ms)
    if [[ $((now - SUITE_T0)) -gt $((SUITE_DEADLINE_S * 1000)) ]]; then
      handle_timeout "$c" "suite_deadline"
      break
    fi
    rc=0
    case "$c" in
      1) run_case1 || rc=$? ;;
      2) run_case2 || rc=$? ;;
      3) run_case3 || rc=$? ;;
      4) run_case4 || rc=$? ;;
    esac
    if [[ "$WORST_EXIT" -eq 1 || "$WORST_EXIT" -eq 124 ]]; then
      break
    fi
  done

  if [[ "${#CASE_SET[@]}" -gt 1 ]]; then
    say "========== rollback-drill =========="
    say "run_id  ${RUN_ID}"
    local p=0 s=0 f=0
    if [[ -f "$JSONL" ]]; then
      while IFS= read -r line; do
        [[ "$line" == *'"event":"suite_start"'* ]] && continue
        if [[ "$line" == *'"result":"pass"'* ]]; then
          p=$((p + 1))
        elif [[ "$line" == *'"result":"skip"'* ]]; then
          s=$((s + 1))
        else
          f=$((f + 1))
        fi
      done <"$JSONL"
    fi
    print_summary >/dev/null || true
    "$PYTHON" - "$JSONL" <<'PY' || true
import json, sys
path = sys.argv[1]
try:
    rows = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
except OSError:
    sys.exit(0)
for o in rows:
    if o.get("event") == "suite_start":
        continue
    n = o.get("case")
    res = (o.get("result") or "")
    if res == "pass":
        auto = o.get("auto_rollback")
        if isinstance(auto, bool):
            auto = "true" if auto else "false"
        print("case {n}  PASS  downtime_ms={d}  serving_confirmed_ms={c}  auto_rollback={a}   deploy_exit={e}".format(
            n=n, d=o.get("downtime_ms"), c=o.get("serving_confirmed_ms"), a=auto, e=o.get("deploy_exit")))
    elif res == "skip":
        print("case {n}  SKIP  reason={r}".format(n=n, r=o.get("reason")))
    else:
        print("case {n}  FAIL  reason={r}".format(n=n, r=o.get("reason") or res))
PY
    local line
    if [[ "$WORST_EXIT" -eq 0 ]]; then
      say "✅ rollback-drill: ALL complete (${p} pass, ${s} skip, ${f} fail)"
    else
      say "❌ rollback-drill: ALL complete (${p} pass, ${s} skip, ${f} fail)"
    fi
    say "jsonl ${JSONL}"
  fi

  FINISHING=1
  local resume_rc=0
  resume_poller || resume_rc=$?
  release_lock
  if [[ -n "$STASH" && -d "$STASH" ]]; then
    rm -rf "$STASH"
    STASH=""
  fi
  trap - EXIT INT TERM
  if [[ "$resume_rc" -ne 0 ]]; then
    exit 1
  fi
  exit "$WORST_EXIT"
}

main() {
  parse_args "$@"
  run_suite
}

main "$@"
