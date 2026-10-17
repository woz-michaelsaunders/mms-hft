#!/usr/bin/env bash
# deploy-lib.sh — source-only. Public steps: snapshot_prev, swap_reload,
# health_check, rollback. Internal: prev_layout.
# No top-level `set`. Callers own set -euo pipefail.
#
# health_check/rollback/snapshot_prev/swap_reload follow the on-box pull-deploy
# contract documented in .forgejo/workflows/release.yml (HTTPS tag pull, green
# CI build + release on the exact commit, on-box cargo test, atomic swap,
# rollback copy kept) and the stakeholder health gates: FATAL in startup.log,
# crash-loop via two restart_time samples, no-Logon warning without auto-rollback.
# Live /home/ubuntu/deploy.sh is not readable from this builder; operators must
# secret-review (C-REQ-4) against the box copy before replacing it.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "FATAL: deploy-lib.sh is source-only" >&2
  exit 2
fi

: "${CWD:=/home/ubuntu/fix_server}"
# The deployed binary is cargo's output, not $CWD/fix_server.
: "${BIN:=$CWD/target/release/fix_server}"
: "${ECOSYSTEM:=/home/ubuntu/ecosystem.config.js}"
: "${PM2_APP:=fix-server}"   # HYPHEN: pm2 reload --only fix_server matches nothing and exits 0
: "${ROLLBACK_LOUD_EXIT:=3}"
: "${CRASH_SAMPLE_GAP_S:=10}"
: "${LOGON_WAIT_S:=30}"
: "${LOGON_GREP:=Logon ACK (35=A) received}"
# fix_commands.log is the verbatim FIX audit trail, written by direct file I/O
# rather than through the logger, so it is INDEPENDENT OF RUST_LOG. The
# LOGON_GREP line above is emitted at INFO by the same module as the per-tick
# noise, which is the only reason UAT had to stay at `info`: 35,373 of 40,000
# sampled lines were INFO fix_server]. Asserting on the audit trail instead
# lets fix-server run at `warn` like production.
: "${COMMAND_LOG:=$CWD/fix_commands.log}"
: "${COMMAND_LOG_GREP:=FIX IN Logon (35=A)}"
# Inbound market data counts as proof of serving too, and is far denser: prod
# writes ~155k of these an hour versus one logon per session. That matters
# because the audit trail rotates at ~100MB keeping a single generation, which
# prod reaches in about an hour -- so a rotation landing inside the 30s
# assertion window could carry the logon line away. Market data cannot all
# disappear the same way while the feed is up.
: "${COMMAND_LOG_GREP_ALT:=FIX IN MarketDataIncrementalRefresh (35=X)}"
: "${FATAL_GREP_ERE:=FATAL: (could not load config\\.toml|config validation failed)}"
: "${STARTUP_LOG:=$CWD/startup.log}"
: "${PYTHON:=/usr/bin/python3}"
: "${PREV_PATH:=${HOME}/.fix_server.prev}"

_lib_pm2_env_field() {
  local field=$1
  pm2 jlist 2>/dev/null | "$PYTHON" -c '
import json, sys
name, field = sys.argv[1], sys.argv[2]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if not isinstance(data, list):
    sys.exit(0)
for p in data:
    if p.get("name") == name:
        env = p.get("pm2_env") or {}
        v = env.get(field, "")
        if v is None:
            v = ""
        print(v)
        break
' "$PM2_APP" "$field"
}

_lib_pm2_app_exists() {
  pm2 jlist 2>/dev/null | "$PYTHON" -c '
import json, sys
name = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
if not isinstance(data, list):
    sys.exit(1)
sys.exit(0 if any(p.get("name") == name for p in data) else 1)
' "$PM2_APP"
}

_lib_pm2_log_path() {
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
    _lib_pm2_env_field pm_out_log_path
  else
    _lib_pm2_env_field pm_err_log_path
  fi
}

_lib_file_size() {
  local p=$1
  if [[ -f "$p" ]]; then
    stat -c %s "$p" 2>/dev/null || echo 0
  else
    echo 0
  fi
}

_lib_grep_after() {
  local file=$1
  local offset=$2
  local pattern=$3
  local ere=${4:-0}
  [[ -f "$file" ]] || return 1
  local sz
  sz=$(_lib_file_size "$file")
  if [[ "$sz" -le "$offset" ]]; then
    return 1
  fi
  # NOTE: `grep -q` exits on the first match, which kills `tail` with SIGPIPE.
  # Callers set `set -o pipefail` (deploy.sh line 29), so the pipeline then
  # reports 141 and a FOUND match is read as NOT FOUND. That is the whole of
  # the deploy "no Logon ACK" false negative: it fired on every poller deploy
  # and never on a hand-run health_check, because an interactive shell has no
  # pipefail. Proven:
  #   set -o pipefail; tail -c +1 log | grep -F -q PATTERN  -> 141
  #   set +o pipefail; tail -c +1 log | grep -F -q PATTERN  -> 0
  # Drop -q so grep consumes its input and exits 0/1 honestly, and disable
  # pipefail for just this pipeline so a SIGPIPE'd tail cannot mask the result.
  local rc=0
  if [[ "$ere" == "1" ]]; then
    { set +o pipefail; tail -c +"$((offset + 1))" "$file" | grep -E -c -- "$pattern" >/dev/null; rc=$?; }
  else
    { set +o pipefail; tail -c +"$((offset + 1))" "$file" | grep -F -c -- "$pattern" >/dev/null; rc=$?; }
  fi
  return "$rc"
}

# The OS pid of the running app. pm2 reports this at the TOP level of each
# jlist entry, not inside pm2_env (where it is present but always null), so
# _lib_pm2_env_field cannot read it -- that returns empty and would fail the
# serving check closed on every deploy.
_lib_pm2_pid() {
  pm2 jlist 2>/dev/null | "$PYTHON" -c '
import json, sys
name = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if not isinstance(data, list):
    sys.exit(0)
for p in data:
    if p.get("name") == name:
        v = p.get("pid")
        print("" if v is None else v)
        break
' "$PM2_APP"
}

# Epoch-ms at which the CURRENTLY RUNNING pm2 process started.
#
# This is the anchor the serving check needs. A byte offset into the audit
# trail cannot distinguish the incoming process from the outgoing one: a pm2
# reload is graceful, so the process being replaced keeps writing market data
# into the same fix_commands.log for a second or more after the cursor is
# taken, and production writes ~155k of those lines an hour.
#
# /proc is used rather than startup.log because it cannot be truncated out
# from under us (hft-health.sh empties startup.log before healing) and needs
# no parsing agreement with the application. Field 22 of /proc/PID/stat is the
# process start in clock ticks since boot; /proc/stat btime is the boot epoch.
_lib_proc_start_ms() {
  local pid=$1
  [[ -n "$pid" && "$pid" != "0" ]] || return 1
  "$PYTHON" - "$pid" <<'PYEOF' 2>/dev/null
import os, sys
pid = sys.argv[1]
try:
    with open(f"/proc/{pid}/stat") as f:
        after_comm = f.read().rsplit(") ", 1)[1].split()
    ticks = int(after_comm[19])                      # field 22 overall
    hz = os.sysconf("SC_CLK_TCK")
    btime = 0
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("btime "):
                btime = int(line.split()[1])
                break
    if not btime:
        raise SystemExit(1)
    print(int((btime + ticks / hz) * 1000))
except Exception:
    raise SystemExit(1)
PYEOF
}

# True when the audit trail shows the NEW process serving.
#
# $1 file, $2 byte offset to start scanning from, $3 epoch-ms the new process
# started. Only lines whose own timestamp is at or after $3 count, so output
# from the retired process can never satisfy the gate.
#
# The offset is a performance hint only (the trail reaches 100MB); correctness
# rests entirely on the timestamp. If the file shrank the scan restarts at 0.
_lib_audit_serving_since() {
  local file=$1 offset=$2 since_ms=$3
  [[ -f "$file" ]] || return 1
  [[ -n "$since_ms" ]] || return 1
  "$PYTHON" - "$file" "$offset" "$since_ms" "$COMMAND_LOG_GREP" "$COMMAND_LOG_GREP_ALT" <<'PYEOF'
import datetime, os, sys
path, offset, since_ms = sys.argv[1], int(sys.argv[2] or 0), int(sys.argv[3])
needles = [n for n in sys.argv[4:] if n]
try:
    size = os.path.getsize(path)
except OSError:
    raise SystemExit(1)
if offset > size:          # rotated or truncated since the cursor was taken
    offset = 0
found = 1
with open(path, "r", errors="replace") as f:
    f.seek(offset)
    f.readline()           # the first line may be partial; the next poll gets it
    for line in f:
        if not line.startswith("["):
            continue
        end = line.find("]")
        if end < 0:
            continue
        stamp = line[1:end].replace(" UTC", "").strip()
        try:
            ts = datetime.datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            continue
        ts = ts.replace(tzinfo=datetime.timezone.utc)
        if ts.timestamp() * 1000 < since_ms:
            continue
        if any(n in line for n in needles):
            found = 0
            break
raise SystemExit(found)
PYEOF
}

_lib_write_cursor() {
  # Deploys must write a cursor too. This used to return early unless a drill
  # was running, while health_check fell back to reading
  # ${CWD}/.rollback-drill-reload.cursor regardless -- so a deploy's health
  # check read a cursor left behind by the last DRILL. On 2026-10-06 that
  # cursor said err=85186344 while the log had since rotated down to 54MB, so
  # the check read past EOF, found nothing, and reported a false
  # "serving NOT CONFIRMED" on a deploy whose real fault was elsewhere.
  local out err startup dest os es ss mono
  out=$(_lib_pm2_log_path out)
  err=$(_lib_pm2_log_path err)
  startup=$STARTUP_LOG
  os=$(_lib_file_size "${out:-/dev/null}")
  es=$(_lib_file_size "${err:-/dev/null}")
  ss=$(_lib_file_size "${startup:-/dev/null}")
  mono=$("$PYTHON" -c 'import time; print(int(time.monotonic()*1000))' 2>/dev/null || echo 0)
  local cs
  cs=$(_lib_file_size "${COMMAND_LOG:-/dev/null}")
  if [[ -n "${CURSOR_FILE:-}" ]]; then
    dest=$CURSOR_FILE
  else
    dest="${CWD}/.rollback-drill-reload.cursor"
  fi
  mkdir -p "$(dirname "$dest")" 2>/dev/null || true
  # Two-integer prefix keeps existing drill parsers; key=value is the LLD line.
  printf 'out=%s err=%s startup=%s mono_ms=%s cmd=%s\n' "$os" "$es" "$ss" "$mono" "$cs" >"$dest"
  chmod 0600 "$dest" 2>/dev/null || true
  if [[ -d "$CWD" && "$dest" != "${CWD}/.rollback-drill-reload.cursor" ]]; then
    cp -a "$dest" "${CWD}/.rollback-drill-reload.cursor" 2>/dev/null || true
    chmod 0600 "${CWD}/.rollback-drill-reload.cursor" 2>/dev/null || true
  fi
}

# Internal. Field 1 = dir|tar|tgz|siblings|binary_only|absent|ambiguous
prev_layout() {
  local n=0 token=""
  if [[ -d "${HOME}/.fix_server.prev" ]]; then
    token=dir
    n=$((n + 1))
  fi
  if [[ -f "${HOME}/.fix_server.prev.tar" ]]; then
    token=tar
    n=$((n + 1))
  fi
  if [[ -f "${HOME}/.fix_server.prev.tgz" ]]; then
    token=tgz
    n=$((n + 1))
  fi
  if [[ -f "${HOME}/.fix_server.prev" && -f "${HOME}/.fix_server.prev.config" ]]; then
    token=siblings
    n=$((n + 1))
  elif [[ -f "${HOME}/.fix_server.prev" ]]; then
    token=binary_only
    n=$((n + 1))
  fi
  if [[ "$n" -gt 1 ]]; then
    printf 'ambiguous\n'
  elif [[ "$n" -eq 0 ]]; then
    printf 'absent\n'
  else
    printf '%s\n' "$token"
  fi
}

_lib_dir_members() {
  # Sets PREV_BIN_MEMBER PREV_CFG_MEMBER for a directory pair.
  local root=$1
  PREV_BIN_MEMBER=""
  PREV_CFG_MEMBER=""
  if [[ -f "${root}/fix_server" ]]; then
    PREV_BIN_MEMBER="${root}/fix_server"
  elif [[ -f "${root}/bin" ]]; then
    PREV_BIN_MEMBER="${root}/bin"
  fi
  if [[ -f "${root}/config.toml" ]]; then
    PREV_CFG_MEMBER="${root}/config.toml"
  fi
}

_lib_upgrade_binary_only() {
  local path=$1
  local parent tmp
  parent=$(dirname "$path")
  tmp=$(mktemp -d "${parent}/.fix_server.prev.upgrade.XXXXXX")
  chmod 0700 "$tmp"
  mv "$path" "$tmp/legacy.bin"
  cp "${BIN}" "$tmp/fix_server"
  chmod o-w "$tmp/fix_server" || chmod 0755 "$tmp/fix_server"
  cp "${CWD}/config.toml" "$tmp/config.toml"
  chmod 0600 "$tmp/config.toml"
  mv "$tmp" "$path"
  chmod 0700 "$path"
  echo "INFO prev_layout_upgraded binary_only→dir" >&2
}

snapshot_prev() {
  local kind
  kind=$(prev_layout)
  kind=${kind%% *}
  case "$kind" in
    ambiguous)
      echo "ERROR: prev_layout_ambiguous" >&2
      return 1
      ;;
    binary_only)
      _lib_upgrade_binary_only "${HOME}/.fix_server.prev"
      ;;
    absent)
      local tmp
      tmp=$(mktemp -d /tmp/rollback-prev-new-XXXXXX)
      chmod 0700 "$tmp"
      if [[ ! -f "${BIN}" || ! -f "${CWD}/config.toml" ]]; then
        echo "ERROR: snapshot_prev missing cwd member: ${BIN} or config.toml" >&2
        rm -rf "$tmp"
        return 1
      fi
      cp "${BIN}" "$tmp/fix_server"
      chmod o-w "$tmp/fix_server" || chmod 0755 "$tmp/fix_server"
      cp "${CWD}/config.toml" "$tmp/config.toml"
      chmod 0600 "$tmp/config.toml"
      mv "$tmp" "${HOME}/.fix_server.prev"
      chmod 0700 "${HOME}/.fix_server.prev"
      return 0
      ;;
    siblings)
      if [[ ! -f "${BIN}" || ! -f "${CWD}/config.toml" ]]; then
        echo "ERROR: snapshot_prev missing cwd member: ${BIN}" >&2
        return 1
      fi
      cp "${BIN}" "${HOME}/.fix_server.prev"
      chmod o-w "${HOME}/.fix_server.prev" || true
      cp "${CWD}/config.toml" "${HOME}/.fix_server.prev.config"
      chmod 0600 "${HOME}/.fix_server.prev.config"
      return 0
      ;;
    tar|tgz)
      local workdir packflags archive
      workdir=$(mktemp -d /tmp/rollback-prev-tar-XXXXXX)
      chmod 0700 "$workdir"
      # shellcheck disable=SC2064
      trap 'rm -rf "$workdir"' RETURN
      if [[ "$kind" == "tgz" ]]; then
        archive="${HOME}/.fix_server.prev.tgz"
        packflags=-czf
        tar -xzf "$archive" -C "$workdir"
      else
        archive="${HOME}/.fix_server.prev.tar"
        packflags=-cf
        tar -xf "$archive" -C "$workdir"
      fi
      cp "${BIN}" "$workdir/fix_server"
      chmod o-w "$workdir/fix_server" || true
      cp "${CWD}/config.toml" "$workdir/config.toml"
      chmod 0600 "$workdir/config.toml"
      local tmpa
      tmpa=$(mktemp "${archive}.XXXXXX")
      tar $packflags "$tmpa" -C "$workdir" fix_server config.toml
      mv -f "$tmpa" "$archive"
      rm -rf "$workdir"
      trap - RETURN
      return 0
      ;;
    dir) ;;
    *)
      echo "ERROR: snapshot_prev: unknown layout $kind" >&2
      return 1
      ;;
  esac
  if [[ ! -d "${HOME}/.fix_server.prev" ]]; then
    echo "ERROR: snapshot_prev: ${HOME}/.fix_server.prev is not a directory" >&2
    return 1
  fi
  if [[ ! -f "${BIN}" ]]; then
    echo "ERROR: snapshot_prev missing cwd member: ${BIN}" >&2
    return 1
  fi
  if [[ ! -f "${CWD}/config.toml" ]]; then
    echo "ERROR: snapshot_prev missing cwd member: ${CWD}/config.toml" >&2
    return 1
  fi
  chmod 0700 "${HOME}/.fix_server.prev" || true
  cp "${BIN}" "${HOME}/.fix_server.prev/fix_server"
  chmod o-w "${HOME}/.fix_server.prev/fix_server" || chmod 0755 "${HOME}/.fix_server.prev/fix_server"
  cp "${CWD}/config.toml" "${HOME}/.fix_server.prev/config.toml"
  chmod 0600 "${HOME}/.fix_server.prev/config.toml"
  return 0
}

swap_reload() {
  local dir=${1:-}
  if [[ -z "$dir" ]]; then
    echo "ERROR: swap_reload: staged DIR missing" >&2
    return 2
  fi
  if [[ "$dir" == *$'\n'* ]]; then
    echo "ERROR: swap_reload: DIR contains a newline" >&2
    return 2
  fi
  local real
  real=$(realpath -e "$dir" 2>/dev/null) || {
    echo "ERROR: swap_reload: staged DIR missing" >&2
    return 2
  }
  if [[ ! -d "$real" ]]; then
    echo "ERROR: swap_reload: staged DIR missing" >&2
    return 2
  fi
  if [[ ! -f "${real}/fix_server" || ! -f "${real}/config.toml" ]]; then
    echo "ERROR: swap_reload: staged pair incomplete in $real" >&2
    return 2
  fi
  local mode last
  mode=$(stat -c %a "${real}/config.toml")
  last=${mode: -1}
  if [[ "$last" != [0-5] ]]; then
    echo "ERROR: swap_reload: config.toml is world-writable" >&2
    return 2
  fi
  local f
  shopt -s nullglob
  for f in "$real"/*seqnum* "$real"/data/seqnum_*; do
    if [[ -e "$f" ]]; then
      echo "ERROR: swap_reload: staged DIR contains seqnum" >&2
      shopt -u nullglob
      return 2
    fi
  done
  shopt -u nullglob

  local tmpb tmpc
  tmpb="${BIN}.drill-new"
  tmpc="${CWD}/config.toml.drill-new"
  cp "${real}/fix_server" "$tmpb"
  chmod o-w "$tmpb" || chmod 0755 "$tmpb"
  cp "${real}/config.toml" "$tmpc"
  chmod 0600 "$tmpc"
  mv -f "$tmpb" "${BIN}"
  mv -f "$tmpc" "${CWD}/config.toml"

  # Cursor BEFORE pm2 reload returns.
  _lib_write_cursor
  # Reload from the ecosystem file with --update-env, NOT `pm2 reload <app>`.
  # A bare reload reuses pm2's stored environment. On 2026-09-23 an
  # apt-triggered `pm2 resurrect` rebuilt fix-server from a dump that predated
  # the credentials moving into the environment; the binary treats a missing
  # credential as fatal and crash-looped for six hours. Re-reading the
  # ecosystem file is what keeps the deploy honest about env changes.
  # `pm2 reload --only <name>` EXITS 0 when <name> matches no app. Measured on
  # an isolated pm2 instance with a two-app ecosystem:
  #   --only demo_app (no such app) -> exit 0, restarts unchanged (0,0)
  #   --only demo-app (correct)     -> exit 0, demo-app 0->1, other untouched
  # So the exit status cannot be trusted to mean "it restarted". Assert the app
  # exists first, and assert afterwards that it really came back.
  if ! _lib_pm2_app_exists; then
    echo "ERROR: swap_reload: pm2 has no app named '${PM2_APP}' -- refusing to claim a reload" >&2
    pm2 jlist 2>/dev/null | "$PYTHON" -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d=[]
print("       pm2 knows:", [p.get("name") for p in d] if isinstance(d,list) else d, file=sys.stderr)' || true
    return 1
  fi
  local pid_before uptime_before
  pid_before=$(_lib_pm2_env_field pm_id)
  uptime_before=$(_lib_pm2_env_field pm_uptime)

  if ! pm2 reload "$ECOSYSTEM" --only "$PM2_APP" --update-env; then
    if ! pm2 reload "$PM2_APP" --update-env; then
      echo "ERROR: swap_reload: pm2 reload $PM2_APP failed" >&2
      return 1
    fi
  fi

  local uptime_after
  uptime_after=$(_lib_pm2_env_field pm_uptime)
  if [[ -n "$uptime_before" && "$uptime_after" == "$uptime_before" ]]; then
    echo "ERROR: swap_reload: ${PM2_APP} did not restart (pm_uptime unchanged: ${uptime_before})" >&2
    echo "       the binary on disk was replaced but the OLD process is still serving" >&2
    return 1
  fi
  echo "swap_reload: ${PM2_APP} restarted (pm_uptime ${uptime_before:-?} -> ${uptime_after:-?}, pm_id ${pid_before:-?})"
  # Persist, so the next resurrect does not lose the environment again.
  pm2 save >/dev/null 2>&1 || echo "WARN: swap_reload: pm2 save failed" >&2
  return 0
}

health_check() {
  # Live-contract body: FATAL → crash-loop restart_time → Logon wait.
  # return N (never exit) so Case 4 can reach rollback. Callers must not
  # coerce a non-zero return to 0.
  HEALTH_STATUS=""
  export HEALTH_STATUS
  local startup_sz=0
  if [[ -f "$STARTUP_LOG" ]]; then
    startup_sz=$(_lib_file_size "$STARTUP_LOG")
  fi
  sleep 2
  local out err
  out=$(_lib_pm2_log_path out)
  err=$(_lib_pm2_log_path err)
  local cursor_out=0 cursor_err=0 cursor_cmd=0
  local cfile=${CURSOR_FILE:-}
  if [[ -z "$cfile" && -f "${CWD}/.rollback-drill-reload.cursor" ]]; then
    cfile="${CWD}/.rollback-drill-reload.cursor"
  fi
  if [[ -n "$cfile" && -f "$cfile" ]]; then
    local line
    line=$(head -n1 "$cfile")
    if [[ "$line" == out=* ]]; then
      cursor_out=${line#*out=}
      cursor_out=${cursor_out%% *}
      cursor_err=${line#*err=}
      cursor_err=${cursor_err%% *}
      if [[ "$line" == *cmd=* ]]; then
        cursor_cmd=${line#*cmd=}
        cursor_cmd=${cursor_cmd%% *}
      fi
    else
      # shellcheck disable=SC2086
      read -r cursor_out cursor_err _ <<<"$line" || true
    fi
  fi

  # A cursor larger than the file means the log rotated or was truncated after
  # the cursor was taken. Reading from it returns nothing, which looks exactly
  # like "the app never logged" -- a false negative. Start from 0 instead.
  local _sz
  if [[ -n "$out" && -f "$out" ]]; then
    _sz=$(_lib_file_size "$out")
    if [[ "${cursor_out:-0}" -gt "$_sz" ]]; then
      echo "health_check: cursor_out ${cursor_out} > size ${_sz} (log rotated?) -- resetting to 0" >&2
      cursor_out=0
    fi
  fi
  if [[ -n "$err" && -f "$err" ]]; then
    _sz=$(_lib_file_size "$err")
    if [[ "${cursor_err:-0}" -gt "$_sz" ]]; then
      echo "health_check: cursor_err ${cursor_err} > size ${_sz} (log rotated?) -- resetting to 0" >&2
      cursor_err=0
    fi
  fi
  # The audit trail has its own rotation (rotate_log_if_needed in main.rs), so
  # it needs the same clamp.
  if [[ -n "${COMMAND_LOG:-}" && -f "${COMMAND_LOG}" ]]; then
    _sz=$(_lib_file_size "$COMMAND_LOG")
    if [[ "${cursor_cmd:-0}" -gt "$_sz" ]]; then
      echo "health_check: cursor_cmd ${cursor_cmd} > size ${_sz} (audit log rotated?) -- resetting to 0" >&2
      cursor_cmd=0
    fi
  fi

  local fatal_hit=0
  if [[ -f "$STARTUP_LOG" ]]; then
    if tail -c +"$((startup_sz + 1))" "$STARTUP_LOG" 2>/dev/null | grep -E -q -- "$FATAL_GREP_ERE"; then
      fatal_hit=1
    fi
  fi
  if [[ "$fatal_hit" -eq 0 && -n "$err" ]]; then
    if _lib_grep_after "$err" "${cursor_err:-0}" "$FATAL_GREP_ERE" 1; then
      fatal_hit=1
    fi
  fi
  if [[ "$fatal_hit" -eq 0 && -n "$out" ]]; then
    if _lib_grep_after "$out" "${cursor_out:-0}" "$FATAL_GREP_ERE" 1; then
      fatal_hit=1
    fi
  fi
  if [[ "$fatal_hit" -eq 1 ]]; then
    echo "deploy.sh: FATAL detected in startup logs — automatic rollback" >&2
    HEALTH_STATUS=fatal
    export HEALTH_STATUS
    return 1
  fi

  local t1 t2 st1 st2
  t1=$(_lib_pm2_env_field restart_time)
  st1=$(_lib_pm2_env_field status)
  sleep "$CRASH_SAMPLE_GAP_S"
  t2=$(_lib_pm2_env_field restart_time)
  st2=$(_lib_pm2_env_field status)
  if [[ -n "$t1" && -n "$t2" && "$t1" != "$t2" ]]; then
    echo "WARNING: crash-loop detected: restart_time changed ($t1 -> $t2) while status=${st2:-$st1}" >&2
    HEALTH_STATUS=crash_loop
    export HEALTH_STATUS
    return 1
  fi

  # Diagnostics for the logon wait. On 2026-10-01 (v0.6.0) and 2026-10-02
  # (v0.6.1) this reported no_logon for a release that was demonstrably
  # serving: 16 logons, health ok. It reproduces on EVERY poller deploy and
  # NEVER on a manual `pm2 reload` + health_check. Static analysis eliminated
  # the cursor (none is written outside the drill, so the offset is 0), log
  # rotation (none at either deploy), pm2 access under systemd's environment,
  # the resolved log paths, and the grep pattern (-F, matches the real line).
  # Record what the loop actually sees so the next deploy identifies the cause.
  echo "health_check: out=[${out}] size=$(_lib_file_size "${out:-/dev/null}") cursor_out=${cursor_out:-0}" >&2
  echo "health_check: err=[${err}] size=$(_lib_file_size "${err:-/dev/null}") cursor_err=${cursor_err:-0}" >&2
  echo "health_check: LOGON_GREP=[${LOGON_GREP}] LOGON_WAIT_S=${LOGON_WAIT_S}" >&2
  echo "health_check: cmd=[${COMMAND_LOG:-}] size=$(_lib_file_size "${COMMAND_LOG:-/dev/null}") cursor_cmd=${cursor_cmd:-0} grep=[${COMMAND_LOG_GREP}]" >&2
  if [[ -n "$err" ]]; then
    echo "health_check: ACKs in err (whole file): $(grep -cF -- "$LOGON_GREP" "$err" 2>/dev/null || echo 0)" >&2
  fi

  # Anchor every piece of evidence to the process that is running NOW.
  #
  # Before 2026-10-17 this loop accepted anything appearing after a byte
  # cursor, in the audit trail or in either pm2 log. All three are shared
  # across a reload, and a reload is graceful, so the outgoing process could
  # satisfy the gate on its way out. Measured in drill run
  # 20261017T151306Z-1427385: case 3 deploys a config whose every host is
  # 127.0.0.1:1 and whose process can emit no FIX traffic at all, yet
  # deploy.sh returned exit 0 in 18s -- 9,616 bytes were already past the
  # cursor when the check first looked, written by the process being retired.
  # "Serving confirmed" meant nothing in that run.
  local new_pid new_start_ms
  new_pid=$(_lib_pm2_pid)
  new_start_ms=$(_lib_proc_start_ms "$new_pid" || true)
  echo "health_check: anchoring on pid=${new_pid:-?} start_ms=${new_start_ms:-UNKNOWN}" >&2

  local waited=0
  local logon=0
  if [[ -z "$new_start_ms" ]]; then
    # Fail closed. Without an anchor the only evidence available is evidence
    # we have just proven can come from the wrong process, and no_logon
    # reports rather than rolling back, so this is the safe direction.
    echo "ERROR: health_check: cannot read the start time of pm2 pid '${new_pid:-}' — refusing to confirm serving" >&2
  else
    while [[ "$waited" -lt "$LOGON_WAIT_S" ]]; do
      # The audit trail is the only source of truth here: it is written by
      # direct file I/O, so it survives RUST_LOG=warn, and the timestamp
      # filter ties it to this process. The pm2 logs are kept as diagnostics
      # below but no longer decide the outcome.
      if [[ -n "${COMMAND_LOG:-}" && -f "${COMMAND_LOG}" ]] \
         && _lib_audit_serving_since "$COMMAND_LOG" "${cursor_cmd:-0}" "$new_start_ms"; then
        logon=1
        break
      fi
      sleep 2
      waited=$((waited + 2))
    done
  fi
  if [[ "$logon" -eq 0 ]]; then
    echo "ERROR: no Logon ACK (35=A) from pid ${new_pid:-?} itself — serving NOT CONFIRMED" >&2
    echo "       (not rolling back: a venue outage and a broken binary look alike here)" >&2
    echo "       NOTE: counts below are for the WHOLE file and include the process this" >&2
    echo "       deploy replaced. Only evidence timestamped at or after the running" >&2
    echo "       process started (${new_start_ms:-unknown}) confirms serving." >&2
    echo "       diagnostics: err=[${err}] size=$(_lib_file_size "${err:-/dev/null}") cursor_err=${cursor_err:-0} waited=${waited}s" >&2
    if [[ -n "$err" ]]; then
      echo "       ACKs in err at give-up: $(grep -cF -- "$LOGON_GREP" "$err" 2>/dev/null || echo 0)" >&2
      echo "       audit-trail logons at give-up: $(grep -cF -- "$COMMAND_LOG_GREP" "${COMMAND_LOG:-/dev/null}" 2>/dev/null || echo 0)" >&2
      echo "       audit-trail market data at give-up: $(grep -cF -- "$COMMAND_LOG_GREP_ALT" "${COMMAND_LOG:-/dev/null}" 2>/dev/null || echo 0)" >&2
      echo "       last err line: $(tail -c 2000 "$err" 2>/dev/null | tail -1 | cut -c1-160)" >&2
    fi
    HEALTH_STATUS=no_logon
    export HEALTH_STATUS
    return 1
  fi
  HEALTH_STATUS=ok
  export HEALTH_STATUS
  return 0
}

rollback() {
  # LOUD_FAIL_MISSING_PREV
  local kind
  kind=$(prev_layout)
  kind=${kind%% *}
  local missing_path first_missing
  case "$kind" in
    absent)
      missing_path="${HOME}/.fix_server.prev"
      echo "FATAL: rollback cannot restore previous pair; missing path: ${missing_path}" >&2
      return "$ROLLBACK_LOUD_EXIT"
      ;;
    ambiguous)
      echo "ERROR: rollback: prev_layout_ambiguous" >&2
      return 1
      ;;
    binary_only)
      echo "ERROR: rollback: binary_only .prev at ${HOME}/.fix_server.prev — no config to restore" >&2
      return 1
      ;;
    siblings)
      first_missing=""
      if [[ ! -f "${HOME}/.fix_server.prev" ]]; then
        missing_path="${HOME}/.fix_server.prev"
        echo "ERROR: missing previous pair member: ${missing_path}" >&2
        first_missing=$missing_path
      fi
      if [[ ! -f "${HOME}/.fix_server.prev.config" ]]; then
        missing_path="${HOME}/.fix_server.prev.config"
        echo "ERROR: missing previous pair member: ${missing_path}" >&2
        first_missing=${first_missing:-$missing_path}
      fi
      if [[ -n "$first_missing" ]]; then
        echo "FATAL: rollback cannot restore previous pair; missing path: ${first_missing}" >&2
        return "$ROLLBACK_LOUD_EXIT"
      fi
      cp "${HOME}/.fix_server.prev" "${BIN}.drill-new"
      chmod o-w "${BIN}.drill-new" || true
      cp "${HOME}/.fix_server.prev.config" "${CWD}/config.toml.drill-new"
      chmod 0600 "${CWD}/config.toml.drill-new"
      mv -f "${BIN}.drill-new" "${BIN}"
      mv -f "${CWD}/config.toml.drill-new" "${CWD}/config.toml"
      if ! pm2 reload "$PM2_APP"; then
        echo "ERROR: rollback: pm2 reload failed" >&2
        return 1
      fi
      return 0
      ;;
    tar|tgz)
      echo "ERROR: rollback: archive layout restore not used on this path" >&2
      return 1
      ;;
    dir)
      _lib_dir_members "${HOME}/.fix_server.prev"
      first_missing=""
      if [[ -z "$PREV_BIN_MEMBER" || ! -f "$PREV_BIN_MEMBER" ]]; then
        missing_path="${HOME}/.fix_server.prev/fix_server"
        echo "ERROR: missing previous pair member: ${missing_path}" >&2
        first_missing=$missing_path
      fi
      if [[ -z "$PREV_CFG_MEMBER" || ! -f "$PREV_CFG_MEMBER" ]]; then
        missing_path="${HOME}/.fix_server.prev/config.toml"
        echo "ERROR: missing previous pair member: ${missing_path}" >&2
        first_missing=${first_missing:-$missing_path}
      fi
      if [[ -n "$first_missing" ]]; then
        echo "FATAL: rollback cannot restore previous pair; missing path: ${first_missing}" >&2
        return "$ROLLBACK_LOUD_EXIT"
      fi
      cp "$PREV_BIN_MEMBER" "${BIN}.drill-new"
      chmod o-w "${BIN}.drill-new" || true
      cp "$PREV_CFG_MEMBER" "${CWD}/config.toml.drill-new"
      chmod 0600 "${CWD}/config.toml.drill-new"
      mv -f "${BIN}.drill-new" "${BIN}"
      mv -f "${CWD}/config.toml.drill-new" "${CWD}/config.toml"
      if ! pm2 reload "$PM2_APP"; then
        echo "ERROR: rollback: pm2 reload failed" >&2
        return 1
      fi
      return 0
      ;;
    *)
      echo "ERROR: rollback: unknown layout $kind" >&2
      return 1
      ;;
  esac
}
