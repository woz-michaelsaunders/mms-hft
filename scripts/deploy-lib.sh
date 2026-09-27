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
: "${PM2_APP:=fix_server}"
: "${ROLLBACK_LOUD_EXIT:=3}"
: "${CRASH_SAMPLE_GAP_S:=10}"
: "${LOGON_WAIT_S:=30}"
: "${LOGON_GREP:=Logon ACK (35=A) received}"
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
  if [[ "$ere" == "1" ]]; then
    tail -c +"$((offset + 1))" "$file" | grep -E -q -- "$pattern"
  else
    tail -c +"$((offset + 1))" "$file" | grep -F -q -- "$pattern"
  fi
}

_lib_write_cursor() {
  [[ "${ROLLBACK_DRILL:-}" == "1" || -n "${CURSOR_FILE:-}" ]] || return 0
  local out err startup dest os es ss mono
  out=$(_lib_pm2_log_path out)
  err=$(_lib_pm2_log_path err)
  startup=$STARTUP_LOG
  os=$(_lib_file_size "${out:-/dev/null}")
  es=$(_lib_file_size "${err:-/dev/null}")
  ss=$(_lib_file_size "${startup:-/dev/null}")
  mono=$("$PYTHON" -c 'import time; print(int(time.monotonic()*1000))' 2>/dev/null || echo 0)
  if [[ -n "${CURSOR_FILE:-}" ]]; then
    dest=$CURSOR_FILE
  else
    dest="${CWD}/.rollback-drill-reload.cursor"
  fi
  mkdir -p "$(dirname "$dest")" 2>/dev/null || true
  # Two-integer prefix keeps existing drill parsers; key=value is the LLD line.
  printf 'out=%s err=%s startup=%s mono_ms=%s\n' "$os" "$es" "$ss" "$mono" >"$dest"
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
  if ! pm2 reload "$ECOSYSTEM" --only "$PM2_APP" --update-env; then
    if ! pm2 reload "$PM2_APP" --update-env; then
      echo "ERROR: swap_reload: pm2 reload $PM2_APP failed" >&2
      return 1
    fi
  fi
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
  local cursor_out=0 cursor_err=0
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
    else
      # shellcheck disable=SC2086
      read -r cursor_out cursor_err _ <<<"$line" || true
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

  local waited=0
  local logon=0
  while [[ "$waited" -lt "$LOGON_WAIT_S" ]]; do
    if [[ -n "$out" ]] && _lib_grep_after "$out" "${cursor_out:-0}" "$LOGON_GREP" 0; then
      logon=1
      break
    fi
    if [[ -n "$err" ]] && _lib_grep_after "$err" "${cursor_err:-0}" "$LOGON_GREP" 0; then
      logon=1
      break
    fi
    sleep 2
    waited=$((waited + 2))
  done
  if [[ "$logon" -eq 0 ]]; then
    echo "WARNING: no Logon ACK (35=A) received within check window — not rolling back (ambiguous: venue/network vs binary)" >&2
    HEALTH_STATUS=no_logon
    export HEALTH_STATUS
    return 0
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
