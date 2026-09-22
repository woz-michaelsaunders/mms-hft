#!/usr/bin/env bash
# deploy.sh — UAT pull-deploy (poller) and drill --staged path.
#
# There is deliberately no CI push/SSH deploy. The runner cluster cannot open
# outbound port 22 (.forgejo/workflows/release.yml:134-144). Deploys are PULLED:
# hft-deploy-poll.timer (every 2 min) → deploy-poll.sh → this file with no args.
#
# Poller contract (same as the on-box path the retired SSH job used to invoke
# as `deploy $SHA`, now self-hosted):
#   1. HTTPS fetch of tags; pick the highest vX.Y.Z newer than .deployed-tag
#   2. Require a published Forgejo release for that tag (release.yml green)
#   3. Require combined commit status success (CI build green on exact SHA)
#   4. Rerun `cargo test --release --locked` and `cargo build --release --locked`
#      on the box in a throwaway worktree
#   5. snapshot_prev → swap_reload → health_check → rollback on FATAL/crash-loop
# Forward only: a tag that fails a gate is recorded in .deploy-skip-tags and
# is not retried automatically.
#
# --staged DIR is a child-only drill interlock: both ROLLBACK_DRILL=1 and
# HFT_DRILL_I_AM_ON_UAT=1 must be the string 1. The parent drill never exports
# them. The poller unsets both and rejects --staged. --staged still runs
# snapshot + health-check + rollback; it only skips tag-fetch / CI / cargo.
#
# Live /home/ubuntu/deploy.sh was not readable on this builder (home mode 0750,
# host is not the UAT box). Operators must secret-review (C-REQ-4) against the
# box copy before replacing it. Box behavior is unchanged until that copy.
# After that review, re-diff health_check/rollback/snapshot_prev/swap_reload
# against the live step bodies (Physical LLD PR1 slots 8–9).
set -euo pipefail

LIB="$(dirname "$0")/deploy-lib.sh"
# shellcheck source=deploy-lib.sh
source "$LIB"
# LOUD_FAIL_MISSING_PREV — Case 4 skip detection greps this file (grep -F).

CWD="${CWD:-/home/ubuntu/fix_server}"
PM2_APP="${PM2_APP:-fix_server}"
DEPLOY_GIT_URL="${DEPLOY_GIT_URL:-https://git.gafondl.com/wozinga-group/HFT-Fix-API.git}"
DEPLOY_API_URL="${DEPLOY_API_URL:-https://git.gafondl.com/api/v1/repos/wozinga-group/HFT-Fix-API}"
DEPLOYED_TAG_FILE="${DEPLOYED_TAG_FILE:-$CWD/.deployed-tag}"
DEPLOYED_SHA_FILE="${DEPLOYED_SHA_FILE:-$CWD/.deployed-sha}"
DEPLOY_SKIP_TAGS="${DEPLOY_SKIP_TAGS:-$CWD/.deploy-skip-tags}"

_require_dual_env() {
  if [[ "${ROLLBACK_DRILL:-}" != "1" || "${HFT_DRILL_I_AM_ON_UAT:-}" != "1" ]]; then
    echo "ERROR: --staged requires ROLLBACK_DRILL=1 and HFT_DRILL_I_AM_ON_UAT=1" >&2
    exit 2
  fi
}

_http_get() {
  local url=$1
  local token=${FORGEJO_TOKEN:-${GITEA_TOKEN:-${GITHUB_TOKEN:-}}}
  if [[ -n "$token" ]]; then
    curl -fsS --max-time 30 -H "Authorization: token ${token}" "$url"
  else
    curl -fsS --max-time 30 "$url"
  fi
}

_http_code() {
  local url=$1
  local token=${FORGEJO_TOKEN:-${GITEA_TOKEN:-${GITHUB_TOKEN:-}}}
  if [[ -n "$token" ]]; then
    curl -sS -o /dev/null -w '%{http_code}' --max-time 30 -H "Authorization: token ${token}" "$url"
  else
    curl -sS -o /dev/null -w '%{http_code}' --max-time 30 "$url"
  fi
}

_record_skip_tag() {
  local tag=$1
  mkdir -p "$(dirname "$DEPLOY_SKIP_TAGS")"
  touch "$DEPLOY_SKIP_TAGS"
  if ! grep -F -qx "$tag" "$DEPLOY_SKIP_TAGS" 2>/dev/null; then
    printf '%s\n' "$tag" >>"$DEPLOY_SKIP_TAGS"
  fi
}

_tag_is_skipped() {
  local tag=$1
  [[ -f "$DEPLOY_SKIP_TAGS" ]] && grep -F -qx "$tag" "$DEPLOY_SKIP_TAGS"
}

_semver_gt() {
  # true if $1 > $2 for vX.Y.Z
  local a=$1 b=$2
  a=${a#v}
  b=${b#v}
  local IFS=.
  # shellcheck disable=SC2086
  set -- $a
  local a1=${1:-0} a2=${2:-0} a3=${3:-0}
  # shellcheck disable=SC2086
  set -- $b
  local b1=${1:-0} b2=${2:-0} b3=${3:-0}
  if [[ "$a1" -gt "$b1" ]]; then return 0; fi
  if [[ "$a1" -lt "$b1" ]]; then return 1; fi
  if [[ "$a2" -gt "$b2" ]]; then return 0; fi
  if [[ "$a2" -lt "$b2" ]]; then return 1; fi
  if [[ "$a3" -gt "$b3" ]]; then return 0; fi
  return 1
}

_list_version_tags() {
  local raw
  raw=$(git ls-remote --tags --refs "$DEPLOY_GIT_URL") || return 1
  printf '%s\n' "$raw" \
    | awk '{print $2}' \
    | sed 's#refs/tags/##' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -t. -k1.2,1n -k2,2n -k3,3n \
    || true
}

_sha_for_tag() {
  local tag=$1
  git ls-remote --tags --refs "$DEPLOY_GIT_URL" "refs/tags/${tag}" \
    | awk '{print $1}' \
    | head -n1
}

_require_release_green() {
  local tag=$1
  local code
  code=$(_http_code "${DEPLOY_API_URL}/releases/tags/${tag}" || echo 000)
  if [[ "$code" != "200" ]]; then
    echo "ERROR: deploy.sh: release job not green for ${tag} (HTTP ${code})" >&2
    return 1
  fi
  return 0
}

_require_ci_build_green() {
  local sha=$1
  local body state
  body=$(_http_get "${DEPLOY_API_URL}/commits/${sha}/status" || true)
  if [[ -z "$body" ]]; then
    echo "ERROR: deploy.sh: cannot read CI status for ${sha} (fail closed)" >&2
    return 1
  fi
  state=$(printf '%s' "$body" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)
  if [[ "$state" != "success" ]]; then
    echo "ERROR: deploy.sh: CI build not green on ${sha} (state=${state:-empty})" >&2
    return 1
  fi
  return 0
}

_build_and_test() {
  local sha=$1
  local work stage
  work=$(mktemp -d /tmp/hft-deploy-src-XXXXXX)
  stage=$(mktemp -d /tmp/hft-deploy-stage-XXXXXX)
  chmod 0700 "$work" "$stage"
  # Delete the clone on return; leave $stage for swap_reload.
  # shellcheck disable=SC2064
  trap 'rm -rf "$work"' RETURN
  if ! git clone --quiet "$DEPLOY_GIT_URL" "$work/repo" 2>/dev/null; then
    echo "ERROR: deploy.sh: git clone failed" >&2
    rm -rf "$stage"
    return 1
  fi
  git -C "$work/repo" checkout --quiet "$sha"
  if ! (
    cd "$work/repo"
    cargo test --release --locked
    cargo build --release --locked
  ); then
    echo "ERROR: deploy.sh: on-box cargo test/build failed for ${sha}" >&2
    rm -rf "$stage"
    return 1
  fi
  if [[ ! -f "$work/repo/target/release/fix_server" ]]; then
    echo "ERROR: deploy.sh: cargo build produced no fix_server" >&2
    rm -rf "$stage"
    return 1
  fi
  cp "$work/repo/target/release/fix_server" "$stage/fix_server"
  chmod o-w "$stage/fix_server" || chmod 0755 "$stage/fix_server"
  if [[ ! -f "${CWD}/config.toml" ]]; then
    echo "ERROR: deploy.sh: live config.toml missing under $CWD" >&2
    rm -rf "$stage"
    return 1
  fi
  cp "${CWD}/config.toml" "$stage/config.toml"
  chmod 0600 "$stage/config.toml"
  trap - RETURN
  printf '%s\n' "$stage"
}

_swap_health_rollback() {
  local staged=$1
  snapshot_prev || return $?
  swap_reload "$staged" || return $?
  local hc=0
  set +e
  health_check
  hc=$?
  set -e
  case "${HEALTH_STATUS:-}" in
    fatal|crash_loop)
      local rb=0
      set +e
      rollback
      rb=$?
      set -e
      return "$rb"
      ;;
    no_logon|ok)
      return 0
      ;;
    *)
      echo "ERROR: health_check did not classify" >&2
      if [[ "$hc" -ne 0 ]]; then
        return "$hc"
      fi
      return 1
      ;;
  esac
}

run_staged() {
  local dir=$1
  _require_dual_env
  if [[ -z "$dir" || "$dir" == -* ]]; then
    echo "ERROR: --staged DIR missing" >&2
    exit 2
  fi
  _swap_health_rollback "$dir"
  exit $?
}

run_poller() {
  unset ROLLBACK_DRILL || true
  unset HFT_DRILL_I_AM_ON_UAT || true

  local current="v0.0.0"
  if [[ -f "$DEPLOYED_TAG_FILE" ]]; then
    current=$(tr -d ' \n' <"$DEPLOYED_TAG_FILE")
    current=${current:-v0.0.0}
  fi

  local tags tag candidate="" sha=""
  if ! tags=$(_list_version_tags); then
    echo "ERROR: deploy.sh: cannot list tags from ${DEPLOY_GIT_URL}" >&2
    exit 1
  fi
  while IFS= read -r tag; do
    [[ -n "$tag" ]] || continue
    if _tag_is_skipped "$tag"; then
      continue
    fi
    if _semver_gt "$tag" "$current"; then
      candidate=$tag
    fi
  done <<<"$tags"

  if [[ -z "$candidate" ]]; then
    echo "deploy.sh: poller: no vX.Y.Z newer than ${current}; nothing to deploy"
    exit 0
  fi

  sha=$(_sha_for_tag "$candidate")
  if [[ -z "$sha" || ! "$sha" =~ ^[0-9a-f]{7,40}$ ]]; then
    echo "ERROR: deploy.sh: cannot resolve SHA for ${candidate}" >&2
    _record_skip_tag "$candidate"
    exit 1
  fi

  echo "deploy.sh: poller: candidate ${candidate} (${sha}) current=${current}"

  if ! _require_release_green "$candidate"; then
    _record_skip_tag "$candidate"
    exit 1
  fi
  if ! _require_ci_build_green "$sha"; then
    _record_skip_tag "$candidate"
    exit 1
  fi

  local staged
  if ! staged=$(_build_and_test "$sha"); then
    _record_skip_tag "$candidate"
    exit 1
  fi

  local rc=0
  set +e
  _swap_health_rollback "$staged"
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    _record_skip_tag "$candidate"
    exit "$rc"
  fi
  mkdir -p "$CWD"
  printf '%s\n' "$candidate" >"$DEPLOYED_TAG_FILE"
  printf '%s\n' "$sha" >"$DEPLOYED_SHA_FILE"
  echo "deploy.sh: poller: deployed ${candidate} (${sha})"
  exit 0
}

parse_deploy_args() {
  local staged=""
  local saw_staged=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --staged)
        if [[ "$saw_staged" -eq 1 ]]; then
          echo "ERROR: unknown flag: --staged" >&2
          exit 2
        fi
        if [[ $# -lt 2 || "$2" == -* ]]; then
          echo "ERROR: --staged DIR missing" >&2
          exit 2
        fi
        saw_staged=1
        staged=$2
        shift 2
        ;;
      --*)
        echo "ERROR: unknown flag: $1" >&2
        exit 2
      ;;
      -*)
        echo "ERROR: unknown flag: $1" >&2
        exit 2
        ;;
      *)
        echo "ERROR: unexpected argument: $1" >&2
        exit 2
        ;;
    esac
  done
  if [[ -n "$staged" ]]; then
    run_staged "$staged"
  else
    run_poller
  fi
}

parse_deploy_args "$@"
