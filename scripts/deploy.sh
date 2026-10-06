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
# pm2 lists this app HYPHENATED. With the underscore, `pm2 reload <eco> --only
# fix_server` matches nothing and EXITS 0, so the binary is swapped on disk and
# nothing restarts -- the exact failure that recorded v0.5.0 as deployed on
# 2026-09-27, and v0.6.0/v0.6.1/v0.7.0 since, while the old process kept
# running. rollback-drill.sh already carried this fix; the deploy path did not.
PM2_APP="${PM2_APP:-fix-server}"
DEPLOY_GIT_URL="${DEPLOY_GIT_URL:-https://git.gandetl.com/wozinga-group/HFT-Fix-API.git}"
DEPLOY_API_URL="${DEPLOY_API_URL:-https://git.gandetl.com/api/v1/repos/wozinga-group/HFT-Fix-API}"
DEPLOYED_TAG_FILE="${DEPLOYED_TAG_FILE:-$CWD/.deployed-tag}"
DEPLOYED_SHA_FILE="${DEPLOYED_SHA_FILE:-$CWD/.deployed-sha}"
DEPLOY_SKIP_TAGS="${DEPLOY_SKIP_TAGS:-$CWD/.deploy-skip-tags}"

_require_dual_env() {
  if [[ "${ROLLBACK_DRILL:-}" != "1" || "${HFT_DRILL_I_AM_ON_UAT:-}" != "1" ]]; then
    echo "ERROR: --staged requires ROLLBACK_DRILL=1 and HFT_DRILL_I_AM_ON_UAT=1" >&2
    exit 2
  fi
}

# Git over HTTPS needs the same Forgejo credential the API calls use. Without
# it `git ls-remote` and `git clone` fail on a private repo with "could not read
# Username", and the poller can never see a tag. FORGEJO_TOKEN_FILE keeps the
# secret off the process list and out of the URL.
: "${FORGEJO_USER:=michael}"
: "${FORGEJO_TOKEN_FILE:=/home/ubuntu/.tokens/forgejo.txt}"
git_net() {
  local token=${FORGEJO_TOKEN:-}
  if [[ -z "$token" && -r "$FORGEJO_TOKEN_FILE" ]]; then
    token=$(tr -d ' \n\r' <"$FORGEJO_TOKEN_FILE")
  fi
  if [[ -z "$token" ]]; then
    git "$@"
    return
  fi
  git -c credential.helper="!f(){ echo username=${FORGEJO_USER}; echo password=${token}; };f" "$@"
}

# Single source of truth for the API token. This MUST agree with git_net
# above, which falls back to FORGEJO_TOKEN_FILE: the systemd poller unit sets
# no Environment=FORGEJO_TOKEN, so when the HTTP helpers looked at the env var
# alone they sent every API call unauthenticated. On a private repo Forgejo
# answers 404, so _require_release_green and _require_ci_build_green failed
# with "HTTP 404" for releases that existed and CI that was green -- and then
# skip-listed the tag. That is what actually stranded v0.7.0.
_forgejo_token() {
  local token=${FORGEJO_TOKEN:-${GITEA_TOKEN:-${GITHUB_TOKEN:-}}}
  if [[ -z "$token" && -r "$FORGEJO_TOKEN_FILE" ]]; then
    token=$(tr -d ' \n\r' <"$FORGEJO_TOKEN_FILE")
  fi
  printf '%s' "$token"
}

_http_get() {
  local url=$1
  local token; token=$(_forgejo_token)
  if [[ -n "$token" ]]; then
    curl -fsS --max-time 30 -H "Authorization: token ${token}" "$url"
  else
    curl -fsS --max-time 30 "$url"
  fi
}

_http_code() {
  local url=$1
  local token; token=$(_forgejo_token)
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
  raw=$(git_net ls-remote --tags --refs "$DEPLOY_GIT_URL") || return 1
  printf '%s\n' "$raw" \
    | awk '{print $2}' \
    | sed 's#refs/tags/##' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -t. -k1.2,1n -k2,2n -k3,3n \
    || true
}

# Resolve a tag to the COMMIT it names.
#
# ls-remote hands back the sha of the ref itself, which for an ANNOTATED tag is
# the tag OBJECT, not the commit. Forgejo does not advertise the peeled
# "refs/tags/<t>^{}" line when a pattern is given, so there is nothing to strip
# locally -- the tag has to be dereferenced through the API.
#
# This matters because commit statuses hang off the commit. v0.7.0 is the
# annotated tag object cce22f8b naming commit db217b18:
#   /commits/cce22f8b.../status -> {"state":"","total_count":0}   (nothing)
#   /commits/db217b18.../status -> {"state":"success","total_count":4}
# so the CI gate read state=empty and refused a build that was green.
_sha_for_tag() {
  local tag=$1 sha body obj
  sha=$(git_net ls-remote --tags "$DEPLOY_GIT_URL" "refs/tags/${tag}" \
    | awk '$2 ~ /\^\{\}$/ {p=$1} $2 == "refs/tags/'"${tag}"'" {t=$1} END {print (p != "" ? p : t)}')
  [[ -z "$sha" ]] && return 0

  # If $sha is a tag object, peel it to object.sha. A 404/empty body means it
  # was already a commit, so keep it as-is.
  body=$(_http_get "${DEPLOY_API_URL}/git/tags/${sha}" 2>/dev/null || true)
  if [[ -n "$body" ]]; then
    obj=$(printf '%s' "$body" \
      | sed -n 's/.*"object"[[:space:]]*:[[:space:]]*{[^}]*"sha"[[:space:]]*:[[:space:]]*"\([0-9a-f]\{7,40\}\)".*/\1/p' \
      | head -n1)
    if [[ -n "$obj" ]]; then
      sha=$obj
    fi
  fi
  printf '%s\n' "$sha"
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
  if ! git_net clone --quiet "$DEPLOY_GIT_URL" "$work/repo" 2>/dev/null; then
    echo "ERROR: deploy.sh: git clone failed" >&2
    rm -rf "$stage"
    return 1
  fi
  git -C "$work/repo" checkout --quiet "$sha"
  # >&2 is load-bearing. This function communicates the staged directory to its
  # caller on stdout (printf at the end), and `cargo test` writes its results
  # ("running 14 tests", "test result: ok. ...") to STDOUT, not stderr. Without
  # the redirect, staged=$(_build_and_test ...) captures the whole test log
  # followed by the directory, and the swap then refuses it:
  #
  #   ERROR: swap_reload: DIR contains a newline
  #
  # which is what stopped v0.7.0 on 2026-10-06 after an 18-minute build that
  # had already passed. Sending all cargo output to stderr keeps stdout clean
  # and still puts the build log in the journal.
  if ! (
    cd "$work/repo"
    cargo test --release --locked
    cargo build --release --locked
  ) >&2; then
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
  # Do NOT clear the RETURN trap here. It only ever removed "$work" (the clone),
  # never "$stage", so clearing it protected nothing and leaked the clone on
  # every SUCCESSFUL build -- the failure paths return early with the trap still
  # armed, so only successes leaked. 16 of them had accumulated by 2026-10-06,
  # about 650MB each, and the box was at 88% of its 48G root with 6.1G free.
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
    ok)
      return 0
      ;;
    no_logon)
      # NOT a success. The binary IS installed, so .deployed-tag is still
      # written by the caller -- the poller is forward-only and withholding it
      # would redeploy the same tag every two minutes. But "installed" and
      # "serving" are different claims, and conflating them is how v0.5.0 was
      # recorded as deployed on 2026-09-27 while the OLD process was still
      # running. Leave a marker the health check can surface, and exit
      # non-zero so systemd records the unit as failed.
      #
      # Rollback stays off deliberately: a venue outage is indistinguishable
      # from a broken binary here, and rolling back a healthy release during a
      # venue blip would be worse than the thing being guarded against.
      : > "${CWD}/.deploy-unconfirmed" 2>/dev/null || true
      echo "ALERT: deploy installed but serving NOT CONFIRMED — marker at ${CWD}/.deploy-unconfirmed" >&2
      return 4
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
    # TRANSIENT, not permanent. This is a network call to Forgejo; a 503 from
    # the front end looks exactly like a missing tag here. Skip-listing on it
    # strands a perfectly good release forever, because the poller is
    # forward-only and never revisits a skipped tag.
    #
    # That is not hypothetical: v0.7.0 was blacklisted on 2026-10-06 by a
    # Forgejo 503 ("remote: no available server"), and the deploy then reported
    # "nothing to deploy" indefinitely while the release sat there.
    echo "deploy.sh: poller: cannot resolve SHA for ${candidate} (Forgejo unreachable?) — will retry next poll" >&2
    exit 0
  fi

  echo "deploy.sh: poller: candidate ${candidate} (${sha}) current=${current}"

  # Also transient. The Release workflow starts when the tag is pushed and takes
  # minutes; the poller runs every two. Neither "the release does not exist yet"
  # nor "CI has not finished" is a permanent verdict, and a genuinely failed CI
  # simply never goes green, so retrying costs one API call per poll and strands
  # nothing. Only a definite failure below earns a skip.
  if ! _require_release_green "$candidate"; then
    echo "deploy.sh: poller: ${candidate} not ready yet (release pending or unreachable) — will retry next poll"
    exit 0
  fi
  if ! _require_ci_build_green "$sha"; then
    echo "deploy.sh: poller: ${candidate} not ready yet (CI pending or not green) — will retry next poll"
    exit 0
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
  if [[ "$rc" -eq 4 ]]; then
    # Installed but serving unconfirmed. Record the tag anyway: the binary IS
    # on disk, and leaving the state file naming the previous tag would be its
    # own lie -- and the poller is forward-only, so a withheld tag is simply
    # never retried. Do NOT skip-list it: there is nothing to retry, it is
    # already installed. Exit non-zero so systemd marks the unit failed and the
    # marker is visible.
    mkdir -p "$CWD"
    printf '%s\n' "$candidate" >"$DEPLOYED_TAG_FILE"
    printf '%s\n' "$sha" >"$DEPLOYED_SHA_FILE"
    echo "deploy.sh: poller: INSTALLED ${candidate} (${sha}) but serving NOT CONFIRMED" >&2
    exit 4
  fi
  if [[ "$rc" -ne 0 ]]; then
    _record_skip_tag "$candidate"
    exit "$rc"
  fi
  mkdir -p "$CWD"
  rm -f "${CWD}/.deploy-unconfirmed" 2>/dev/null || true
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
