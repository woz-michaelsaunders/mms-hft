#!/usr/bin/env bash
# Compare the operational files INSTALLED ON THE BOX against the repository.
#
# Why this exists: between 2026-09-27 and 2026-10-07, eight fixes were made by
# editing files in place on the box, never committed, and then silently
# reverted when the box was synced from master. Four of them surfaced together
# on 6 October and cost thirteen hours of a stuck release:
#
#   - _http_get/_http_code read FORGEJO_TOKEN from the environment only, so
#     every API call went out unauthenticated and a private repo answered 404
#   - _sha_for_tag returned the annotated tag OBJECT, so the CI gate read
#     state=empty on a green build
#   - three gates skip-listed on transient failures, stranding a good release
#   - cargo test stdout was captured as the staged directory
#
# None of that was detectable, because nothing compared the box to the repo.
# This script does, and exits non-zero when they differ.
#
# It is read-only. It never installs, never overwrites, never restarts.
set -euo pipefail

: "${DEPLOY_API_URL:=https://git.gandetl.com/api/v1/repos/wozinga-group/HFT-Fix-API}"
: "${DRIFT_REF:=master}"
: "${FORGEJO_TOKEN_FILE:=/home/ubuntu/.tokens/forgejo.txt}"

_token() {
  local t=${FORGEJO_TOKEN:-${GITEA_TOKEN:-${GITHUB_TOKEN:-}}}
  if [[ -z "$t" && -r "$FORGEJO_TOKEN_FILE" ]]; then
    t=$(tr -d ' \n\r' <"$FORGEJO_TOKEN_FILE")
  fi
  printf '%s' "$t"
}

# repo path -> installed path. Keep in step with whatever the box actually runs.
MAP=(
  "scripts/deploy.sh|/home/ubuntu/hft-deploy/deploy.sh"
  "scripts/deploy-lib.sh|/home/ubuntu/hft-deploy/deploy-lib.sh"
  "scripts/deploy-poll.sh|/home/ubuntu/hft-deploy/deploy-poll.sh"
  "scripts/rollback-drill.sh|/home/ubuntu/hft-deploy/rollback-drill.sh"
  "scripts/hft-health.sh|/home/ubuntu/hft-health.sh"
  "scripts/check-drift.sh|/home/ubuntu/hft-deploy/check-drift.sh"
  "ecosystem.config.js|/home/ubuntu/ecosystem.config.js"
  "mgmt-server/app.py|/home/ubuntu/mgmt-server/app.py"
  "mgmt-server/kdb_ipc.py|/home/ubuntu/mgmt-server/kdb_ipc.py"
  "mgmt-server/kdb_client.py|/home/ubuntu/mgmt-server/kdb_client.py"
  "mgmt-server/logging_config.py|/home/ubuntu/mgmt-server/logging_config.py"
  "systemd/hft-health.service|/etc/systemd/system/hft-health.service"
  "systemd/hft-health.timer|/etc/systemd/system/hft-health.timer"
  "systemd/hft-deploy-poll.service|/etc/systemd/system/hft-deploy-poll.service"
  "systemd/hft-deploy-poll.timer|/etc/systemd/system/hft-deploy-poll.timer"
  "systemd/kdb-server.service|/etc/systemd/system/kdb-server.service"
  "systemd/kdb-server-prod.service|/etc/systemd/system/kdb-server-prod.service"
  "systemd/hft-drift.service|/etc/systemd/system/hft-drift.service"
  "systemd/hft-drift.timer|/etc/systemd/system/hft-drift.timer"
)

TOKEN=$(_token)
tmp=$(mktemp -d /tmp/drift-XXXXXX)
trap 'rm -rf "$tmp"' EXIT

drift=0
unreadable=0
checked=0

for entry in "${MAP[@]}"; do
  repo_path=${entry%%|*}
  box_path=${entry##*|}

  # Fetch the repo copy. A transient failure must not be reported as drift:
  # that mistake is what blacklisted a good release on 6 October.
  code=0
  if [[ -n "$TOKEN" ]]; then
    code=$(curl -sS -o "$tmp/want" -w '%{http_code}' --max-time 30 \
      -H "Authorization: token ${TOKEN}" \
      "${DEPLOY_API_URL}/raw/${repo_path}?ref=${DRIFT_REF}" || echo 000)
  else
    code=$(curl -sS -o "$tmp/want" -w '%{http_code}' --max-time 30 \
      "${DEPLOY_API_URL}/raw/${repo_path}?ref=${DRIFT_REF}" || echo 000)
  fi

  if [[ "$code" == "404" ]]; then
    printf 'NOT-IN-REPO  %s\n' "$repo_path"
    drift=1
    continue
  fi
  if [[ "$code" != "200" ]]; then
    printf 'UNREADABLE   %s (HTTP %s) -- not counted as drift\n' "$repo_path" "$code"
    unreadable=1
    continue
  fi

  if [[ ! -f "$box_path" ]]; then
    printf 'MISSING      %s (expected from %s)\n' "$box_path" "$repo_path"
    drift=1
    continue
  fi

  checked=$((checked + 1))
  if diff -q "$tmp/want" "$box_path" >/dev/null 2>&1; then
    printf 'OK           %s\n' "$box_path"
  else
    printf 'DRIFT        %s differs from %s@%s\n' "$box_path" "$repo_path" "$DRIFT_REF"
    diff -u "$tmp/want" "$box_path" 2>/dev/null | head -20 | sed 's/^/             /'
    drift=1
  fi
done

printf -- '--- %s file(s) compared against %s ---\n' "$checked" "$DRIFT_REF"

if [[ "$drift" -ne 0 ]]; then
  echo "DRIFT: the box does not match the repository. Commit the box change, or reinstall from the repo." >&2
  exit 1
fi
if [[ "$unreadable" -ne 0 ]]; then
  echo "WARN: some files could not be read from the repository; drift undetermined for those." >&2
  exit 0
fi
echo "no drift"
exit 0
