#!/usr/bin/env bash
# deploy-poll.sh — systemd ExecStart for hft-deploy-poll.service / timer.
# Pulls over HTTPS (never SSH). Unsets drill dual-env so a sticky parent cannot
# enable --staged. Rejects --staged / --staged=* on this argv. Then execs
# deploy.sh with no args so run_poller() does the real tag-fetch + CI-gate
# + on-box cargo test + snapshot → swap → health → rollback path.
set -euo pipefail

unset ROLLBACK_DRILL || true
unset HFT_DRILL_I_AM_ON_UAT || true

arg=""
for arg in "$@"; do
  case "$arg" in
    --staged|--staged=*)
      echo "ERROR: deploy-poll.sh does not accept --staged" >&2
      exit 2
      ;;
  esac
done

DEPLOY="$(dirname "$0")/deploy.sh"
exec "$DEPLOY" "$@"
