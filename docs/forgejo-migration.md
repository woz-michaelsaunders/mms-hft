# Moving this repo back to a canonical Forgejo

Decision (2026-10-10, Michael): **Forgejo stays canonical.** A second Forgejo
instance will be stood up for *external* sites, and this repo's history will be
pushed there when it is up. GitHub is a temporary holding location, not the
long-term home.

## Why the move happened at all

`git.gandetl.com` is an internal-only name, reachable only over the VPN. This
box lost its path to the internal network across the kernel-upgrade reboot on
2026-10-10, so the name returned NXDOMAIN while `gandetl.com`,
`www.gandetl.com` and `wozinga.ppm.gandetl.com` still resolved. The deploy
poller and drift check both went down, and nothing could be pushed.

HFT is an external project, which is why it was the only repo affected.

## Current state

| thing | where |
|---|---|
| history, temporarily | `github.com/woz-michaelsaunders/mms-hft` (42 branches, 32 tags) |
| canonical, pending | the new external Forgejo (host TBD) |
| production runs | `v0.11.0` @ `6a619d5b` — matches GitHub master |

**Lost in the move and not recreated:** the `#83` merge commit and tags
`v0.8.0`, `v0.9.0`, `v0.10.0`. The last fetch predated them, the objects are
not in any local store, and the clone the v0.10.0 binary was built from had
been cleaned up. The *content* of `#83` was recreated by hand in `6a619d5b`
(one `info!` → `warn!` on the kdb reconnect success line). Tagging continues
from `v0.7.0` to `v0.11.0`; the three missing numbers are deliberately skipped
rather than faked onto different commits.

## No code changes are needed

Every endpoint and credential is already env-overridable, and the poller unit
currently sets no overrides — it is simply falling back to the old host:

    deploy.sh:43   DEPLOY_GIT_URL="${DEPLOY_GIT_URL:-https://git.gandetl.com/...}"
    deploy.sh:44   DEPLOY_API_URL="${DEPLOY_API_URL:-https://git.gandetl.com/api/v1/...}"
    deploy.sh:60   FORGEJO_USER        (default: michael)
    deploy.sh:61   FORGEJO_TOKEN_FILE  (default: /home/ubuntu/.tokens/forgejo.txt)
    check-drift.sh:22-24  DEPLOY_API_URL, DRIFT_REF, FORGEJO_TOKEN_FILE

The code depends on four Forgejo API endpoints, all standard:

| endpoint | used for |
|---|---|
| `/commits/{sha}/status` | the CI gate |
| `/git/tags/{sha}` | dereferencing an annotated tag to its commit |
| `/releases/tags/{tag}` | the release gate |
| `/raw/{path}` | the drift check |

This is the reason to prefer Forgejo over GitHub here: GitHub would have
required all four gates rewritten against a different API shape, and the
Forgejo Actions workflows replaced with GitHub Actions. Staying on Forgejo
keeps the hard-won fixes in those gates untouched — in particular the
annotated-tag dereference and the authenticated-request fixes, both of which
cost a stuck release to find.

## Switchover, once the external Forgejo is up

1. **Create the repo** on the new instance, e.g. `wozinga-group/HFT-Fix-API`.

2. **Push the history.** From this box:

       cd /home/ubuntu/deploy-build
       git remote add fj2 https://<new-host>/wozinga-group/HFT-Fix-API.git
       git push fj2 master:master
       git push fj2 --tags
       git push fj2 'refs/heads/*:refs/heads/*'

   Or clone from GitHub and push up, if that is easier from wherever the new
   instance lives.

3. **Mint a token** and write it to `/home/ubuntu/.tokens/forgejo.txt`,
   `chmod 0600`. Scopes needed:

   - `read:repository` — the four gates above
   - `write:repository` — pushing branches, opening and merging PRs
   - `write:issue` — **worth including this time.** The old token lacked it,
     which is why two asks are sitting in `~/asks/` as files instead of in a
     tracker: `order-entry-gate.md` (HFT-059) and
     `repoint-health-assertions.md`.

4. **Point the units at it** — three lines each, no code edits:

       # /etc/systemd/system/hft-deploy-poll.service
       Environment=DEPLOY_GIT_URL=https://<new-host>/wozinga-group/HFT-Fix-API.git
       Environment=DEPLOY_API_URL=https://<new-host>/api/v1/repos/wozinga-group/HFT-Fix-API
       Environment=FORGEJO_USER=<user>

       # /etc/systemd/system/hft-drift.service
       Environment=DEPLOY_API_URL=https://<new-host>/api/v1/repos/wozinga-group/HFT-Fix-API

   then `sudo systemctl daemon-reload`. Both units are versioned under
   `systemd/`, so commit the change rather than only editing on the box —
   `scripts/check-drift.sh` compares them.

5. **Recreate the Release workflow.** The release gate requires a release
   object per tag; without it the poller logs
   `not ready yet (release pending)` forever and never deploys. It retries
   rather than blacklisting, so this fails safe, but it fails.

6. **Verify, in this order:**

       # tags are listable (this was the first thing to break)
       sudo systemctl start hft-deploy-poll.service
       sudo journalctl -u hft-deploy-poll -n 20 --no-pager

       # drift check compares 21 files instead of reporting UNREADABLE
       /home/ubuntu/hft-deploy/check-drift.sh

       # then a real tag, end to end; the deploy must print the restart
       # assertion before it records the tag:
       #   swap_reload: fix-server restarted (pm_uptime X -> Y)

## Watch for

- **The box must reach the new host.** That is the whole point: the old one was
  VPN-only. Confirm with `getent hosts <new-host>` from this box, not from a
  laptop.
- **`check-drift.sh` reports `UNREADABLE`, not `DRIFT`, when the host is
  unreachable** — deliberate, so an outage is never mistaken for divergence.
  It currently compares 0 files; after the switch it should compare 21.
- **Don't re-use the lost tag numbers.** `v0.8.0`–`v0.10.0` refer to commits
  that no longer exist anywhere.
