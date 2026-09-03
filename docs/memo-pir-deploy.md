# Memo PIR fleet deployment

The `Deploy memo PIR fleet` workflow deploys the full-pool mainnet POC after the
repository's `CI` workflow succeeds on `main`. It is independent of the legacy
single-host `Deploy spend-server` workflow.

## Deployment environment

Create a GitHub Environment named `memo-pir-mainnet-poc`, restrict it to the
`main` branch, and do not add a required-reviewer rule. Configure these
environment variables:

| Variable | Meaning |
| --- | --- |
| `MEMO_COORDINATOR_HOST` | Public SSH hostname or IPv4 address of the coordinator |
| `MEMO_DEPLOY_USER` | SSH user authorized to install and restart the memo services |
| `MEMO_PUBLIC_URL` | HTTPS origin used for final health and dummy-query checks |
| `MEMO_SSH_KNOWN_HOSTS` | Pinned OpenSSH host-key lines for every configured host |
| `MEMO_WORKERS_JSON` | Ordered worker deployment and private-service inventory |

The deployment job runs on the repository-scoped coordinator runner labeled
`memo-pir-deploy`. Build and pull-request jobs remain on GitHub-hosted runners.
The coordinator runner reaches itself over loopback and workers over their
private VPC addresses, so fleet SSH does not need to be exposed to GitHub's
public runner address ranges. Do not use the deployment runner from a
`pull_request`-triggered workflow; this repository is public.

Configure these GitHub Environment secrets:

| Secret | Meaning |
| --- | --- |
| `MEMO_DEPLOY_SSH_KEY` | Dedicated SSH private key authorized on every fleet host |
| `PIR_APM_SLACK_WEBHOOK_URL` | Optional. Slack incoming webhook for `pir-apm` alerts; without it the sidecar logs alerts to the journal |

`PIR_APM_SLACK_WEBHOOK_URL` lives in the same Infisical project and path as the
deployment key (see below). The deploy never prints it; it is written only to
`/etc/default/pir-apm` (root, mode 0600) on the coordinator.

The worker inventory has this shape:

```json
[
  {
    "name": "worker-1",
    "ssh_host": "worker-1.example.net",
    "service_url": "http://10.142.0.4:8091"
  },
  {
    "name": "worker-2",
    "ssh_host": "worker-2.example.net",
    "service_url": "http://10.142.0.2:8091"
  }
]
```

The array is append-only. Never rename, reorder, remove, or change the private
URL of an existing entry: shard ownership is derived from this order. Adding a
machine means appending one object and ensuring the coordinator can reach its
`service_url` through the private firewall.

`MEMO_SSH_KNOWN_HOSTS` must be populated from host keys verified through an
operator-controlled channel. The workflow deliberately never calls
`ssh-keyscan` and uses strict host-key checking.

## Deployment key

Store a dedicated fleet SSH private key as `MEMO_DEPLOY_SSH_KEY` in the
Valargroup Infisical production environment, project `spendability-pir-deploy`,
path `/memo-pir`. Mirror the same value into the protected
`memo-pir-mainnet-poc` GitHub Environment secret. Do not reuse an operator's
personal key. Authorize only its public half on the configured hosts.

Infisical remains the rotation source of truth; GitHub holds the runtime copy
used by Actions. To rotate the key, create and store a replacement in Infisical,
authorize its public half on every host, update the GitHub Environment secret,
run a manual preflight, and then remove the previous public key from the hosts.

## Monitoring sidecar and reverse proxy

The coordinator also runs `pir-apm` (`deploy/pir-apm`), an APM dashboard and
Slack alerting sidecar that scrapes the coordinator's loopback `/metrics`,
`/memo/health`, and `/ready`. The deploy installs the binary at
`/usr/local/bin/pir-apm`, its unit, and `/etc/default/pir-apm`, then enables and
restarts the service.

The same deploy now owns `/etc/caddy/Caddyfile` on the coordinator. The
committed template `infra/digitalocean/memo-poc/deploy/Caddyfile` is rendered
with the host of `MEMO_PUBLIC_URL`, validated with `caddy validate`, installed,
and reloaded. It proxies `/apm/*` to the sidecar, returns 404 for `/metrics` and
`/ready`, and sends everything else to the coordinator. A preflight run prints
the diff between the live and staged Caddyfile without installing it.

After a deploy the dashboard is at `<MEMO_PUBLIC_URL>/apm/`. It is
unauthenticated by design and shows only aggregate metrics; see
`deploy/pir-apm/README.md` for the alert catalogue and manual test commands.

## Rollout and rollback

The workflow builds `memo-pir-server`, `memo-pir-worker`, `memo-pir-cli`, and
`pir-apm` at the exact successful CI revision. It uploads and checksum-verifies
everything before changing a service. The coordinator is then stopped, workers
are updated and checked, and the coordinator is updated with
`/etc/memo-pir/workers.json`, followed by the sidecar and Caddyfile.

Success requires all of the following:

1. every worker returns `status: ok` from its loopback health endpoint;
2. the coordinator reaches `serving` within 45 minutes;
3. public metadata remains mainnet/Ironwood and does not regress in height or
   tree size;
4. `memo-pir-cli dummy` completes a private query through the public endpoint;
5. the coordinator's loopback `/ready` returns 200; and
6. `<MEMO_PUBLIC_URL>/apm/` renders the dashboard while `<MEMO_PUBLIC_URL>/metrics`
   returns 404.

Each activation saves the prior binaries and service configuration under
`/opt/memo-pir/rollback`, including the previous `pir-apm` binary, unit,
environment file, and Caddyfile. A failed rollout restores every host already
changed, restarts the prior coordinator and sidecar (or disables the sidecar if
this was its first install), reloads the previous Caddyfile, prints bounded
service logs, and leaves the workflow failed. Persistent Zakura, memo-journal, and PIR-artifact directories
are never replaced.

## Manual preflight and redeployment

Use `workflow_dispatch` with a full commit SHA that is reachable from `main` and
already has a successful `CI` run. Leave `preflight_only` enabled to verify SSH,
host architecture, disk space, topology, uploads, and checksums without stopping
services. Disable it only to redeploy that exact tested revision.
