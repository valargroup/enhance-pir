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

Configure this GitHub Environment secret:

| Secret | Meaning |
| --- | --- |
| `MEMO_DEPLOY_SSH_KEY` | Dedicated SSH private key authorized on every fleet host |

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

## Rollout and rollback

The workflow builds `memo-pir-server`, `memo-pir-worker`, and `memo-pir-cli` at
the exact successful CI revision. It uploads and checksum-verifies everything
before changing a service. The coordinator is then stopped, workers are updated
and checked, and the coordinator is updated with `/etc/memo-pir/workers.json`.

Success requires all of the following:

1. every worker returns `status: ok` from its loopback health endpoint;
2. the coordinator reaches `serving` within 45 minutes;
3. public metadata remains mainnet/Ironwood and does not regress in height or
   tree size; and
4. `memo-pir-cli dummy` completes a private query through the public endpoint.

Each activation saves the prior binaries and service configuration under
`/opt/memo-pir/rollback`. A failed rollout restores every host already changed,
restarts the prior coordinator, prints bounded service logs, and leaves the
workflow failed. Persistent Zakura, memo-journal, and PIR-artifact directories
are never replaced.

## Manual preflight and redeployment

Use `workflow_dispatch` with a full commit SHA that is reachable from `main` and
already has a successful `CI` run. Leave `preflight_only` enabled to verify SSH,
host architecture, disk space, topology, uploads, and checksums without stopping
services. Disable it only to redeploy that exact tested revision.
