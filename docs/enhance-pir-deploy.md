# Enhance PIR deployment

The production workflow builds and rolls out the Enhance coordinator, workers,
and APM sidecar. Wallet clients were not deployed against the superseded API,
so the server can move directly to schema 5, protocol
`ironwood-enhance-pir-v1`, and the `/v1/enhance/*` endpoints.

The manual workflow accepts a full commit SHA that must be the current `main`
revision and must already have a successful CI run. It builds and checksums:

- `enhance-pir-server`
- `enhance-pir-worker`
- `enhance-pir-cli`
- `pir-apm`
- the matching systemd and Caddy configuration

Operational configuration uses the `ENHANCE_` prefix. The deployment helper
expects `ENHANCE_COORDINATOR_HOST`, `ENHANCE_DEPLOY_USER`,
`ENHANCE_PUBLIC_URL`, and `ENHANCE_WORKERS_JSON`; preflight/deploy modes also
require the artifact, SSH, release, and service-file variables validated by the
script.

```sh
ENHANCE_COORDINATOR_HOST=coordinator.example.net \
ENHANCE_DEPLOY_USER=deploy \
ENHANCE_PUBLIC_URL=https://enhance.example.net \
ENHANCE_WORKERS_JSON='[{"name":"shard-group-01","replicas":[{"name":"worker-01a","ssh_host":"worker-01a.example.net","service_url":"http://10.0.0.2:8091"},{"name":"worker-01b","ssh_host":"worker-01b.example.net","service_url":"http://10.0.0.3:8091"}]}]' \
ops/scripts/deploy-enhance-pir.sh validate
```

Each ordered shard group owns six shards and has exactly two active-active
replicas. Group order is append-only because it determines shard placement;
replicas inside an existing group may be replaced without moving shards. A
generation publishes once at least one replica in every used group is ready.
The first rollout from the legacy flat inventory is an intentional topology
format migration and requires `ENHANCE_ALLOW_TOPOLOGY_CHANGE=true`; later
replica replacements do not require that override.
Each worker service is cgroup-limited to 2 GiB with swap disabled. A replica
that exceeds the limit is restarted by systemd; its peer continues serving the
group while it rebuilds on the next publication.

Current runtime paths are `/etc/enhance-pir`, `/opt/enhance-pir`,
`/srv/zakura/enhance-data`, and `/srv/enhance-pir/artifacts`. Active
DigitalOcean resources use Enhance names. The attached Zakura data volume is
the sole exception: DigitalOcean cannot rename it in place, so Terraform keeps
its historical provider name and protects it from replacement.

The deploy verifies `GET /v1/health`, retrieves
`GET /v1/enhance/generation`, and completes a dummy query through the public
origin before declaring the rollout successful.
