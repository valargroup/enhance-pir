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
ENHANCE_WORKERS_JSON='[{"name":"worker-1","ssh_host":"worker-1.example.net","service_url":"http://10.0.0.2:8091"},{"name":"worker-2","ssh_host":"worker-2.example.net","service_url":"http://10.0.0.3:8091"}]' \
ops/scripts/deploy-enhance-pir.sh validate
```

Current runtime paths are `/etc/enhance-pir`, `/opt/enhance-pir`,
`/srv/zakura/enhance-data`, and `/srv/enhance-pir/artifacts`. Existing
DigitalOcean resource names retain their historical names to avoid unintended
Terraform replacement; application binaries, services, paths, and API names
use Enhance.

The deploy verifies `GET /v1/health`, retrieves
`GET /v1/enhance/generation`, and completes a dummy query through the public
origin before declaring the rollout successful.
