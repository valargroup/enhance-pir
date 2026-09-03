#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 validate|preflight|deploy" >&2
  exit 2
}

MODE="${1:-}"
case "$MODE" in
  validate | preflight | deploy) ;;
  *) usage ;;
esac

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required environment variable: $name" >&2
    exit 2
  fi
}

for name in MEMO_COORDINATOR_HOST MEMO_DEPLOY_USER MEMO_PUBLIC_URL MEMO_WORKERS_JSON; do
  require_env "$name"
done

if [[ ! "$MEMO_COORDINATOR_HOST" =~ ^[A-Za-z0-9.-]+$ ]]; then
  echo "invalid coordinator SSH host" >&2
  exit 2
fi
if [[ ! "$MEMO_DEPLOY_USER" =~ ^[a-z_][a-z0-9_-]*$ ]]; then
  echo "invalid deployment user" >&2
  exit 2
fi
if [[ ! "$MEMO_PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+(:[0-9]+)?/?$ ]]; then
  echo "MEMO_PUBLIC_URL must be an HTTPS origin" >&2
  exit 2
fi

if ! jq -e --arg coordinator "$MEMO_COORDINATOR_HOST" '
  type == "array" and length >= 2 and
  all(.[];
    (keys | sort) == ["name", "service_url", "ssh_host"] and
    (.name | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9-]*$")) and
    (.ssh_host | type == "string" and test("^[A-Za-z0-9.-]+$")) and
    (.service_url | type == "string" and test("^https?://[A-Za-z0-9.-]+:[0-9]+/?$"))
  ) and
  ([.[].name] | length == (unique | length)) and
  ([.[].ssh_host] | length == (unique | length)) and
  ([.[].service_url | rtrimstr("/")] | length == (unique | length)) and
  all(.[].ssh_host; . != $coordinator)
' >/dev/null <<<"$MEMO_WORKERS_JSON"; then
  echo "MEMO_WORKERS_JSON is invalid or contains duplicate workers" >&2
  exit 2
fi

SERVER_CONFIG="$(jq -c '{workers: map({name, url: (.service_url | rtrimstr("/"))})}' <<<"$MEMO_WORKERS_JSON")"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MEMO_CADDYFILE="${MEMO_CADDYFILE:-$SCRIPT_DIR/../infra/digitalocean/memo-poc/deploy/Caddyfile}"
MEMO_APM_ENVIRONMENT="${MEMO_APM_ENVIRONMENT:-memo-pir-mainnet-poc}"
PUBLIC_HOST="${MEMO_PUBLIC_URL#https://}"
PUBLIC_HOST="${PUBLIC_HOST%%/*}"
PUBLIC_HOST="${PUBLIC_HOST%%:*}"

# The committed Caddyfile is a template keyed on MEMO_PUBLIC_HOST so the public
# hostname lives in one place (the GitHub Environment) rather than in git.
render_caddyfile() {
  local template="$1" output="$2"
  if [[ ! -r "$template" ]]; then
    echo "Caddyfile template is unreadable: $template" >&2
    exit 2
  fi
  if ! grep -q '^MEMO_PUBLIC_HOST {' "$template"; then
    echo "Caddyfile template must start its site block with 'MEMO_PUBLIC_HOST {'" >&2
    exit 2
  fi
  sed "s/MEMO_PUBLIC_HOST/$PUBLIC_HOST/g" "$template" >"$output"
  if ! grep -q "^$PUBLIC_HOST {" "$output"; then
    echo "rendered Caddyfile does not serve $PUBLIC_HOST" >&2
    exit 2
  fi
  if grep -q 'MEMO_PUBLIC_HOST' "$output"; then
    echo "rendered Caddyfile still contains the MEMO_PUBLIC_HOST placeholder" >&2
    exit 2
  fi
}

# Sealed shard ownership is a function of worker order, so an existing
# inventory may only ever be extended at the end. `-n` matters: without it jq
# waits for stdin and, given none, produces no result, which `-e` reports as
# failure for every input, including an unchanged inventory.
topology_is_append_only() {
  local old="$1" new="$2"
  jq -n -e --argjson old "$old" --argjson new "$new" '
    ($old.workers | type == "array") and
    ($new.workers | length) >= ($old.workers | length) and
    all(range(0; $old.workers | length); $new.workers[.] == $old.workers[.])
  ' >/dev/null
}

if [[ "$MODE" == "validate" ]]; then
  extended="$(jq -c '.workers += [{name: "self-test", url: "http://127.0.0.1:1"}]' <<<"$SERVER_CONFIG")"
  reordered="$(jq -c '.workers |= reverse' <<<"$SERVER_CONFIG")"
  topology_is_append_only "$SERVER_CONFIG" "$SERVER_CONFIG" || { echo "append-only check rejects an unchanged inventory" >&2; exit 1; }
  topology_is_append_only "$SERVER_CONFIG" "$extended" || { echo "append-only check rejects an appended worker" >&2; exit 1; }
  if topology_is_append_only "$SERVER_CONFIG" "$reordered"; then echo "append-only check accepts a reordered inventory" >&2; exit 1; fi
  rendered="$(mktemp)"
  render_caddyfile "$MEMO_CADDYFILE" "$rendered"
  grep -q 'handle_path /apm\*' "$rendered" || { echo "Caddyfile does not route /apm to the sidecar" >&2; exit 1; }
  grep -q 'handle /metrics' "$rendered" || { echo "Caddyfile does not block /metrics" >&2; exit 1; }
  rm -f "$rendered"
  echo "$SERVER_CONFIG"
  exit 0
fi

for name in MEMO_RELEASE_SHA MEMO_ARTIFACT_DIR MEMO_SSH_KEY_PATH MEMO_KNOWN_HOSTS_PATH \
  MEMO_SERVER_SERVICE_FILE MEMO_WORKER_SERVICE_FILE MEMO_APM_SERVICE_FILE; do
  require_env "$name"
done
if [[ ! "$MEMO_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "MEMO_RELEASE_SHA must be a full Git commit SHA" >&2
  exit 2
fi
for file in memo-pir-server memo-pir-worker memo-pir-cli pir-apm; do
  if [[ ! -x "$MEMO_ARTIFACT_DIR/$file" ]]; then
    echo "missing executable deployment artifact: $file" >&2
    exit 2
  fi
done
# The Slack webhook is optional: without it the sidecar logs alerts to the
# journal. When present it must be a plain URL so it cannot break the env file.
if [[ -n "${PIR_APM_SLACK_WEBHOOK_URL:-}" && ! "$PIR_APM_SLACK_WEBHOOK_URL" =~ ^https://[A-Za-z0-9./_-]+$ ]]; then
  echo "PIR_APM_SLACK_WEBHOOK_URL is set but is not a plain https URL" >&2
  exit 2
fi
if [[ ! -r "$MEMO_SSH_KEY_PATH" || ! -r "$MEMO_KNOWN_HOSTS_PATH" ]]; then
  echo "SSH key or known-hosts file is unreadable" >&2
  exit 2
fi

SSH=(ssh -i "$MEMO_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$MEMO_KNOWN_HOSTS_PATH"
  -o ConnectTimeout=10)
SCP=(scp -q -i "$MEMO_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$MEMO_KNOWN_HOSTS_PATH"
  -o ConnectTimeout=10)

remote() {
  local host="$1"
  shift
  "${SSH[@]}" "$MEMO_DEPLOY_USER@$host" "$@"
}

copy_to() {
  local source="$1"
  local host="$2"
  local destination="$3"
  "${SCP[@]}" "$source" "$MEMO_DEPLOY_USER@$host:$destination"
}

mapfile -t WORKER_HOSTS < <(jq -r '.[].ssh_host' <<<"$MEMO_WORKERS_JSON")
REMOTE_STAGE="/tmp/memo-pir-$MEMO_RELEASE_SHA"

preflight_host() {
  local host="$1"
  remote "$host" bash -s -- "$REMOTE_STAGE" <<'REMOTE'
set -euo pipefail
stage="$1"
[[ "$(uname -m)" == "x86_64" ]]
command -v curl >/dev/null
command -v jq >/dev/null
command -v sha256sum >/dev/null
command -v systemctl >/dev/null
if [[ "$(id -u)" -ne 0 ]]; then sudo -n true; fi
mkdir -p "$stage"
available_kb="$(df -Pk /opt | awk 'NR == 2 {print $4}')"
[[ "$available_kb" -ge 1048576 ]]
REMOTE
}

preflight_coordinator_extras() {
  remote "$MEMO_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
command -v caddy >/dev/null
systemctl is-enabled --quiet caddy
REMOTE
}

echo "Preflighting coordinator and ${#WORKER_HOSTS[@]} workers"
preflight_host "$MEMO_COORDINATOR_HOST"
preflight_coordinator_extras
for host in "${WORKER_HOSTS[@]}"; do
  preflight_host "$host"
done

existing_config="$(remote "$MEMO_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
if [[ -r /etc/memo-pir/workers.json ]]; then
  cat /etc/memo-pir/workers.json
fi
REMOTE
)"
if [[ -n "$existing_config" ]] && ! topology_is_append_only "$existing_config" "$SERVER_CONFIG"; then
  echo "worker topology is not append-only; refusing deployment" >&2
  exit 1
fi

server_config_file="$(mktemp)"
caddyfile_rendered="$(mktemp)"
apm_env_file="$(mktemp)"
cleanup() {
  rm -f "$server_config_file" "$caddyfile_rendered" "$apm_env_file"
}
trap cleanup EXIT
printf '%s\n' "$SERVER_CONFIG" >"$server_config_file"
render_caddyfile "$MEMO_CADDYFILE" "$caddyfile_rendered"
# mktemp creates the file 0600; the webhook value is never echoed anywhere else.
{
  echo "PIR_APM_SCRAPE_URL=http://127.0.0.1:8080"
  echo "PIR_APM_LISTEN=127.0.0.1:3002"
  echo "PIR_APM_METRICS_PATH=/metrics"
  echo "PIR_APM_HEALTH_PATH=/memo/health"
  echo "PIR_APM_READY_PATH=/ready"
  echo "PIR_APM_METRIC_PREFIX=memo"
  echo "PIR_APM_ENDPOINTS=metadata,params,public_params,query"
  echo "PIR_APM_PROCESSING_ENDPOINTS=query"
  echo "PIR_APM_LATENCY_P99_SECONDS=1.0"
  echo "PIR_APM_LATENCY_P99_OVERRIDES=query=5.0,public_params=2.0"
  echo "PIR_APM_TITLE=Memo PIR APM"
  echo "PIR_APM_ENVIRONMENT=$MEMO_APM_ENVIRONMENT"
  echo "PIR_APM_DATA_DIR=/srv/zakura/memo-data"
  if [[ -n "${PIR_APM_SLACK_WEBHOOK_URL:-}" ]]; then
    echo "PIR_APM_SLACK_WEBHOOK_URL=$PIR_APM_SLACK_WEBHOOK_URL"
  fi
} >"$apm_env_file"

stage_file() {
  local source="$1"
  local host="$2"
  local name="$3"
  local mode="${4:-0644}"
  local digest
  digest="$(sha256sum "$source" | awk '{print $1}')"
  copy_to "$source" "$host" "$REMOTE_STAGE/$name"
  remote "$host" bash -s -- "$REMOTE_STAGE/$name" "$digest" "$mode" <<'REMOTE'
set -euo pipefail
printf '%s  %s\n' "$2" "$1" | sha256sum --check --status
chmod "$3" "$1"
REMOTE
}

stage_file "$MEMO_ARTIFACT_DIR/memo-pir-server" "$MEMO_COORDINATOR_HOST" memo-pir-server
stage_file "$server_config_file" "$MEMO_COORDINATOR_HOST" workers.json
stage_file "$MEMO_SERVER_SERVICE_FILE" "$MEMO_COORDINATOR_HOST" memo-pir-server.service
stage_file "$MEMO_ARTIFACT_DIR/pir-apm" "$MEMO_COORDINATOR_HOST" pir-apm
stage_file "$MEMO_APM_SERVICE_FILE" "$MEMO_COORDINATOR_HOST" pir-apm.service
stage_file "$apm_env_file" "$MEMO_COORDINATOR_HOST" pir-apm.env 0600
stage_file "$caddyfile_rendered" "$MEMO_COORDINATOR_HOST" Caddyfile

# Validate the staged Caddyfile before anything is installed, and show how it
# differs from the live one so a preflight run makes the upcoming change visible.
remote "$MEMO_COORDINATOR_HOST" bash -s -- "$REMOTE_STAGE" <<'REMOTE'
set -euo pipefail
stage="$1"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
as_root caddy validate --config "$stage/Caddyfile" --adapter caddyfile >/dev/null
if as_root test -r /etc/caddy/Caddyfile; then
  echo "Caddyfile changes (live -> staged):"
  as_root diff -u /etc/caddy/Caddyfile "$stage/Caddyfile" || true
else
  echo "No live Caddyfile; the staged one will be installed"
fi
REMOTE
for host in "${WORKER_HOSTS[@]}"; do
  stage_file "$MEMO_ARTIFACT_DIR/memo-pir-worker" "$host" memo-pir-worker
  stage_file "$MEMO_WORKER_SERVICE_FILE" "$host" memo-pir-worker.service
done

if [[ "$MODE" == "preflight" ]]; then
  echo "Preflight and artifact verification succeeded; no services changed"
  exit 0
fi

coordinator_activated=0
activated_workers=()

show_logs() {
  remote "$MEMO_COORDINATOR_HOST" bash -s <<'REMOTE' || true
if [[ "$(id -u)" -eq 0 ]]; then
  journalctl -u memo-pir-server --no-pager -n 80
  journalctl -u pir-apm --no-pager -n 40
  journalctl -u caddy --no-pager -n 20
else
  sudo -n journalctl -u memo-pir-server --no-pager -n 80
  sudo -n journalctl -u pir-apm --no-pager -n 40
  sudo -n journalctl -u caddy --no-pager -n 20
fi
REMOTE
  for host in "${WORKER_HOSTS[@]}"; do
    remote "$host" bash -s <<'REMOTE' || true
if [[ "$(id -u)" -eq 0 ]]; then
  journalctl -u memo-pir-worker --no-pager -n 50
else
  sudo -n journalctl -u memo-pir-worker --no-pager -n 50
fi
REMOTE
  done
}

coordinator_service() {
  local action="$1"
  remote "$MEMO_COORDINATOR_HOST" bash -s -- "$action" <<'REMOTE'
set -euo pipefail
if [[ "$(id -u)" -eq 0 ]]; then
  systemctl "$1" memo-pir-server
else
  sudo -n systemctl "$1" memo-pir-server
fi
REMOTE
}

rollback_worker() {
  local host="$1"
  remote "$host" bash -s <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
rollback=/opt/memo-pir/rollback
as_root systemctl stop memo-pir-worker
[[ -x "$rollback/memo-pir-worker" ]] && as_root install -m 0755 "$rollback/memo-pir-worker" /usr/local/bin/memo-pir-worker
[[ -r "$rollback/memo-pir-worker.service" ]] && as_root install -m 0644 "$rollback/memo-pir-worker.service" /etc/systemd/system/memo-pir-worker.service
as_root systemctl daemon-reload
as_root systemctl restart memo-pir-worker
REMOTE
}

rollback_coordinator() {
  remote "$MEMO_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
rollback=/opt/memo-pir/rollback
as_root systemctl stop memo-pir-server || true
[[ -x "$rollback/memo-pir-server" ]] && as_root install -m 0755 "$rollback/memo-pir-server" /usr/local/bin/memo-pir-server
[[ -r "$rollback/memo-pir-server.service" ]] && as_root install -m 0644 "$rollback/memo-pir-server.service" /etc/systemd/system/memo-pir-server.service
if [[ -r "$rollback/workers.json" ]]; then
  as_root install -d -m 0755 /etc/memo-pir
  as_root install -m 0644 "$rollback/workers.json" /etc/memo-pir/workers.json
fi
# Sidecar and reverse proxy. A first deploy has no previous sidecar to restore,
# so disable it rather than leave it paging against a server without /metrics.
if [[ -x "$rollback/pir-apm" ]]; then
  as_root install -m 0755 "$rollback/pir-apm" /usr/local/bin/pir-apm
  [[ -r "$rollback/pir-apm.service" ]] && as_root install -m 0644 "$rollback/pir-apm.service" /etc/systemd/system/pir-apm.service
  if as_root test -r "$rollback/pir-apm.env"; then
    as_root install -m 0600 -o root -g root "$rollback/pir-apm.env" /etc/default/pir-apm
  fi
else
  as_root systemctl disable --now pir-apm 2>/dev/null || true
fi
if as_root test -r "$rollback/Caddyfile"; then
  as_root install -m 0644 -o root -g root "$rollback/Caddyfile" /etc/caddy/Caddyfile
  as_root systemctl reload caddy || true
fi
as_root systemctl daemon-reload
as_root systemctl restart memo-pir-server
if [[ -x "$rollback/pir-apm" ]]; then
  as_root systemctl restart pir-apm || true
fi
REMOTE
}

rollback() {
  set +e
  echo "Deployment failed; restoring the previous fleet release" >&2
  for host in "${activated_workers[@]}"; do rollback_worker "$host"; done
  if [[ "$coordinator_activated" -eq 1 ]]; then
    rollback_coordinator
  else
    coordinator_service start || true
  fi
  show_logs
  set -e
}

activate_worker() {
  local host="$1"
  remote "$host" bash -s -- "$REMOTE_STAGE" "$MEMO_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
stage="$1"; sha="$2"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
release="/opt/memo-pir/releases/$sha"
rollback=/opt/memo-pir/rollback
as_root install -d -m 0755 "$release" "$rollback"
as_root install -m 0755 "$stage/memo-pir-worker" "$release/memo-pir-worker"
[[ -x /usr/local/bin/memo-pir-worker ]] && as_root cp -L /usr/local/bin/memo-pir-worker "$rollback/memo-pir-worker"
[[ -r /etc/systemd/system/memo-pir-worker.service ]] && as_root cp /etc/systemd/system/memo-pir-worker.service "$rollback/memo-pir-worker.service"
as_root systemctl stop memo-pir-worker
as_root install -m 0755 "$release/memo-pir-worker" /usr/local/bin/memo-pir-worker.next
as_root mv -f /usr/local/bin/memo-pir-worker.next /usr/local/bin/memo-pir-worker
as_root install -m 0644 "$stage/memo-pir-worker.service" /etc/systemd/system/memo-pir-worker.service
as_root systemctl daemon-reload
as_root systemctl restart memo-pir-worker
printf '%s\n' "$sha" | as_root tee /opt/memo-pir/current-worker-release >/dev/null
REMOTE
  remote "$host" "curl --fail --silent --show-error http://127.0.0.1:8091/internal/health | jq -e '.status == \"ok\"' >/dev/null"
}

activate_coordinator() {
  remote "$MEMO_COORDINATOR_HOST" bash -s -- "$REMOTE_STAGE" "$MEMO_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
stage="$1"; sha="$2"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
release="/opt/memo-pir/releases/$sha"
rollback=/opt/memo-pir/rollback
as_root install -d -m 0755 "$release" "$rollback" /etc/memo-pir
as_root install -m 0755 "$stage/memo-pir-server" "$release/memo-pir-server"
[[ -x /usr/local/bin/memo-pir-server ]] && as_root cp -L /usr/local/bin/memo-pir-server "$rollback/memo-pir-server"
[[ -r /etc/systemd/system/memo-pir-server.service ]] && as_root cp /etc/systemd/system/memo-pir-server.service "$rollback/memo-pir-server.service"
[[ -r /etc/memo-pir/workers.json ]] && as_root cp /etc/memo-pir/workers.json "$rollback/workers.json"
as_root install -m 0755 "$release/memo-pir-server" /usr/local/bin/memo-pir-server.next
as_root mv -f /usr/local/bin/memo-pir-server.next /usr/local/bin/memo-pir-server
as_root install -m 0644 "$stage/workers.json" /etc/memo-pir/workers.json
as_root install -m 0644 "$stage/memo-pir-server.service" /etc/systemd/system/memo-pir-server.service
as_root systemctl daemon-reload
as_root systemctl restart memo-pir-server
printf '%s\n' "$sha" | as_root tee /opt/memo-pir/current-coordinator-release >/dev/null

# pir-apm sidecar: same save-then-replace pattern as the server binary.
[[ -x /usr/local/bin/pir-apm ]] && as_root cp -L /usr/local/bin/pir-apm "$rollback/pir-apm"
[[ -r /etc/systemd/system/pir-apm.service ]] && as_root cp /etc/systemd/system/pir-apm.service "$rollback/pir-apm.service"
if as_root test -r /etc/default/pir-apm; then
  as_root install -m 0600 -o root -g root /etc/default/pir-apm "$rollback/pir-apm.env"
fi
if as_root test -r /etc/caddy/Caddyfile; then
  as_root install -m 0644 -o root -g root /etc/caddy/Caddyfile "$rollback/Caddyfile"
fi
as_root install -m 0755 "$stage/pir-apm" "$release/pir-apm"
as_root install -m 0755 "$release/pir-apm" /usr/local/bin/pir-apm.next
as_root mv -f /usr/local/bin/pir-apm.next /usr/local/bin/pir-apm
as_root install -m 0644 "$stage/pir-apm.service" /etc/systemd/system/pir-apm.service
as_root install -m 0600 -o root -g root "$stage/pir-apm.env" /etc/default/pir-apm
as_root caddy validate --config "$stage/Caddyfile" --adapter caddyfile >/dev/null
as_root install -m 0644 -o root -g root "$stage/Caddyfile" /etc/caddy/Caddyfile
as_root systemctl daemon-reload
as_root systemctl enable pir-apm
as_root systemctl restart pir-apm
as_root systemctl reload caddy
for _ in $(seq 1 10); do
  if curl --fail --silent http://127.0.0.1:3002/healthz >/dev/null; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:3002/healthz >/dev/null
as_root systemctl is-active --quiet pir-apm
REMOTE
}

old_metadata="$(curl --fail --silent --show-error "$MEMO_PUBLIC_URL/memo/metadata" || true)"

if ! coordinator_service stop; then
  show_logs
  exit 1
fi

rollout_ok=1
for host in "${WORKER_HOSTS[@]}"; do
  activated_workers+=("$host")
  if ! activate_worker "$host"; then
    rollout_ok=0
    break
  fi
done
if [[ "$rollout_ok" -eq 1 ]]; then
  coordinator_activated=1
  if ! activate_coordinator; then rollout_ok=0; fi
fi

if [[ "$rollout_ok" -eq 1 ]]; then
  # A record-layout change sets the journal aside and re-ingests from activation
  # (~42K blocks at today's tip), so allow up to two hours before rolling back.
  serving=0
  for _ in $(seq 1 720); do
    if remote "$MEMO_COORDINATOR_HOST" "curl --fail --silent http://127.0.0.1:8080/memo/health | jq -e '.phase.phase == \"serving\"' >/dev/null"; then
      serving=1
      break
    fi
    sleep 10
  done
  [[ "$serving" -eq 1 ]] || rollout_ok=0
fi

if [[ "$rollout_ok" -eq 1 ]]; then
  health_json="$(curl --fail --silent --show-error "$MEMO_PUBLIC_URL/memo/health")" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  expected_workers="${#WORKER_HOSTS[@]}"
  if ! jq -e --argjson expected "$expected_workers" '
    .phase.phase == "serving" and .workers == $expected
  ' >/dev/null <<<"$health_json"; then
    rollout_ok=0
  fi
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  new_metadata="$(curl --fail --silent --show-error "$MEMO_PUBLIC_URL/memo/metadata")" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  if ! jq -e --argjson expected "$expected_workers" '
    .network == "main" and .pool == "ironwood" and
    (.setup_seed | type == "number") and
    ([.shards[].worker] | unique | length) <= $expected and
    (.shards | length) > 0
  ' >/dev/null <<<"$new_metadata"; then
    rollout_ok=0
  fi
fi
if [[ "$rollout_ok" -eq 1 && -n "$old_metadata" ]]; then
  if ! jq -e --argjson old "$old_metadata" '
    .anchor_height >= $old.anchor_height and
    .ironwood_tree_size >= $old.ironwood_tree_size
  ' >/dev/null <<<"$new_metadata"; then
    rollout_ok=0
  fi
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  "$MEMO_ARTIFACT_DIR/memo-pir-cli" --server "$MEMO_PUBLIC_URL" dummy || rollout_ok=0
fi
# Readiness gate and sidecar exposure: /ready is loopback-only, /apm/ is public,
# and the raw /metrics exposition must stay behind Caddy's 404.
if [[ "$rollout_ok" -eq 1 ]]; then
  remote "$MEMO_COORDINATOR_HOST" "curl --fail --silent http://127.0.0.1:8080/ready >/dev/null" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  curl --fail --silent --show-error "$MEMO_PUBLIC_URL/apm/" | grep -q '<title>' || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  metrics_code="$(curl --silent --output /dev/null --write-out '%{http_code}' "$MEMO_PUBLIC_URL/metrics" || true)"
  [[ "$metrics_code" == "404" ]] || rollout_ok=0
fi

if [[ "$rollout_ok" -ne 1 ]]; then
  rollback
  exit 1
fi

echo "Memo PIR fleet deployed successfully at $MEMO_RELEASE_SHA"
