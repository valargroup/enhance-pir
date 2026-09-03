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

if [[ "$MODE" == "validate" ]]; then
  echo "$SERVER_CONFIG"
  exit 0
fi

for name in MEMO_RELEASE_SHA MEMO_ARTIFACT_DIR MEMO_SSH_KEY_PATH MEMO_KNOWN_HOSTS_PATH \
  MEMO_SERVER_SERVICE_FILE MEMO_WORKER_SERVICE_FILE; do
  require_env "$name"
done
if [[ ! "$MEMO_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "MEMO_RELEASE_SHA must be a full Git commit SHA" >&2
  exit 2
fi
for file in memo-pir-server memo-pir-worker memo-pir-cli; do
  if [[ ! -x "$MEMO_ARTIFACT_DIR/$file" ]]; then
    echo "missing executable deployment artifact: $file" >&2
    exit 2
  fi
done
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

echo "Preflighting coordinator and ${#WORKER_HOSTS[@]} workers"
preflight_host "$MEMO_COORDINATOR_HOST"
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
if [[ -n "$existing_config" ]] && ! jq -e --argjson old "$existing_config" --argjson new "$SERVER_CONFIG" '
  ($old.workers | type == "array") and
  ($new.workers | length) >= ($old.workers | length) and
  all(range(0; $old.workers | length); $new.workers[.] == $old.workers[.])
' >/dev/null; then
  echo "worker topology is not append-only; refusing deployment" >&2
  exit 1
fi

server_config_file="$(mktemp)"
cleanup() {
  rm -f "$server_config_file"
}
trap cleanup EXIT
printf '%s\n' "$SERVER_CONFIG" >"$server_config_file"

stage_file() {
  local source="$1"
  local host="$2"
  local name="$3"
  local digest
  digest="$(sha256sum "$source" | awk '{print $1}')"
  copy_to "$source" "$host" "$REMOTE_STAGE/$name"
  remote "$host" bash -s -- "$REMOTE_STAGE/$name" "$digest" <<'REMOTE'
set -euo pipefail
printf '%s  %s\n' "$2" "$1" | sha256sum --check --status
REMOTE
}

stage_file "$MEMO_ARTIFACT_DIR/memo-pir-server" "$MEMO_COORDINATOR_HOST" memo-pir-server
stage_file "$server_config_file" "$MEMO_COORDINATOR_HOST" workers.json
stage_file "$MEMO_SERVER_SERVICE_FILE" "$MEMO_COORDINATOR_HOST" memo-pir-server.service
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
else
  sudo -n journalctl -u memo-pir-server --no-pager -n 80
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
as_root systemctl daemon-reload
as_root systemctl restart memo-pir-server
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

if [[ "$rollout_ok" -ne 1 ]]; then
  rollback
  exit 1
fi

echo "Memo PIR fleet deployed successfully at $MEMO_RELEASE_SHA"
