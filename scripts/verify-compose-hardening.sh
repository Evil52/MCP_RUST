#!/bin/bash

# Verifies the hardening declared by the Compose files that actually ship.
#
# The CI container job asserts the flags it passes to `docker run` itself,
# which cannot catch a regression in compose.yaml — and compose.yaml is what
# operators deploy. This checks the rendered configuration instead, so removing
# read_only, widening a published port, enabling a preview in production, or
# dropping a resource limit fails.

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for dependency in docker jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

# Compose interpolation is intentionally isolated from the project's `.env`,
# and `--no-env-resolution` leaves service env files unresolved. The verifier
# therefore inspects only declarations from version-controlled Compose files;
# marketplace credentials never enter the rendered JSON.
scratch="$(mktemp -d "${TMPDIR:-/tmp}/mcp-ozon-compose-verify.XXXXXX")"
chmod 700 "$scratch"
cleanup() {
  rm -rf "$scratch"
}
trap cleanup EXIT

interpolation_env="$scratch/interpolation.env"
service_env="$scratch/service.env"
main_access="$scratch/access.json"
canary_access="$scratch/access.canary.json"
printf '# Intentionally empty: Compose must not read the project .env during verification.\n' \
  >"$service_env"
printf '{}\n' >"$main_access"
printf '{}\n' >"$canary_access"
chmod 600 "$service_env" "$main_access" "$canary_access"
printf '%s\n' \
  "MCP_ENV_FILE=$service_env" \
  "MCP_ACCESS_CONFIG_HOST=$main_access" \
  "MCP_CANARY_ACCESS_CONFIG=$canary_access" \
  'POSITION_DB_ADMIN_PASSWORD=verify-only-admin-not-a-secret' \
  'POSITION_COLLECTOR_DB_PASSWORD=verify-only-collector-not-a-secret' \
  'POSITION_READER_DB_PASSWORD=verify-only-reader-not-a-secret' \
  >"$interpolation_env"
chmod 600 "$interpolation_env"

failures=0
check() {
  local description="$1" document="$2"
  shift 2
  if jq -e "$@" <<<"$document" >/dev/null; then
    printf 'ok   %s\n' "$description"
  else
    printf 'FAIL %s\n' "$description" >&2
    failures=$((failures + 1))
  fi
}

check_contains() {
  local description="$1" path="$2" expected="$3"
  if grep -Fq -- "$expected" "$path"; then
    printf 'ok   %s\n' "$description"
  else
    printf 'FAIL %s\n' "$description" >&2
    failures=$((failures + 1))
  fi
}

render_compose() {
  local compose_file="$1"
  docker compose \
    --env-file "$interpolation_env" \
    -f "$compose_file" \
    config --no-env-resolution --format json
}

# The dollar-prefixed names below are jq variables supplied with `--arg`; the
# single quotes deliberately prevent the shell from expanding them.
# shellcheck disable=SC2016
verify_server() {
  local label="$1"
  local rendered="$2"
  local expected_access_source="$3"
  local expected_published_port="$4"
  local expected_preview="$5"
  local expected_restart="$6"
  local expected_network_name="$7"
  local server
  server="$(jq -c '.services.server' <<<"$rendered")"

  check "$label: server service exists" "$server" 'type == "object"'
  check "$label: root filesystem is read-only" "$server" '.read_only == true'
  check "$label: all Linux capabilities are dropped" "$server" '.cap_drop == ["ALL"]'
  check "$label: privilege escalation is blocked" "$server" \
    '(.security_opt // []) | index("no-new-privileges:true") != null'
  check "$label: the container is never privileged" "$server" \
    '(.privileged // false) == false'
  check "$label: writable state is exactly the bounded /tmp tmpfs" "$server" \
    '(.tmpfs // []) == ["/tmp:size=16m,mode=1777"]'
  check "$label: memory limit matches the deployment contract" "$server" \
    '.mem_limit == "805306368"'
  check "$label: CPU limit matches the deployment contract" "$server" \
    '.cpus == 2'
  check "$label: process limit matches the deployment contract" "$server" \
    '.pids_limit == 256'
  check "$label: graceful shutdown has a bounded window" "$server" \
    '.stop_grace_period == "1m10s"'
  check "$label: logging limits match the deployment contract" "$server" \
    '.logging == {
       "driver": "json-file",
       "options": {"max-file": "3", "max-size": "10m"}
     }'
  check "$label: healthcheck matches the local HTTP readiness contract" "$server" \
    '.healthcheck == {
       "test": ["CMD", "wget", "-q", "-T", "3", "-O", "/dev/null",
                "http://127.0.0.1:8787/health"],
       "timeout": "3s", "interval": "10s", "retries": 5,
       "start_period": "10s"
     }'

  # The MCP trusts its network position, so the one expected port must stay on
  # IPv4 loopback and must always target the internal HTTP listener.
  check "$label: published port is the expected loopback endpoint" "$server" \
    --arg published "$expected_published_port" \
    '(.ports // [])
     | length == 1
       and .[0].host_ip == "127.0.0.1"
       and .[0].target == 8787
       and .[0].published == $published'

  # The registry source and in-container target are deployment invariants. The
  # default source is checked with interpolation isolated from operator env.
  check "$label: the access registry is the only volume and is read-only" "$server" \
    --arg source "$expected_access_source" \
    '(.volumes // [])
     | length == 1
       and .[0].target == "/etc/mcp-ozon/access.json"
       and .[0].type == "bind"
       and .[0].source == $source
       and .[0].read_only == true'

  check "$label: non-loopback container bind has an explicit dev opt-in" "$server" \
    '.environment.MCP_BIND == "0.0.0.0:8787"
     and .environment.MCP_DEV_ALLOW_NON_LOOPBACK == "true"'

  # Outbound access is required for the marketplace, so this cannot be an
  # internal network. Disabling ICC on a dedicated bridge preserves egress but
  # prevents ordinary peer containers from reaching the all-interface listener.
  check "$label: dedicated outbound bridge disables inter-container traffic" "$rendered" \
    --arg network_name "$expected_network_name" \
    '(.services.server.networks | keys) == ["outbound"]
     and (.networks | keys) == ["outbound"]
     and .networks.outbound.name == $network_name
     and .networks.outbound.driver == "bridge"
     and (.networks.outbound.internal // false) == false
     and .networks.outbound.driver_opts == {
       "com.docker.network.bridge.enable_icc": "false",
       "com.docker.network.bridge.host_binding_ipv4": "127.0.0.1"
     }'

  # The image must not be able to reach a marketplace with a rewritten base URL.
  check "$label: no environment override redirects Ozon egress" "$server" \
    '(.environment.OZON_API_BASE_URL // "https://api-seller.ozon.ru")
     == "https://api-seller.ozon.ru"'
  check "$label: preview flags match the deployment contract" "$server" \
    --arg preview "$expected_preview" \
    '.environment.OZON_POSTINGS_VNEXT == $preview
     and .environment.OZON_FINANCE_ACCRUALS_PREVIEW == $preview'
  check "$label: restart policy matches the deployment contract" "$server" \
    --arg restart "$expected_restart" '.restart == $restart'
}

verify_position() {
  local rendered="$1"
  local service
  service="$(jq -c '.services["position-db"]' <<<"$rendered")"

  check "position: database service exists" "$service" 'type == "object"'
  check "position: database is never published on a host port" "$service" \
    '(.ports // []) | length == 0'
  check "position: the named data volume is the only service mount" "$service" \
    '(.volumes // [])
     | length == 1
       and .[0].type == "volume"
       and .[0].source == "position-data"
       and .[0].target == "/var/lib/postgresql/data"
       and (.[0].read_only // false) == false'
  check "position: named volume identity is fixed" "$rendered" \
    '(.volumes | keys) == ["position-data"]
     and .volumes["position-data"].name == "mcp-ozon-position-data"'
  check "position: service is confined to the internal database network" "$rendered" \
    '(.services["position-db"].networks | keys) == ["position-internal"]
     and (.networks | keys) == ["position-internal"]
     and .networks["position-internal"].name == "mcp-ozon-position-internal"
     and .networks["position-internal"].internal == true'
  check "position: privilege escalation is blocked" "$service" \
    '.security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false'
  check "position: resource limits match the deployment contract" "$service" \
    '.mem_limit == "536870912"
     and .cpus == 1
     and .pids_limit == 128
     and .shm_size == "67108864"
     and .stop_grace_period == "30s"'
  check "position: writable tmpfs mounts match the deployment contract" "$service" \
    '(.tmpfs // [] | sort) == [
       "/tmp:size=32m,mode=1777",
       "/var/run/postgresql:size=8m,mode=3775"
     ]'
  check "position: restart and logging limits match the deployment contract" "$service" \
    '.restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "3", "max-size": "10m"}
     }'
  check "position: host authentication is SCRAM-only" "$service" \
    '.environment.POSTGRES_INITDB_ARGS
       == "--auth-host=scram-sha-256 --auth-local=scram-sha-256"'
  check "position: healthcheck authenticates the admin and validates role ACLs" "$service" \
    '.healthcheck.test == ["CMD", "/usr/local/bin/position-db-healthcheck"]
       and (.healthcheck.timeout == "3s")
       and (.healthcheck.interval == "10s")
       and (.healthcheck.retries == 5)
       and (.healthcheck.start_period == "10s")'
}

main_rendered="$(render_compose "$project_dir/compose.yaml")"
canary_rendered="$(render_compose "$project_dir/compose.canary.yaml")"
position_rendered="$(render_compose "$project_dir/compose.position.yaml")"

# Rendering uses isolated, existing placeholder files so the result is the
# same in a clean checkout and on a developer machine with ignored secrets.
# Keep the shipped defaults independently pinned to their expected paths.
check_contains \
  "main: default service env path remains .env" \
  "$project_dir/compose.yaml" \
  "\${MCP_ENV_FILE:-.env}"
check_contains \
  "canary: default service env path remains .env" \
  "$project_dir/compose.canary.yaml" \
  "\${MCP_ENV_FILE:-.env}"
check_contains \
  "main: default access registry path is fixed" \
  "$project_dir/compose.yaml" \
  "\${MCP_ACCESS_CONFIG_HOST:-./config/access.json}"
check_contains \
  "canary: default access registry path is fixed" \
  "$project_dir/compose.canary.yaml" \
  "\${MCP_CANARY_ACCESS_CONFIG:-./config/access.canary.json}"
check_contains \
  "main: missing registry paths are never auto-created" \
  "$project_dir/compose.yaml" \
  'create_host_path: false'
check_contains \
  "canary: missing registry paths are never auto-created" \
  "$project_dir/compose.canary.yaml" \
  'create_host_path: false'

verify_server \
  "main" \
  "$main_rendered" \
  "$main_access" \
  "8787" \
  "false" \
  "unless-stopped" \
  "mcp-ozon-outbound"
verify_server \
  "canary" \
  "$canary_rendered" \
  "$canary_access" \
  "8789" \
  "true" \
  "no" \
  "mcp-ozon-canary-outbound"
verify_position "$position_rendered"

if (( failures > 0 )); then
  echo "compose hardening verification failed with $failures problem(s)" >&2
  exit 1
fi

echo "Compose hardening verified for main, canary, and position database: exact resource/mount/health contracts, loopback-only publication, isolated egress, and an internal database network."
