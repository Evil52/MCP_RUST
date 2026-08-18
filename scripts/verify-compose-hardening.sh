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
  'REPORT_WORKER_DB_PASSWORD=verify-only-report-worker-not-a-secret' \
  'REPORT_COLLECTOR_DB_PASSWORD=verify-only-report-collector-not-a-secret' \
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

check_control_mount_source_contract() {
  local description="$1" path="$2" expected actual
  expected=$'    volumes:\n      - type: bind\n        source: ${CONTROL_MCP_ACCESS_CONFIG_HOST:-./config/access.example.json}\n        target: /etc/mcp-ozon/access.json\n        read_only: true\n        bind:\n          create_host_path: false\n      - type: bind\n        source: ${CONTROL_MCP_POLICY_HOST:-./config/control-policy.example.json}\n        target: /etc/mcp-ozon/control-policy.json\n        read_only: true\n        bind:\n          create_host_path: false'
  actual="$(awk '
    /^    volumes:$/ { capture = 1 }
    capture && /^    restart:/ { exit }
    capture { print }
  ' "$path")"

  if [[ "$actual" == "$expected" ]]; then
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

render_control_compose() {
  # Shell variables take precedence over `--env-file` in Compose. Pin both
  # interpolated bind sources explicitly so an operator's ambient environment
  # cannot make this release-gate inspect a different file. The shipped
  # defaults are checked independently below.
  CONTROL_MCP_ACCESS_CONFIG_HOST="$project_dir/config/access.example.json" \
    CONTROL_MCP_POLICY_HOST="$project_dir/config/control-policy.example.json" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.control.yaml" \
      config --no-env-resolution --format json
}

render_position_compose() {
  MCP_ACCESS_CONFIG_HOST="$main_access" \
    DAILY_REPORT_POLICY_HOST="$project_dir/config/daily-report-pilot.example.json" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.position.yaml" \
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
  check "position: named volume identities are fixed" "$rendered" \
    '(.volumes | keys | sort) == ["position-data", "report-artifacts"]
     and .volumes["position-data"].name == "mcp-ozon-position-data"
     and .volumes["report-artifacts"].name == "mcp-ozon-report-artifacts"'
  check "position: service is confined to the internal database network" "$rendered" \
    '(.services["position-db"].networks | keys) == ["position-internal"]
     and (.networks | keys | sort) == ["outbound", "ozon-egress-internal", "position-internal"]
     and .networks["position-internal"].name == "mcp-ozon-position-internal"
     and .networks["position-internal"].internal == true
     and .networks["ozon-egress-internal"].name == "mcp-ozon-egress-internal"
     and .networks["ozon-egress-internal"].internal == true
     and .networks.outbound.name == "mcp-ozon-outbound"
     and .networks.outbound.external == true'
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
       "/var/run/postgresql:size=8m,mode=3775,uid=70,gid=70"
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

verify_position_collector() {
  local rendered="$1"
  local service expected_database_url
  service="$(jq -c '.services["position-collector"]' <<<"$rendered")"
  expected_database_url='postgresql://position_collector:verify-only-collector-not-a-secret@position-db:5432/ozon_positions'

  check "position collector: service exists" "$service" 'type == "object"'
  check "position collector: no host ingress or mutable mounts exist" "$service" \
    '((.ports // []) | length == 0)
     and ((.volumes // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  # The jq expression intentionally references the --arg variable.
  # shellcheck disable=SC2016
  check "position collector: disabled credential-isolated environment is exact" "$service" \
    --arg database_url "$expected_database_url" \
    '.environment == {
       "POSITION_COLLECTOR_DATABASE_URL": $database_url,
       "POSITION_COLLECTOR_MODE": "disabled",
       "RUST_LOG": "mcp_ozon::position_collector=info"
     }'
  check "position collector: waits for the authenticated database healthcheck" "$service" \
    '.depends_on == {
       "position-db": {"condition": "service_healthy", "required": true}
     }'
  check "position collector: only the internal database network is attached" "$service" \
    '(.networks | keys) == ["position-internal"]'
  check "position collector: filesystem and privilege hardening are exact" "$service" \
    '.read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false'
  check "position collector: bounded resources and shutdown are exact" "$service" \
    '.mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .stop_grace_period == "10s"'
  check "position collector: restart and logs are bounded" "$service" \
    '.restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "5m"}
     }'
  check "position collector: healthcheck is local and exact" "$service" \
    '.healthcheck == {
       "test": ["CMD", "/usr/local/bin/position-collector", "healthcheck"],
       "timeout": "8s", "interval": "30s", "retries": 3,
       "start_period": "10s"
     }'
}

verify_reporting_service() {
  local rendered="$1" service_name="$2" binary="$3" database_user="$4"
  local database_password="$5" memory_bytes="$6" cpu_limit="$7"
  local mode_name database_name service expected_database_url expected_networks expected_depends
  service="$(jq -c --arg name "$service_name" '.services[$name]' <<<"$rendered")"
  mode_name="$(tr '[:lower:]-' '[:upper:]_' <<<"${service_name}_MODE")"
  database_name="$(tr '[:lower:]-' '[:upper:]_' <<<"${service_name}_DATABASE_URL")"
  expected_database_url="postgresql://${database_user}:${database_password}@position-db:5432/ozon_positions"
  if [[ "$service_name" == "report-collector" ]]; then
    expected_networks='["ozon-egress-internal", "position-internal"]'
    expected_depends='{
      "ozon-egress": {"condition": "service_healthy", "required": true},
      "position-db": {"condition": "service_healthy", "required": true}
    }'
  else
    expected_networks='["position-internal"]'
    expected_depends='{
      "position-db": {"condition": "service_healthy", "required": true}
    }'
  fi

  check "$service_name: service exists" "$service" 'type == "object"'
  check "$service_name: no host ingress, env file, secrets, or configs exist" "$service" \
    '((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  if [[ "$service_name" == "report-worker" ]]; then
    # The jq expression intentionally references variables supplied with --arg.
    # shellcheck disable=SC2016
    check "$service_name: disabled environment includes only the isolated artifact root" "$service" \
      --arg mode_name "$mode_name" \
      --arg database_name "$database_name" \
      --arg database_url "$expected_database_url" \
      '.environment == {
         ($mode_name): "disabled",
         ($database_name): $database_url,
         "MCP_ACCESS_CONFIG": "/etc/mcp-ozon/access.json",
         "DAILY_REPORT_POLICY": "/etc/mcp-ozon/daily-report-policy.json",
         "REPORT_ARTIFACT_ROOT": "/var/lib/mcp-ozon/report-artifacts",
         "RUST_LOG": "mcp_ozon::reporting=info"
       }'
    # shellcheck disable=SC2016
    check "$service_name: metadata is read-only and only its artifact volume is writable" "$service" \
      --arg access "$main_access" \
      --arg policy "$project_dir/config/daily-report-pilot.example.json" \
      '((.volumes // []) | map(.read_only //= false | del(.bind)) | sort_by(.target)) == [
         {
           "type": "bind", "source": $access,
           "target": "/etc/mcp-ozon/access.json", "read_only": true
         },
         {
           "type": "bind", "source": $policy,
           "target": "/etc/mcp-ozon/daily-report-policy.json", "read_only": true
         },
         {
           "type": "volume", "source": "report-artifacts",
           "target": "/var/lib/mcp-ozon/report-artifacts",
           "read_only": false
         }
       ]
       and all((.volumes // [])[];
         .bind == null or .bind == {} or .bind == {"create_host_path": false})'
  else
    # The jq expression intentionally references variables supplied with --arg.
    # shellcheck disable=SC2016
    check "$service_name: disabled environment is exact and credential-isolated" "$service" \
      --arg mode_name "$mode_name" \
      --arg database_name "$database_name" \
      --arg database_url "$expected_database_url" \
      '.environment == {
         ($mode_name): "disabled",
         ($database_name): $database_url,
         "MCP_ACCESS_CONFIG": "/etc/mcp-ozon/access.json",
         "DAILY_REPORT_POLICY": "/etc/mcp-ozon/daily-report-policy.json",
         "RUST_LOG": "mcp_ozon::reporting=info"
       }'
    # shellcheck disable=SC2016
    check "$service_name: exactly two fixed read-only metadata mounts exist" "$service" \
      --arg access "$main_access" \
      --arg policy "$project_dir/config/daily-report-pilot.example.json" \
      '((.volumes // []) | map(del(.bind)) | sort_by(.target)) == [
         {
           "type": "bind", "source": $access,
           "target": "/etc/mcp-ozon/access.json", "read_only": true
         },
         {
           "type": "bind", "source": $policy,
           "target": "/etc/mcp-ozon/daily-report-policy.json", "read_only": true
         }
       ]
       and all((.volumes // [])[];
         .bind == null or .bind == {} or .bind == {"create_host_path": false})'
  fi
  # jq variables below are intentionally literal and supplied with --argjson.
  # shellcheck disable=SC2016
  check "$service_name: waits for the authenticated database healthcheck" "$service" \
    --argjson depends "$expected_depends" '.depends_on == $depends'
  # shellcheck disable=SC2016
  check "$service_name: only its exact internal networks are attached" "$service" \
    --argjson networks "$expected_networks" '(.networks | keys | sort) == $networks'
  check "$service_name: filesystem and privilege hardening are exact" "$service" \
    '.read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false'
  # shellcheck disable=SC2016
  check "$service_name: bounded resources and shutdown are exact" "$service" \
    --arg memory "$memory_bytes" \
    --argjson cpu "$cpu_limit" \
    '.mem_limit == $memory
     and .cpus == $cpu
     and .pids_limit == 64
     and .stop_grace_period == "10s"'
  check "$service_name: restart and logs are bounded" "$service" \
    '.restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "5m"}
     }'
  # shellcheck disable=SC2016
  check "$service_name: healthcheck is local and exact" "$service" \
    --arg binary "/usr/local/bin/$binary" \
    '.healthcheck == {
       "test": ["CMD", $binary, "healthcheck"],
       "timeout": "8s", "interval": "30s", "retries": 3,
       "start_period": "10s"
     }'
}

verify_ozon_egress() {
  local rendered="$1" service
  service="$(jq -c '.services["ozon-egress"]' <<<"$rendered")"

  check "ozon egress: service exists" "$service" 'type == "object"'
  check "ozon egress: no host ingress, mounts, environment, or secrets exist" "$service" \
    '((.ports // []) | length == 0)
     and ((.volumes // []) | length == 0)
     and ((.environment // {}) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  check "ozon egress: bridge topology is exactly proxy-internal plus outbound" "$service" \
    '(.networks | keys | sort) == ["outbound", "ozon-egress-internal"]'
  check "ozon egress: filesystem and privilege hardening are exact" "$service" \
    '.read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and (.tmpfs // [] | sort) == [
       "/var/cache/squid:size=8m,mode=1777",
       "/var/log/squid:size=4m,mode=1777",
       "/var/run/squid:size=1m,mode=1777"
     ]'
  # The expected Compose healthcheck contains a literal shell substitution.
  # shellcheck disable=SC2016
  check "ozon egress: bounded resources, logs, and local healthcheck are exact" "$service" \
    '.mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .stop_grace_period == "10s"
     and .logging == {"driver": "json-file", "options": {"max-file": "2", "max-size": "1m"}}
     and .healthcheck == {
       "test": ["CMD-SHELL", "test -s /var/run/squid/squid.pid && kill -0 $$(cat /var/run/squid/squid.pid)"],
       "timeout": "8s", "interval": "30s", "retries": 3, "start_period": "10s"
     }'
}

# The disabled Control MCP is intentionally a separate, credentialless service
# with no Internet route. Its exact environment is allowlisted here: adding a
# marketplace credential name or any new service setting must fail review.
# shellcheck disable=SC2016
verify_control() {
  local rendered="$1"
  local service
  service="$(jq -c '.services.control' <<<"$rendered")"

  check "control: service exists" "$service" 'type == "object"'
  check "control: no env_file, secrets, or configs are attached" "$service" \
    '(has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  check "control: no top-level secrets, configs, or named volumes exist" "$rendered" \
    '((.secrets // {}) | length == 0)
     and ((.configs // {}) | length == 0)
     and ((.volumes // {}) | length == 0)'

  check "control: environment is credentialless and exactly allowlisted" "$service" \
    '.environment == {
       "CONTROL_MCP_ACCESS_CONFIG": "/etc/mcp-ozon/access.json",
       "CONTROL_MCP_ACTOR_ID": "admin",
       "CONTROL_MCP_AUTH_MODE": "dev",
       "CONTROL_MCP_BIND": "0.0.0.0:8790",
       "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK": "true",
       "CONTROL_MCP_MAX_SESSIONS": "64",
       "CONTROL_MCP_POLICY": "/etc/mcp-ozon/control-policy.json",
       "CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS": "120",
       "CONTROL_MCP_TRANSPORT": "http",
       "RUST_LOG": "mcp_ozon::control=info,rmcp=info"
     }
     and ([.environment | keys[]
           | select(test("(?i)(api[_-]?(key|token)|client[_-]?secret|seller[_-]?token|wb[_-]?token|performance[_-]?client[_-]?id)"))]
          | length == 0)'

  check "control: exactly two fixed read-only bind mounts are present" "$service" \
    --arg access "$project_dir/config/access.example.json" \
    --arg policy "$project_dir/config/control-policy.example.json" \
    '((.volumes // []) | map(del(.bind)) | sort_by(.target)) == [
       {
         "type": "bind",
         "source": $access,
         "target": "/etc/mcp-ozon/access.json",
         "read_only": true
       },
       {
         "type": "bind",
         "source": $policy,
         "target": "/etc/mcp-ozon/control-policy.json",
         "read_only": true
       }
     ]
     and all((.volumes // [])[];
       .bind == null
       or .bind == {}
       or .bind == {"create_host_path": false})'

  check "control: published port is exactly 127.0.0.1:8790" "$service" \
    '(.ports // []) == [{
       "mode": "ingress",
       "host_ip": "127.0.0.1",
       "target": 8790,
       "published": "8790",
       "protocol": "tcp"
     }]'
  check "control: only the internal isolated bridge is attached" "$rendered" \
    '(.services.control.networks | keys) == ["control_isolated"]
     and (.services.control.network_mode? == null)
     and (.networks | keys) == ["control_isolated"]
     and .networks.control_isolated.name == "mcp-ozon-control_control_isolated"
     and .networks.control_isolated.driver == "bridge"
     and .networks.control_isolated.internal == true
     and .networks.control_isolated.driver_opts == {
       "com.docker.network.bridge.enable_icc": "false",
       "com.docker.network.bridge.host_binding_ipv4": "127.0.0.1"
     }'

  check "control: filesystem and privilege hardening match the contract" "$service" \
    '.read_only == true
     and .cap_drop == ["ALL"]
     and ((.cap_add // []) | length == 0)
     and ((.security_opt // []) | index("no-new-privileges:true") != null)
     and (.privileged // false) == false
     and (.tmpfs // []) == ["/tmp:size=8m,mode=1777"]'
  check "control: resource and graceful-stop limits match the contract" "$service" \
    '.mem_limit == "268435456"
     and .cpus == 1
     and .pids_limit == 128
     and .stop_grace_period == "1m10s"'
  check "control: restart and bounded logging match the contract" "$service" \
    '.restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "5m"}
     }'
  check "control: healthcheck is local, bounded, and exact" "$service" \
    '.healthcheck == {
       "test": ["CMD", "wget", "-q", "-T", "3", "-O", "/dev/null",
                "http://127.0.0.1:8790/health"],
       "timeout": "3s", "interval": "10s", "retries": 5,
       "start_period": "10s"
     }'
}

main_rendered="$(render_compose "$project_dir/compose.yaml")"
canary_rendered="$(render_compose "$project_dir/compose.canary.yaml")"
position_rendered="$(render_position_compose)"
control_rendered="$(render_control_compose)"

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
check_contains \
  "control: default access registry is the credentialless example" \
  "$project_dir/compose.control.yaml" \
  "\${CONTROL_MCP_ACCESS_CONFIG_HOST:-./config/access.example.json}"
check_contains \
  "control: default policy is the disabled example" \
  "$project_dir/compose.control.yaml" \
  "\${CONTROL_MCP_POLICY_HOST:-./config/control-policy.example.json}"
check_control_mount_source_contract \
  "control: both bind mounts exactly refuse implicit host-path creation" \
  "$project_dir/compose.control.yaml"
check_contains \
  "reporting: default access registry path is fixed" \
  "$project_dir/compose.position.yaml" \
  "\${MCP_ACCESS_CONFIG_HOST:-./config/access.example.json}"
check_contains \
  "reporting: default policy path is the disabled pilot" \
  "$project_dir/compose.position.yaml" \
  "\${DAILY_REPORT_POLICY_HOST:-./config/daily-report-pilot.example.json}"
check_contains \
  "report egress: proxy permits the exact Ozon and WB report API hosts" \
  "$project_dir/position-monitor/ozon-egress/squid.conf" \
  "acl marketplace_read_api dstdomain api-seller.ozon.ru api-performance.ozon.ru seller-analytics-api.wildberries.ru discounts-prices-api.wildberries.ru advert-api.wildberries.ru"
check_contains \
  "report egress: proxy applies the exact read API host allowlist" \
  "$project_dir/position-monitor/ozon-egress/squid.conf" \
  "http_access allow connect marketplace_read_api tls_port"
check_contains \
  "ozon egress: proxy denies every other destination" \
  "$project_dir/position-monitor/ozon-egress/squid.conf" \
  "http_access deny all"

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
verify_position_collector "$position_rendered"
verify_ozon_egress "$position_rendered"
verify_reporting_service \
  "$position_rendered" \
  "report-collector" \
  "report-collector" \
  "report_collector" \
  "verify-only-report-collector-not-a-secret" \
  "134217728" \
  "0.25"
verify_reporting_service \
  "$position_rendered" \
  "report-worker" \
  "report-worker" \
  "report_worker" \
  "verify-only-report-worker-not-a-secret" \
  "201326592" \
  "0.5"
verify_control "$control_rendered"

if (( failures > 0 )); then
  echo "compose hardening verification failed with $failures problem(s)" >&2
  exit 1
fi

echo "Compose hardening verified for main, canary, Control, position database, Ozon read-API egress proxy, and disabled collector/reporting runtimes: exact resource/mount/health contracts, loopback-only publication, isolated egress, and internal database networks."
