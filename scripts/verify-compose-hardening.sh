#!/bin/bash
# shellcheck disable=SC2016
# jq filters and nginx snippets below deliberately use single quotes so their
# dollar-prefixed variables remain literal rather than being expanded by bash.

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
  'CONTROL_WRITER_DB_PASSWORD=verify-only-control-writer-not-a-secret' \
  'WB_AUTOMATION_DB_PASSWORD=verify-only-wb-automation-not-a-secret' \
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
  local actor_id="${1:-admin}"
  # Shell variables take precedence over `--env-file` in Compose. Pin both
  # interpolated bind sources explicitly so an operator's ambient environment
  # cannot make this release-gate inspect a different file. The shipped
  # defaults are checked independently below.
  CONTROL_MCP_ACCESS_CONFIG_HOST="$project_dir/config/access.example.json" \
    CONTROL_MCP_POLICY_HOST="$project_dir/config/control-policy.example.json" \
    CONTROL_MCP_ACTOR_ID="$actor_id" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.control.yaml" \
      config --no-env-resolution --format json
}

render_control_wb_plan_compose() {
  local read_token_file="$scratch/wb-promotion-read.token"
  printf 'verification-read-token-not-a-secret\n' >"$read_token_file"
  chmod 600 "$read_token_file"
  CONTROL_MCP_ACCESS_CONFIG_HOST="$project_dir/config/access.example.json" \
    CONTROL_MCP_POLICY_HOST="$project_dir/config/control-policy.example.json" \
    CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE_HOST="$read_token_file" \
    CONTROL_MCP_JWT_ISSUER="https://auth.example.test/realms/ofk" \
    CONTROL_MCP_JWT_JWKS_HOST="auth.example.test" \
    CONTROL_MCP_JWT_JWKS_PATH="/realms/ofk/protocol/openid-connect/certs" \
    CONTROL_MCP_PUBLIC_URL="https://control.example.test/mcp" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.control.yaml" \
      -f "$project_dir/compose.control-wb-plan.yaml" \
      config --no-env-resolution --format json
}

render_control_wb_live_compose() {
  local read_token_file="$scratch/wb-promotion-read.token"
  local write_token_file="$scratch/wb-promotion-write.token"
  printf 'verification-read-token-not-a-secret\n' >"$read_token_file"
  printf 'verification-write-token-not-a-secret\n' >"$write_token_file"
  chmod 600 "$read_token_file" "$write_token_file"
  CONTROL_MCP_ACCESS_CONFIG_HOST="$project_dir/config/access.example.json" \
    CONTROL_MCP_POLICY_HOST="$project_dir/config/control-policy.example.json" \
    CONTROL_MCP_MARKETPLACE_WRITES_ENABLED="true" \
    CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE_HOST="$read_token_file" \
    CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE_HOST="$write_token_file" \
    CONTROL_MCP_JWT_ISSUER="https://auth.example.test/realms/ofk" \
    CONTROL_MCP_JWT_JWKS_HOST="auth.example.test" \
    CONTROL_MCP_JWT_JWKS_PATH="/realms/ofk/protocol/openid-connect/certs" \
    CONTROL_MCP_PUBLIC_URL="https://control.example.test/mcp" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.control.yaml" \
      -f "$project_dir/compose.control-wb-plan.yaml" \
      -f "$project_dir/compose.control-wb-live.yaml" \
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

render_wb_automation_shadow_compose() {
  local policy="$scratch/wb-automation-policy.json"
  local access="$scratch/wb-automation-access.json"
  local read_token="$scratch/wb-automation-read.token"
  local legacy_state="$scratch/wb-automation-legacy-state.json"
  printf '{}\n' >"$policy"
  printf '{}\n' >"$access"
  printf 'verification-read-token-not-a-secret\n' >"$read_token"
  printf '{}\n' >"$legacy_state"
  chmod 600 "$policy" "$access" "$read_token" "$legacy_state"
  WB_AUTOMATION_POLICY_HOST="$policy" \
    WB_AUTOMATION_ACCESS_CONFIG_HOST="$access" \
    WB_AUTOMATION_READ_TOKEN_FILE_HOST="$read_token" \
    WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.wb-automation-shadow.yaml" \
      config --no-env-resolution --format json
}

render_wb_automation_live_compose() {
  local shadow_policy="$scratch/wb-automation-shadow-policy.json"
  local live_policy="$scratch/wb-automation-live-policy.json"
  local access="$scratch/wb-automation-live-access.json"
  local read_token="$scratch/wb-automation-live-read.token"
  local write_token="$scratch/wb-automation-live-write.token"
  local legacy_state="$scratch/wb-automation-live-legacy-state.json"
  printf '{}\n' >"$shadow_policy"
  printf '{}\n' >"$live_policy"
  printf '{}\n' >"$access"
  printf 'verification-read-token-not-a-secret\n' >"$read_token"
  printf 'verification-write-token-not-a-secret\n' >"$write_token"
  printf '{}\n' >"$legacy_state"
  chmod 600 \
    "$shadow_policy" "$live_policy" "$access" "$read_token" \
    "$write_token" "$legacy_state"
  WB_AUTOMATION_SHADOW_POLICY_HOST="$shadow_policy" \
    WB_AUTOMATION_LIVE_POLICY_HOST="$live_policy" \
    WB_AUTOMATION_ACCESS_CONFIG_HOST="$access" \
    WB_AUTOMATION_READ_TOKEN_FILE_HOST="$read_token" \
    WB_AUTOMATION_WRITE_TOKEN_FILE_HOST="$write_token" \
    WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.wb-automation-live.yaml" \
      config --no-env-resolution --format json
}

render_reporting_reader_compose() {
  MCP_ACCESS_CONFIG_HOST="$main_access" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.yaml" \
      -f "$project_dir/compose.reporting-reader.yaml" \
      config --no-env-resolution --format json
}

render_reporting_live_compose() {
  local live_policy="$scratch/daily-report-policy.json"
  local credential_directory="$scratch/report-credentials"
  printf '{}\n' >"$live_policy"
  mkdir "$credential_directory"
  chmod 500 "$credential_directory"
  MCP_ACCESS_CONFIG_HOST="$main_access" \
    DAILY_REPORT_POLICY_HOST="$live_policy" \
    REPORT_COLLECTOR_CREDENTIAL_DIR_HOST="$credential_directory" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.position.yaml" \
      -f "$project_dir/compose.reporting-live.yaml" \
      --profile reporting-live \
      config --no-env-resolution --format json
}

render_reporting_canary_compose() {
  local canary_policy="$scratch/daily-report-canary-policy.json"
  local credential_directory="$scratch/report-canary-credentials"
  printf '{}\n' >"$canary_policy"
  mkdir "$credential_directory"
  chmod 500 "$credential_directory"
  MCP_ACCESS_CONFIG_HOST="$main_access" \
    DAILY_REPORT_CANARY_POLICY_HOST="$canary_policy" \
    REPORT_COLLECTOR_CREDENTIAL_DIR_HOST="$credential_directory" \
    REPORT_COLLECTOR_CANARY_MODE=ozon_dry_run \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.position.yaml" \
      -f "$project_dir/compose.reporting-canary.yaml" \
      --profile reporting-canary \
      config --no-env-resolution --format json
}

render_reporting_mail_canary_compose() {
  local mail_policy="$scratch/daily-report-mail-policy.json"
  local mail_routing="$scratch/mail-routing.json"
  local oauth_directory="$scratch/gmail-oauth"
  printf '{}\n' >"$mail_policy"
  printf '{}\n' >"$mail_routing"
  mkdir "$oauth_directory"
  chmod 600 "$mail_policy" "$mail_routing"
  chmod 500 "$oauth_directory"
  MCP_ACCESS_CONFIG_HOST="$main_access" \
    DAILY_REPORT_MAIL_POLICY_HOST="$mail_policy" \
    REPORT_MAIL_ROUTING_HOST="$mail_routing" \
    REPORT_GMAIL_OAUTH_DIR_HOST="$oauth_directory" \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.position.yaml" \
      -f "$project_dir/compose.reporting-mail-canary.yaml" \
      --profile reporting-mail-canary \
      config --no-env-resolution --format json
}

render_reporting_mail_live_compose() {
  local mail_policy="$scratch/daily-report-mail-policy.json"
  local mail_routing="$scratch/mail-routing.json"
  local oauth_directory="$scratch/gmail-oauth"
  # Reuse the canary's exact fixture sources so the comparison below can prove
  # that only profile, command and mode differ between canary and live.
  [[ -f "$mail_policy" ]] || printf '{}\n' >"$mail_policy"
  [[ -f "$mail_routing" ]] || printf '{}\n' >"$mail_routing"
  [[ -d "$oauth_directory" ]] || mkdir "$oauth_directory"
  chmod 600 "$mail_policy" "$mail_routing"
  chmod 500 "$oauth_directory"
  MCP_ACCESS_CONFIG_HOST="$main_access" \
    DAILY_REPORT_MAIL_POLICY_HOST="$mail_policy" \
    REPORT_MAIL_ROUTING_HOST="$mail_routing" \
    REPORT_GMAIL_OAUTH_DIR_HOST="$oauth_directory" \
    REPORT_MAIL_CANARY_AUDIENCE_ID=pilot_owner \
    docker compose \
      --env-file "$interpolation_env" \
      -f "$project_dir/compose.position.yaml" \
      -f "$project_dir/compose.reporting-mail-live.yaml" \
      --profile reporting-mail-live \
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
  local expected_restart="$5"
  local expected_network_name="$6"
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
  check "$label: legacy preview flags remain disabled" "$server" \
    '.environment.OZON_POSTINGS_VNEXT == "false"
     and .environment.OZON_FINANCE_ACCRUALS_PREVIEW == "false"'
  check "$label: restart policy matches the deployment contract" "$server" \
    --arg restart "$expected_restart" '.restart == $restart'
}

# The reporting-reader overlay may change exactly two things on the verified
# main MCP service: add its restricted database URL and attach the existing
# internal database network. Everything else, including the outbound bridge,
# mounts, published port, filesystem and resource hardening, must remain byte-
# for-byte equivalent after Compose has merged the files.
# shellcheck disable=SC2016
verify_reporting_reader() {
  local rendered="$1" base_rendered="$2" service base_service
  local expected_database_url
  service="$(jq -c '.services.server' <<<"$rendered")"
  base_service="$(jq -c '.services.server' <<<"$base_rendered")"
  expected_database_url='postgresql://position_reader:verify-only-reader-not-a-secret@position-db:5432/ozon_positions'

  check "reporting reader: server service exists" "$service" 'type == "object"'
  check "reporting reader: only the URL and network attachment differ from main" "$service" \
    --argjson base "$base_service" \
    'del(.environment.MCP_REPORTING_DATABASE_URL, .networks)
     == ($base | del(.environment.MCP_REPORTING_DATABASE_URL, .networks))'
  check "reporting reader: only the restricted reader URL is added" "$service" \
    --arg database_url "$expected_database_url" \
    '.environment.MCP_REPORTING_DATABASE_URL == $database_url
     and (.environment | has("POSITION_DB_ADMIN_PASSWORD") | not)
     and (.environment | has("POSITION_COLLECTOR_DB_PASSWORD") | not)
     and (.environment | has("POSITION_READER_DB_PASSWORD") | not)
     and (.environment | has("REPORT_WORKER_DB_PASSWORD") | not)
     and (.environment | has("REPORT_COLLECTOR_DB_PASSWORD") | not)'
  check "reporting reader: exact outbound and fixed database networks are attached" "$rendered" \
    '(.services.server.networks | keys | sort) == ["outbound", "position-internal"]
     and (.networks | keys | sort) == ["outbound", "position-internal"]
     and .networks["position-internal"].name == "mcp-ozon-position-internal"
     and .networks["position-internal"].external == true
     and (.networks["position-internal"].internal // false) == false
     and .networks.outbound.name == "mcp-ozon-outbound"
     and .networks.outbound.driver == "bridge"
     and (.networks.outbound.internal // false) == false
     and .networks.outbound.driver_opts == {
       "com.docker.network.bridge.enable_icc": "false",
       "com.docker.network.bridge.host_binding_ipv4": "127.0.0.1"
     }'
  check "reporting reader: overlay does not add lifecycle coupling or secret mounts" "$service" \
    '(has("depends_on") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
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

verify_wb_automation_shadow() {
  local rendered="$1" service expected_database_url
  service="$(jq -c '.services["wb-automation-shadow"]' <<<"$rendered")"
  expected_database_url='postgresql://wb_automation_writer:verify-only-wb-automation-not-a-secret@position-db:5432/ozon_positions'

  check "WB automation shadow: service exists" "$service" 'type == "object"'
  check "WB automation shadow: has no host ingress or persistent writable mount" "$service" \
    '((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.volumes | length == 4)
     and all(.volumes[]; .type == "bind" and .read_only == true)'
  # shellcheck disable=SC2016
  check "WB automation shadow: only least-privilege DB URL and safe logging are present" "$service" \
    --arg database_url "$expected_database_url" \
    '.environment == {
       "RUST_LOG": "mcp_ozon::control=info",
       "WB_AUTOMATION_DATABASE_URL": $database_url
     }'
  check "WB automation shadow: command has read token and no writer capability" "$service" \
    '.command == [
       "shadow-once-pg",
       "/etc/mcp-ozon/wb-automation-policy.json",
       "/etc/mcp-ozon/access.json",
       "/run/secrets/wb-promotion-read.token",
       "/var/lib/mcp-ozon-legacy/execution-state.json",
       "true",
       "http://ozon-egress:3128"
     ]
     and ([.volumes[].target] | index("/run/secrets/wb-promotion-write.token") == null)'
  check "WB automation shadow: mounts are exact and fail closed" "$service" \
    '([.volumes[].target] | sort) == [
       "/etc/mcp-ozon/access.json",
       "/etc/mcp-ozon/wb-automation-policy.json",
       "/run/secrets/wb-promotion-read.token",
       "/var/lib/mcp-ozon-legacy/execution-state.json"
     ]'
  check "WB automation shadow: only internal DB and credentialless read-egress networks exist" "$rendered" \
    '(.services["wb-automation-shadow"].networks | keys | sort)
       == ["ozon-egress-internal", "position-internal"]
     and (.networks | keys | sort)
       == ["ozon-egress-internal", "position-internal"]
     and .networks["position-internal"].name == "mcp-ozon-position-internal"
     and .networks["position-internal"].external == true
     and .networks["ozon-egress-internal"].name == "mcp-ozon-egress-internal"
     and .networks["ozon-egress-internal"].external == true'
  check "WB automation shadow: one-shot filesystem and privilege hardening are exact" "$service" \
    '.restart == "no"
     and .read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and .tmpfs == ["/tmp:size=8m,mode=1777"]'
  check "WB automation shadow: resources and logs are bounded" "$service" \
    '.mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "1m"}
     }'
}

verify_wb_automation_live() {
  local rendered="$1" service proxy expected_database_url
  service="$(jq -c '.services["wb-automation-live"]' <<<"$rendered")"
  proxy="$(jq -c '.services["write-egress"]' <<<"$rendered")"
  expected_database_url='postgresql://wb_automation_writer:verify-only-wb-automation-not-a-secret@position-db:5432/ozon_positions'

  check "WB automation live: service exists" "$service" 'type == "object"'
  check "WB automation live: only exact protected inputs are mounted read-only" "$service" \
    '((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.volumes | length == 6)
     and all(.volumes[]; .type == "bind" and .read_only == true)
     and ([.volumes[].target] | sort) == [
       "/etc/mcp-ozon/access.json",
       "/etc/mcp-ozon/wb-automation-live-policy.json",
       "/etc/mcp-ozon/wb-automation-shadow-policy.json",
       "/run/secrets/wb-promotion-read.token",
       "/run/secrets/wb-promotion-write.token",
       "/var/lib/mcp-ozon-legacy/execution-state.json"
     ]'
  check "WB automation live: command and database role are exact" "$service" \
    --arg database_url "$expected_database_url" \
    '.environment == {
       "RUST_LOG": "mcp_ozon::control=info",
       "WB_AUTOMATION_DATABASE_URL": $database_url
     }
     and .command == [
       "execute-once-pg",
       "/etc/mcp-ozon/wb-automation-live-policy.json",
       "/etc/mcp-ozon/access.json",
       "/run/secrets/wb-promotion-read.token",
       "/run/secrets/wb-promotion-write.token",
       "/var/lib/mcp-ozon-legacy/execution-state.json",
       "true",
       "http://write-egress:3130",
       "http://ozon-egress:3128"
     ]'
  check "WB automation live: worker has only internal DB and proxy routes" "$rendered" \
    '(.services["wb-automation-live"].networks | keys | sort)
       == ["ozon-egress-internal", "position-internal", "write-egress-internal"]
     and ((.services["wb-automation-live"].networks | has("outbound")) | not)
     and .services["wb-automation-live"].depends_on
       == {"write-egress":{"condition":"service_healthy","required":true}}
     and .networks["position-internal"].external == true
     and .networks["ozon-egress-internal"].external == true
     and .networks["write-egress-internal"].internal == true
     and .networks.outbound.external == true'
  check "WB automation live: one-shot resource and privilege bounds are exact" "$service" \
    '.restart == "no"
     and .read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and .tmpfs == ["/tmp:size=8m,mode=1777"]
     and .mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "1m"}
     }'
  check "WB automation live: write proxy is credentialless and not host-published" "$proxy" \
    '(has("environment") | not)
     and (has("env_file") | not)
     and ((.ports // []) | length == 0)
     and ((.volumes // []) | length == 0)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.networks | keys | sort) == ["outbound", "write-egress-internal"]
     and .read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and .restart == "unless-stopped"
     and .mem_limit == "268435456"
     and .cpus == 0.25
     and .pids_limit == 32
     and .stop_grace_period == "10s"
     and .healthcheck == {
       "test": ["CMD-SHELL", "printf '"'"'GET http://healthcheck.invalid/ HTTP/1.0\r\n\r\n'"'"' | nc -w 3 127.0.0.1 3130 | head -1 | grep -q '"'"'403 Forbidden'"'"'"],
       "timeout":"8s", "interval":"30s", "retries":3, "start_period":"10s"
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

verify_reporting_live() {
  local rendered="$1" service expected_database_url live_policy credential_directory
  service="$(jq -c '.services["report-collector"]' <<<"$rendered")"
  expected_database_url='postgresql://report_collector:verify-only-report-collector-not-a-secret@position-db:5432/ozon_positions'
  live_policy="$scratch/daily-report-policy.json"
  credential_directory="$scratch/report-credentials"

  check "live reporting: collector is guarded by the explicit profile and command" "$service" \
    '.profiles == ["reporting-live"]
     and .command == ["run-scheduler"]
     and .image == "mcp-ozon-report-collector:local"'
  check "live reporting: no ingress, env file, Compose secret, or config exists" "$service" \
    '((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  # jq variables below are supplied with --arg and remain literal in this file.
  # shellcheck disable=SC2016
  check "live reporting: environment contains paths but no marketplace values" "$service" \
    --arg database_url "$expected_database_url" \
    '.environment == {
       "REPORT_COLLECTOR_MODE": "scheduled",
       "REPORT_COLLECTOR_DATABASE_URL": $database_url,
       "REPORT_COLLECTOR_CREDENTIAL_DIR": "/run/mcp-ozon/report-credentials",
       "MCP_ACCESS_CONFIG": "/etc/mcp-ozon/access.json",
       "DAILY_REPORT_POLICY": "/etc/mcp-ozon/daily-report-policy.json",
       "RUST_LOG": "mcp_ozon::reporting=info"
     }'
  # shellcheck disable=SC2016
  check "live reporting: metadata and credential directory are exactly read-only" "$service" \
    --arg access "$main_access" \
    --arg policy "$live_policy" \
    --arg credentials "$credential_directory" \
    '((.volumes // []) | map(del(.bind)) | sort_by(.target)) == [
       {
         "type": "bind", "source": $access,
         "target": "/etc/mcp-ozon/access.json", "read_only": true
       },
       {
         "type": "bind", "source": $policy,
         "target": "/etc/mcp-ozon/daily-report-policy.json", "read_only": true
       },
       {
         "type": "bind", "source": $credentials,
         "target": "/run/mcp-ozon/report-credentials", "read_only": true
       }
     ]
     and all((.volumes // [])[];
       .bind == null or .bind == {} or .bind == {"create_host_path": false})'
  check "live reporting: network, privilege, resource, restart and health bounds remain exact" "$service" \
    '(.networks | keys | sort) == ["ozon-egress-internal", "position-internal"]
     and .depends_on == {
       "ozon-egress": {"condition": "service_healthy", "required": true},
       "position-db": {"condition": "service_healthy", "required": true}
     }
     and .read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and .mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "5m"}
     }
     and .healthcheck == {
       "test": ["CMD", "/usr/local/bin/report-collector", "healthcheck"],
       "timeout": "8s", "interval": "30s", "retries": 3,
       "start_period": "10s"
     }'
  check "live reporting: internal and outbound network definitions remain exact" "$rendered" \
    '(.networks | keys | sort) == ["outbound", "ozon-egress-internal", "position-internal"]
     and .networks["position-internal"].name == "mcp-ozon-position-internal"
     and .networks["position-internal"].internal == true
     and .networks["ozon-egress-internal"].name == "mcp-ozon-egress-internal"
     and .networks["ozon-egress-internal"].internal == true
     and .networks.outbound.name == "mcp-ozon-outbound"
     and .networks.outbound.external == true'
}

verify_reporting_canary() {
  local rendered="$1" service expected_database_url canary_policy credential_directory
  service="$(jq -c '.services["report-collector"]' <<<"$rendered")"
  expected_database_url='postgresql://report_collector:verify-only-report-collector-not-a-secret@position-db:5432/ozon_positions'
  canary_policy="$scratch/daily-report-canary-policy.json"
  credential_directory="$scratch/report-canary-credentials"

  check "reporting canary: collector is inert unless an operator overrides the command" "$service" \
    '.profiles == ["reporting-canary"]
     and .command == ["healthcheck"]
     and .image == "mcp-ozon-report-collector:local"'
  check "reporting canary: no ingress, env file, Compose secret, or config exists" "$service" \
    '((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  # shellcheck disable=SC2016
  check "reporting canary: environment contains paths but no marketplace values" "$service" \
    --arg database_url "$expected_database_url" \
    '.environment == {
       "REPORT_COLLECTOR_MODE": "ozon_dry_run",
       "REPORT_COLLECTOR_DATABASE_URL": $database_url,
       "REPORT_COLLECTOR_CREDENTIAL_DIR": "/run/mcp-ozon/report-credentials",
       "MCP_ACCESS_CONFIG": "/etc/mcp-ozon/access.json",
       "DAILY_REPORT_POLICY": "/etc/mcp-ozon/daily-report-policy.json",
       "RUST_LOG": "mcp_ozon::reporting=info"
     }'
  # shellcheck disable=SC2016
  check "reporting canary: metadata and credential directory are exactly read-only" "$service" \
    --arg access "$main_access" \
    --arg policy "$canary_policy" \
    --arg credentials "$credential_directory" \
    '((.volumes // []) | map(del(.bind)) | sort_by(.target)) == [
       {
         "type": "bind", "source": $access,
         "target": "/etc/mcp-ozon/access.json", "read_only": true
       },
       {
         "type": "bind", "source": $policy,
         "target": "/etc/mcp-ozon/daily-report-policy.json", "read_only": true
       },
       {
         "type": "bind", "source": $credentials,
         "target": "/run/mcp-ozon/report-credentials", "read_only": true
       }
     ]
     and all((.volumes // [])[];
       .bind == null or .bind == {} or .bind == {"create_host_path": false})'
  check "reporting canary: network, privilege and resource bounds remain exact" "$service" \
    '(.networks | keys | sort) == ["ozon-egress-internal", "position-internal"]
     and .depends_on == {
       "ozon-egress": {"condition": "service_healthy", "required": true},
       "position-db": {"condition": "service_healthy", "required": true}
     }
     and .read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and .mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "5m"}
     }
     and .healthcheck == {
       "test": ["CMD", "/usr/local/bin/report-collector", "healthcheck"],
       "timeout": "8s", "interval": "30s", "retries": 3,
       "start_period": "10s"
     }'
}

verify_reporting_mail_canary() {
  local rendered="$1" worker proxy expected_database_url mail_policy mail_routing oauth_directory
  worker="$(jq -c '.services["report-worker"]' <<<"$rendered")"
  proxy="$(jq -c '.services["mail-egress"]' <<<"$rendered")"
  expected_database_url='postgresql://report_worker:verify-only-report-worker-not-a-secret@position-db:5432/ozon_positions'
  mail_policy="$scratch/daily-report-mail-policy.json"
  mail_routing="$scratch/mail-routing.json"
  oauth_directory="$scratch/gmail-oauth"

  check "mail canary: worker is inert unless an operator overrides the command" "$worker" \
    '.profiles == ["reporting-mail-canary"]
     and .command == ["healthcheck"]
     and .image == "mcp-ozon-report-worker:local"'
  check "mail canary: worker has no ingress, env file, Compose secret, or config" "$worker" \
    '((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'
  # shellcheck disable=SC2016
  check "mail canary: environment contains only fixed paths and no secret values" "$worker" \
    --arg database_url "$expected_database_url" \
    '.environment == {
       "REPORT_WORKER_MODE": "delivery_canary",
       "REPORT_WORKER_DATABASE_URL": $database_url,
       "MCP_ACCESS_CONFIG": "/etc/mcp-ozon/access.json",
       "DAILY_REPORT_POLICY": "/etc/mcp-ozon/daily-report-policy.json",
       "REPORT_ARTIFACT_ROOT": "/var/lib/mcp-ozon/report-artifacts",
       "REPORT_MAIL_ROUTING": "/run/mcp-ozon/mail-routing.json",
       "REPORT_GMAIL_OAUTH_DIR": "/run/mcp-ozon/gmail-oauth",
       "RUST_LOG": "mcp_ozon::reporting=info"
     }
     and ([.environment | to_entries[]
       | select(.key | test("(?i)(gmail.*(client|refresh|secret|token)|oauth.*(client|refresh|secret|token))"))]
       | length == 0)'
  # shellcheck disable=SC2016
  check "mail canary: metadata, routing and OAuth inputs are exactly read-only" "$worker" \
    --arg access "$main_access" \
    --arg policy "$mail_policy" \
    --arg routing "$mail_routing" \
    --arg oauth "$oauth_directory" \
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
         "type": "bind", "source": $oauth,
         "target": "/run/mcp-ozon/gmail-oauth", "read_only": true
       },
       {
         "type": "bind", "source": $routing,
         "target": "/run/mcp-ozon/mail-routing.json", "read_only": true
       },
       {
         "type": "volume", "source": "report-artifacts",
         "target": "/var/lib/mcp-ozon/report-artifacts", "read_only": false
       }
     ]
     and all((.volumes // [])[];
       .bind == null or .bind == {} or .bind == {"create_host_path": false})'
  check "mail canary: worker keeps exact isolation and resource bounds" "$worker" \
    '(.networks | keys | sort) == ["mail-egress-internal", "position-internal"]
     and .depends_on == {
       "mail-egress": {"condition": "service_healthy", "required": true},
       "position-db": {"condition": "service_healthy", "required": true}
     }
     and .read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and .mem_limit == "201326592"
     and .cpus == 0.5
     and .pids_limit == 64
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {
       "driver": "json-file",
       "options": {"max-file": "2", "max-size": "5m"}
     }
     and .healthcheck == {
       "test": ["CMD", "/usr/local/bin/report-worker", "healthcheck"],
       "timeout": "8s", "interval": "30s", "retries": 3,
       "start_period": "10s"
     }'

  check "mail egress: proxy is isolated, credentialless and has no ingress" "$proxy" \
    '.profiles == ["reporting-mail-canary"]
     and .image == "mcp-ozon-mail-egress:local"
     and ((.ports // []) | length == 0)
     and ((.volumes // []) | length == 0)
     and ((.environment // {}) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.networks | keys | sort) == ["mail-egress-internal", "outbound"]'
  check "mail egress: filesystem, privileges, resources and denial probe are exact" "$proxy" \
    '.read_only == true
     and .cap_drop == ["ALL"]
     and .security_opt == ["no-new-privileges:true"]
     and (.privileged // false) == false
     and (.tmpfs // [] | sort) == [
       "/var/cache/squid:size=8m,mode=1777",
       "/var/log/squid:size=4m,mode=1777",
       "/var/run/squid:size=1m,mode=1777"
     ]
     and .mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {"driver": "json-file", "options": {"max-file": "2", "max-size": "1m"}}
     and .healthcheck == {
       "test": ["CMD-SHELL", "printf '"'"'GET http://healthcheck.invalid/ HTTP/1.0\r\n\r\n'"'"' | nc -w 3 127.0.0.1 3129 | head -1 | grep -q '"'"'403 Forbidden'"'"'"],
       "timeout": "8s", "interval": "30s", "retries": 3, "start_period": "10s"
     }'
  check "mail canary: network definitions keep worker internal and proxy-only outbound" "$rendered" \
    '.networks["mail-egress-internal"].name == "mcp-ozon-mail-egress-internal"
     and .networks["mail-egress-internal"].internal == true
     and .networks.outbound.name == "mcp-ozon-outbound"
     and .networks.outbound.external == true'
}

verify_reporting_mail_live() {
  local rendered="$1" canary="$2" worker comparison
  worker="$(jq -c '.services["report-worker"]' <<<"$rendered")"

  check "scheduled mail: worker is selected only through its explicit profile" "$worker" \
    '.profiles == ["reporting-mail-live"]
     and (.command // []) == []
     and .environment.REPORT_WORKER_MODE == "scheduled_delivery"
     and .environment.REPORT_MAIL_CANARY_AUDIENCE_ID == "pilot_owner"'
  check "scheduled mail: worker remains internal and has no ingress or Compose secrets" "$worker" \
    '(.networks | keys | sort) == ["mail-egress-internal", "position-internal"]
     and ((.ports // []) | length == 0)
     and (has("env_file") | not)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)'

  # Every field except the three explicit activation differences must remain
  # equivalent to the fully verified one-shot canary topology.
  comparison="$(jq -cn \
    --argjson live "$rendered" \
    --argjson canary "$canary" \
    '{
       live: ($live
         | .services["report-worker"].profiles = ["reporting-mail-canary"]
         | .services["report-worker"].command = ["healthcheck"]
         | .services["report-worker"].environment.REPORT_WORKER_MODE = "delivery_canary"
         | del(.services["report-worker"].environment.REPORT_MAIL_CANARY_AUDIENCE_ID)
         | .services["mail-egress"].profiles = ["reporting-mail-canary"]),
       canary: $canary
     }')"
  check "scheduled mail: topology is exactly the verified canary with only activation fields changed" \
    "$comparison" '.live == .canary'
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
  # The healthcheck must prove the proxy is enforcing its policy, not merely
  # that a process exists. Pinning it here means opening the allowlist cannot
  # pass review by quietly relaxing the probe alongside it.
  check "ozon egress: bounded resources, logs, and local healthcheck are exact" "$service" \
    '.mem_limit == "134217728"
     and .cpus == 0.25
     and .pids_limit == 64
     and .stop_grace_period == "10s"
     and .logging == {"driver": "json-file", "options": {"max-file": "2", "max-size": "1m"}}
     and .healthcheck == {
       "test": ["CMD-SHELL", "printf '"'"'GET http://healthcheck.invalid/ HTTP/1.0\r\n\r\n'"'"' | nc -w 3 127.0.0.1 3128 | head -1 | grep -q '"'"'403 Forbidden'"'"'"],
       "timeout": "8s", "interval": "30s", "retries": 3, "start_period": "10s"
     }'
}

# The disabled Control MCP is intentionally a separate, credentialless service
# with no Internet route. Its exact environment is allowlisted here: adding a
# marketplace credential name or any new service setting must fail review.
# shellcheck disable=SC2016
verify_control() {
  local rendered="$1"
  local service ingress
  service="$(jq -c '.services.control' <<<"$rendered")"
  ingress="$(jq -c '.services["control-ingress"]' <<<"$rendered")"

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

  check "control: has no direct host ingress" "$service" \
    '((.ports // []) | length) == 0'
  check "control: only internal bridges are attached" "$rendered" \
    '(.services.control.networks | keys | sort) == ["control_ingress_internal", "control_isolated"]
     and (.services.control.network_mode? == null)
     and (.networks | keys | sort) == ["control_host_ingress", "control_ingress_internal", "control_isolated"]
     and .networks.control_isolated.name == "mcp-ozon-control_control_isolated"
     and .networks.control_isolated.driver == "bridge"
     and .networks.control_isolated.internal == true
     and .networks.control_isolated.driver_opts == {
       "com.docker.network.bridge.enable_icc": "false",
       "com.docker.network.bridge.host_binding_ipv4": "127.0.0.1"
     }
     and .networks.control_ingress_internal.internal == true
     and ((.networks.control_host_ingress.internal // false) | not)'

  check "control ingress: service is credentialless and publishes exactly localhost:8790" "$ingress" \
    '(.ports // []) == [{
       "mode": "ingress",
       "host_ip": "127.0.0.1",
       "target": 8790,
       "published": "8790",
       "protocol": "tcp"
     }]
     and (has("environment") | not)
     and (has("env_file") | not)
     and ((.volumes // []) | length == 0)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.networks | keys | sort) == ["control_host_ingress", "control_ingress_internal"]'
  check "control ingress: relay image and hardening are exact" "$ingress" \
    --arg project_dir "$project_dir" \
    '.build.context == $project_dir
     and .build.dockerfile == "Dockerfile.control-ingress"
     and .read_only == true
     and .cap_drop == ["ALL"]
     and ((.cap_add // []) | length == 0)
     and ((.security_opt // []) | index("no-new-privileges:true") != null)
     and (.privileged // false) == false
     and .mem_limit == "33554432"
     and .cpus == 0.1
     and .pids_limit == 32
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {"driver":"json-file","options":{"max-file":"2","max-size":"1m"}}
     and .healthcheck == {
       "test":["CMD-SHELL","nc -z -w 3 127.0.0.1 8790"],
       "timeout":"3s", "interval":"10s", "retries":5, "start_period":"10s"
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

verify_control_actor_override() {
  local rendered="$1" baseline="$2"
  check "control: explicit dev actor override changes only the actor identity" "$rendered" \
    --argjson baseline "$baseline" \
    '.services.control.environment.CONTROL_MCP_ACTOR_ID == "verify_actor"
     and (.services.control.environment
          | .CONTROL_MCP_ACTOR_ID = "admin") == $baseline.services.control.environment
     and (del(.services.control.environment.CONTROL_MCP_ACTOR_ID)
          == ($baseline | del(.services.control.environment.CONTROL_MCP_ACTOR_ID)))'
}

verify_control_wb_plan() {
  local rendered="$1" base_rendered="$2" service base_service write_proxy auth_proxy read_token_file expected_database_url
  service="$(jq -c '.services.control' <<<"$rendered")"
  base_service="$(jq -c '.services.control' <<<"$base_rendered")"
  write_proxy="$(jq -c '.services["control-write-egress"]' <<<"$rendered")"
  auth_proxy="$(jq -c '.services["control-auth-egress"]' <<<"$rendered")"
  read_token_file="$scratch/wb-promotion-read.token"
  expected_database_url='postgresql://control_writer:verify-only-control-writer-not-a-secret@position-db:5432/ozon_positions'

  check "control WB plan: only reviewed environment, mounts, networks and proxy dependencies differ from base" "$service" \
    --argjson base "$base_service" \
    'del(.depends_on, .environment, .volumes, .networks)
       == ($base | del(.depends_on, .environment, .volumes, .networks))'
  check "control WB plan: final environment is exactly base plus reviewed JWT and planner keys" "$service" \
    --argjson base "$base_service" \
    --arg database_url "$expected_database_url" \
    '.environment == ($base.environment + {
       "CONTROL_MCP_AUTH_MODE": "jwt",
       "CONTROL_MCP_DATABASE_URL": $database_url,
       "CONTROL_MCP_JWT_AUDIENCE": "https://control.example.test/mcp",
       "CONTROL_MCP_JWT_ISSUER": "https://auth.example.test/realms/ofk",
       "CONTROL_MCP_JWT_JWKS_URL": "http://control-auth-egress:8080/jwks",
       "CONTROL_MCP_ALLOW_BROAD_READ_TOKEN": "false",
       "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED": "false",
       "CONTROL_MCP_PUBLIC_URL": "https://control.example.test/mcp",
       "CONTROL_MCP_WB_ACCOUNT_ID": "ip_domnyshev_wb",
       "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE": "/run/mcp-ozon/control-credentials/wb-promotion-read.token",
       "CONTROL_MCP_WB_PROXY": "http://control-write-egress:3130",
       "CONTROL_MCP_WB_TIMEOUT_SECONDS": "20"
     })
     and ([.environment[] | select(type == "string" and contains("verification-"))] | length == 0)'
  check "control WB plan: base metadata plus read token are the only mounts; no write path exists" "$service" \
    --arg access "$project_dir/config/access.example.json" \
    --arg policy "$project_dir/config/control-policy.example.json" \
    --arg read_token "$read_token_file" \
    '((.volumes // []) | map(del(.bind)) | sort_by(.target)) == [
       {"type":"bind","source":$access,"target":"/etc/mcp-ozon/access.json","read_only":true},
       {"type":"bind","source":$policy,"target":"/etc/mcp-ozon/control-policy.json","read_only":true},
       {"type":"bind","source":$read_token,"target":"/run/mcp-ozon/control-credentials/wb-promotion-read.token","read_only":true}
     ]
     and all((.volumes // [])[];
       .bind == null or .bind == {} or .bind == {"create_host_path":false})'
  check "control WB plan: both egress proxies must be healthy before Control starts" "$service" \
    '.depends_on == {
       "control-auth-egress": {"condition":"service_healthy","required":true},
       "control-write-egress": {"condition":"service_healthy","required":true}
     }'
  check "control WB plan: Control has internal DB/proxy routes and no direct outbound" "$rendered" \
    '(.services.control.networks | keys | sort) == ["control-auth-egress-internal","control-write-egress-internal","control_ingress_internal","control_isolated","position-internal"]
     and ((.services.control.networks | has("outbound")) | not)
     and .networks["position-internal"].name == "mcp-ozon-position-internal"
     and .networks["position-internal"].external == true
     and .networks["control-write-egress-internal"].internal == true
     and .networks["control-auth-egress-internal"].internal == true
     and .networks.outbound.name == "mcp-ozon-outbound"
     and .networks.outbound.external == true
     and .networks.control_isolated.internal == true
     and .networks.control_ingress_internal.internal == true'

  check "control write egress: service is credentialless, private and exactly connected" "$write_proxy" \
    '(has("environment") | not)
     and (has("env_file") | not)
     and ((.ports // []) | length == 0)
     and ((.volumes // []) | length == 0)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.networks | keys | sort) == ["control-write-egress-internal","outbound"]'
  check "control write egress: image, resource and privilege contract is exact" "$write_proxy" \
    --arg project_dir "$project_dir" \
    '.image == "mcp-ozon-control-write-egress:local"
     and .build.context == $project_dir
     and .build.dockerfile == "Dockerfile.control-write-egress"
     and .read_only == true
     and .cap_drop == ["ALL"]
     and ((.cap_add // []) | length == 0)
     and ((.security_opt // []) | index("no-new-privileges:true") != null)
     and (.privileged // false) == false
     and .mem_limit == "268435456"
     and .cpus == 0.25
     and .pids_limit == 32
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {"driver":"json-file","options":{"max-file":"2","max-size":"1m"}}
     and (.tmpfs | sort) == [
       "/var/cache/squid:size=4m,mode=1777",
       "/var/log/squid:size=2m,mode=1777",
       "/var/run/squid:size=1m,mode=1777"
     ]'
  check "control write egress: healthcheck proves deny-by-default listener" "$write_proxy" \
    '.healthcheck == {
       "test": ["CMD-SHELL", "printf '"'"'GET http://healthcheck.invalid/ HTTP/1.0\r\n\r\n'"'"' | nc -w 3 127.0.0.1 3130 | head -1 | grep -q '"'"'403 Forbidden'"'"'"],
       "timeout":"8s", "interval":"30s", "retries":3, "start_period":"10s"
     }'

  check "control auth egress: only public exact-host/path metadata is configured" "$auth_proxy" \
    '.environment == {
       "CONTROL_AUTH_JWKS_HOST":"auth.example.test",
       "CONTROL_AUTH_JWKS_PATH":"/realms/ofk/protocol/openid-connect/certs"
     }
     and (has("env_file") | not)
     and ((.ports // []) | length == 0)
     and ((.volumes // []) | length == 0)
     and ((.secrets // []) | length == 0)
     and ((.configs // []) | length == 0)
     and (.networks | keys | sort) == ["control-auth-egress-internal","outbound"]'
  check "control auth egress: image, resource and privilege contract is exact" "$auth_proxy" \
    --arg project_dir "$project_dir" \
    '.image == "mcp-ozon-control-auth-egress:local"
     and .build.context == $project_dir
     and .build.dockerfile == "Dockerfile.control-auth-egress"
     and .read_only == true
     and .cap_drop == ["ALL"]
     and ((.cap_add // []) | length == 0)
     and ((.security_opt // []) | index("no-new-privileges:true") != null)
     and (.privileged // false) == false
     and .mem_limit == "67108864"
     and .cpus == 0.25
     and .pids_limit == 32
     and .stop_grace_period == "10s"
     and .restart == "unless-stopped"
     and .logging == {"driver":"json-file","options":{"max-file":"2","max-size":"1m"}}
     and .tmpfs == ["/tmp:size=8m,mode=1777"]'
  check "control auth egress: healthcheck is local and cannot fetch upstream" "$auth_proxy" \
    '.healthcheck == {
       "test":["CMD","wget","-q","-T","3","-O","/dev/null","http://127.0.0.1:8080/health"],
       "timeout":"8s", "interval":"30s", "retries":3, "start_period":"10s"
     }'
}

verify_control_wb_live() {
  local rendered="$1" plan_rendered="$2" service plan_service read_token_file write_token_file
  service="$(jq -c '.services.control' <<<"$rendered")"
  plan_service="$(jq -c '.services.control' <<<"$plan_rendered")"
  read_token_file="$scratch/wb-promotion-read.token"
  write_token_file="$scratch/wb-promotion-write.token"

  check "control WB live: executor overlay cannot change services or network topology" "$rendered" \
    --argjson plan "$plan_rendered" \
    'del(.services.control.environment, .services.control.volumes)
       == ($plan | del(.services.control.environment, .services.control.volumes))'
  check "control WB live: executor layer changes only environment and mounted credentials" "$service" \
    --argjson plan "$plan_service" \
    'del(.environment, .volumes) == ($plan | del(.environment, .volumes))'
  check "control WB live: environment differs from verified planner by exactly write opt-in and write-token path" "$service" \
    --argjson plan "$plan_service" \
    '.environment == ($plan.environment + {
       "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED": "true",
       "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE": "/run/mcp-ozon/control-credentials/wb-promotion-write.token"
     })'
  check "control WB live: the write credential is the only mount added to planner" "$service" \
    --arg access "$project_dir/config/access.example.json" \
    --arg policy "$project_dir/config/control-policy.example.json" \
    --arg read_token "$read_token_file" \
    --arg write_token "$write_token_file" \
    '((.volumes // []) | map(del(.bind)) | sort_by(.target)) == [
       {"type":"bind","source":$access,"target":"/etc/mcp-ozon/access.json","read_only":true},
       {"type":"bind","source":$policy,"target":"/etc/mcp-ozon/control-policy.json","read_only":true},
       {"type":"bind","source":$read_token,"target":"/run/mcp-ozon/control-credentials/wb-promotion-read.token","read_only":true},
       {"type":"bind","source":$write_token,"target":"/run/mcp-ozon/control-credentials/wb-promotion-write.token","read_only":true}
     ]
     and all((.volumes // [])[];
       .bind == null or .bind == {} or .bind == {"create_host_path":false})'
}

main_rendered="$(render_compose "$project_dir/compose.yaml")"
canary_rendered="$(render_compose "$project_dir/compose.canary.yaml")"
position_rendered="$(render_position_compose)"
wb_automation_shadow_rendered="$(render_wb_automation_shadow_compose)"
wb_automation_live_rendered="$(render_wb_automation_live_compose)"
reporting_reader_rendered="$(render_reporting_reader_compose)"
reporting_live_rendered="$(render_reporting_live_compose)"
reporting_canary_rendered="$(render_reporting_canary_compose)"
reporting_mail_canary_rendered="$(render_reporting_mail_canary_compose)"
reporting_mail_live_rendered="$(render_reporting_mail_live_compose)"
control_rendered="$(render_control_compose)"
control_actor_override_rendered="$(render_control_compose verify_actor)"
control_wb_plan_rendered="$(render_control_wb_plan_compose)"
control_wb_live_rendered="$(render_control_wb_live_compose)"

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
check_contains \
  "control WB plan: JWKS upstream host has no fallback" \
  "$project_dir/compose.control-wb-plan.yaml" \
  "\${CONTROL_MCP_JWT_JWKS_HOST:?CONTROL_MCP_JWT_JWKS_HOST is required}"
check_contains \
  "control WB plan: JWKS upstream path has no fallback" \
  "$project_dir/compose.control-wb-plan.yaml" \
  "\${CONTROL_MCP_JWT_JWKS_PATH:?CONTROL_MCP_JWT_JWKS_PATH is required}"
check_contains \
  "control WB plan: read token host path has no fallback" \
  "$project_dir/compose.control-wb-plan.yaml" \
  "\${CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE_HOST:?CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE_HOST is required}"
check_contains \
  "control WB live: write capability defaults off even when executor overlay is selected" \
  "$project_dir/compose.control-wb-live.yaml" \
  "\${CONTROL_MCP_MARKETPLACE_WRITES_ENABLED:-false}"
check_contains \
  "control WB live: write token host path has no fallback" \
  "$project_dir/compose.control-wb-live.yaml" \
  "\${CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE_HOST:?CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE_HOST is required}"
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
  "reporting reader: reader password has no fallback value" \
  "$project_dir/compose.reporting-reader.yaml" \
  "\${POSITION_READER_DB_PASSWORD:?POSITION_READER_DB_PASSWORD is required}"
check_contains \
  "reporting reader: fixed database network is external" \
  "$project_dir/compose.reporting-reader.yaml" \
  "name: mcp-ozon-position-internal"
check_contains \
  "live reporting: access registry has no fallback path" \
  "$project_dir/compose.reporting-live.yaml" \
  "\${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required for live reporting}"
check_contains \
  "live reporting: enabled policy has no fallback path" \
  "$project_dir/compose.reporting-live.yaml" \
  "\${DAILY_REPORT_POLICY_HOST:?DAILY_REPORT_POLICY_HOST is required for live reporting}"
check_contains \
  "live reporting: credential directory has no fallback path" \
  "$project_dir/compose.reporting-live.yaml" \
  "\${REPORT_COLLECTOR_CREDENTIAL_DIR_HOST:?REPORT_COLLECTOR_CREDENTIAL_DIR_HOST is required for live reporting}"
check_contains \
  "reporting canary: access registry has no fallback path" \
  "$project_dir/compose.reporting-canary.yaml" \
  "\${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required for reporting canary}"
check_contains \
  "reporting canary: disabled policy has no fallback path" \
  "$project_dir/compose.reporting-canary.yaml" \
  "\${DAILY_REPORT_CANARY_POLICY_HOST:?DAILY_REPORT_CANARY_POLICY_HOST is required for reporting canary}"
check_contains \
  "reporting canary: credential directory has no fallback path" \
  "$project_dir/compose.reporting-canary.yaml" \
  "\${REPORT_COLLECTOR_CREDENTIAL_DIR_HOST:?REPORT_COLLECTOR_CREDENTIAL_DIR_HOST is required for reporting canary}"
check_contains \
  "reporting canary: runner never creates or recreates dependencies" \
  "$project_dir/scripts/run-report-canary.sh" \
  "run --rm --no-deps report-collector"
check_contains \
  "mail canary: access registry has no fallback path" \
  "$project_dir/compose.reporting-mail-canary.yaml" \
  "\${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required for mail canary}"
check_contains \
  "mail canary: enabled policy has no fallback path" \
  "$project_dir/compose.reporting-mail-canary.yaml" \
  "\${DAILY_REPORT_MAIL_POLICY_HOST:?DAILY_REPORT_MAIL_POLICY_HOST is required for mail canary}"
check_contains \
  "mail canary: routing file has no fallback path" \
  "$project_dir/compose.reporting-mail-canary.yaml" \
  "\${REPORT_MAIL_ROUTING_HOST:?REPORT_MAIL_ROUTING_HOST is required for mail canary}"
check_contains \
  "mail canary: OAuth directory has no fallback path" \
  "$project_dir/compose.reporting-mail-canary.yaml" \
  "\${REPORT_GMAIL_OAUTH_DIR_HOST:?REPORT_GMAIL_OAUTH_DIR_HOST is required for mail canary}"
check_contains \
  "scheduled mail: access registry has no fallback path" \
  "$project_dir/compose.reporting-mail-live.yaml" \
  "\${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required for scheduled mail}"
check_contains \
  "scheduled mail: enabled policy has no fallback path" \
  "$project_dir/compose.reporting-mail-live.yaml" \
  "\${DAILY_REPORT_MAIL_POLICY_HOST:?DAILY_REPORT_MAIL_POLICY_HOST is required for scheduled mail}"
check_contains \
  "scheduled mail: routing file has no fallback path" \
  "$project_dir/compose.reporting-mail-live.yaml" \
  "\${REPORT_MAIL_ROUTING_HOST:?REPORT_MAIL_ROUTING_HOST is required for scheduled mail}"
check_contains \
  "scheduled mail: OAuth directory has no fallback path" \
  "$project_dir/compose.reporting-mail-live.yaml" \
  "\${REPORT_GMAIL_OAUTH_DIR_HOST:?REPORT_GMAIL_OAUTH_DIR_HOST is required for scheduled mail}"
check_contains \
  "scheduled mail: activation audience has no fallback value" \
  "$project_dir/compose.reporting-mail-live.yaml" \
  "\${REPORT_MAIL_CANARY_AUDIENCE_ID:?REPORT_MAIL_CANARY_AUDIENCE_ID is required for scheduled mail}"
check_contains \
  "mail canary: runner waits for the isolated proxy only" \
  "$project_dir/scripts/run-report-mail-canary.sh" \
  "up --detach --wait --wait-timeout 30 --no-deps mail-egress"
check_contains \
  "mail canary: runner performs exactly an explicit one-shot delivery" \
  "$project_dir/scripts/run-report-mail-canary.sh" \
  "run --rm --no-deps report-worker deliver-one"
check_contains \
  "scheduled collection: runner requires explicit canary reconciliation" \
  "$project_dir/scripts/start-report-collector-scheduler.sh" \
  "--confirm-canaries-published-and-reconciled"
check_contains \
  "scheduled collection: runner requires a database-backed activation preflight" \
  "$project_dir/scripts/start-report-collector-scheduler.sh" \
  "run --rm --no-deps report-collector collection-preflight"
check_contains \
  "scheduled collection: runner activates only collector and marketplace egress" \
  "$project_dir/scripts/start-report-collector-scheduler.sh" \
  "up --detach --wait --wait-timeout 60 ozon-egress report-collector"
check_contains \
  "scheduled mail: runner requires explicit canary reconciliation" \
  "$project_dir/scripts/start-report-mail-scheduler.sh" \
  "--confirm-canary-sent-and-reconciled"
check_contains \
  "scheduled mail: runner activates only the exact mail services" \
  "$project_dir/scripts/start-report-mail-scheduler.sh" \
  'up --detach --wait --wait-timeout 60 mail-egress report-worker'
# shellcheck disable=SC2016
check_contains \
  "scheduled mail: runner requires a database-backed activation preflight" \
  "$project_dir/scripts/start-report-mail-scheduler.sh" \
  'mail-preflight "$REPORT_MAIL_CANARY_AUDIENCE_ID"'
check_contains \
  "WB automation shadow installer requires explicit legacy-writer shutdown" \
  "$project_dir/scripts/install-wb-automation-shadow-agent.sh" \
  "--confirm-stop-legacy-writer-and-start-shadow"
check_contains \
  "WB automation shadow installer stops the legacy LaunchAgent" \
  "$project_dir/scripts/install-wb-automation-shadow-agent.sh" \
  'launchctl bootout "$domain/$legacy_label"'
check_contains \
  "WB automation shadow installer disables legacy restart persistence" \
  "$project_dir/scripts/install-wb-automation-shadow-agent.sh" \
  'mv "$legacy_plist" "$legacy_plist_disabled"'
check_contains \
  "WB automation shadow installer removes legacy write egress" \
  "$project_dir/scripts/install-wb-automation-shadow-agent.sh" \
  'stop write-egress'
check_contains \
  "WB automation shadow installer proves read egress before cutover" \
  "$project_dir/scripts/install-wb-automation-shadow-agent.sh" \
  'up -d --no-deps --wait --wait-timeout 60 ozon-egress'
check_contains \
  "WB automation shadow runner reads protected env outside Documents" \
  "$project_dir/scripts/run-wb-automation-shadow.sh" \
  'position_env="${WB_AUTOMATION_POSITION_ENV:-$runtime_dir/position.env}"'
check_contains \
  "WB automation shadow runner uses an ephemeral no-dependency job" \
  "$project_dir/scripts/run-wb-automation-shadow.sh" \
  'run --rm --no-deps wb-automation-shadow'
if grep -Fq -- 'WRITE_TOKEN' "$project_dir/scripts/run-wb-automation-shadow.sh"; then
  printf 'FAIL WB automation shadow runner references a writer token\n' >&2
  failures=$((failures + 1))
else
  printf 'ok   WB automation shadow runner has no writer-token capability\n'
fi
check_contains \
  "WB automation live installer requires explicit shadow-to-live cutover" \
  "$project_dir/scripts/install-wb-automation-live-agent.sh" \
  "--confirm-stop-shadow-and-start-protective-live"
check_contains \
  "WB automation live installer stops the shadow LaunchAgent" \
  "$project_dir/scripts/install-wb-automation-live-agent.sh" \
  'launchctl bootout "$domain/$shadow_label"'
check_contains \
  "WB automation live installer activates policy through the audited CLI" \
  "$project_dir/scripts/install-wb-automation-live-agent.sh" \
  "activate-protective-live-pg"
check_contains \
  "WB automation bid-live installer requires explicit bid-write cutover" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  "--confirm-enable-reviewed-bid-writes"
check_contains \
  "WB automation bid-live installer proves the new observer without writes" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  "observe-once"
check_contains \
  "WB automation bid-live preflight uses a private umask" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  "umask 077"
check_contains \
  "WB automation bid-live preflight creates its ephemeral state directory" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  'mkdir "$state_directory"'
check_contains \
  "WB automation bid-live installer validates the read-only preflight" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  'WB automation bid-live read-only preflight did not complete'
check_contains \
  "WB automation bid-live installer stops the protective LaunchAgent" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  'launchctl bootout "$domain/$label"'
check_contains \
  "WB automation bid-live installer waits for the one-shot worker" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  'label=com.docker.compose.service=wb-automation-live'
check_contains \
  "WB automation bid-live installer uses the audited PostgreSQL transition" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  "activate-bid-writes-pg"
check_contains \
  "WB automation bid-live installer validates activation outcome" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  '.outcome == "bid_writes_activated"'
check_contains \
  "WB automation bid-live installer enables the runner mode explicitly" \
  "$project_dir/scripts/enable-wb-automation-bid-writes.sh" \
  "WB_AUTOMATION_BID_WRITES_ENABLED=true"
check_contains \
  "WB automation protective installer pins bid writes off" \
  "$project_dir/scripts/install-wb-automation-live-agent.sh" \
  's|__BID_WRITES_ENABLED__|false|g'
check_contains \
  "WB automation live runner uses an ephemeral no-dependency job" \
  "$project_dir/scripts/run-wb-automation-live.sh" \
  'run --rm --no-deps wb-automation-live'
check_contains \
  "WB automation live runner performs immediate bounded read-back" \
  "$project_dir/scripts/run-wb-automation-live.sh" \
  'write_sent_reconciliation_required'
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
check_contains \
  "mail egress: proxy permits only the fixed OAuth and Gmail API hosts" \
  "$project_dir/position-monitor/mail-egress/squid.conf" \
  "acl google_mail_api dstdomain oauth2.googleapis.com gmail.googleapis.com"
check_contains \
  "mail egress: proxy applies the exact Google host allowlist" \
  "$project_dir/position-monitor/mail-egress/squid.conf" \
  "http_access allow connect google_mail_api tls_port"
check_contains \
  "mail egress: proxy denies every other destination" \
  "$project_dir/position-monitor/mail-egress/squid.conf" \
  "http_access deny all"
check_contains \
  "control write egress: proxy permits only the fixed WB Promotion host" \
  "$project_dir/position-monitor/control-write-egress/squid.conf" \
  "acl wb_promotion_write_api dstdomain advert-api.wildberries.ru"
check_contains \
  "control write egress: proxy applies exact CONNECT/TLS policy" \
  "$project_dir/position-monitor/control-write-egress/squid.conf" \
  "http_access allow connect wb_promotion_write_api tls_port"
check_contains \
  "control write egress: proxy denies every other destination" \
  "$project_dir/position-monitor/control-write-egress/squid.conf" \
  "http_access deny all"
check_contains \
  "WB automation runtime: legacy state parent is private to the automation user" \
  "$project_dir/Dockerfile.wb-automation-shadow" \
  "&& chmod 0700 /var/lib/mcp-ozon-legacy"
check_contains \
  "control auth egress: only the exact local JWKS path reaches upstream" \
  "$project_dir/position-monitor/control-auth-egress/nginx.conf.template" \
  'location = /jwks {'
check_contains \
  "control auth egress: local JWKS query strings are rejected" \
  "$project_dir/position-monitor/control-auth-egress/nginx.conf.template" \
  'if ($args != "") { return 404; }'
check_contains \
  "control auth egress: upstream host/path are the only routing substitutions" \
  "$project_dir/position-monitor/control-auth-egress/nginx.conf.template" \
  'proxy_pass https://${CONTROL_AUTH_JWKS_HOST}${CONTROL_AUTH_JWKS_PATH};'
check_contains \
  "control auth egress: upstream certificate verification is mandatory" \
  "$project_dir/position-monitor/control-auth-egress/nginx.conf.template" \
  "proxy_ssl_verify on;"
check_contains \
  "control auth egress: caller headers are never forwarded" \
  "$project_dir/position-monitor/control-auth-egress/nginx.conf.template" \
  "proxy_pass_request_headers off;"
check_contains \
  "control auth egress: every other local path is rejected" \
  "$project_dir/position-monitor/control-auth-egress/nginx.conf.template" \
  'location / {'
check_contains \
  "control auth egress: hostname and path are validated before substitution" \
  "$project_dir/position-monitor/control-auth-egress/entrypoint.sh" \
  "CONTROL_AUTH_JWKS_PATH must be one bounded absolute path"
check_contains \
  "control auth egress: generated configuration is validated before startup" \
  "$project_dir/position-monitor/control-auth-egress/entrypoint.sh" \
  "nginx -t -c /tmp/nginx.conf"

verify_server \
  "main" \
  "$main_rendered" \
  "$main_access" \
  "8787" \
  "unless-stopped" \
  "mcp-ozon-outbound"
verify_server \
  "canary" \
  "$canary_rendered" \
  "$canary_access" \
  "8789" \
  "no" \
  "mcp-ozon-canary-outbound"
verify_reporting_reader "$reporting_reader_rendered" "$main_rendered"
verify_position "$position_rendered"
verify_position_collector "$position_rendered"
verify_wb_automation_shadow "$wb_automation_shadow_rendered"
verify_wb_automation_live "$wb_automation_live_rendered"
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
verify_reporting_live "$reporting_live_rendered"
verify_reporting_canary "$reporting_canary_rendered"
verify_reporting_mail_canary "$reporting_mail_canary_rendered"
verify_reporting_mail_live "$reporting_mail_live_rendered" "$reporting_mail_canary_rendered"
verify_control "$control_rendered"
verify_control_actor_override "$control_actor_override_rendered" "$control_rendered"
verify_control_wb_plan "$control_wb_plan_rendered" "$control_rendered"
verify_control_wb_live "$control_wb_live_rendered" "$control_wb_plan_rendered"

if (( failures > 0 )); then
  echo "compose hardening verification failed with $failures problem(s)" >&2
  exit 1
fi

echo "Compose hardening verified for main, canary, base/plan/executor Control, position database, PostgreSQL-backed WB automation shadow, the opt-in MCP reporting reader, dedicated Control write/JWKS proxies, Ozon read-API and Gmail egress proxies, disabled collector/reporting runtimes, and the explicit reporting collection/mail canary and live overlays: exact resource/mount/health contracts, write-secret separation, loopback-only publication, isolated egress, and internal database networks."
