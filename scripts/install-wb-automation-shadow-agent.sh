#!/bin/bash

set -euo pipefail

confirmation="--confirm-stop-legacy-writer-and-start-shadow"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
policy_source="$project_root/config/wb-automation-robot.json"
runner_source="$project_root/scripts/run-wb-automation-shadow.sh"
plist_template="$project_root/ops/macos/com.ofk.mcp-ozon-wb-automation-shadow.plist.in"
shadow_compose="$project_root/compose.wb-automation-shadow.yaml"
position_compose="$project_root/compose.position.yaml"
legacy_egress_compose="$project_root/compose.wb-automation-egress.yaml"
roles_script="$project_root/position-monitor/initdb/003_roles.sh"
migration="$project_root/position-monitor/initdb/021_wb_automation_state.sql"
position_env="$project_root/.position.env"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
position_env_target="$runtime_dir/position.env"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
legacy_state="${WB_AUTOMATION_LEGACY_STATE:-$runtime_dir/wb-automation-robot/execution-state.json}"
policy_target="$runtime_dir/wb-automation-shadow-policy.json"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
runner_target="$libexec_dir/run-wb-automation-shadow.sh"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
label="com.ofk.mcp-ozon-wb-automation-shadow"
legacy_label="com.ofk.mcp-ozon-wb-automation-observer"
plist="$agent_dir/$label.plist"
legacy_plist="$agent_dir/$legacy_label.plist"
legacy_plist_disabled="$agent_dir/$legacy_label.plist.shadow-disabled"
domain="gui/$(id -u)"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/wb-automation-shadow.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_plist"
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "WB automation shadow LaunchAgent installer supports only macOS" >&2
  exit 1
fi
for path in \
  "$policy_source" "$runner_source" "$plist_template" "$shadow_compose" \
  "$position_compose" "$legacy_egress_compose" "$roles_script" "$migration" "$position_env" \
  "$registry" "$reader_token" "$legacy_state"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required WB automation shadow file is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ "$(/usr/bin/stat -f '%Lp' "$position_env")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$reader_token")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$legacy_state")" != "600" ]]; then
  echo "database env, WB read token and legacy execution state must have mode 600" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
' "$policy_source" >/dev/null; then
  echo "repository WB automation policy is not the approved shadow policy" >&2
  exit 1
fi
if [[ -e "$legacy_plist" && ( ! -f "$legacy_plist" || -L "$legacy_plist" ) ]]; then
  echo "legacy WB automation LaunchAgent plist is unsafe" >&2
  exit 1
fi
if [[ -e "$legacy_plist" && -e "$legacy_plist_disabled" ]]; then
  echo "refusing to overwrite the existing disabled legacy LaunchAgent backup" >&2
  exit 1
fi
if ! jq -e '
  .schema_version == 1
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .pending == null
' "$legacy_state" >/dev/null; then
  echo "legacy WB automation state cannot be migrated safely" >&2
  exit 1
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi

if ! grep -q '^WB_AUTOMATION_DB_PASSWORD=' "$position_env"; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate the restricted database password" >&2
    exit 1
  fi
  printf '\nWB_AUTOMATION_DB_PASSWORD=%s\n' "$(openssl rand -hex 32)" >>"$position_env"
fi
if ! grep -Eq '^WB_AUTOMATION_DB_PASSWORD=.{24,}$' "$position_env"; then
  echo "WB_AUTOMATION_DB_PASSWORD is invalid" >&2
  exit 1
fi

POSITION_DB_NAME="${POSITION_DB_NAME:-}"
POSITION_DB_ADMIN_USER="${POSITION_DB_ADMIN_USER:-}"
POSITION_DB_ADMIN_PASSWORD="${POSITION_DB_ADMIN_PASSWORD:-}"
POSITION_COLLECTOR_DB_PASSWORD="${POSITION_COLLECTOR_DB_PASSWORD:-}"
POSITION_READER_DB_PASSWORD="${POSITION_READER_DB_PASSWORD:-}"
REPORT_WORKER_DB_PASSWORD="${REPORT_WORKER_DB_PASSWORD:-}"
REPORT_COLLECTOR_DB_PASSWORD="${REPORT_COLLECTOR_DB_PASSWORD:-}"
CONTROL_WRITER_DB_PASSWORD="${CONTROL_WRITER_DB_PASSWORD:-}"
WB_AUTOMATION_DB_PASSWORD="${WB_AUTOMATION_DB_PASSWORD:-}"
set -a
# shellcheck disable=SC1090,SC1091 # Protected local deployment environment.
source "$position_env"
set +a
: "${POSITION_DB_ADMIN_PASSWORD:?POSITION_DB_ADMIN_PASSWORD is required}"
: "${POSITION_COLLECTOR_DB_PASSWORD:?POSITION_COLLECTOR_DB_PASSWORD is required}"
: "${POSITION_READER_DB_PASSWORD:?POSITION_READER_DB_PASSWORD is required}"
: "${REPORT_WORKER_DB_PASSWORD:?REPORT_WORKER_DB_PASSWORD is required}"
: "${REPORT_COLLECTOR_DB_PASSWORD:?REPORT_COLLECTOR_DB_PASSWORD is required}"
: "${CONTROL_WRITER_DB_PASSWORD:?CONTROL_WRITER_DB_PASSWORD is required}"
: "${WB_AUTOMATION_DB_PASSWORD:?WB_AUTOMATION_DB_PASSWORD is required}"

position_container="$("$docker_bin" ps \
  --filter label=com.docker.compose.project=mcp-ozon-position \
  --filter label=com.docker.compose.service=position-db \
  --format '{{.ID}}' | head -n 1)"
if [[ -z "$position_container" ]]; then
  echo "production position database container is unavailable" >&2
  exit 1
fi

# Additive database preparation and image construction cannot touch WB. Finish
# them while the working legacy timer is still available, minimizing the
# guarded no-writer window that begins below.
"$docker_bin" exec -i \
  --env POSTGRES_PASSWORD="$POSITION_DB_ADMIN_PASSWORD" \
  --env POSTGRES_USER="${POSITION_DB_ADMIN_USER:-position_admin}" \
  --env POSTGRES_DB="${POSITION_DB_NAME:-ozon_positions}" \
  --env POSITION_COLLECTOR_DB_PASSWORD="$POSITION_COLLECTOR_DB_PASSWORD" \
  --env POSITION_READER_DB_PASSWORD="$POSITION_READER_DB_PASSWORD" \
  --env REPORT_WORKER_DB_PASSWORD="$REPORT_WORKER_DB_PASSWORD" \
  --env REPORT_COLLECTOR_DB_PASSWORD="$REPORT_COLLECTOR_DB_PASSWORD" \
  --env CONTROL_WRITER_DB_PASSWORD="$CONTROL_WRITER_DB_PASSWORD" \
  --env WB_AUTOMATION_DB_PASSWORD="$WB_AUTOMATION_DB_PASSWORD" \
  "$position_container" /bin/sh -s <"$roles_script" >/dev/null
"$docker_bin" exec -i \
  --env PGPASSWORD="$POSITION_DB_ADMIN_PASSWORD" \
  "$position_container" psql \
    --no-psqlrc --set ON_ERROR_STOP=1 \
    --username "${POSITION_DB_ADMIN_USER:-position_admin}" \
    --dbname "${POSITION_DB_NAME:-ozon_positions}" \
    <"$migration" >/dev/null

mkdir -p "$runtime_dir" "$libexec_dir" "$agent_dir" "$log_dir"
chmod 700 "$runtime_dir" "$libexec_dir"
install -m 700 "$runner_source" "$runner_target"
install -m 644 "$policy_source" "$policy_target"
install -m 600 "$position_env" "$position_env_target"

WB_AUTOMATION_POLICY_HOST="$policy_target" \
WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry" \
WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token" \
WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state" \
  "$docker_bin" compose \
    --project-directory "$project_root" \
    --env-file "$position_env" \
    -f "$shadow_compose" \
    build wb-automation-shadow

sed \
  -e "s|__RUNNER__|$runner_target|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
  -e "s|__PROJECT_DIR__|$project_root|g" \
  "$plist_template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

# The shadow has no direct network route. Prove its credentialless read proxy
# is healthy before disabling the working legacy timer.
"$docker_bin" compose \
  --project-directory "$project_root" \
  --env-file "$position_env" \
  -f "$position_compose" \
  up -d --no-deps --wait --wait-timeout 60 ozon-egress

# The legacy job is the only host process with the WB writer token. Stop it
# before importing its state, then remove its network write capability. Any
# failure after this point deliberately leaves writer automation stopped.
launchctl bootout "$domain/$legacy_label" >/dev/null 2>&1 || true
legacy_pid="$(launchctl print "$domain/$legacy_label" 2>/dev/null | awk '/pid = / { print $3; exit }' || true)"
if [[ -n "$legacy_pid" ]]; then
  echo "legacy WB automation agent is still running" >&2
  exit 1
fi
if [[ -f "$legacy_plist" ]]; then
  mv "$legacy_plist" "$legacy_plist_disabled"
  chmod 600 "$legacy_plist_disabled"
fi
"$docker_bin" compose \
  --project-directory "$project_root" \
  -f "$legacy_egress_compose" \
  stop write-egress >/dev/null

# Prove one PostgreSQL-backed read-only cycle before scheduling future cycles.
WB_AUTOMATION_PROJECT_DIR="$project_root" \
MCP_RUNTIME_DIR="$runtime_dir" \
WB_AUTOMATION_POSITION_ENV="$position_env_target" \
  "$runner_target"

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed $label; legacy writer stopped and PostgreSQL shadow started"
