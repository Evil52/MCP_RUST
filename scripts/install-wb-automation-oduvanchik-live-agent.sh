#!/bin/bash

set -euo pipefail
umask 077

confirmation="--confirm-enable-oduvanchik-safe-auto"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
shadow_policy_source="$project_root/config/wb-automation-oduvanchik.json"
protective_policy_source="$project_root/config/wb-automation-oduvanchik.live.json"
bid_policy_source="$project_root/config/wb-automation-oduvanchik.bid-live.json"
runner_source="$project_root/scripts/run-wb-automation-live.sh"
plist_template="$project_root/ops/macos/com.ofk.mcp-ozon-wb-automation-oduvanchik-live.plist.in"
shadow_compose="$project_root/compose.wb-automation-shadow.yaml"
live_compose="$project_root/compose.wb-automation-live.yaml"
position_compose="$project_root/compose.position.yaml"
position_env="$project_root/.position.env"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
position_env_target="$runtime_dir/position.env"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ofk-region-wb-promotion-read.token}"
writer_token="${WB_AUTOMATION_WRITE_TOKEN_FILE:-$runtime_dir/ofk-region-wb-promotion-write.token}"
campaign_runtime="$runtime_dir/wb-automation-oduvanchik"
legacy_state="$campaign_runtime/execution-state.json"
shadow_policy_target="$campaign_runtime/shadow-policy.json"
protective_policy_target="$campaign_runtime/protective-policy.json"
bid_policy_target="$campaign_runtime/live-policy.json"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
runner_target="$libexec_dir/run-wb-automation-live.sh"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
label="com.ofk.mcp-ozon-wb-automation-oduvanchik-live"
plist="$agent_dir/$label.plist"
domain="gui/$(id -u)"
compose_project="mcp-ozon-wb-automation-oduvanchik-live"
write_egress_container_name="mcp-ozon-wb-automation-oduvanchik-write-egress"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/wb-automation-oduvanchik.XXXXXX")"

# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_plist"
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "WB Oduvanchik automation installer supports only macOS" >&2
  exit 1
fi
for path in \
  "$shadow_policy_source" "$protective_policy_source" "$bid_policy_source" \
  "$runner_source" "$plist_template" "$shadow_compose" "$live_compose" \
  "$position_compose" "$position_env" "$registry" "$reader_token" "$writer_token"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required WB Oduvanchik automation file is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ "$(/usr/bin/stat -f '%Lp' "$position_env")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$reader_token")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$writer_token")" != "600" ]]; then
  echo "database env and WB Oduvanchik tokens must have mode 600" >&2
  exit 1
fi
if ! grep -Eq '^WB_AUTOMATION_DB_PASSWORD=.{24,}$' "$position_env"; then
  echo "WB_AUTOMATION_DB_PASSWORD is unavailable" >&2
  exit 1
fi

policy_guard='
  .policy_version == "wb_ads_robot.v1"
  and .account_id == "ofk_region_wb"
  and .campaign_id == 39807762
  and .campaign_name == "Одуванчик"
  and .nm_ids == [44081446, 41774347, 99236811, 38943938, 44081434]
  and .target_drr_basis_points == 1500
  and .hard_drr_basis_points == 2500
  and .target_impressions_per_day == 1500
  and .target_orders_per_day == 3
  and .autonomous_pacing == "traffic_frontier_v3"
  and .traffic_frontier_bid_kopecks == 525
  and .traffic_frontier_feedback_timeout_seconds == 1800
  and .traffic_frontier_min_feedback_impressions == 200
  and .traffic_frontier_min_feedback_clicks == 10
  and .min_bid_kopecks == 500
  and .max_bid_kopecks == 1050
  and .bid_step_percent == 5
  and .daily_spend_cap_minor == 50000
  and .daily_pause_threshold_minor == 45000
  and .max_actions_per_day == 48
  and .cooldown_seconds == 1800
  and .allow_budget_top_up == false'
if ! jq -e "$policy_guard and .write_enabled == false and ((.bid_writes_enabled // false) == false)" \
  "$shadow_policy_source" >/dev/null; then
  echo "WB Oduvanchik shadow policy exceeds the approved scope" >&2
  exit 1
fi
if ! jq -e "$policy_guard and .write_enabled == true and .bid_writes_enabled == false" \
  "$protective_policy_source" >/dev/null; then
  echo "WB Oduvanchik protective policy exceeds the approved scope" >&2
  exit 1
fi
if ! jq -e "$policy_guard and .write_enabled == true and .bid_writes_enabled == true" \
  "$bid_policy_source" >/dev/null; then
  echo "WB Oduvanchik bid policy exceeds the approved scope" >&2
  exit 1
fi
shadow_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$shadow_policy_source")"
protective_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$protective_policy_source")"
bid_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$bid_policy_source")"
if [[ "$shadow_scope" != "$protective_scope" || "$protective_scope" != "$bid_scope" ]]; then
  echo "WB Oduvanchik policies expand scope between activation stages" >&2
  exit 1
fi
if [[ -e "$plist" && ( ! -f "$plist" || -L "$plist" ) ]]; then
  echo "WB Oduvanchik LaunchAgent plist is unsafe" >&2
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

mkdir -p "$runtime_dir" "$campaign_runtime" "$libexec_dir" "$agent_dir" "$log_dir"
chmod 700 "$runtime_dir" "$campaign_runtime" "$libexec_dir"
install -m 700 "$runner_source" "$runner_target"
install -m 644 "$shadow_policy_source" "$shadow_policy_target"
install -m 644 "$protective_policy_source" "$protective_policy_target"
install -m 644 "$bid_policy_source" "$bid_policy_target"
install -m 600 "$position_env" "$position_env_target"

shadow_digest="$(jq -c '.' "$shadow_policy_source" | shasum -a 256 | awk '{print $1}')"
if [[ ! -e "$legacy_state" ]]; then
  business_date="$(TZ=Europe/Moscow date '+%Y-%m-%d')"
  jq -n \
    --arg policy_sha256 "$shadow_digest" \
    --arg business_date "$business_date" \
    '{
      schema_version: 1,
      policy_sha256: $policy_sha256,
      account_id: "ofk_region_wb",
      campaign_id: 39807762,
      business_date: $business_date,
      actions_today: 0,
      last_action_at: null,
      paused_for_daily_cap_on: null,
      pending: null,
      incident_class: null
    }' >"$legacy_state"
  chmod 600 "$legacy_state"
fi
if [[ ! -f "$legacy_state" || -L "$legacy_state" \
   || "$(/usr/bin/stat -f '%Lp' "$legacy_state")" != "600" ]] \
  || ! jq -e \
    --arg digest "$shadow_digest" '
      .schema_version == 1
      and .policy_sha256 == $digest
      and .account_id == "ofk_region_wb"
      and .campaign_id == 39807762
      and .pending == null
      and .incident_class == null
    ' "$legacy_state" >/dev/null; then
  echo "WB Oduvanchik legacy state is unavailable or unsafe" >&2
  exit 1
fi

sed \
  -e "s|__RUNNER__|$runner_target|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
  -e "s|__PROJECT_DIR__|$project_root|g" \
  "$plist_template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

export WB_AUTOMATION_POLICY_HOST="$shadow_policy_target"
export WB_AUTOMATION_SHADOW_POLICY_HOST="$shadow_policy_target"
export WB_AUTOMATION_LIVE_POLICY_HOST="$protective_policy_target"
export WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry"
export WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token"
export WB_AUTOMATION_WRITE_TOKEN_FILE_HOST="$writer_token"
export WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state"
export WB_AUTOMATION_WRITE_EGRESS_CONTAINER_NAME="$write_egress_container_name"

shadow=(
  "$docker_bin" compose
  --project-name "$compose_project"
  --project-directory "$project_root"
  --env-file "$position_env"
  -f "$shadow_compose"
)
live=(
  "$docker_bin" compose
  --project-name "$compose_project"
  --project-directory "$project_root"
  --env-file "$position_env"
  -f "$live_compose"
)
"${shadow[@]}" config --quiet
"${live[@]}" config --quiet
"${shadow[@]}" build wb-automation-shadow
"${live[@]}" build wb-automation-live write-egress

"$docker_bin" compose \
  --project-directory "$project_root" \
  --env-file "$position_env" \
  -f "$position_compose" \
  up -d --no-deps --wait --wait-timeout 60 ozon-egress
"${live[@]}" up -d --no-deps --wait --wait-timeout 60 write-egress

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true

shadow_output="$("${shadow[@]}" run --rm --no-deps wb-automation-shadow)"
printf '%s\n' "$shadow_output"
if ! jq -e '
  .outcome == "shadow_persisted"
  and .account_id == "ofk_region_wb"
  and .campaign_id == 39807762
' <<<"$shadow_output" >/dev/null; then
  echo "WB Oduvanchik PostgreSQL shadow bootstrap did not complete" >&2
  exit 1
fi

protective_output="$("${live[@]}" run --rm --no-deps wb-automation-live \
  activate-protective-live-pg \
  /etc/mcp-ozon/wb-automation-shadow-policy.json \
  /etc/mcp-ozon/wb-automation-live-policy.json \
  /etc/mcp-ozon/access.json \
  /run/secrets/wb-promotion-read.token \
  true \
  http://ozon-egress:3128)"
printf '%s\n' "$protective_output"
if ! jq -e '
  .outcome == "protective_live_activated"
  or .outcome == "protective_live_already_active"
' <<<"$protective_output" >/dev/null; then
  echo "WB Oduvanchik protective activation did not complete" >&2
  exit 1
fi

export WB_AUTOMATION_SHADOW_POLICY_HOST="$protective_policy_target"
export WB_AUTOMATION_LIVE_POLICY_HOST="$bid_policy_target"
bid_output="$("${live[@]}" run --rm --no-deps wb-automation-live \
  activate-bid-writes-pg \
  /etc/mcp-ozon/wb-automation-shadow-policy.json \
  /etc/mcp-ozon/wb-automation-live-policy.json \
  /etc/mcp-ozon/access.json \
  /run/secrets/wb-promotion-read.token \
  true \
  http://ozon-egress:3128)"
printf '%s\n' "$bid_output"
if ! jq -e '
  .outcome == "bid_writes_activated"
  or .outcome == "bid_writes_already_active"
' <<<"$bid_output" >/dev/null; then
  echo "WB Oduvanchik bid activation did not complete" >&2
  exit 1
fi

WB_AUTOMATION_PROJECT_DIR="$project_root" \
MCP_RUNTIME_DIR="$runtime_dir" \
WB_AUTOMATION_POSITION_ENV="$position_env_target" \
WB_AUTOMATION_SHADOW_POLICY="$shadow_policy_target" \
WB_AUTOMATION_LIVE_POLICY="$bid_policy_target" \
WB_AUTOMATION_READ_TOKEN_FILE="$reader_token" \
WB_AUTOMATION_WRITE_TOKEN_FILE="$writer_token" \
WB_AUTOMATION_LEGACY_STATE="$legacy_state" \
WB_AUTOMATION_EXPECTED_ACCOUNT_ID=ofk_region_wb \
WB_AUTOMATION_EXPECTED_CAMPAIGN_ID=39807762 \
WB_AUTOMATION_RUNTIME_ID=oduvanchik \
WB_AUTOMATION_COMPOSE_PROJECT_NAME="$compose_project" \
WB_AUTOMATION_WRITE_EGRESS_CONTAINER_NAME="$write_egress_container_name" \
WB_AUTOMATION_BID_WRITES_ENABLED=true \
  "$runner_target"

install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed $label with reviewed Oduvanchik safe-auto enabled"
