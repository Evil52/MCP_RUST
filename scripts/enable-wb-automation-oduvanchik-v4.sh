#!/bin/bash

set -euo pipefail
umask 077

confirmation="--confirm-enable-oduvanchik-traffic-frontier-v4-drr-15"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_policy="$project_root/config/wb-automation-oduvanchik.bid-live.json"
target_policy="$project_root/config/wb-automation-oduvanchik.v4.json"
live_compose="$project_root/compose.wb-automation-live.yaml"
position_compose="$project_root/compose.position.yaml"
runner_source="$project_root/scripts/run-wb-automation-live.sh"
plist_template="$project_root/ops/macos/com.ofk.mcp-ozon-wb-automation-oduvanchik-live.plist.in"
position_env="$project_root/.position.env"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
campaign_runtime="$runtime_dir/wb-automation-oduvanchik"
registry="$runtime_dir/access.json"
reader_token="$runtime_dir/ofk-region-wb-promotion-read.token"
writer_token="$runtime_dir/ofk-region-wb-promotion-write.token"
legacy_state="$campaign_runtime/execution-state.json"
runtime_live_policy="$campaign_runtime/live-policy.json"
pending_policy="$campaign_runtime/live-policy.v4.pending.json"
position_env_target="$runtime_dir/position.env"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
runner_target="$libexec_dir/run-wb-automation-live.sh"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
label="com.ofk.mcp-ozon-wb-automation-oduvanchik-live"
plist="$agent_dir/$label.plist"
domain="gui/$(id -u)"
compose_project="mcp-ozon-wb-automation-oduvanchik-live"
write_egress_container="mcp-ozon-wb-automation-oduvanchik-write-egress"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/wb-automation-oduvanchik-v4.XXXXXX")"
agent_stopped=false
policy_installed=false
success=false

# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_plist" "$pending_policy"
  if [[ "$success" != true && "$agent_stopped" == true && "$policy_installed" != true \
     && -f "$plist" && ! -L "$plist" ]]; then
    launchctl bootstrap "$domain" "$plist" >/dev/null 2>&1 || true
    launchctl kickstart -k "$domain/$label" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "WB Oduvanchik v4 rollout supports only macOS" >&2
  exit 1
fi
for path in \
  "$source_policy" "$target_policy" "$live_compose" "$position_compose" \
  "$runner_source" "$plist_template" "$position_env" "$registry" \
  "$reader_token" "$writer_token" "$legacy_state" "$runtime_live_policy"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required WB Oduvanchik v4 file is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ "$({ /usr/bin/stat -f '%Lp' "$position_env"; })" != "600" \
   || "$({ /usr/bin/stat -f '%Lp' "$reader_token"; })" != "600" \
   || "$({ /usr/bin/stat -f '%Lp' "$writer_token"; })" != "600" \
   || "$({ /usr/bin/stat -f '%Lp' "$legacy_state"; })" != "600" ]]; then
  echo "database env, tokens and legacy state must have mode 600" >&2
  exit 1
fi
if ! grep -Eq '^WB_AUTOMATION_DB_PASSWORD=.{24,}$' "$position_env"; then
  echo "WB_AUTOMATION_DB_PASSWORD is unavailable" >&2
  exit 1
fi

target_guard='.
  | .policy_version == "wb_ads_robot.v1"
  and .write_enabled == true
  and .bid_writes_enabled == true
  and .account_id == "ofk_region_wb"
  and .campaign_id == 39807762
  and .campaign_name == "Одуванчик"
  and .nm_ids == [44081446,41774347,99236811,38943938,44081434]
  and .target_drr_basis_points == 1500
  and .hard_drr_basis_points == 1500
  and .autonomous_pacing == "traffic_frontier_v4"
  and .traffic_frontier_bid_kopecks == 700
  and .traffic_frontier_feedback_timeout_seconds == 1800
  and .traffic_frontier_min_feedback_impressions == 200
  and .traffic_frontier_min_feedback_clicks == 10
  and .min_bid_kopecks == 500
  and .max_bid_kopecks == 1050
  and .bid_step_percent == 10
  and .daily_spend_cap_minor == 50000
  and .daily_pause_threshold_minor == 45000
  and .max_actions_per_day == 48
  and .cooldown_seconds == 1800
  and .allow_budget_top_up == false
  and .authorization_reference == "chat/2026-08-28/oduvanchik-traffic-frontier-v4-drr-15"'
if ! jq -e "$target_guard" "$target_policy" >/dev/null; then
  echo "WB Oduvanchik v4 target policy exceeds the reviewed scope" >&2
  exit 1
fi

expected_source="$(jq -cS '
  .authorization_reference = "chat/2026-08-28/oduvanchik-safe-auto"
  | .authorized_at = "2026-08-28T09:41:00Z"
  | .observe_until = "2026-08-28T09:41:01Z"
  | .target_drr_basis_points = 1500
  | .hard_drr_basis_points = 2500
  | .autonomous_pacing = "traffic_frontier_v3"
  | .traffic_frontier_bid_kopecks = 525
  | .traffic_frontier_feedback_timeout_seconds = 1800
  | .bid_step_percent = 5
' "$target_policy")"
if [[ "$expected_source" != "$(jq -cS '.' "$source_policy")" ]]; then
  echo "WB Oduvanchik v4 target changes fields outside the reviewed transition" >&2
  exit 1
fi
if ! cmp -s "$runtime_live_policy" "$source_policy" \
   && ! cmp -s "$runtime_live_policy" "$target_policy"; then
  echo "WB Oduvanchik runtime policy is neither the reviewed source nor target" >&2
  exit 1
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]] || ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi

mkdir -p "$campaign_runtime" "$libexec_dir" "$agent_dir" "$log_dir"
chmod 700 "$runtime_dir" "$campaign_runtime" "$libexec_dir"
install -m 644 "$target_policy" "$pending_policy"
install -m 600 "$position_env" "$position_env_target"
sed \
  -e "s|__RUNNER__|$runner_target|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
  -e "s|__PROJECT_DIR__|$project_root|g" \
  "$plist_template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

export WB_AUTOMATION_SHADOW_POLICY_HOST="$source_policy"
export WB_AUTOMATION_LIVE_POLICY_HOST="$pending_policy"
export WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry"
export WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token"
export WB_AUTOMATION_WRITE_TOKEN_FILE_HOST="$writer_token"
export WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state"
export WB_AUTOMATION_WRITE_EGRESS_CONTAINER_NAME="$write_egress_container"
live=(
  "$docker_bin" compose
  --project-name "$compose_project"
  --project-directory "$project_root"
  --env-file "$position_env"
  -f "$live_compose"
)
"${live[@]}" config --quiet
"${live[@]}" build wb-automation-live write-egress
"$docker_bin" compose \
  --project-directory "$project_root" \
  --env-file "$position_env" \
  -f "$position_compose" \
  up -d --no-deps --wait --wait-timeout 60 ozon-egress
"${live[@]}" up -d --no-deps --wait --wait-timeout 60 write-egress

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
agent_stopped=true
activation_output="$("${live[@]}" run --rm --no-deps wb-automation-live \
  activate-traffic-frontier-v4-pg \
  /etc/mcp-ozon/wb-automation-shadow-policy.json \
  /etc/mcp-ozon/wb-automation-live-policy.json \
  /etc/mcp-ozon/access.json \
  /run/secrets/wb-promotion-read.token \
  true \
  http://ozon-egress:3128)"
printf '%s\n' "$activation_output"
if ! jq -e '
  (.outcome == "traffic_frontier_v4_activated"
   or .outcome == "traffic_frontier_v4_already_active")
  and .account_id == "ofk_region_wb"
  and .campaign_id == 39807762
  and .target_drr_basis_points == 1500
  and .hard_drr_basis_points == 1500
  and .zero_cost_probe_enabled == true
  and .bid_writes_enabled == true
' <<<"$activation_output" >/dev/null; then
  echo "WB Oduvanchik v4 durable activation did not complete" >&2
  exit 1
fi

install -m 644 "$target_policy" "$runtime_live_policy"
policy_installed=true
install -m 700 "$runner_source" "$runner_target"
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"
success=true
echo "Enabled $label with Traffic Frontier v4 and one 15% DRR limit"
