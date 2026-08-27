#!/bin/bash

set -euo pipefail

confirmation="--confirm-enable-reviewed-bid-writes"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
shadow_policy_source="$project_root/config/wb-automation-robot.json"
protective_policy_source="$project_root/config/wb-automation-robot.live.json"
bid_policy_source="$project_root/config/wb-automation-robot.bid-live.json"
runner_source="$project_root/scripts/run-wb-automation-live.sh"
plist_template="$project_root/ops/macos/com.ofk.mcp-ozon-wb-automation-live.plist.in"
live_compose="$project_root/compose.wb-automation-live.yaml"
position_compose="$project_root/compose.position.yaml"
position_env="$project_root/.position.env"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
position_env_target="$runtime_dir/position.env"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
writer_token="${WB_AUTOMATION_WRITE_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-write.token}"
legacy_state="${WB_AUTOMATION_LEGACY_STATE:-$runtime_dir/wb-automation-robot/execution-state.json}"
shadow_policy_target="$runtime_dir/wb-automation-shadow-policy.json"
protective_policy_target="$runtime_dir/wb-automation-protective-policy.json"
bid_policy_target="$runtime_dir/wb-automation-live-policy.json"
migration_policy_target="$runtime_dir/wb-automation-source-bid-policy.json"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
runner_target="$libexec_dir/run-wb-automation-live.sh"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
label="com.ofk.mcp-ozon-wb-automation-live"
plist="$agent_dir/$label.plist"
domain="gui/$(id -u)"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/wb-automation-bid-live.XXXXXX")"

cleanup() {
  rm -f "$temporary_plist"
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "WB automation bid-live LaunchAgent installer supports only macOS" >&2
  exit 1
fi
for path in \
  "$shadow_policy_source" "$protective_policy_source" "$bid_policy_source" \
  "$runner_source" "$plist_template" "$live_compose" "$position_compose" \
  "$position_env" "$registry" "$reader_token" "$writer_token" "$legacy_state"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required WB automation bid-live file is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ "$(/usr/bin/stat -f '%Lp' "$position_env")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$reader_token")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$writer_token")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$legacy_state")" != "600" ]]; then
  echo "database env, WB tokens and legacy state must have mode 600" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == false
  and (.bid_writes_enabled // false) == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
' "$shadow_policy_source" >/dev/null; then
  echo "repository WB shadow policy is not the reviewed source" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == true
  and .bid_writes_enabled == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .allow_budget_top_up == false
' "$protective_policy_source" >/dev/null; then
  echo "repository WB protective policy is not the reviewed source" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == true
  and .bid_writes_enabled == true
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .nm_ids == [449627598, 449627015, 497424314]
  and .target_impressions_per_day == 5000
  and .autonomous_pacing == "traffic_frontier_v2"
  and .traffic_frontier_bid_kopecks == 1000
  and .traffic_frontier_feedback_timeout_seconds == 1800
  and .min_bid_kopecks == 102
  and .max_bid_kopecks == 3000
  and .bid_step_percent == 5
  and .daily_pause_threshold_minor == 45000
  and .daily_spend_cap_minor == 50000
  and .max_actions_per_day == 50
  and .cooldown_seconds == 300
  and .allow_budget_top_up == false
' "$bid_policy_source" >/dev/null; then
  echo "repository WB bid-live policy exceeds the approved limits" >&2
  exit 1
fi
shadow_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$shadow_policy_source")"
protective_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$protective_policy_source")"
protective_bid_scope="$(jq -cS 'del(.bid_writes_enabled)' "$protective_policy_source")"
bid_scope="$(jq -cS 'del(.bid_writes_enabled)' "$bid_policy_source")"
if [[ "$shadow_scope" != "$protective_scope" \
   || "$protective_bid_scope" != "$bid_scope" ]]; then
  echo "WB bid-live policy expands the reviewed account, campaign or limits" >&2
  exit 1
fi
if ! jq -e '
  .schema_version == 1
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .pending == null
  and .incident_class == null
' "$legacy_state" >/dev/null; then
  echo "legacy WB automation state cannot enter bid-live mode" >&2
  exit 1
fi
if [[ -e "$plist" && ( ! -f "$plist" || -L "$plist" ) ]]; then
  echo "WB live LaunchAgent plist is unsafe" >&2
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

mkdir -p "$runtime_dir" "$libexec_dir" "$agent_dir" "$log_dir"
chmod 700 "$runtime_dir" "$libexec_dir"
if [[ -e "$migration_policy_target" \
   && ( ! -f "$migration_policy_target" || -L "$migration_policy_target" ) ]]; then
  echo "WB automation policy migration source is unsafe" >&2
  exit 1
fi
migration_required=false
if [[ -e "$bid_policy_target" ]]; then
  if [[ ! -f "$bid_policy_target" || -L "$bid_policy_target" ]]; then
    echo "installed WB automation bid-live policy is unsafe" >&2
    exit 1
  fi
  installed_policy="$(jq -cS '.' "$bid_policy_target")"
  target_policy="$(jq -cS '.' "$bid_policy_source")"
  if [[ "$installed_policy" != "$target_policy" ]]; then
    migrated_policy="$(jq -cS --slurpfile target "$bid_policy_source" '
      .authorization_reference = $target[0].authorization_reference
      | .authorized_at = $target[0].authorized_at
      | .authorization_expires_at = $target[0].authorization_expires_at
      | .observe_until = $target[0].observe_until
      | .autonomous_pacing = $target[0].autonomous_pacing
      | .traffic_frontier_bid_kopecks = $target[0].traffic_frontier_bid_kopecks
      | .traffic_frontier_feedback_timeout_seconds = $target[0].traffic_frontier_feedback_timeout_seconds
      | .max_bid_kopecks = $target[0].max_bid_kopecks
      | .bid_step_percent = $target[0].bid_step_percent
      | .daily_pause_threshold_minor = $target[0].daily_pause_threshold_minor
      | .daily_spend_cap_minor = $target[0].daily_spend_cap_minor
      | .max_actions_per_day = $target[0].max_actions_per_day
      | .cooldown_seconds = $target[0].cooldown_seconds
    ' "$bid_policy_target")"
    if ! jq -e '
      .write_enabled == true
      and .bid_writes_enabled == true
      and .authorization_reference == "chat/2026-08-27/traffic-frontier-v2"
      and .autonomous_pacing == "traffic_frontier_v2"
      and .traffic_frontier_bid_kopecks == 540
      and .traffic_frontier_feedback_timeout_seconds == 1800
      and .max_bid_kopecks == 3000
      and .bid_step_percent == 5
      and .daily_pause_threshold_minor == 25000
      and .daily_spend_cap_minor == 30000
      and .max_actions_per_day == 50
      and .cooldown_seconds == 300
      and .allow_budget_top_up == false
    ' "$bid_policy_target" >/dev/null \
      || [[ "$migrated_policy" != "$target_policy" ]]; then
      echo "installed WB automation policy is not an approved migration source" >&2
      exit 1
    fi
    install -m 644 "$bid_policy_target" "$migration_policy_target"
    migration_required=true
  fi
fi
install -m 700 "$runner_source" "$runner_target"
install -m 644 "$shadow_policy_source" "$shadow_policy_target"
install -m 644 "$protective_policy_source" "$protective_policy_target"
install -m 644 "$bid_policy_source" "$bid_policy_target"
install -m 600 "$position_env" "$position_env_target"

# Mount the target policy in the source slot for the credentialless preflight.
# `observe-once` cannot write, and proves every current bid is already at or
# below the emergency 3000-kopeck hard cap before the durable digest changes.
export WB_AUTOMATION_SHADOW_POLICY_HOST="$bid_policy_target"
export WB_AUTOMATION_LIVE_POLICY_HOST="$bid_policy_target"
export WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry"
export WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token"
export WB_AUTOMATION_WRITE_TOKEN_FILE_HOST="$writer_token"
export WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state"

compose=(
  "$docker_bin" compose
  --project-directory "$project_root"
  --env-file "$position_env"
  -f "$live_compose"
)
"${compose[@]}" config --quiet
"${compose[@]}" build wb-automation-live write-egress

sed \
  -e "s|__RUNNER__|$runner_target|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
  -e "s|__PROJECT_DIR__|$project_root|g" \
  -e "s|__BID_WRITES_ENABLED__|true|g" \
  "$plist_template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

# Build and prove both credentialless egresses before stopping the currently
# healthy protective timer.
"$docker_bin" compose \
  --project-directory "$project_root" \
  --env-file "$position_env" \
  -f "$position_compose" \
  up -d --no-deps --wait --wait-timeout 60 ozon-egress
"${compose[@]}" up -d --no-deps --wait --wait-timeout 60 write-egress

# This preflight has no writer-token argument and cannot mutate WB or the
# PostgreSQL execution state. The unprivileged container creates a private
# snapshot directory on its ephemeral /tmp tmpfs before proving the exact
# campaign, SKU and current minimum-bid payload through the new observer.
# shellcheck disable=SC2016 # The inner /bin/sh expands its private path.
preflight_output="$("${compose[@]}" run --rm --no-deps \
  --entrypoint /bin/sh \
  wb-automation-live \
  -eu -c '
    umask 077
    state_directory=/tmp/wb-automation-bid-live-preflight
    mkdir "$state_directory"
    exec /usr/local/bin/wb-automation observe-once \
      /etc/mcp-ozon/wb-automation-shadow-policy.json \
      /etc/mcp-ozon/access.json \
      /run/secrets/wb-promotion-read.token \
      "$state_directory" \
      true \
      http://ozon-egress:3128
  ')"
printf '%s\n' "$preflight_output"
if ! jq -e '
  .outcome == "observed"
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
' <<<"$preflight_output" >/dev/null; then
  echo "WB automation bid-live read-only preflight did not complete" >&2
  exit 1
fi

# For the one-off durable transition, the source-policy mount carries either
# the current bid-live policy being migrated or the protective policy for a
# first activation. Scheduled cycles later restore the actual shadow file
# through the runner and execute only the bid-live target policy.
if [[ "$migration_required" == "true" ]]; then
  export WB_AUTOMATION_SHADOW_POLICY_HOST="$migration_policy_target"
else
  export WB_AUTOMATION_SHADOW_POLICY_HOST="$protective_policy_target"
fi

# From this point any failure leaves the single live timer stopped. A running
# one-shot worker must finish before the digest can change under the same
# campaign advisory lock.
launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
live_pid="$(launchctl print "$domain/$label" 2>/dev/null | awk '/pid = / { print $3; exit }' || true)"
if [[ -n "$live_pid" ]]; then
  echo "WB automation live agent is still running" >&2
  exit 1
fi
worker_filter="label=com.docker.compose.project=mcp-ozon-wb-automation-live"
service_filter="label=com.docker.compose.service=wb-automation-live"
for _attempt in $(seq 1 45); do
  worker_ids="$("$docker_bin" ps --filter "$worker_filter" --filter "$service_filter" -q)"
  if [[ -z "$worker_ids" ]]; then
    break
  fi
  sleep 1
done
if [[ -n "${worker_ids:-}" ]]; then
  echo "WB automation one-shot worker did not finish before bid-live cutover" >&2
  exit 1
fi

if [[ "$migration_required" == "true" ]]; then
  activation_output="$("${compose[@]}" run --rm --no-deps wb-automation-live \
    raise-traffic-frontier-limits-pg \
    /etc/mcp-ozon/wb-automation-shadow-policy.json \
    /etc/mcp-ozon/wb-automation-live-policy.json \
    /etc/mcp-ozon/access.json \
    /run/secrets/wb-promotion-read.token \
    true \
    http://ozon-egress:3128)"
else
  activation_output="$("${compose[@]}" run --rm --no-deps wb-automation-live \
    activate-bid-writes-pg \
    /etc/mcp-ozon/wb-automation-shadow-policy.json \
    /etc/mcp-ozon/wb-automation-live-policy.json \
    /etc/mcp-ozon/access.json \
    /run/secrets/wb-promotion-read.token \
    true \
    http://ozon-egress:3128)"
fi
printf '%s\n' "$activation_output"
if ! jq -e '
  .outcome == "bid_writes_activated"
  or .outcome == "bid_writes_already_active"
  or .outcome == "traffic_frontier_v2_activated"
  or .outcome == "traffic_frontier_v2_already_active"
  or .outcome == "traffic_frontier_limits_raised"
  or .outcome == "traffic_frontier_limits_already_active"
' <<<"$activation_output" >/dev/null; then
  echo "WB automation bid-live activation did not complete" >&2
  exit 1
fi
if [[ "$migration_required" == "true" ]]; then
  rm -f "$migration_policy_target"
fi

# The first bid-live cycle may send at most one bounded SKU change. The runner
# immediately performs the durable read-back cycle when a write is sent.
WB_AUTOMATION_PROJECT_DIR="$project_root" \
MCP_RUNTIME_DIR="$runtime_dir" \
WB_AUTOMATION_POSITION_ENV="$position_env_target" \
WB_AUTOMATION_BID_WRITES_ENABLED=true \
  "$runner_target"

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed $label with reviewed bid writes enabled"
