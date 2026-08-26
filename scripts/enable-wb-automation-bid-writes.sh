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
  and .min_bid_kopecks == 102
  and .max_bid_kopecks == 600
  and .bid_step_percent == 15
  and .daily_pause_threshold_minor == 25000
  and .daily_spend_cap_minor == 30000
  and .max_actions_per_day == 2
  and .cooldown_seconds == 21600
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
install -m 700 "$runner_source" "$runner_target"
install -m 644 "$shadow_policy_source" "$shadow_policy_target"
install -m 644 "$protective_policy_source" "$protective_policy_target"
install -m 644 "$bid_policy_source" "$bid_policy_target"
install -m 600 "$position_env" "$position_env_target"

# Start with the non-writing shadow policy so the new image and the live WB
# minimum-bid response contract can be proved before the protective timer or
# the durable policy digest is changed.
export WB_AUTOMATION_SHADOW_POLICY_HOST="$shadow_policy_target"
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
# PostgreSQL execution state. It proves the exact campaign, SKU and current
# minimum-bid payload through the newly built observer before cutover.
preflight_output="$("${compose[@]}" run --rm --no-deps wb-automation-live \
  observe-once \
  /etc/mcp-ozon/wb-automation-shadow-policy.json \
  /etc/mcp-ozon/access.json \
  /run/secrets/wb-promotion-read.token \
  /tmp/wb-automation-bid-live-preflight \
  true \
  http://ozon-egress:3128)"
printf '%s\n' "$preflight_output"
if ! jq -e '
  .outcome == "observed"
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
' <<<"$preflight_output" >/dev/null; then
  echo "WB automation bid-live read-only preflight did not complete" >&2
  exit 1
fi

# For the one-off durable transition, the shadow-policy mount now carries the
# exact current protective policy. Scheduled cycles later restore the actual
# shadow file through the runner and execute only the bid-live target policy.
export WB_AUTOMATION_SHADOW_POLICY_HOST="$protective_policy_target"

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

activation_output="$("${compose[@]}" run --rm --no-deps wb-automation-live \
  activate-bid-writes-pg \
  /etc/mcp-ozon/wb-automation-shadow-policy.json \
  /etc/mcp-ozon/wb-automation-live-policy.json \
  /etc/mcp-ozon/access.json \
  /run/secrets/wb-promotion-read.token \
  true \
  http://ozon-egress:3128)"
printf '%s\n' "$activation_output"
if ! jq -e '
  .outcome == "bid_writes_activated"
  or .outcome == "bid_writes_already_active"
' <<<"$activation_output" >/dev/null; then
  echo "WB automation bid-live activation did not complete" >&2
  exit 1
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
