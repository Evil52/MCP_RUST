#!/bin/bash

set -euo pipefail

confirmation="--confirm-stop-shadow-and-start-protective-live"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
shadow_policy_source="$project_root/config/wb-automation-robot.json"
live_policy_source="$project_root/config/wb-automation-robot.live.json"
runner_source="$project_root/scripts/run-wb-automation-live.sh"
plist_template="$project_root/ops/macos/com.ofk.mcp-ozon-wb-automation-live.plist.in"
live_compose="$project_root/compose.wb-automation-live.yaml"
position_compose="$project_root/compose.position.yaml"
legacy_egress_compose="$project_root/compose.wb-automation-egress.yaml"
position_env="$project_root/.position.env"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
position_env_target="$runtime_dir/position.env"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
writer_token="${WB_AUTOMATION_WRITE_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-write.token}"
legacy_state="${WB_AUTOMATION_LEGACY_STATE:-$runtime_dir/wb-automation-robot/execution-state.json}"
shadow_policy_target="$runtime_dir/wb-automation-shadow-policy.json"
live_policy_target="$runtime_dir/wb-automation-live-policy.json"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
runner_target="$libexec_dir/run-wb-automation-live.sh"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
label="com.ofk.mcp-ozon-wb-automation-live"
shadow_label="com.ofk.mcp-ozon-wb-automation-shadow"
plist="$agent_dir/$label.plist"
shadow_plist="$agent_dir/$shadow_label.plist"
shadow_plist_disabled="$agent_dir/$shadow_label.plist.protective-live-disabled"
domain="gui/$(id -u)"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/wb-automation-live.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_plist"
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "WB automation live LaunchAgent installer supports only macOS" >&2
  exit 1
fi
for path in \
  "$shadow_policy_source" "$live_policy_source" "$runner_source" \
  "$plist_template" "$live_compose" "$position_compose" \
  "$legacy_egress_compose" "$position_env" "$registry" \
  "$reader_token" "$writer_token" "$legacy_state"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required WB automation live file is unavailable or unsafe: $path" >&2
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
  echo "repository WB shadow policy is not the approved cutover source" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == true
  and .bid_writes_enabled == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .allow_budget_top_up == false
' "$live_policy_source" >/dev/null; then
  echo "repository WB live policy is not protective-only" >&2
  exit 1
fi
shadow_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$shadow_policy_source")"
live_scope="$(jq -cS 'del(.write_enabled, .bid_writes_enabled)' "$live_policy_source")"
if [[ "$shadow_scope" != "$live_scope" ]]; then
  echo "WB live policy expands the reviewed shadow scope" >&2
  exit 1
fi
if ! jq -e '
  .schema_version == 1
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .pending == null
  and .incident_class == null
' "$legacy_state" >/dev/null; then
  echo "legacy WB automation state cannot enter protective live mode" >&2
  exit 1
fi
if [[ -e "$shadow_plist" && ( ! -f "$shadow_plist" || -L "$shadow_plist" ) ]]; then
  echo "WB shadow LaunchAgent plist is unsafe" >&2
  exit 1
fi
if [[ -e "$shadow_plist" && -e "$shadow_plist_disabled" ]]; then
  echo "refusing to overwrite the disabled WB shadow LaunchAgent backup" >&2
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
install -m 644 "$live_policy_source" "$live_policy_target"
install -m 600 "$position_env" "$position_env_target"

export WB_AUTOMATION_SHADOW_POLICY_HOST="$shadow_policy_target"
export WB_AUTOMATION_LIVE_POLICY_HOST="$live_policy_target"
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
  "$plist_template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

# Prove both credentialless egresses before stopping the shadow timer. The
# live worker itself has no direct outbound network route.
"$docker_bin" compose \
  --project-directory "$project_root" \
  --env-file "$position_env" \
  -f "$position_compose" \
  up -d --no-deps --wait --wait-timeout 60 ozon-egress
"${compose[@]}" up -d --no-deps --wait --wait-timeout 60 write-egress

# Only one runtime may own the campaign. From this point on, any failure leaves
# both autonomous writers stopped until the installer is rerun.
launchctl bootout "$domain/$shadow_label" >/dev/null 2>&1 || true
shadow_pid="$(launchctl print "$domain/$shadow_label" 2>/dev/null | awk '/pid = / { print $3; exit }' || true)"
if [[ -n "$shadow_pid" ]]; then
  echo "WB automation shadow agent is still running" >&2
  exit 1
fi
if [[ -f "$shadow_plist" ]]; then
  mv "$shadow_plist" "$shadow_plist_disabled"
  chmod 600 "$shadow_plist_disabled"
fi
"$docker_bin" compose \
  --project-directory "$project_root" \
  -f "$legacy_egress_compose" \
  stop write-egress >/dev/null

# The cutover command changes only the policy digest, preserves state and
# records an append-only PostgreSQL audit event. It cannot call WB.
"${compose[@]}" run --rm --no-deps wb-automation-live \
  activate-protective-live-pg \
  /etc/mcp-ozon/wb-automation-shadow-policy.json \
  /etc/mcp-ozon/wb-automation-live-policy.json \
  /etc/mcp-ozon/access.json \
  /run/secrets/wb-promotion-read.token \
  true \
  http://ozon-egress:3128

# Prove one live-policy cycle before scheduling future cycles. With bid writes
# disabled it can only observe or issue the approved daily-cap pause.
WB_AUTOMATION_PROJECT_DIR="$project_root" \
MCP_RUNTIME_DIR="$runtime_dir" \
WB_AUTOMATION_POSITION_ENV="$position_env_target" \
  "$runner_target"

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed $label; shadow stopped and protective live automation started"
