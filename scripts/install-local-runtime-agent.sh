#!/bin/bash

set -eu

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$project_root/ops/macos/com.ofk.mcp-ozon-runtime.plist.in"
label="com.ofk.mcp-ozon-runtime"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
watchdog_dir="$HOME/.local/libexec/mcp-ozon"
watchdog="$watchdog_dir/ensure-local-runtime.sh"
plist="$agent_dir/$label.plist"
domain="gui/$(id -u)"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/mcp-ozon-launch-agent.XXXXXX")"
trap 'rm -f "$temporary_plist"' EXIT

mkdir -p "$agent_dir" "$log_dir" "$watchdog_dir"
install -m 700 "$project_root/scripts/ensure-local-runtime.sh" "$watchdog"
sed \
  -e "s|__WATCHDOG__|$watchdog|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  "$template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed and started $label"
