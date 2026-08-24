#!/bin/bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
policy_source="$project_root/config/wb-automation-robot.json"
runner_source="$project_root/scripts/run-wb-automation-observer.sh"
plist_template="$project_root/ops/macos/com.ofk.mcp-ozon-wb-automation-observer.plist.in"
egress_compose="$project_root/compose.wb-automation-egress.yaml"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
writer_token="${WB_AUTOMATION_WRITE_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-write.token}"
state_directory="$runtime_dir/wb-automation-robot"
policy_target="$runtime_dir/wb-automation-robot.json"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
binary_target="$libexec_dir/wb-automation"
runner_target="$libexec_dir/run-wb-automation-observer.sh"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
label="com.ofk.mcp-ozon-wb-automation-observer"
plist="$agent_dir/$label.plist"
domain="gui/$(id -u)"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/wb-automation-observer.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_plist"
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "WB automation observer LaunchAgent installer supports only macOS" >&2
  exit 1
fi
for path in "$policy_source" "$runner_source" "$plist_template" "$egress_compose" "$registry" "$reader_token" "$writer_token"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required WB automation observer file is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ "$(/usr/bin/stat -f '%Lp' "$reader_token")" != "600" \
   || "$(/usr/bin/stat -f '%Lp' "$writer_token")" != "600" ]]; then
  echo "WB automation token files must have mode 600" >&2
  exit 1
fi

cd "$project_root"
cargo build --quiet --locked --release --bin wb-automation

mkdir -p "$runtime_dir" "$state_directory" "$libexec_dir" "$agent_dir" "$log_dir"
chmod 700 "$runtime_dir" "$state_directory" "$libexec_dir"
install -m 700 "$project_root/target/release/wb-automation" "$binary_target"
install -m 700 "$runner_source" "$runner_target"
install -m 600 "$policy_source" "$policy_target"

docker compose \
  --project-directory "$project_root" \
  -f "$egress_compose" \
  up --detach --build --wait --wait-timeout 120 write-egress

sed \
  -e "s|__RUNNER__|$runner_target|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
  "$plist_template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed and started $label in safe-auto mode; policy controls observe-only cutoff"
