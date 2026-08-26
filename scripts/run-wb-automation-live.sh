#!/bin/bash

set -euo pipefail

project_root="${WB_AUTOMATION_PROJECT_DIR:?WB_AUTOMATION_PROJECT_DIR is required}"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
position_env="${WB_AUTOMATION_POSITION_ENV:-$runtime_dir/position.env}"
shadow_policy="${WB_AUTOMATION_SHADOW_POLICY:-$runtime_dir/wb-automation-shadow-policy.json}"
live_policy="${WB_AUTOMATION_LIVE_POLICY:-$runtime_dir/wb-automation-live-policy.json}"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
writer_token="${WB_AUTOMATION_WRITE_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-write.token}"
legacy_state="${WB_AUTOMATION_LEGACY_STATE:-$runtime_dir/wb-automation-robot/execution-state.json}"
compose_file="$project_root/compose.wb-automation-live.yaml"
lock_directory="${TMPDIR:-/tmp}/mcp-ozon-wb-automation-live.lock"
lock_pid_file="$lock_directory/pid"

umask 077
if ! mkdir "$lock_directory" 2>/dev/null; then
  if [[ -f "$lock_pid_file" && ! -L "$lock_pid_file" ]]; then
    lock_pid="$(head -n 1 "$lock_pid_file" 2>/dev/null || true)"
    if [[ "$lock_pid" =~ ^[1-9][0-9]*$ ]] && kill -0 "$lock_pid" 2>/dev/null; then
      exit 0
    fi
    rm -f "$lock_pid_file"
  fi
  if ! rmdir "$lock_directory" 2>/dev/null || ! mkdir "$lock_directory" 2>/dev/null; then
    echo "WB automation live lock is occupied or unsafe" >&2
    exit 1
  fi
fi
printf '%s\n' "$$" >"$lock_pid_file"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$lock_pid_file"
  rmdir "$lock_directory" 2>/dev/null || true
}
trap cleanup EXIT

for path in \
  "$position_env" "$shadow_policy" "$live_policy" "$registry" \
  "$reader_token" "$writer_token" "$legacy_state" "$compose_file"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "WB automation live input is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ ! -d "$project_root" || -L "$project_root" ]]; then
  echo "WB automation project directory is unavailable or unsafe" >&2
  exit 1
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  env_mode="$(/usr/bin/stat -f '%Lp' "$position_env")"
  reader_mode="$(/usr/bin/stat -f '%Lp' "$reader_token")"
  writer_mode="$(/usr/bin/stat -f '%Lp' "$writer_token")"
  legacy_mode="$(/usr/bin/stat -f '%Lp' "$legacy_state")"
else
  env_mode="$(stat -c '%a' "$position_env")"
  reader_mode="$(stat -c '%a' "$reader_token")"
  writer_mode="$(stat -c '%a' "$writer_token")"
  legacy_mode="$(stat -c '%a' "$legacy_state")"
fi
if [[ "$env_mode" != "600" || "$reader_mode" != "600" \
   || "$writer_mode" != "600" || "$legacy_mode" != "600" ]]; then
  echo "WB automation live requires database env, tokens and legacy state mode 600" >&2
  exit 1
fi
if ! grep -Eq '^WB_AUTOMATION_DB_PASSWORD=.{24,}$' "$position_env"; then
  echo "WB automation database password is unavailable" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == false
  and (.bid_writes_enabled // false) == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
' "$shadow_policy" >/dev/null; then
  echo "WB automation shadow policy does not match the guarded cutover source" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == true
  and .bid_writes_enabled == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
  and .allow_budget_top_up == false
' "$live_policy" >/dev/null; then
  echo "WB automation live policy is not the approved protective-only policy" >&2
  exit 1
fi

export WB_AUTOMATION_SHADOW_POLICY_HOST="$shadow_policy"
export WB_AUTOMATION_LIVE_POLICY_HOST="$live_policy"
export WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry"
export WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token"
export WB_AUTOMATION_WRITE_TOKEN_FILE_HOST="$writer_token"
export WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state"

compose=(
  docker compose
  --project-directory "$project_root"
  --env-file "$position_env"
  -f "$compose_file"
)

run_cycle() {
  "${compose[@]}" run --rm --no-deps wb-automation-live
}

first_output="$(run_cycle)"
printf '%s\n' "$first_output"
if jq -e '.outcome == "write_sent_reconciliation_required"' \
  <<<"$first_output" >/dev/null 2>&1; then
  # Make the first read-back attempt immediately. If WB has not converged yet,
  # the durable pending row remains fail-closed for the next scheduled cycle.
  run_cycle
fi
