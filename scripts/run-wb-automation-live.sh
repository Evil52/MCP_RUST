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
bid_writes_enabled="${WB_AUTOMATION_BID_WRITES_ENABLED:-false}"
compose_file="$project_root/compose.wb-automation-live.yaml"
expected_account_id="${WB_AUTOMATION_EXPECTED_ACCOUNT_ID:-ip_domnyshev_wb}"
expected_campaign_id="${WB_AUTOMATION_EXPECTED_CAMPAIGN_ID:-39682633}"
runtime_id="${WB_AUTOMATION_RUNTIME_ID:-robot}"
compose_project="${WB_AUTOMATION_COMPOSE_PROJECT_NAME:-mcp-ozon-wb-automation-live}"
write_egress_container_name="${WB_AUTOMATION_WRITE_EGRESS_CONTAINER_NAME:-mcp-ozon-wb-automation-live-write-egress}"
lock_directory="${TMPDIR:-/tmp}/mcp-ozon-wb-automation-live-$runtime_id.lock"
lock_pid_file="$lock_directory/pid"

umask 077
# This lock only avoids the cost of starting a redundant container. It is not
# the mutual-exclusion guarantee: reclaiming a stale lock is not atomic, so two
# racing runs can both believe they hold it. Correctness comes from the
# session-scoped `pg_try_advisory_lock` in automation_postgres.rs, where the
# loser gets `Ok(None)` and does nothing, and which PostgreSQL releases by
# itself when a crashed run's connection drops.
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
if [[ "$bid_writes_enabled" != "false" && "$bid_writes_enabled" != "true" ]]; then
  echo "WB automation bid-writes mode must be true or false" >&2
  exit 1
fi
if [[ ! "$expected_account_id" =~ ^[A-Za-z0-9_-]{1,128}$ \
   || ! "$expected_campaign_id" =~ ^[1-9][0-9]*$ \
   || ! "$runtime_id" =~ ^[A-Za-z0-9_-]{1,64}$ \
   || ! "$compose_project" =~ ^[a-z0-9][a-z0-9_-]{0,62}$ \
   || ! "$write_egress_container_name" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "WB automation runtime identity is invalid" >&2
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
if ! jq -e --arg account_id "$expected_account_id" \
  --argjson campaign_id "$expected_campaign_id" '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == false
  and (.bid_writes_enabled // false) == false
  and .account_id == $account_id
  and .campaign_id == $campaign_id
' "$shadow_policy" >/dev/null; then
  echo "WB automation shadow policy does not match the guarded cutover source" >&2
  exit 1
fi
if ! jq -e --arg account_id "$expected_account_id" \
  --argjson campaign_id "$expected_campaign_id" \
  --argjson bid_writes_enabled "$bid_writes_enabled" '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == true
  and .bid_writes_enabled == $bid_writes_enabled
  and .account_id == $account_id
  and .campaign_id == $campaign_id
  and .allow_budget_top_up == false
' "$live_policy" >/dev/null; then
  echo "WB automation live policy does not match the approved bid-writes mode" >&2
  exit 1
fi

export WB_AUTOMATION_SHADOW_POLICY_HOST="$shadow_policy"
export WB_AUTOMATION_LIVE_POLICY_HOST="$live_policy"
export WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry"
export WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token"
export WB_AUTOMATION_WRITE_TOKEN_FILE_HOST="$writer_token"
export WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state"
export WB_AUTOMATION_WRITE_EGRESS_CONTAINER_NAME="$write_egress_container_name"

compose=(
  docker compose
  --project-name "$compose_project"
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
