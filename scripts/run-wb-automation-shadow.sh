#!/bin/bash

set -euo pipefail

project_root="${WB_AUTOMATION_PROJECT_DIR:?WB_AUTOMATION_PROJECT_DIR is required}"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
position_env="${WB_AUTOMATION_POSITION_ENV:-$runtime_dir/position.env}"
policy="${WB_AUTOMATION_SHADOW_POLICY:-$runtime_dir/wb-automation-shadow-policy.json}"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
legacy_state="${WB_AUTOMATION_LEGACY_STATE:-$runtime_dir/wb-automation-robot/execution-state.json}"
compose_file="$project_root/compose.wb-automation-shadow.yaml"
lock_directory="${TMPDIR:-/tmp}/mcp-ozon-wb-automation-shadow.lock"
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
    echo "WB automation shadow lock is occupied or unsafe" >&2
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

for path in "$position_env" "$policy" "$registry" "$reader_token" "$legacy_state" "$compose_file"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "WB automation shadow input is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ ! -d "$project_root" || -L "$project_root" ]]; then
  echo "WB automation project directory is unavailable or unsafe" >&2
  exit 1
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  env_mode="$(/usr/bin/stat -f '%Lp' "$position_env")"
  token_mode="$(/usr/bin/stat -f '%Lp' "$reader_token")"
  legacy_mode="$(/usr/bin/stat -f '%Lp' "$legacy_state")"
else
  env_mode="$(stat -c '%a' "$position_env")"
  token_mode="$(stat -c '%a' "$reader_token")"
  legacy_mode="$(stat -c '%a' "$legacy_state")"
fi
if [[ "$env_mode" != "600" || "$token_mode" != "600" || "$legacy_mode" != "600" ]]; then
  echo "WB automation shadow requires database env, read token and legacy state mode 600" >&2
  exit 1
fi
if ! grep -Eq '^WB_AUTOMATION_DB_PASSWORD=.{24,}$' "$position_env"; then
  echo "WB_AUTOMATION_DB_PASSWORD is unavailable from the protected database env" >&2
  exit 1
fi
if ! jq -e '
  .policy_version == "wb_ads_robot.v1"
  and .write_enabled == false
  and .account_id == "ip_domnyshev_wb"
  and .campaign_id == 39682633
' "$policy" >/dev/null; then
  echo "WB automation shadow policy does not match the approved fail-closed scope" >&2
  exit 1
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi

export WB_AUTOMATION_POLICY_HOST="$policy"
export WB_AUTOMATION_ACCESS_CONFIG_HOST="$registry"
export WB_AUTOMATION_READ_TOKEN_FILE_HOST="$reader_token"
export WB_AUTOMATION_LEGACY_STATE_HOST="$legacy_state"

exec "$docker_bin" compose \
  --project-directory "$project_root" \
  --env-file "$position_env" \
  -f "$compose_file" \
  run --rm --no-deps wb-automation-shadow
