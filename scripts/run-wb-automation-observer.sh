#!/bin/bash

set -euo pipefail

runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
binary="${WB_AUTOMATION_BIN:-$HOME/.local/libexec/mcp-ozon/wb-automation}"
policy="${WB_AUTOMATION_POLICY:-$runtime_dir/wb-automation-robot.json}"
registry="${WB_AUTOMATION_ACCESS_CONFIG:-$runtime_dir/access.json}"
reader_token="${WB_AUTOMATION_READ_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-read.token}"
writer_token="${WB_AUTOMATION_WRITE_TOKEN_FILE:-$runtime_dir/ip-domnyshev-wb-promotion-write.token}"
state_directory="${WB_AUTOMATION_STATE_DIR:-$runtime_dir/wb-automation-robot}"
writer_proxy_url="${WB_AUTOMATION_WRITE_PROXY:-http://127.0.0.1:3130}"
lock_directory="${TMPDIR:-/tmp}/mcp-ozon-wb-automation-observer.lock"
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
    echo "WB automation observer lock is occupied or unsafe" >&2
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

for path in "$binary" "$policy" "$registry" "$reader_token" "$writer_token"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "WB automation observer input is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ ! -x "$binary" ]]; then
  echo "WB automation observer binary is not executable" >&2
  exit 1
fi
if [[ ! -d "$state_directory" || -L "$state_directory" ]]; then
  echo "WB automation observer state directory is unavailable or unsafe" >&2
  exit 1
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  state_mode="$(/usr/bin/stat -f '%Lp' "$state_directory")"
  token_mode="$(/usr/bin/stat -f '%Lp' "$reader_token")"
  writer_token_mode="$(/usr/bin/stat -f '%Lp' "$writer_token")"
else
  state_mode="$(stat -c '%a' "$state_directory")"
  token_mode="$(stat -c '%a' "$reader_token")"
  writer_token_mode="$(stat -c '%a' "$writer_token")"
fi
if [[ "$state_mode" != "700" || "$token_mode" != "600" || "$writer_token_mode" != "600" ]]; then
  echo "WB automation requires state mode 700 and both token files mode 600" >&2
  exit 1
fi

"$binary" auto-once \
  "$policy" \
  "$registry" \
  "$reader_token" \
  "$writer_token" \
  "$state_directory" \
  true \
  "$writer_proxy_url"
