#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/compose.canary.yaml"
state_dir="$project_root/target/canary"
state_file="$state_dir/runtime-registry"

if [[ "$(uname -s)" == "Darwin" ]]; then
  temporary_root="/private/tmp"
else
  temporary_root="/tmp"
fi

if [[ -e "$state_file" ]]; then
  echo "Canary runtime state already exists. Run ./scripts/canary-down.sh first." >&2
  exit 1
fi

"$project_root/scripts/canary-init.sh"

runtime_dir="$(mktemp -d "$temporary_root/mcp-ozon-canary.XXXXXX")"
runtime_registry="$runtime_dir/access.json"
cleanup_on_error=true

cleanup() {
  if [[ "$cleanup_on_error" == true ]]; then
    rm -rf "$runtime_dir"
    rm -f "$state_file.tmp" "$state_file"
  fi
}
trap cleanup EXIT

chmod 700 "$runtime_dir"
install -m 600 "$project_root/config/access.canary.json" "$runtime_registry"
cmp -s "$project_root/config/access.canary.json" "$runtime_registry"

mkdir -p "$state_dir"
chmod 700 "$state_dir"
printf '%s\n' "$runtime_registry" >"$state_file.tmp"
chmod 600 "$state_file.tmp"
mv -f "$state_file.tmp" "$state_file"

MCP_CANARY_ACCESS_CONFIG="$runtime_registry" \
  docker compose -f "$compose_file" up -d --build

cleanup_on_error=false
echo "MCP_OZON canary started: http://127.0.0.1:8789/mcp"
