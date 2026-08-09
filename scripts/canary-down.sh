#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/compose.canary.yaml"
state_file="$project_root/target/canary/runtime-registry"

if [[ ! -f "$state_file" || -L "$state_file" ]]; then
  echo "Missing canary runtime state; nothing to stop." >&2
  exit 1
fi

runtime_registry="$(sed -n '1p' "$state_file")"
runtime_dir="$(dirname "$runtime_registry")"
case "$runtime_registry" in
  /private/tmp/mcp-ozon-canary.*/access.json|/tmp/mcp-ozon-canary.*/access.json) ;;
  *)
    echo "Refusing to remove an unexpected canary runtime path." >&2
    exit 1
    ;;
esac

MCP_CANARY_ACCESS_CONFIG="$runtime_registry" \
  docker compose -f "$compose_file" down --remove-orphans

rm -rf "$runtime_dir"
rm -f "$state_file"
echo "MCP_OZON canary stopped; temporary registry removed."
