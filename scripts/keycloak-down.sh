#!/usr/bin/env bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
state_file="$project_dir/target/keycloak/runtime-registry"
runtime_dir=""

if [[ -L "$env_file" || ! -f "$env_file" ]]; then
  echo ".keycloak.env must be a regular file, not a symlink" >&2
  exit 1
fi

if [[ -f "$state_file" && ! -L "$state_file" ]]; then
  runtime_registry="$(sed -n '1p' "$state_file")"
  case "$runtime_registry" in
    /private/tmp/mcp-ozon-auth.*/access.json|/tmp/mcp-ozon-auth.*/access.json) ;;
    *)
      echo "Refusing to remove an unexpected Keycloak runtime path" >&2
      exit 1
      ;;
  esac
  runtime_dir="$(dirname "$runtime_registry")"
  export MCP_AUTH_CONFIG_DIR="$runtime_dir"
elif [[ -e "$state_file" ]]; then
  echo "Keycloak runtime state must be a regular file, not a symlink" >&2
  exit 1
fi

docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  down --remove-orphans

if [[ -n "$runtime_dir" ]]; then
  rm -rf "$runtime_dir"
  rm -f "$state_file"
fi

echo "Keycloak/JWT stack stopped; PostgreSQL volume was preserved"
