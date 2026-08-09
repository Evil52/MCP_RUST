#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
access_config="$project_dir/config/access.json"
state_dir="$project_dir/target/keycloak"
state_file="$state_dir/runtime-registry"

if [[ "$(uname -s)" == "Darwin" ]]; then
  temporary_root="/private/tmp"
else
  temporary_root="/tmp"
fi

if [[ -L "$env_file" ]]; then
  echo ".keycloak.env must be a regular file, not a symlink" >&2
  exit 1
fi
"$project_dir/scripts/keycloak-init.sh"
if [[ ! -f "$env_file" ]]; then
  echo ".keycloak.env was not created" >&2
  exit 1
fi
if [[ -L "$access_config" || ! -f "$access_config" ]]; then
  echo "config/access.json must be a regular file, not a symlink" >&2
  exit 1
fi

chmod 600 "$env_file"
if grep -q 'replace-with-' "$env_file"; then
  echo ".keycloak.env still contains placeholder values" >&2
  exit 1
fi

mkdir -p "$state_dir"
chmod 700 "$state_dir"

if [[ -f "$state_file" && ! -L "$state_file" ]]; then
  runtime_registry="$(sed -n '1p' "$state_file")"
  case "$runtime_registry" in
    /private/tmp/mcp-ozon-auth.*/access.json|/tmp/mcp-ozon-auth.*/access.json) ;;
    *)
      echo "Refusing an unexpected Keycloak runtime registry path" >&2
      exit 1
      ;;
  esac
  runtime_dir="$(dirname "$runtime_registry")"
  mkdir -p "$runtime_dir"
else
  if [[ -e "$state_file" ]]; then
    echo "Keycloak runtime state must be a regular file, not a symlink" >&2
    exit 1
  fi
  runtime_dir="$(mktemp -d "$temporary_root/mcp-ozon-auth.XXXXXX")"
  runtime_registry="$runtime_dir/access.json"
  printf '%s\n' "$runtime_registry" >"$state_file.tmp"
  chmod 600 "$state_file.tmp"
  mv -f "$state_file.tmp" "$state_file"
fi

chmod 700 "$runtime_dir"
install -m 644 "$access_config" "$runtime_registry.tmp"
mv -f "$runtime_registry.tmp" "$runtime_registry"
cmp -s "$access_config" "$runtime_registry"
export MCP_AUTH_CONFIG_DIR="$runtime_dir"

docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  up -d --build --wait --wait-timeout 180

"$project_dir/scripts/keycloak-sync-config.sh"

docker compose --env-file "$env_file" -f "$compose_file" ps
