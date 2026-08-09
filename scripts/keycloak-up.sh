#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
access_config="$project_dir/config/access.json"
state_dir="$project_dir/target/keycloak"
state_file="$state_dir/runtime-registry"
e2e_actor_id="keycloak_e2e_manager"
e2e_username="keycloak-e2e-manager"

for dependency in docker jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

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
jq -e \
  --arg actor_id "$e2e_actor_id" \
  --arg username "$e2e_username" \
  'if any(.actors[]; .id == $actor_id or .oidc.username == $username)
   then error("dedicated Keycloak E2E identity collides with access registry")
   else .actors += [{
     id: $actor_id,
     name: "Keycloak E2E Manager",
     role: "manager",
     account_ids: [],
     oidc: {username: $username}
   }]
   end' \
  "$access_config" >"$runtime_registry.tmp"
chmod 644 "$runtime_registry.tmp"
mv -f "$runtime_registry.tmp" "$runtime_registry"
export MCP_AUTH_CONFIG_FILE="$runtime_registry"

docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  up -d --build --force-recreate --wait --wait-timeout 180

"$project_dir/scripts/keycloak-sync-config.sh"

docker compose --env-file "$env_file" -f "$compose_file" ps
