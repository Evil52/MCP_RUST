#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"

if [[ ! -f "$env_file" ]]; then
  "$project_dir/scripts/keycloak-init.sh"
fi

chmod 600 "$env_file"
if grep -q 'replace-with-' "$env_file"; then
  echo ".keycloak.env still contains placeholder values" >&2
  exit 1
fi

docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  up -d --build --wait --wait-timeout 180

"$project_dir/scripts/keycloak-sync-config.sh"

docker compose --env-file "$env_file" -f "$compose_file" ps
