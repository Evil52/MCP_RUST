#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
# shellcheck source=scripts/keycloak-env.sh
source "$project_dir/scripts/keycloak-env.sh"

if ! command -v openssl >/dev/null 2>&1; then
  echo "Missing dependency: openssl" >&2
  exit 1
fi

if [[ -L "$env_file" ]]; then
  echo ".keycloak.env must be a regular file, not a symlink" >&2
  exit 1
fi

if [[ -e "$env_file" ]]; then
  if [[ ! -f "$env_file" ]]; then
    echo ".keycloak.env must be a regular file" >&2
    exit 1
  fi
  chmod 600 "$env_file"
  if ! grep -q '^KEYCLOAK_TEST_USER_PASSWORD=' "$env_file"; then
    keycloak_test_user_secret="$(openssl rand -hex 32)"
    umask 077
    printf 'KEYCLOAK_TEST_USER_PASSWORD=%s\n' "$keycloak_test_user_secret" >>"$env_file"
    echo "Added missing Keycloak test-user secret to .keycloak.env"
  else
    echo ".keycloak.env already exists; secrets were not changed"
  fi
  keycloak_load_env_file "$env_file"
  exit 0
fi

keycloak_db_secret="$(openssl rand -hex 32)"
keycloak_admin_secret="$(openssl rand -hex 32)"
keycloak_test_user_secret="$(openssl rand -hex 32)"

umask 077
{
  printf 'KEYCLOAK_DB_NAME=keycloak\n'
  printf 'KEYCLOAK_DB_USER=keycloak\n'
  printf 'KEYCLOAK_DB_PASSWORD=%s\n' "$keycloak_db_secret"
  printf 'KEYCLOAK_ADMIN_USER=ofk-admin\n'
  printf 'KEYCLOAK_ADMIN_PASSWORD=%s\n' "$keycloak_admin_secret"
  printf 'KEYCLOAK_TEST_USER_PASSWORD=%s\n' "$keycloak_test_user_secret"
} >"$env_file"

keycloak_load_env_file "$env_file"

echo "Created $env_file with mode 600"
