#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"

if [[ -e "$env_file" ]]; then
  chmod 600 "$env_file"
  if ! grep -q '^KEYCLOAK_TEST_USER_PASSWORD=' "$env_file"; then
    keycloak_test_user_secret="$(openssl rand -hex 32)"
    umask 077
    printf 'KEYCLOAK_TEST_USER_PASSWORD=%s\n' "$keycloak_test_user_secret" >>"$env_file"
    echo "Added missing Keycloak test-user secret to .keycloak.env"
  else
    echo ".keycloak.env already exists; secrets were not changed"
  fi
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

echo "Created $env_file with mode 600"
