#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stack_env="$project_root/.sonar-stack.env"
scanner_env="$project_root/.sonar.env"
sonar_url="http://127.0.0.1:9000"
token_name="mcp-ozon-local-analysis"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to bootstrap the local SonarQube token." >&2
  exit 1
fi

read_env_value() {
  local file="$1"
  local wanted="$2"
  local key value
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    value="${value%$'\r'}"
    if [[ "$key" == "$wanted" ]]; then
      printf '%s' "$value"
      return 0
    fi
  done < "$file"
}

write_admin_password() {
  local password="$1"
  local temporary="$stack_env.tmp"
  local key value
  local previous_umask
  previous_umask="$(umask)"
  umask 077
  : > "$temporary"
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    if [[ "$key" != "SONAR_ADMIN_PASSWORD" ]]; then
      printf '%s=%s\n' "$key" "$value" >> "$temporary"
    fi
  done < "$stack_env"
  printf 'SONAR_ADMIN_PASSWORD=%s\n' "$password" >> "$temporary"
  mv "$temporary" "$stack_env"
  umask "$previous_umask"
}

existing_token=""
if [[ -f "$scanner_env" ]]; then
  existing_token="$(read_env_value "$scanner_env" SONAR_TOKEN)"
fi
if [[ -n "$existing_token" ]]; then
  token_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    --header "Authorization: Bearer $existing_token" \
    "$sonar_url/api/v2/analysis/version")"
  if [[ "$token_status" == "200" ]]; then
    echo "Local MCP_OZON SonarQube analysis token is valid."
    exit 0
  fi
fi

admin_password="$(read_env_value "$stack_env" SONAR_ADMIN_PASSWORD)"
if [[ -z "$admin_password" ]]; then
  admin_password="Aa1!$(openssl rand -hex 16)"
  write_admin_password "$admin_password"
fi

stored_admin_valid="$(curl --fail --silent --user "admin:$admin_password" \
  "$sonar_url/api/authentication/validate" || true)"
if [[ "$stored_admin_valid" != *'"valid":true'* ]]; then
  default_admin_valid="$(curl --fail --silent --user admin:admin \
    "$sonar_url/api/authentication/validate" || true)"
  if [[ "$default_admin_valid" != *'"valid":true'* ]]; then
    echo "Cannot authenticate the dedicated local SonarQube admin account." >&2
    exit 1
  fi
  admin_password="Aa1!$(openssl rand -hex 16)"
  write_admin_password "$admin_password"
  curl --fail --silent --show-error --user admin:admin \
    --request POST \
    --data-urlencode login=admin \
    --data-urlencode previousPassword=admin \
    --data-urlencode "password=$admin_password" \
    "$sonar_url/api/users/change_password" >/dev/null
fi

curl --fail --silent --user "admin:$admin_password" \
  --request POST \
  --data-urlencode "name=$token_name" \
  "$sonar_url/api/user_tokens/revoke" >/dev/null 2>&1 || true

token_response="$(curl --fail --silent --show-error --user "admin:$admin_password" \
  --request POST \
  --data-urlencode "name=$token_name" \
  --data-urlencode type=GLOBAL_ANALYSIS_TOKEN \
  "$sonar_url/api/user_tokens/generate")"
analysis_token="$(printf '%s' "$token_response" | jq --exit-status --raw-output '.token')"

previous_umask="$(umask)"
umask 077
printf 'SONAR_HOST_URL=%s\nSONAR_TOKEN=%s\n' "$sonar_url" "$analysis_token" > "$scanner_env"
chmod 600 "$scanner_env"
umask "$previous_umask"
echo "Created a dedicated MCP_OZON SonarQube analysis token in .sonar.env."
