#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stack_env="$project_root/.sonar-stack.env"
scanner_env="$project_root/.sonar.env"
sonar_url="http://127.0.0.1:9000"
token_name="mcp-ozon-local-analysis"

if [[ ! -f "$stack_env" ]]; then
  echo "Missing .sonar-stack.env. Run ./scripts/sonar-up.sh first." >&2
  exit 1
fi

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

bearer_http_status() {
  local token="$1"
  local url="$2"
  printf 'header = "Authorization: Bearer %s"\n' "$token" \
    | curl --config - \
      --silent \
      --show-error \
      --output /dev/null \
      --write-out '%{http_code}' \
      "$url"
}

curl_config_escape() {
  local value="$1"
  if [[ "$value" == *[[:cntrl:]]* ]]; then
    echo "Refusing to pass a control character through curl configuration." >&2
    return 1
  fi
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s' "$value"
}

curl_with_basic_auth() {
  local username="$1"
  local password="$2"
  shift 2
  local credentials
  credentials="$(curl_config_escape "$username:$password")"
  printf 'user = "%s"\n' "$credentials" \
    | curl --config - "$@"
}

change_default_admin_password() {
  local new_password="$1"
  local url="$2"
  local credentials login_field previous_password_field password_field
  credentials="$(curl_config_escape 'admin:admin')"
  login_field="$(curl_config_escape 'login=admin')"
  previous_password_field="$(curl_config_escape 'previousPassword=admin')"
  password_field="$(curl_config_escape "password=$new_password")"
  {
    printf 'user = "%s"\n' "$credentials"
    printf 'data-urlencode = "%s"\n' "$login_field"
    printf 'data-urlencode = "%s"\n' "$previous_password_field"
    printf 'data-urlencode = "%s"\n' "$password_field"
  } | curl --config - \
    --fail \
    --silent \
    --show-error \
    --request POST \
    "$url"
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
if [[ "$existing_token" =~ ^[[:alnum:]_.-]+$ ]]; then
  token_status="$(bearer_http_status "$existing_token" "$sonar_url/api/v2/analysis/version")"
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

stored_admin_valid="$(curl_with_basic_auth admin "$admin_password" --fail --silent \
  "$sonar_url/api/authentication/validate" || true)"
if [[ "$stored_admin_valid" != *'"valid":true'* ]]; then
  default_admin_valid="$(curl_with_basic_auth admin admin --fail --silent \
    "$sonar_url/api/authentication/validate" || true)"
  if [[ "$default_admin_valid" != *'"valid":true'* ]]; then
    echo "Cannot authenticate the dedicated local SonarQube admin account." >&2
    exit 1
  fi
  admin_password="Aa1!$(openssl rand -hex 16)"
  write_admin_password "$admin_password"
  change_default_admin_password \
    "$admin_password" \
    "$sonar_url/api/users/change_password" >/dev/null
fi

curl_with_basic_auth admin "$admin_password" --fail --silent \
  --request POST \
  --data-urlencode "name=$token_name" \
  "$sonar_url/api/user_tokens/revoke" >/dev/null 2>&1 || true

token_response="$(curl_with_basic_auth admin "$admin_password" \
  --fail --silent --show-error \
  --request POST \
  --data-urlencode "name=$token_name" \
  --data-urlencode type=GLOBAL_ANALYSIS_TOKEN \
  "$sonar_url/api/user_tokens/generate")"
analysis_token="$(printf '%s' "$token_response" | jq --exit-status --raw-output '.token')"
if [[ ! "$analysis_token" =~ ^[[:alnum:]_.-]+$ ]]; then
  echo "SonarQube returned a token with an unexpected format." >&2
  exit 1
fi

previous_umask="$(umask)"
umask 077
printf 'SONAR_HOST_URL=%s\nSONAR_TOKEN=%s\n' "$sonar_url" "$analysis_token" > "$scanner_env"
chmod 600 "$scanner_env"
umask "$previous_umask"
echo "Created a dedicated MCP_OZON SonarQube analysis token in .sonar.env."
