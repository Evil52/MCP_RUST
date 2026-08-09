#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
keycloak_url="http://localhost:8180"
mcp_url="http://127.0.0.1:8788/mcp"
resource_url="http://localhost:8788/mcp"
metadata_url="http://127.0.0.1:8788/.well-known/oauth-protected-resource"
required_scope="mcp:tools"
username="admin"
kcadm_config="/tmp/mcp-ozon-kcadm.config"

for dependency in curl docker jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

if [[ ! -r "$env_file" ]]; then
  echo "Missing $env_file; run ./scripts/keycloak-init.sh" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

for required_name in \
  KEYCLOAK_ADMIN_USER \
  KEYCLOAK_ADMIN_PASSWORD \
  KEYCLOAK_TEST_USER_PASSWORD
do
  if [[ -z "${!required_name:-}" ]]; then
    echo "Missing $required_name in .keycloak.env" >&2
    exit 1
  fi
done

compose=(
  docker compose
  --env-file "$env_file"
  -f "$compose_file"
)

smoke_dir="$(mktemp -d "${TMPDIR:-/tmp}/mcp-ozon-keycloak-smoke.XXXXXX")"
chmod 700 "$smoke_dir"

cleanup() {
  "${compose[@]}" exec -T keycloak rm -f "$kcadm_config" >/dev/null 2>&1 || true
  rm -rf "$smoke_dir"
}
trap cleanup EXIT

for _attempt in $(seq 1 90); do
  if curl --fail --silent --show-error \
    "$keycloak_url/realms/ofk/.well-known/openid-configuration" >/dev/null 2>&1 \
    && curl --fail --silent --show-error \
      "http://127.0.0.1:8788/health" >/dev/null 2>&1
  then
    break
  fi
  sleep 1
done

curl --fail --silent --show-error \
  "$keycloak_url/realms/ofk/.well-known/openid-configuration" \
  | jq -e \
    --arg issuer "$keycloak_url/realms/ofk" \
    --arg scope "$required_scope" \
    '.issuer == $issuer
     and (.scopes_supported | index($scope)) != null
     and (.code_challenge_methods_supported | index("S256")) != null' >/dev/null

curl --fail --silent --show-error "$metadata_url" \
  | jq -e \
    --arg resource "$resource_url" \
    --arg scope "$required_scope" \
    '.resource == $resource and (.scopes_supported | index($scope)) != null' >/dev/null

KC_CLI_PASSWORD="$KEYCLOAK_ADMIN_PASSWORD" "${compose[@]}" exec -T \
  -e KC_CLI_PASSWORD \
  keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --config "$kcadm_config" \
  --server http://127.0.0.1:8080 \
  --realm master \
  --user "$KEYCLOAK_ADMIN_USER" >/dev/null

user_json="$("${compose[@]}" exec -T keycloak \
  /opt/keycloak/bin/kcadm.sh get users \
  --config "$kcadm_config" \
  --target-realm ofk \
  --query "username=$username" \
  --fields id,username)"
user_id="$(jq -er --arg username "$username" \
  '.[] | select(.username == $username) | .id' <<<"$user_json")"

KC_CLI_PASSWORD="$KEYCLOAK_TEST_USER_PASSWORD" "${compose[@]}" exec -T \
  -e KC_CLI_PASSWORD \
  keycloak /opt/keycloak/bin/kcadm.sh set-password \
  --config "$kcadm_config" \
  --target-realm ofk \
  --userid "$user_id" >/dev/null

"${compose[@]}" exec -T keycloak \
  /opt/keycloak/bin/kcadm.sh update "users/$user_id" \
  --config "$kcadm_config" \
  --target-realm ofk \
  --set 'email=admin@localhost.invalid' \
  --set 'emailVerified=true' \
  --set 'requiredActions=[]' >/dev/null

token_response="$(
  printf '%s' "$KEYCLOAK_TEST_USER_PASSWORD" \
    | curl --fail --silent --show-error \
      --request POST \
      --data-urlencode 'grant_type=password' \
      --data-urlencode 'client_id=ozonofk-mcp' \
      --data-urlencode "scope=openid profile email $required_scope" \
      --data-urlencode "username=$username" \
      --data-urlencode 'password@-' \
      "$keycloak_url/realms/ofk/protocol/openid-connect/token"
)"
access_token="$(jq -er '.access_token' <<<"$token_response")"

auth_header_file="$smoke_dir/authorization.header"
printf 'Authorization: Bearer %s\n' "$access_token" >"$auth_header_file"
chmod 600 "$auth_header_file"

without_token_status="$(curl --silent --output /dev/null --write-out '%{http_code}' "$mcp_url")"
if [[ "$without_token_status" != "401" ]]; then
  echo "Expected MCP without JWT to return 401, got $without_token_status" >&2
  exit 1
fi

missing_scope_token_response="$(
  printf '%s' "$KEYCLOAK_TEST_USER_PASSWORD" \
    | curl --fail --silent --show-error \
      --request POST \
      --data-urlencode 'grant_type=password' \
      --data-urlencode 'client_id=ozonofk-mcp' \
      --data-urlencode 'scope=openid profile email' \
      --data-urlencode "username=$username" \
      --data-urlencode 'password@-' \
      "$keycloak_url/realms/ofk/protocol/openid-connect/token"
)"
missing_scope_token="$(jq -er '.access_token' <<<"$missing_scope_token_response")"
missing_scope_header_file="$smoke_dir/missing-scope-authorization.header"
printf 'Authorization: Bearer %s\n' "$missing_scope_token" >"$missing_scope_header_file"
chmod 600 "$missing_scope_header_file"
missing_scope_status="$(
  curl --silent --output /dev/null --write-out '%{http_code}' \
    --header @"$missing_scope_header_file" \
    "$mcp_url"
)"
if [[ "$missing_scope_status" != "401" ]]; then
  echo "Expected MCP JWT without $required_scope to return 401, got $missing_scope_status" >&2
  exit 1
fi

request() {
  local body="$1"
  local response_file="$2"
  local headers_file="$3"
  shift 3
  curl --fail --silent --show-error \
    --request POST \
    --header @"$auth_header_file" \
    --header 'Accept: application/json, text/event-stream' \
    --header 'Content-Type: application/json' \
    "$@" \
    --dump-header "$headers_file" \
    --output "$response_file" \
    --data "$body" \
    "$mcp_url"
}

json_response() {
  local response_file="$1"
  local data
  data="$(sed -n 's/^data: //p' "$response_file" | tail -n 1)"
  if [[ -n "$data" ]]; then
    printf '%s' "$data"
  else
    sed -n '1p' "$response_file"
  fi
}

initialize_headers="$smoke_dir/initialize.headers"
initialize_body="$smoke_dir/initialize.body"
request \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"keycloak-smoke","version":"1.0"}}}' \
  "$initialize_body" \
  "$initialize_headers"

json_response "$initialize_body" \
  | jq -e '.result.serverInfo.name != null' >/dev/null
session_id="$(awk 'BEGIN { IGNORECASE=1 } /^mcp-session-id:/ { gsub("\r", "", $2); print $2; exit }' "$initialize_headers")"
if [[ -z "$session_id" ]]; then
  echo "MCP initialize response did not contain mcp-session-id" >&2
  exit 1
fi

notification_headers="$smoke_dir/notification.headers"
notification_body="$smoke_dir/notification.body"
request \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  "$notification_body" \
  "$notification_headers" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'

members_headers="$smoke_dir/members.headers"
members_body="$smoke_dir/members.body"
request \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_members","arguments":{}}}' \
  "$members_body" \
  "$members_headers" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'

members_json="$(json_response "$members_body")"
jq -e \
  --arg actor "$username" \
  '.result.isError != true
   and (.result.content[0].text | fromjson | .actor.id == $actor)
   and (.result.content[0].text | fromjson | any(.members[]; .id == $actor and .role == "admin"))' \
  <<<"$members_json" >/dev/null

member_count="$(jq -r '.result.content[0].text | fromjson | .members | length' <<<"$members_json")"
echo "Keycloak JWT E2E passed: no-token=401, missing-scope=401, actor=$username, visible_members=$member_count"
