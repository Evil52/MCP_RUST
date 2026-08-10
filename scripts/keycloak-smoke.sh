#!/bin/bash

set -euo pipefail
set +x
umask 077

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
keycloak_url="http://localhost:8180"
mcp_url="http://127.0.0.1:8788/mcp"
resource_url="http://localhost:8788/mcp"
metadata_url="http://127.0.0.1:8788/.well-known/oauth-protected-resource"
resource_metadata_url="http://localhost:8788/.well-known/oauth-protected-resource"
required_scope="mcp:tools"
kcadm_config="/tmp/mcp-ozon-kcadm.$$.config"
actor_id="keycloak_e2e_manager"
username="keycloak-e2e-manager"

for dependency in curl docker jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

safe_curl() {
  command curl --disable --noproxy '*' "$@"
}

for identity_value in "$actor_id" "$username"; do
  if [[ ! "$identity_value" =~ ^[A-Za-z0-9._@-]{1,128}$ ]]; then
    echo "Admin actor id and OIDC username must use safe identifier characters" >&2
    exit 1
  fi
done

if [[ -L "$env_file" || ! -f "$env_file" || ! -r "$env_file" ]]; then
  echo "Missing $env_file; run ./scripts/keycloak-init.sh" >&2
  exit 1
fi

# shellcheck source=scripts/keycloak-env.sh
source "$project_dir/scripts/keycloak-env.sh"
keycloak_load_env_file "$env_file"
export -n KEYCLOAK_DB_PASSWORD KEYCLOAK_ADMIN_PASSWORD KEYCLOAK_TEST_USER_PASSWORD

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
owns_test_user=false
user_id=""
session_id=""
rotated_key_component_name="mcp-ozon-e2e-rotated-rsa"
owns_rotated_key=false

cleanup() {
  terminate_mcp_session >/dev/null 2>&1 || true
  delete_rotated_key >/dev/null 2>&1 || true
  delete_test_user >/dev/null 2>&1 || true
  "${compose[@]}" exec -T keycloak rm -f "$kcadm_config" >/dev/null 2>&1 || true
  rm -rf "$smoke_dir"
}
trap cleanup EXIT

rotated_key_component_id() {
  "${compose[@]}" exec -T keycloak \
    /opt/keycloak/bin/kcadm.sh get components \
    --config "$kcadm_config" \
    --target-realm ofk \
    --fields id,name \
    | jq -r --arg name "$rotated_key_component_name" \
      '[.[] | select(.name == $name)] | if length == 1 then .[0].id else "" end'
}

delete_rotated_key() {
  local component_id
  if [[ "$owns_rotated_key" != "true" ]]; then
    return
  fi
  component_id="$(rotated_key_component_id)"
  if [[ -n "$component_id" ]]; then
    "${compose[@]}" exec -T keycloak \
      /opt/keycloak/bin/kcadm.sh delete "components/$component_id" \
      --config "$kcadm_config" \
      --target-realm ofk >/dev/null
  fi
  owns_rotated_key=false
}

realm_signing_kids() {
  safe_curl --fail --silent --show-error \
    "$keycloak_url/realms/ofk/protocol/openid-connect/certs" \
    | jq -c '[.keys[] | select(.use == "sig") | .kid] | sort'
}

jwt_header_kid() {
  local segment padding
  segment="$(cut -d. -f1 <"$1" | tr '_-' '/+')"
  padding=$(( ${#segment} % 4 ))
  if (( padding == 2 )); then
    segment="$segment=="
  elif (( padding == 3 )); then
    segment="$segment="
  fi
  printf '%s' "$segment" | base64 -d 2>/dev/null | jq -er '.kid'
}

issue_access_token() {
  local scope="$1" destination="$2" response_file status oauth_error
  response_file="$destination.response.json"
  status="$(
    printf '%s' "$KEYCLOAK_TEST_USER_PASSWORD" \
      | safe_curl --silent --show-error \
        --request POST \
        --output "$response_file" \
        --write-out '%{http_code}' \
        --data-urlencode 'grant_type=password' \
        --data-urlencode 'client_id=ozonofk-mcp' \
        --data-urlencode "scope=$scope" \
        --data-urlencode "username=$username" \
        --data-urlencode 'password@-' \
        "$keycloak_url/realms/ofk/protocol/openid-connect/token"
  )"
  chmod 600 "$response_file"
  if [[ "$status" != "200" ]]; then
    oauth_error="$(jq -r '.error // "unknown_error"' "$response_file" 2>/dev/null || printf 'invalid_json')"
    echo "Token request failed: HTTP $status, OAuth error=$oauth_error" >&2
    return 1
  fi
  jq --exit-status --raw-output --join-output '.access_token' "$response_file" >"$destination"
  chmod 600 "$destination"
}

terminate_mcp_session() {
  if [[ -z "$session_id" ]]; then
    return
  fi
  safe_curl --fail --silent --show-error \
    --request DELETE \
    --header "Mcp-Session-Id: $session_id" \
    --header 'MCP-Protocol-Version: 2025-06-18' \
    --output /dev/null \
    "$mcp_url"
  session_id=""
}

delete_test_user() {
  local remaining_users
  if [[ "$owns_test_user" != "true" || -z "$user_id" ]]; then
    return
  fi
  "${compose[@]}" exec -T keycloak \
    /opt/keycloak/bin/kcadm.sh delete "users/$user_id" \
    --config "$kcadm_config" \
    --target-realm ofk >/dev/null
  remaining_users="$("${compose[@]}" exec -T keycloak \
    /opt/keycloak/bin/kcadm.sh get users \
    --config "$kcadm_config" \
    --target-realm ofk \
    --query "username=$username" \
    --fields id,username)"
  jq -e --arg username "$username" \
    '[.[] | select(.username == $username)] | length == 0' \
    <<<"$remaining_users" >/dev/null
  owns_test_user=false
  user_id=""
}

for _attempt in $(seq 1 90); do
  if safe_curl --fail --silent --show-error \
    "$keycloak_url/realms/ofk/.well-known/openid-configuration" >/dev/null 2>&1 \
    && safe_curl --fail --silent --show-error \
      "http://127.0.0.1:8788/health" >/dev/null 2>&1
  then
    break
  fi
  sleep 1
done

safe_curl --fail --silent --show-error \
  "$keycloak_url/realms/ofk/.well-known/openid-configuration" \
  | jq -e \
    --arg issuer "$keycloak_url/realms/ofk" \
    --arg scope "$required_scope" \
    '.issuer == $issuer
     and (.scopes_supported | index($scope)) != null
     and (.code_challenge_methods_supported | index("S256")) != null' >/dev/null

safe_curl --fail --silent --show-error "$metadata_url" \
  | jq -e \
    --arg resource "$resource_url" \
    --arg issuer "$keycloak_url/realms/ofk" \
    --arg scope "$required_scope" \
    '.resource == $resource
     and .authorization_servers == [$issuer]
     and (.scopes_supported | index($scope)) != null' >/dev/null

KC_CLI_PASSWORD="$KEYCLOAK_ADMIN_PASSWORD" "${compose[@]}" exec -T \
  -e KC_CLI_PASSWORD \
  keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --config "$kcadm_config" \
  --server http://127.0.0.1:8080 \
  --realm master \
  --user "$KEYCLOAK_ADMIN_USER" >/dev/null
"${compose[@]}" exec -T keycloak chmod 600 "$kcadm_config"

user_json="$("${compose[@]}" exec -T keycloak \
  /opt/keycloak/bin/kcadm.sh get users \
  --config "$kcadm_config" \
  --target-realm ofk \
  --query "username=$username" \
  --fields id,username,email,firstName,lastName)"
user_id="$(jq -r --arg username "$username" \
  '[.[] | select(.username == $username)]
   | if length == 1 then .[0].id else "" end' <<<"$user_json")"

if [[ -z "$user_id" ]]; then
  user_payload="$(
    jq -cn --arg username "$username" \
      '{
        username: $username,
        firstName: "Keycloak",
        lastName: "E2E Manager",
        email: "keycloak-e2e-manager@localhost.invalid",
        enabled: true,
        emailVerified: true,
        requiredActions: []
      }'
  )"
  printf '%s' "$user_payload" \
    | "${compose[@]}" exec -T keycloak \
      /opt/keycloak/bin/kcadm.sh create users \
      --config "$kcadm_config" \
      --target-realm ofk \
      --file - >/dev/null
  user_json="$("${compose[@]}" exec -T keycloak \
    /opt/keycloak/bin/kcadm.sh get users \
    --config "$kcadm_config" \
    --target-realm ofk \
    --query "username=$username" \
    --fields id,username,email,firstName,lastName)"
  user_id="$(jq -er --arg username "$username" \
    '[.[] | select(.username == $username)]
     | if length == 1
       and .[0].email == "keycloak-e2e-manager@localhost.invalid"
       and .[0].firstName == "Keycloak"
       and .[0].lastName == "E2E Manager"
       then .[0].id
       else error("reserved E2E user provisioning failed")
       end' \
    <<<"$user_json")"
elif ! jq -e \
  --arg username "$username" \
  '[.[] | select(.username == $username)]
   | length == 1
     and .[0].email == "keycloak-e2e-manager@localhost.invalid"
     and .[0].firstName == "Keycloak"
     and .[0].lastName == "E2E Manager"' \
  <<<"$user_json" >/dev/null; then
  echo "Reserved Keycloak E2E username has an unexpected profile; refusing to modify it" >&2
  exit 1
fi
owns_test_user=true

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
  --set 'requiredActions=[]' >/dev/null

token_response_file="$smoke_dir/token.response.json"
token_status="$(
  printf '%s' "$KEYCLOAK_TEST_USER_PASSWORD" \
    | safe_curl --silent --show-error \
      --request POST \
      --output "$token_response_file" \
      --write-out '%{http_code}' \
      --data-urlencode 'grant_type=password' \
      --data-urlencode 'client_id=ozonofk-mcp' \
      --data-urlencode "scope=openid profile email $required_scope" \
      --data-urlencode "username=$username" \
      --data-urlencode 'password@-' \
      "$keycloak_url/realms/ofk/protocol/openid-connect/token"
)"
chmod 600 "$token_response_file"
if [[ "$token_status" != "200" ]]; then
  token_error="$(jq -r '.error // "unknown_error"' "$token_response_file" 2>/dev/null || printf 'invalid_json')"
  echo "Password-grant JWT smoke failed: HTTP $token_status, OAuth error=$token_error" >&2
  exit 1
fi
jq --exit-status --raw-output --join-output '.access_token' \
  "$token_response_file" >"$smoke_dir/access-token"
chmod 600 "$smoke_dir/access-token"

auth_header_file="$smoke_dir/authorization.header"
printf 'Authorization: Bearer ' >"$auth_header_file"
cat "$smoke_dir/access-token" >>"$auth_header_file"
printf '\n' >>"$auth_header_file"
chmod 600 "$auth_header_file"

missing_scope_token_response="$smoke_dir/missing-scope-token.response.json"
missing_scope_token_status="$(
  printf '%s' "$KEYCLOAK_TEST_USER_PASSWORD" \
    | safe_curl --silent --show-error \
      --request POST \
      --output "$missing_scope_token_response" \
      --write-out '%{http_code}' \
      --data-urlencode 'grant_type=password' \
      --data-urlencode 'client_id=ozonofk-mcp' \
      --data-urlencode 'scope=openid profile email' \
      --data-urlencode "username=$username" \
      --data-urlencode 'password@-' \
      "$keycloak_url/realms/ofk/protocol/openid-connect/token"
)"
chmod 600 "$missing_scope_token_response"
if [[ "$missing_scope_token_status" != "200" ]]; then
  missing_scope_error="$(jq -r '.error // "unknown_error"' "$missing_scope_token_response" 2>/dev/null || printf 'invalid_json')"
  echo "Missing-scope JWT setup failed: HTTP $missing_scope_token_status, OAuth error=$missing_scope_error" >&2
  exit 1
fi
jq --exit-status --raw-output --join-output '.access_token' \
  "$missing_scope_token_response" >"$smoke_dir/missing-scope-token"
chmod 600 "$smoke_dir/missing-scope-token"
missing_scope_header_file="$smoke_dir/missing-scope-authorization.header"
printf 'Authorization: Bearer ' >"$missing_scope_header_file"
cat "$smoke_dir/missing-scope-token" >>"$missing_scope_header_file"
printf '\n' >>"$missing_scope_header_file"
chmod 600 "$missing_scope_header_file"

invalid_header_file="$smoke_dir/invalid-authorization.header"
printf '%s\n' 'Authorization: Bearer not-a-jwt' >"$invalid_header_file"
chmod 600 "$invalid_header_file"

request() {
  local body="$1"
  local response_file="$2"
  local headers_file="$3"
  shift 3
  safe_curl --fail --silent --show-error \
    --request POST \
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

assert_call_auth_error() {
  local scenario="$1"
  local request_id="$2"
  local expected_error="$3"
  local header_file="${4:-}"
  local response_headers="$smoke_dir/$scenario.headers"
  local response_body="$smoke_dir/$scenario.body"
  local response_status
  local response_json
  local request_body
  local curl_args=(
    --silent
    --show-error
    --request POST
    --header 'Accept: application/json, text/event-stream'
    --header 'Content-Type: application/json'
    --header "Mcp-Session-Id: $session_id"
    --header 'MCP-Protocol-Version: 2025-06-18'
  )

  if [[ -n "$header_file" ]]; then
    curl_args+=(--header @"$header_file")
  fi
  request_body="$(jq -cn --argjson id "$request_id" \
    '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:"list_members", arguments:{}}}')"
  response_status="$(
    safe_curl "${curl_args[@]}" \
      --dump-header "$response_headers" \
      --output "$response_body" \
      --write-out '%{http_code}' \
      --data "$request_body" \
      "$mcp_url"
  )"
  chmod 600 "$response_headers" "$response_body"
  if [[ "$response_status" != "200" ]]; then
    echo "Expected $scenario tools/call to return HTTP 200, got $response_status" >&2
    exit 1
  fi
  response_json="$(json_response "$response_body")"
  jq -e \
    --argjson expected_id "$request_id" \
    --arg expected_error "$expected_error" \
    --arg metadata "$resource_metadata_url" \
    --arg scope "$required_scope" '
      .jsonrpc == "2.0"
      and .id == $expected_id
      and (has("error") | not)
      and .result.isError == true
      and ((.result._meta["mcp/www_authenticate"] // null) as $challenges
        | ($challenges | type) == "array"
        and ($challenges | length) > 0
        and any($challenges[];
          type == "string"
          and startswith("Bearer ")
          and contains("resource_metadata=\"" + $metadata + "\"")
          and contains("scope=\"" + $scope + "\"")
          and contains("error=\"" + $expected_error + "\"")
          and test("error_description=\"[^\"]+\"")))' \
    <<<"$response_json" >/dev/null
}

initialize_headers="$smoke_dir/initialize.headers"
initialize_body="$smoke_dir/initialize.body"
request \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"keycloak-smoke","version":"1.0"}}}' \
  "$initialize_body" \
  "$initialize_headers"

json_response "$initialize_body" \
  | jq -e '.id == 1 and .result.serverInfo.name != null' >/dev/null
session_id="$(awk 'tolower($1) == "mcp-session-id:" { gsub("\r", "", $2); print $2; exit }' "$initialize_headers")"
if (( ${#session_id} < 1 || ${#session_id} > 256 )) \
  || [[ ! "$session_id" =~ ^[-A-Za-z0-9._~]+$ ]]; then
  echo "MCP initialize response did not contain a safe mcp-session-id" >&2
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

tools_headers="$smoke_dir/tools.headers"
tools_body="$smoke_dir/tools.body"
request \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  "$tools_body" \
  "$tools_headers" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'

expected_tools='[
  "list_members",
  "marketplace_accounts",
  "ozon_analytics",
  "ozon_fbo_postings",
  "ozon_fbs_postings",
  "ozon_finance_totals",
  "ozon_finance_transactions",
  "ozon_product_prices",
  "ozon_product_stocks",
  "ozon_questions",
  "ozon_returns",
  "ozon_reviews",
  "ozon_rfbs_returns",
  "ozon_seller_rating",
  "ozon_seller_rating_history",
  "ozon_stock_turnover",
  "ozon_stores_status",
  "wb_ping",
  "wb_sales_funnel",
  "wb_sales_funnel_history",
  "wb_sales_funnel_grouped_history",
  "wb_warehouse_stocks",
  "wb_orders",
  "wb_sales",
  "wb_stores_status"
]'
tools_json="$(json_response "$tools_body")"
jq -e --argjson expected "$expected_tools" \
  '.id == 2 and ([.result.tools[].name] | sort) == ($expected | sort)' \
  <<<"$tools_json" >/dev/null

stale_tools_headers="$smoke_dir/stale-tools.headers"
stale_tools_body="$smoke_dir/stale-tools.body"
request \
  '{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}' \
  "$stale_tools_body" \
  "$stale_tools_headers" \
  --header @"$invalid_header_file" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'
jq -e --argjson expected "$expected_tools" \
  '.id == 3 and ([.result.tools[].name] | sort) == ($expected | sort)' \
  <<<"$(json_response "$stale_tools_body")" >/dev/null

assert_call_auth_error missing-credentials 4 invalid_token
assert_call_auth_error invalid-token 5 invalid_token "$invalid_header_file"
# The local Keycloak workaround binds the resource audience to the optional mcp:tools scope.
# Omitting that scope therefore also omits the required audience and is correctly invalid_token;
# deterministic Rust/wire tests cover valid-audience tokens that lack only the required scope.
assert_call_auth_error missing-scope 6 invalid_token "$missing_scope_header_file"

members_headers="$smoke_dir/members.headers"
members_body="$smoke_dir/members.body"
request \
  '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"list_members","arguments":{}}}' \
  "$members_body" \
  "$members_headers" \
  --header @"$auth_header_file" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'

members_json="$(json_response "$members_body")"
jq -e \
  --arg actor "$actor_id" \
  '.id == 7
   and .result.isError != true
   and (.result.content[0].text | fromjson | .actor.id == $actor)
   and (.result.content[0].text | fromjson | .actor.role == "manager")
   and (.result.content[0].text | fromjson | .members | length == 1)
   and (.result.content[0].text | fromjson
        | any(.members[];
            .id == $actor
            and .role == "manager"
            and (.account_ids | length) == 0
            and (.accounts | length) == 0))' \
  <<<"$members_json" >/dev/null

member_count="$(jq -r '.result.content[0].text | fromjson | .members | length' <<<"$members_json")"

# Realm key rotation must be picked up without restarting the MCP and without
# waiting out the JWKS cache TTL, otherwise every rotation is an outage. The
# unit tests cover this against a mock JWKS; only this step proves it against
# a real Keycloak rotation and a real cached verifier.
mcp_container="$("${compose[@]}" ps -q server)"
if [[ -z "$mcp_container" ]]; then
  echo "Could not resolve the MCP container for the rotation check" >&2
  exit 1
fi
mcp_started_before="$(docker inspect -f '{{.State.StartedAt}}' "$mcp_container")"
original_kid="$(jwt_header_kid "$smoke_dir/access-token")"
original_kids="$(realm_signing_kids)"
jq -e --arg kid "$original_kid" 'index($kid) != null' <<<"$original_kids" >/dev/null

jq -cn --arg name "$rotated_key_component_name" \
  '{
    name: $name,
    providerId: "rsa-generated",
    providerType: "org.keycloak.keys.KeyProvider",
    config: {
      priority: ["200"],
      algorithm: ["RS256"],
      enabled: ["true"],
      active: ["true"]
    }
  }' \
  | "${compose[@]}" exec -T keycloak \
    /opt/keycloak/bin/kcadm.sh create components \
    --config "$kcadm_config" \
    --target-realm ofk \
    --file - >/dev/null
owns_rotated_key=true

rotated_kids="$original_kids"
for _attempt in $(seq 1 30); do
  rotated_kids="$(realm_signing_kids)"
  if [[ "$rotated_kids" != "$original_kids" ]]; then
    break
  fi
  sleep 1
done
if [[ "$rotated_kids" == "$original_kids" ]]; then
  echo "Keycloak did not publish a rotated signing key" >&2
  exit 1
fi

issue_access_token "openid profile email $required_scope" "$smoke_dir/rotated-access-token"
rotated_kid="$(jwt_header_kid "$smoke_dir/rotated-access-token")"
if [[ "$rotated_kid" == "$original_kid" ]]; then
  echo "Rotation did not change the token signing key; the check would be vacuous" >&2
  exit 1
fi
jq -e --arg kid "$rotated_kid" 'index($kid) == null' <<<"$original_kids" >/dev/null

rotated_header_file="$smoke_dir/rotated-authorization.header"
printf 'Authorization: Bearer ' >"$rotated_header_file"
cat "$smoke_dir/rotated-access-token" >>"$rotated_header_file"
printf '\n' >>"$rotated_header_file"
chmod 600 "$rotated_header_file"

rotated_headers="$smoke_dir/rotated.headers"
rotated_body="$smoke_dir/rotated.body"
request \
  '{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"list_members","arguments":{}}}' \
  "$rotated_body" \
  "$rotated_headers" \
  --header @"$rotated_header_file" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'
jq -e \
  --arg actor "$actor_id" \
  '.id == 8
   and .result.isError != true
   and (.result.content[0].text | fromjson | .actor.id == $actor)' \
  <<<"$(json_response "$rotated_body")" >/dev/null

# A token signed by the still-published previous key must keep working, so a
# rotation does not invalidate tokens already issued to live clients.
request \
  '{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"list_members","arguments":{}}}' \
  "$smoke_dir/previous-key.body" \
  "$smoke_dir/previous-key.headers" \
  --header @"$auth_header_file" \
  --header "Mcp-Session-Id: $session_id" \
  --header 'MCP-Protocol-Version: 2025-06-18'
jq -e '.id == 9 and .result.isError != true' \
  <<<"$(json_response "$smoke_dir/previous-key.body")" >/dev/null

mcp_started_after="$(docker inspect -f '{{.State.StartedAt}}' "$mcp_container")"
if [[ "$mcp_started_before" != "$mcp_started_after" ]]; then
  echo "The MCP restarted during rotation; the refresh path was not exercised" >&2
  exit 1
fi

delete_rotated_key
terminate_mcp_session
delete_test_user
echo "Keycloak JWT E2E passed: discovery is public; denied tools/call responses use MCP OAuth challenges; realm key rotation ($original_kid -> $rotated_kid) was picked up without restarting the MCP and without invalidating previously issued tokens; actor=$actor_id, visible_members=$member_count"
