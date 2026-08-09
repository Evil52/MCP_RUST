#!/bin/bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
realm="ofk"
client_id="ozonofk-mcp"
scope_name="mcp:tools"
audience="${MCP_OAUTH_RESOURCE_URL:-http://localhost:8788/mcp}"
redirect_uri="${MCP_OAUTH_REDIRECT_URI:-http://localhost:18789/callback}"
web_origin="${MCP_OAUTH_WEB_ORIGIN:-http://localhost:18789}"
direct_access_grants="${MCP_OAUTH_DIRECT_ACCESS_GRANTS:-true}"
kcadm_config="/tmp/mcp-ozon-kcadm-sync-$$.config"

for dependency in docker jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

if [[ "$direct_access_grants" != "true" && "$direct_access_grants" != "false" ]]; then
  echo "MCP_OAUTH_DIRECT_ACCESS_GRANTS must be true or false" >&2
  exit 1
fi

for url_value in "$audience" "$redirect_uri" "$web_origin"; do
  if [[ "$url_value" == *$'\n'* || "$url_value" == *$'\r'* ]]; then
    echo "OAuth URLs must not contain control characters" >&2
    exit 1
  fi
done

redirect_uris_json="$(jq -cn --arg value "$redirect_uri" '[$value]')"
web_origins_json="$(jq -cn --arg value "$web_origin" '[$value]')"

if [[ -L "$env_file" || ! -f "$env_file" || ! -r "$env_file" ]]; then
  echo "Missing $env_file; run ./scripts/keycloak-init.sh" >&2
  exit 1
fi

# shellcheck source=scripts/keycloak-env.sh
source "$project_dir/scripts/keycloak-env.sh"
keycloak_load_env_file "$env_file"
export -n KEYCLOAK_DB_PASSWORD KEYCLOAK_ADMIN_PASSWORD KEYCLOAK_TEST_USER_PASSWORD

for required_name in KEYCLOAK_ADMIN_USER KEYCLOAK_ADMIN_PASSWORD; do
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

cleanup() {
  "${compose[@]}" exec -T keycloak rm -f "$kcadm_config" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! "${compose[@]}" exec -T keycloak true >/dev/null 2>&1; then
  echo "Keycloak is not running; run ./scripts/keycloak-up.sh" >&2
  exit 1
fi

KC_CLI_PASSWORD="$KEYCLOAK_ADMIN_PASSWORD" "${compose[@]}" exec -T \
  -e KC_CLI_PASSWORD \
  keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --config "$kcadm_config" \
  --server http://127.0.0.1:8080 \
  --realm master \
  --user "$KEYCLOAK_ADMIN_USER" >/dev/null

"${compose[@]}" exec -T keycloak chmod 600 "$kcadm_config"

kcadm() {
  "${compose[@]}" exec -T keycloak \
    /opt/keycloak/bin/kcadm.sh "$@" \
    --config "$kcadm_config" \
    --target-realm "$realm"
}

scope_payload="$({
  jq -cn \
    --arg name "$scope_name" \
    '{
      name: $name,
      description: "Access OzonOFK MCP tools",
      protocol: "openid-connect",
      attributes: {
        "display.on.consent.screen": "true",
        "include.in.token.scope": "true"
      }
    }'
})"

scope_id="$(
  kcadm get client-scopes --fields id,name \
    | jq -er --arg name "$scope_name" \
      '[.[] | select(.name == $name)] | if length == 0 then "" else .[0].id end'
)"

if [[ -z "$scope_id" ]]; then
  printf '%s' "$scope_payload" \
    | kcadm create client-scopes --file - >/dev/null
  scope_id="$(
    kcadm get client-scopes --fields id,name \
      | jq -er --arg name "$scope_name" \
        '[.[] | select(.name == $name)] | if length == 1 then .[0].id else error("scope lookup failed") end'
  )"
else
  printf '%s' "$scope_payload" \
    | kcadm update "client-scopes/$scope_id" --file - --merge >/dev/null
fi

mapper_payload="$({
  jq -cn \
    --arg audience "$audience" \
    '{
      name: "mcp-resource-audience",
      protocol: "openid-connect",
      protocolMapper: "oidc-audience-mapper",
      consentRequired: false,
      config: {
        "included.custom.audience": $audience,
        "access.token.claim": "true",
        "id.token.claim": "false",
        "introspection.token.claim": "true",
        "lightweight.claim": "false"
      }
    }'
})"

mapper_id="$(
  kcadm get "client-scopes/$scope_id/protocol-mappers/models" \
    | jq -er \
      '[.[] | select(.name == "mcp-resource-audience")] | if length == 0 then "" else .[0].id end'
)"

if [[ -z "$mapper_id" ]]; then
  printf '%s' "$mapper_payload" \
    | kcadm create "client-scopes/$scope_id/protocol-mappers/models" --file - >/dev/null
  mapper_id="$(
    kcadm get "client-scopes/$scope_id/protocol-mappers/models" \
      | jq -er \
        '[.[] | select(.name == "mcp-resource-audience")] | if length == 1 then .[0].id else error("mapper lookup failed") end'
  )"
else
  printf '%s' "$mapper_payload" \
    | jq -c --arg id "$mapper_id" '. + {id: $id}' \
    | kcadm update \
      "client-scopes/$scope_id/protocol-mappers/models/$mapper_id" \
      --file - >/dev/null
fi

clients_json="$(kcadm get clients --query "clientId=$client_id" --fields id,clientId)"
client_uuid="$(
  jq -er --arg client_id "$client_id" \
    '[.[] | select(.clientId == $client_id)] | if length == 1 then .[0].id else error("client lookup failed") end' \
    <<<"$clients_json"
)"

kcadm update "clients/$client_uuid" \
  --set 'publicClient=true' \
  --set 'standardFlowEnabled=true' \
  --set 'implicitFlowEnabled=false' \
  --set 'serviceAccountsEnabled=false' \
  --set "directAccessGrantsEnabled=$direct_access_grants" \
  --set 'attributes."pkce.code.challenge.method"=S256' \
  --set "redirectUris=$redirect_uris_json" \
  --set "webOrigins=$web_origins_json" >/dev/null

optional_scope_ids="$(kcadm get "clients/$client_uuid/optional-client-scopes" --fields id,name)"
if ! jq -e --arg id "$scope_id" 'any(.[]; .id == $id)' \
  <<<"$optional_scope_ids" >/dev/null; then
  kcadm update "clients/$client_uuid/optional-client-scopes/$scope_id" \
    --no-merge >/dev/null
fi

old_mapper_ids="$(
  kcadm get "clients/$client_uuid/protocol-mappers/models" \
    | jq -r \
      '.[]
       | select(
           .protocolMapper == "oidc-audience-mapper"
           and .config["included.client.audience"] == "ozonofk-mcp"
         )
       | .id'
)"
while IFS= read -r old_mapper_id; do
  if [[ -n "$old_mapper_id" ]]; then
    kcadm delete \
      "clients/$client_uuid/protocol-mappers/models/$old_mapper_id" >/dev/null
  fi
done <<<"$old_mapper_ids"

scope_check="$(
  kcadm get "client-scopes/$scope_id" \
    | jq -e --arg name "$scope_name" '.name == $name and .protocol == "openid-connect"'
)"
mapper_check="$(
  kcadm get "client-scopes/$scope_id/protocol-mappers/models/$mapper_id"
)"
client_check="$(kcadm get "clients/$client_uuid")"
optional_scope_check="$(kcadm get "clients/$client_uuid/optional-client-scopes" --fields id,name)"
client_mappers_check="$(kcadm get "clients/$client_uuid/protocol-mappers/models")"

jq -e --arg audience "$audience" \
  '.name == "mcp-resource-audience"
   and .protocolMapper == "oidc-audience-mapper"
   and .config["included.custom.audience"] == $audience
   and .config["access.token.claim"] == "true"' \
  <<<"$mapper_check" >/dev/null
jq -e \
  --arg redirect_uri "$redirect_uri" \
  --arg web_origin "$web_origin" \
  --argjson direct_access_grants "$direct_access_grants" \
  '.publicClient == true
   and .standardFlowEnabled == true
   and .implicitFlowEnabled == false
   and .serviceAccountsEnabled == false
   and .directAccessGrantsEnabled == $direct_access_grants
   and .attributes["pkce.code.challenge.method"] == "S256"
   and .redirectUris == [$redirect_uri]
   and .webOrigins == [$web_origin]' \
  <<<"$client_check" >/dev/null
jq -e --arg id "$scope_id" 'any(.[]; .id == $id)' \
  <<<"$optional_scope_check" >/dev/null
jq -e \
  'all(.[];
     .protocolMapper != "oidc-audience-mapper"
     or .config["included.client.audience"] != "ozonofk-mcp"
   )' <<<"$client_mappers_check" >/dev/null
[[ "$scope_check" == "true" ]]

echo "Keycloak configuration synchronized: client=$client_id, scope=$scope_name, audience=$audience, redirect=$redirect_uri, PKCE=S256"
