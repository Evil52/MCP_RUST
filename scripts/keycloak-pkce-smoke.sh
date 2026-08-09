#!/usr/bin/env bash

set -euo pipefail
set +x
umask 077

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$project_dir/.keycloak.env"
compose_file="$project_dir/compose.auth.yaml"
keycloak_url="http://localhost:8180"
issuer="$keycloak_url/realms/ofk"
authorization_endpoint="$issuer/protocol/openid-connect/auth"
token_endpoint="$issuer/protocol/openid-connect/token"
revocation_endpoint="$issuer/protocol/openid-connect/revoke"
jwks_uri="$issuer/protocol/openid-connect/certs"
mcp_url="http://127.0.0.1:8788/mcp"
resource_url="http://localhost:8788/mcp"
metadata_url="http://127.0.0.1:8788/.well-known/oauth-protected-resource"
resource_metadata_url="http://localhost:8788/.well-known/oauth-protected-resource"
callback_url="http://localhost:18789/callback"
client_id="ozonofk-mcp"
required_scope="mcp:tools"
full_scope="openid profile email $required_scope"
kcadm_config="/tmp/mcp-ozon-kcadm-pkce-$$.config"
actor_id="keycloak_e2e_manager"
username="keycloak-e2e-manager"

for dependency in curl docker jq python3; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

safe_curl() {
  command curl --disable --noproxy '*' "$@"
}

if [[ -L "$env_file" || ! -f "$env_file" || ! -r "$env_file" ]]; then
  echo "Missing $env_file; run ./scripts/keycloak-init.sh" >&2
  exit 1
fi

for identity_value in "$actor_id" "$username"; do
  if [[ ! "$identity_value" =~ ^[A-Za-z0-9._@-]{1,128}$ ]]; then
    echo "Admin actor id and OIDC username must use safe identifier characters" >&2
    exit 1
  fi
done

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

smoke_dir="$(mktemp -d "${TMPDIR:-/tmp}/mcp-ozon-keycloak-pkce.XXXXXX")"
chmod 700 "$smoke_dir"
owns_test_user=false
user_id=""
session_id=""

cleanup() {
  terminate_mcp_session >/dev/null 2>&1 || true
  delete_test_user >/dev/null 2>&1 || true
  "${compose[@]}" exec -T keycloak rm -f "$kcadm_config" >/dev/null 2>&1 || true
  rm -rf "$smoke_dir"
}
trap cleanup EXIT

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
    "$issuer/.well-known/openid-configuration" >/dev/null 2>&1 \
    && safe_curl --fail --silent --show-error \
      "http://127.0.0.1:8788/health" >/dev/null 2>&1
  then
    break
  fi
  sleep 1
done

discovery_file="$smoke_dir/discovery.json"
safe_curl --fail --silent --show-error \
  --output "$discovery_file" \
  "$issuer/.well-known/openid-configuration"
chmod 600 "$discovery_file"

jq -e \
  --arg issuer "$issuer" \
  --arg authorization_endpoint "$authorization_endpoint" \
  --arg token_endpoint "$token_endpoint" \
  --arg revocation_endpoint "$revocation_endpoint" \
  --arg jwks_uri "$jwks_uri" \
  --arg scope "$required_scope" \
  '.issuer == $issuer
   and .authorization_endpoint == $authorization_endpoint
   and .token_endpoint == $token_endpoint
   and .revocation_endpoint == $revocation_endpoint
   and .jwks_uri == $jwks_uri
   and (.grant_types_supported | index("authorization_code")) != null
   and (.code_challenge_methods_supported | index("S256")) != null
   and (.scopes_supported | index($scope)) != null' \
  "$discovery_file" >/dev/null

safe_curl --fail --silent --show-error "$metadata_url" \
  | jq -e \
    --arg resource "$resource_url" \
    --arg issuer "$issuer" \
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

create_flow_material() {
  local prefix="$1"
  python3 - \
    "$smoke_dir/$prefix.verifier" \
    "$smoke_dir/$prefix.challenge" \
    "$smoke_dir/$prefix.state" \
    "$smoke_dir/$prefix.nonce" <<'PY'
import base64
import hashlib
import secrets
import sys

verifier_path, challenge_path, state_path, nonce_path = sys.argv[1:]
verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
state = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")
nonce = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")

if not 43 <= len(verifier) <= 128 or len(challenge) != 43 or "=" in challenge:
    raise SystemExit("invalid generated PKCE material")

for path, value in (
    (verifier_path, verifier),
    (challenge_path, challenge),
    (state_path, state),
    (nonce_path, nonce),
):
    with open(path, "x", encoding="ascii") as handle:
        handle.write(value)
PY
  chmod 600 \
    "$smoke_dir/$prefix.verifier" \
    "$smoke_dir/$prefix.challenge" \
    "$smoke_dir/$prefix.state" \
    "$smoke_dir/$prefix.nonce"
}

assert_authorization_rejected() {
  local prefix="$1"
  local pkce_mode="$2"
  local redirect_uri="$3"
  local allow_error_redirect="$4"
  local response_headers="$smoke_dir/$prefix.authorization.headers"
  local response_body="$smoke_dir/$prefix.authorization.body"
  local response_status
  local pkce_args=(--data-urlencode "resource=$resource_url")

  create_flow_material "$prefix"
  case "$pkce_mode" in
    none) ;;
    plain)
      pkce_args+=(
        --data-urlencode "code_challenge@$smoke_dir/$prefix.challenge"
        --data-urlencode 'code_challenge_method=plain'
      )
      ;;
    s256)
      pkce_args+=(
        --data-urlencode "code_challenge@$smoke_dir/$prefix.challenge"
        --data-urlencode 'code_challenge_method=S256'
      )
      ;;
    *)
      echo "Unknown PKCE negative-test mode" >&2
      exit 1
      ;;
  esac

  response_status="$(
    safe_curl --silent --show-error \
      --get \
      --output "$response_body" \
      --dump-header "$response_headers" \
      --write-out '%{http_code}' \
      --data-urlencode "client_id=$client_id" \
      --data-urlencode 'response_type=code' \
      --data-urlencode "redirect_uri=$redirect_uri" \
      --data-urlencode "scope=$full_scope" \
      --data-urlencode "state@$smoke_dir/$prefix.state" \
      --data-urlencode "nonce@$smoke_dir/$prefix.nonce" \
      "${pkce_args[@]}" \
      "$authorization_endpoint"
  )"
  chmod 600 "$response_headers" "$response_body"

  if [[ "$response_status" == "400" ]]; then
    if grep -qi '^location:' "$response_headers"; then
      echo "Rejected authorization request unexpectedly contained a redirect" >&2
      exit 1
    fi
    return
  fi
  if [[ "$response_status" != "302" && "$response_status" != "303" ]]; then
    echo "Invalid authorization request was not rejected" >&2
    exit 1
  fi
  if [[ "$allow_error_redirect" != "true" ]]; then
    echo "Keycloak redirected to an unregistered OAuth callback" >&2
    exit 1
  fi

  python3 - \
    "$response_headers" \
    "$smoke_dir/$prefix.state" \
    "$callback_url" <<'PY'
import hmac
import sys
from urllib.parse import parse_qsl, urlsplit

headers_path, state_path, callback_url = sys.argv[1:]
locations = []
with open(headers_path, encoding="iso-8859-1") as handle:
    for line in handle:
        name, separator, value = line.partition(":")
        if separator and name.lower() == "location":
            locations.append(value.strip())
if len(locations) != 1:
    raise SystemExit("expected one OAuth error redirect")
actual = urlsplit(locations[0])
expected = urlsplit(callback_url)
if (
    actual.scheme != expected.scheme
    or actual.hostname != expected.hostname
    or actual.port != expected.port
    or actual.path != expected.path
    or actual.username is not None
    or actual.password is not None
    or actual.fragment
):
    raise SystemExit("OAuth error redirect did not match the registered callback")
params = {}
for name, value in parse_qsl(actual.query, keep_blank_values=True):
    params.setdefault(name, []).append(value)
if params.get("error") != ["invalid_request"] or "code" in params:
    raise SystemExit("invalid authorization request did not return invalid_request")
for forbidden in ("access_token", "refresh_token", "id_token"):
    if forbidden in params:
        raise SystemExit("OAuth error redirect contained a token")
with open(state_path, encoding="ascii") as handle:
    expected_state = handle.read()
if len(params.get("state", [])) != 1 or not hmac.compare_digest(params["state"][0], expected_state):
    raise SystemExit("OAuth error redirect failed state validation")
PY
}

assert_authorization_rejected no-challenge none "$callback_url" true
assert_authorization_rejected plain-challenge plain "$callback_url" true
assert_authorization_rejected unregistered-redirect s256 \
  'http://localhost:18789/wrong' false

obtain_code() {
  local prefix="$1"
  local requested_scope="$2"
  local cookie_file="$smoke_dir/$prefix.cookies"
  local login_page="$smoke_dir/$prefix.login.html"
  local login_action_file="$smoke_dir/$prefix.login-action"
  local hidden_inputs_file="$smoke_dir/$prefix.hidden.json"
  local login_body_file="$smoke_dir/$prefix.login.form"
  local callback_headers="$smoke_dir/$prefix.callback.headers"
  local callback_body="$smoke_dir/$prefix.callback.body"
  local authorize_status
  local login_status
  local login_action

  create_flow_material "$prefix"
  : >"$cookie_file"
  chmod 600 "$cookie_file"

  authorize_status="$(
    safe_curl --silent --show-error \
      --get \
      --cookie "$cookie_file" \
      --cookie-jar "$cookie_file" \
      --output "$login_page" \
      --write-out '%{http_code}' \
      --data-urlencode "client_id=$client_id" \
      --data-urlencode 'response_type=code' \
      --data-urlencode "redirect_uri=$callback_url" \
      --data-urlencode "scope=$requested_scope" \
      --data-urlencode "state@$smoke_dir/$prefix.state" \
      --data-urlencode "nonce@$smoke_dir/$prefix.nonce" \
      --data-urlencode "code_challenge@$smoke_dir/$prefix.challenge" \
      --data-urlencode 'code_challenge_method=S256' \
      --data-urlencode "resource=$resource_url" \
      "$authorization_endpoint"
  )"
  if [[ "$authorize_status" != "200" ]]; then
    echo "Authorization endpoint did not return the login form" >&2
    exit 1
  fi
  chmod 600 "$login_page"

  python3 - \
    "$login_page" \
    "$login_action_file" \
    "$hidden_inputs_file" \
    "$keycloak_url" <<'PY'
import json
import sys
from html.parser import HTMLParser
from urllib.parse import urlsplit


class LoginFormParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.forms = []
        self.current = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form" and attrs.get("id") == "kc-form-login":
            self.current = {"action": attrs.get("action"), "inputs": []}
            self.forms.append(self.current)
        elif tag == "input" and self.current is not None:
            name = attrs.get("name")
            if name:
                self.current["inputs"].append((name, attrs.get("value", "")))

    def handle_endtag(self, tag):
        if tag == "form" and self.current is not None:
            self.current = None


page_path, action_path, inputs_path, expected_origin = sys.argv[1:]
parser = LoginFormParser()
with open(page_path, encoding="utf-8", errors="strict") as handle:
    parser.feed(handle.read())
if len(parser.forms) != 1 or not parser.forms[0]["action"]:
    raise SystemExit("expected exactly one Keycloak login form")

action = parser.forms[0]["action"]
parsed = urlsplit(action)
expected = urlsplit(expected_origin)
if (
    parsed.scheme != expected.scheme
    or parsed.hostname != expected.hostname
    or parsed.port != expected.port
    or parsed.username is not None
    or parsed.password is not None
    or parsed.path != "/realms/ofk/login-actions/authenticate"
    or parsed.fragment
):
    raise SystemExit("Keycloak login form action was outside the expected origin")

with open(action_path, "x", encoding="utf-8") as handle:
    handle.write(action)
with open(inputs_path, "x", encoding="utf-8") as handle:
    json.dump(parser.forms[0]["inputs"], handle)
PY
  chmod 600 "$login_action_file" "$hidden_inputs_file"

  printf '%s' "$KEYCLOAK_TEST_USER_PASSWORD" \
    | python3 -c '
import json
import sys
from urllib.parse import urlencode

inputs_path, username = sys.argv[1:]
password = sys.stdin.read()
with open(inputs_path, encoding="utf-8") as handle:
    fields = [(name, value) for name, value in json.load(handle)
              if name not in {"username", "password", "credentialId"}]
fields.extend((("username", username), ("password", password), ("credentialId", "")))
sys.stdout.write(urlencode(fields))
' "$hidden_inputs_file" "$username" >"$login_body_file"
  chmod 600 "$login_body_file"

  login_action="$(<"$login_action_file")"
  login_status="$(
    safe_curl --silent --show-error \
      --request POST \
      --cookie "$cookie_file" \
      --cookie-jar "$cookie_file" \
      --header 'Content-Type: application/x-www-form-urlencoded' \
      --data-binary "@$login_body_file" \
      --dump-header "$callback_headers" \
      --output "$callback_body" \
      --write-out '%{http_code}' \
      "$login_action"
  )"
  chmod 600 "$callback_headers" "$callback_body"
  if [[ "$login_status" != "302" && "$login_status" != "303" ]]; then
    echo "Keycloak login did not return an OAuth callback redirect" >&2
    exit 1
  fi

  python3 - \
    "$callback_headers" \
    "$smoke_dir/$prefix.state" \
    "$callback_url" \
    "$smoke_dir/$prefix.code" <<'PY'
import hmac
import sys
from urllib.parse import parse_qsl, urlsplit

headers_path, state_path, callback_url, code_path = sys.argv[1:]
locations = []
with open(headers_path, encoding="iso-8859-1") as handle:
    for line in handle:
        name, separator, value = line.partition(":")
        if separator and name.lower() == "location":
            locations.append(value.strip())
if len(locations) != 1:
    raise SystemExit("expected exactly one OAuth callback Location")

actual = urlsplit(locations[0])
expected = urlsplit(callback_url)
if (
    actual.scheme != expected.scheme
    or actual.hostname != expected.hostname
    or actual.port != expected.port
    or actual.path != expected.path
    or actual.username is not None
    or actual.password is not None
    or actual.fragment
):
    raise SystemExit("OAuth callback did not match the exact redirect URI")

params = {}
for name, value in parse_qsl(actual.query, keep_blank_values=True):
    params.setdefault(name, []).append(value)
for forbidden in ("access_token", "refresh_token", "id_token", "error"):
    if forbidden in params:
        raise SystemExit("OAuth callback contained a forbidden parameter")
if len(params.get("state", [])) != 1 or len(params.get("code", [])) != 1:
    raise SystemExit("OAuth callback must contain one state and one code")
with open(state_path, encoding="ascii") as handle:
    expected_state = handle.read()
if not hmac.compare_digest(params["state"][0], expected_state):
    raise SystemExit("OAuth state validation failed")
with open(code_path, "x", encoding="ascii") as handle:
    handle.write(params["code"][0])
PY
  chmod 600 "$smoke_dir/$prefix.code"
}

exchange_code() {
  local code_file="$1"
  local verifier_file="$2"
  local redirect_uri="$3"
  local response_file="$4"
  shift 4
  safe_curl --silent --show-error \
    --request POST \
    --output "$response_file" \
    --write-out '%{http_code}' \
    --data-urlencode 'grant_type=authorization_code' \
    --data-urlencode "client_id=$client_id" \
    --data-urlencode "redirect_uri=$redirect_uri" \
    --data-urlencode "code@$code_file" \
    --data-urlencode "code_verifier@$verifier_file" \
    --data-urlencode "resource=$resource_url" \
    "$@" \
    "$token_endpoint"
}

assert_invalid_grant() {
  local status="$1"
  local response_file="$2"
  local scenario="$3"
  local oauth_error
  if [[ "$status" != "400" ]] \
    || ! jq -e '.error == "invalid_grant"' "$response_file" >/dev/null; then
    oauth_error="$(jq -r '.error // "unknown_error"' "$response_file" 2>/dev/null || printf 'invalid_json')"
    echo "Expected $scenario to fail with invalid_grant; HTTP $status, OAuth error=$oauth_error" >&2
    exit 1
  fi
}

wrong_verifier_file="$smoke_dir/wrong-verifier"
python3 - "$wrong_verifier_file" <<'PY'
import base64
import secrets
import sys

value = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
with open(sys.argv[1], "x", encoding="ascii") as handle:
    handle.write(value)
PY
chmod 600 "$wrong_verifier_file"

obtain_code wrong-verifier "$full_scope"
wrong_verifier_response="$smoke_dir/wrong-verifier.response.json"
wrong_verifier_status="$(exchange_code \
  "$smoke_dir/wrong-verifier.code" \
  "$wrong_verifier_file" \
  "$callback_url" \
  "$wrong_verifier_response")"
chmod 600 "$wrong_verifier_response"
assert_invalid_grant "$wrong_verifier_status" "$wrong_verifier_response" 'wrong PKCE verifier'

obtain_code wrong-redirect "$full_scope"
wrong_redirect_response="$smoke_dir/wrong-redirect.response.json"
wrong_redirect_status="$(exchange_code \
  "$smoke_dir/wrong-redirect.code" \
  "$smoke_dir/wrong-redirect.verifier" \
  "http://localhost:18789/wrong" \
  "$wrong_redirect_response")"
chmod 600 "$wrong_redirect_response"
assert_invalid_grant "$wrong_redirect_status" "$wrong_redirect_response" 'wrong redirect URI'

obtain_code missing-scope 'openid profile email'
missing_scope_response="$smoke_dir/missing-scope.response.json"
missing_scope_status="$(exchange_code \
  "$smoke_dir/missing-scope.code" \
  "$smoke_dir/missing-scope.verifier" \
  "$callback_url" \
  "$missing_scope_response")"
chmod 600 "$missing_scope_response"
if [[ "$missing_scope_status" != "200" ]]; then
  echo "Authorization code exchange without the MCP scope failed unexpectedly" >&2
  exit 1
fi
jq --exit-status --raw-output --join-output \
  '.access_token' "$missing_scope_response" >"$smoke_dir/missing-scope.access-token"
printf 'Authorization: Bearer ' >"$smoke_dir/missing-scope.authorization"
cat "$smoke_dir/missing-scope.access-token" >>"$smoke_dir/missing-scope.authorization"
printf '\n' >>"$smoke_dir/missing-scope.authorization"
chmod 600 "$smoke_dir/missing-scope.access-token" "$smoke_dir/missing-scope.authorization"

obtain_code replay "$full_scope"
replay_first_response="$smoke_dir/replay-first.response.json"
replay_first_status="$(exchange_code \
  "$smoke_dir/replay.code" \
  "$smoke_dir/replay.verifier" \
  "$callback_url" \
  "$replay_first_response")"
chmod 600 "$replay_first_response"
if [[ "$replay_first_status" != "200" ]]; then
  echo "Initial authorization code exchange for the replay test failed" >&2
  exit 1
fi
replay_response="$smoke_dir/replay.response.json"
replay_status="$(exchange_code \
  "$smoke_dir/replay.code" \
  "$smoke_dir/replay.verifier" \
  "$callback_url" \
  "$replay_response")"
chmod 600 "$replay_response"
assert_invalid_grant "$replay_status" "$replay_response" 'authorization code replay'

obtain_code success "$full_scope"
token_response="$smoke_dir/token.response.json"
token_status="$(exchange_code \
  "$smoke_dir/success.code" \
  "$smoke_dir/success.verifier" \
  "$callback_url" \
  "$token_response")"
chmod 600 "$token_response"
if [[ "$token_status" != "200" ]]; then
  echo "Authorization code exchange failed" >&2
  exit 1
fi
jq -e \
  --arg scope "$required_scope" \
  '.token_type == "Bearer"
   and (.expires_in | type == "number" and . > 0)
   and (.access_token | type == "string" and length > 0)
   and (.id_token | type == "string" and length > 0)
   and (.refresh_token | type == "string" and length > 0)
   and ((.scope / " ") | index($scope)) != null' \
  "$token_response" >/dev/null

jq --exit-status --raw-output --join-output '.access_token' \
  "$token_response" >"$smoke_dir/access-token"
jq --exit-status --raw-output --join-output '.id_token' \
  "$token_response" >"$smoke_dir/id-token"
jq --exit-status --raw-output --join-output '.refresh_token' \
  "$token_response" >"$smoke_dir/refresh-token"
chmod 600 "$smoke_dir/access-token" "$smoke_dir/id-token" "$smoke_dir/refresh-token"

jwks_file="$smoke_dir/jwks.json"
safe_curl --fail --silent --show-error --output "$jwks_file" "$jwks_uri"
chmod 600 "$jwks_file"

validate_access_token() {
  local token_file="$1"
  local subject_file="$2"
  python3 - \
    "$token_file" \
    "$jwks_file" \
    "$issuer" \
    "$resource_url" \
    "$client_id" \
    "$required_scope" \
    "$username" \
    "$subject_file" <<'PY'
import base64
import hashlib
import hmac
import json
import sys
import time


def decode_segment(value):
    padding = "=" * (-len(value) % 4)
    return json.loads(base64.urlsafe_b64decode(value + padding))


def decode_bytes(value):
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def verify_rs256(parts, key):
    modulus = int.from_bytes(decode_bytes(key["n"]), "big")
    exponent = int.from_bytes(decode_bytes(key["e"]), "big")
    signature = int.from_bytes(decode_bytes(parts[2]), "big")
    size = (modulus.bit_length() + 7) // 8
    if signature >= modulus:
        raise SystemExit("JWT signature was outside the RSA modulus")
    encoded = pow(signature, exponent, modulus).to_bytes(size, "big")
    digest = hashlib.sha256(f"{parts[0]}.{parts[1]}".encode("ascii")).digest()
    digest_info = bytes.fromhex("3031300d060960864801650304020105000420") + digest
    padding_length = size - len(digest_info) - 3
    if padding_length < 8:
        raise SystemExit("JWKS RSA key was too small")
    expected = b"\x00\x01" + (b"\xff" * padding_length) + b"\x00" + digest_info
    if not hmac.compare_digest(encoded, expected):
        raise SystemExit("JWT RS256 signature validation failed")


token_path, jwks_path, issuer, resource, client_id, scope, username, subject_path = sys.argv[1:]
with open(token_path, encoding="ascii") as handle:
    token = handle.read().strip()
parts = token.split(".")
if len(parts) != 3:
    raise SystemExit("access token was not a signed JWT")
header = decode_segment(parts[0])
claims = decode_segment(parts[1])
with open(jwks_path, encoding="utf-8") as handle:
    jwks = json.load(handle)
kid = header.get("kid")
if header.get("alg") != "RS256" or not isinstance(kid, str) or not kid:
    raise SystemExit("access token did not use RS256 with a kid")
keys = [key for key in jwks.get("keys", []) if key.get("kid") == kid and key.get("kty") == "RSA"]
if len(keys) != 1:
    raise SystemExit("access token kid was absent from JWKS")
verify_rs256(parts, keys[0])
audiences = claims.get("aud")
if isinstance(audiences, str):
    audiences = [audiences]
now = int(time.time())
if (
    claims.get("iss") != issuer
    or not isinstance(audiences, list)
    or resource not in audiences
    or scope not in str(claims.get("scope", "")).split()
    or claims.get("azp") != client_id
    or claims.get("preferred_username") != username
    or not isinstance(claims.get("sub"), str)
    or not claims["sub"]
    or not isinstance(claims.get("exp"), int)
    or claims["exp"] <= now
    or not isinstance(claims.get("iat"), int)
    or claims["iat"] > now + 60
    or ("nbf" in claims and (not isinstance(claims["nbf"], int) or claims["nbf"] > now + 60))
):
    raise SystemExit("access token claims did not match the protected resource")
with open(subject_path, "w", encoding="utf-8") as handle:
    handle.write(claims["sub"])
PY
  chmod 600 "$subject_file"
}

validate_id_token() {
  python3 - \
    "$smoke_dir/id-token" \
    "$jwks_file" \
    "$issuer" \
    "$client_id" \
    "$smoke_dir/success.nonce" \
    "$smoke_dir/access-subject" <<'PY'
import base64
import hashlib
import hmac
import json
import sys
import time


def decode_segment(value):
    padding = "=" * (-len(value) % 4)
    return json.loads(base64.urlsafe_b64decode(value + padding))


def decode_bytes(value):
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def verify_rs256(parts, key):
    modulus = int.from_bytes(decode_bytes(key["n"]), "big")
    exponent = int.from_bytes(decode_bytes(key["e"]), "big")
    signature = int.from_bytes(decode_bytes(parts[2]), "big")
    size = (modulus.bit_length() + 7) // 8
    if signature >= modulus:
        raise SystemExit("ID token signature was outside the RSA modulus")
    encoded = pow(signature, exponent, modulus).to_bytes(size, "big")
    digest = hashlib.sha256(f"{parts[0]}.{parts[1]}".encode("ascii")).digest()
    digest_info = bytes.fromhex("3031300d060960864801650304020105000420") + digest
    padding_length = size - len(digest_info) - 3
    if padding_length < 8:
        raise SystemExit("JWKS RSA key was too small")
    expected = b"\x00\x01" + (b"\xff" * padding_length) + b"\x00" + digest_info
    if not hmac.compare_digest(encoded, expected):
        raise SystemExit("ID token RS256 signature validation failed")


token_path, jwks_path, issuer, client_id, nonce_path, subject_path = sys.argv[1:]
with open(token_path, encoding="ascii") as handle:
    token = handle.read().strip()
parts = token.split(".")
if len(parts) != 3:
    raise SystemExit("ID token was not a signed JWT")
header = decode_segment(parts[0])
claims = decode_segment(parts[1])
with open(jwks_path, encoding="utf-8") as handle:
    jwks = json.load(handle)
kid = header.get("kid")
if header.get("alg") != "RS256" or not isinstance(kid, str) or not kid:
    raise SystemExit("ID token did not use RS256 with a kid")
keys = [key for key in jwks.get("keys", []) if key.get("kid") == kid and key.get("kty") == "RSA"]
if len(keys) != 1:
    raise SystemExit("ID token kid was absent from JWKS")
verify_rs256(parts, keys[0])
audiences = claims.get("aud")
if isinstance(audiences, str):
    audiences = [audiences]
with open(nonce_path, encoding="ascii") as handle:
    expected_nonce = handle.read()
with open(subject_path, encoding="utf-8") as handle:
    expected_subject = handle.read()
now = int(time.time())
if (
    claims.get("iss") != issuer
    or not isinstance(audiences, list)
    or client_id not in audiences
    or claims.get("sub") != expected_subject
    or not isinstance(claims.get("nonce"), str)
    or not hmac.compare_digest(claims["nonce"], expected_nonce)
    or not isinstance(claims.get("exp"), int)
    or claims["exp"] <= now
    or not isinstance(claims.get("iat"), int)
    or claims["iat"] > now + 60
):
    raise SystemExit("ID token claims did not match the authorization request")
PY
}

validate_access_token "$smoke_dir/access-token" "$smoke_dir/access-subject"
validate_id_token

make_authorization_header() {
  local token_file="$1"
  local header_file="$2"
  printf 'Authorization: Bearer ' >"$header_file"
  cat "$token_file" >>"$header_file"
  printf '\n' >>"$header_file"
  chmod 600 "$header_file"
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

mcp_request() {
  local body="$1"
  local response_file="$2"
  local headers_file="$3"
  local header_file="${4:-}"
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
  safe_curl "${curl_args[@]}" \
    --dump-header "$headers_file" \
    --output "$response_file" \
    --write-out '%{http_code}' \
    --data "$body" \
    "$mcp_url"
  chmod 600 "$headers_file" "$response_file"
}

mcp_initialize_public() {
  local response_headers="$smoke_dir/public.initialize.headers"
  local response_body="$smoke_dir/public.initialize.body"
  local response_status

  response_status="$(
    safe_curl --silent --show-error \
      --request POST \
      --header 'Accept: application/json, text/event-stream' \
      --header 'Content-Type: application/json' \
      --dump-header "$response_headers" \
      --output "$response_body" \
      --write-out '%{http_code}' \
      --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"keycloak-pkce-smoke","version":"1.0"}}}' \
      "$mcp_url"
  )"
  chmod 600 "$response_headers" "$response_body"
  if [[ "$response_status" != "200" ]]; then
    echo "Expected public MCP initialize to return HTTP 200, got $response_status" >&2
    exit 1
  fi
  json_response "$response_body" \
    | jq -e '.id == 1 and .result.serverInfo.name != null' >/dev/null
  session_id="$(awk 'tolower($1) == "mcp-session-id:" { gsub("\r", "", $2); print $2; exit }' "$response_headers")"
  if (( ${#session_id} < 1 || ${#session_id} > 256 )) \
    || [[ ! "$session_id" =~ ^[-A-Za-z0-9._~]+$ ]]; then
    echo "MCP initialize response did not contain a safe mcp-session-id" >&2
    exit 1
  fi
}

mcp_notify_initialized() {
  local response_headers="$smoke_dir/public.notification.headers"
  local response_body="$smoke_dir/public.notification.body"
  local response_status

  response_status="$(mcp_request \
    '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
    "$response_body" \
    "$response_headers")"
  case "$response_status" in
    200 | 202 | 204) ;;
    *)
      echo "Public MCP initialized notification failed with HTTP $response_status" >&2
      exit 1
      ;;
  esac
}

mcp_list_tools_public() {
  local response_headers="$smoke_dir/public.tools-list.headers"
  local response_body="$smoke_dir/public.tools-list.body"
  local stale_response_headers="$smoke_dir/stale.tools-list.headers"
  local stale_response_body="$smoke_dir/stale.tools-list.body"
  local response_status
  local stale_response_status

  response_status="$(mcp_request \
    '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
    "$response_body" \
    "$response_headers")"
  if [[ "$response_status" != "200" ]]; then
    echo "Expected public tools/list to return HTTP 200, got $response_status" >&2
    exit 1
  fi
  json_response "$response_body" \
    | jq -e '.id == 2 and (.result.tools | any(.name == "list_members"))' >/dev/null

  stale_response_status="$(mcp_request \
    '{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}' \
    "$stale_response_body" \
    "$stale_response_headers" \
    "$invalid_token_header")"
  if [[ "$stale_response_status" != "200" ]]; then
    echo "Expected tools/list with a stale Authorization header to return HTTP 200, got $stale_response_status" >&2
    exit 1
  fi
  json_response "$stale_response_body" \
    | jq -e '.id == 3 and (.result.tools | any(.name == "list_members"))' >/dev/null
}

assert_call_auth_error() {
  local scenario="$1"
  local request_id="$2"
  local expected_error="$3"
  local header_file="${4:-}"
  local response_headers="$smoke_dir/$scenario.mcp.headers"
  local response_body="$smoke_dir/$scenario.mcp.body"
  local response_status
  local response_json
  local request_body

  request_body="$(jq -cn --argjson id "$request_id" \
    '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:"list_members", arguments:{}}}')"
  response_status="$(mcp_request \
    "$request_body" \
    "$response_body" \
    "$response_headers" \
    "$header_file")"
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

assert_valid_members_call() {
  local scenario="$1"
  local request_id="$2"
  local header_file="$3"
  local response_headers="$smoke_dir/$scenario.mcp.headers"
  local response_body="$smoke_dir/$scenario.mcp.body"
  local response_status
  local request_body
  local response_json

  request_body="$(jq -cn --argjson id "$request_id" \
    '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:"list_members", arguments:{}}}')"
  response_status="$(mcp_request \
    "$request_body" \
    "$response_body" \
    "$response_headers" \
    "$header_file")"
  if [[ "$response_status" != "200" ]]; then
    echo "Expected $scenario tools/call to return HTTP 200, got $response_status" >&2
    exit 1
  fi
  response_json="$(json_response "$response_body")"
  jq -e --argjson expected_id "$request_id" --arg actor "$actor_id" '
    .id == $expected_id
    and .result.isError != true
    and (.result.content[0].text | fromjson | .actor.id == $actor)
    and (.result.content[0].text | fromjson | .actor.role == "manager")' \
    <<<"$response_json" >/dev/null
}

access_token_header="$smoke_dir/access-token.authorization"
invalid_token_header="$smoke_dir/invalid-token.authorization"
refresh_token_header="$smoke_dir/refresh-token.authorization"
make_authorization_header "$smoke_dir/access-token" "$access_token_header"
make_authorization_header "$smoke_dir/refresh-token" "$refresh_token_header"
printf '%s\n' 'Authorization: Bearer not-a-jwt' >"$invalid_token_header"
chmod 600 "$invalid_token_header"

mcp_initialize_public
mcp_notify_initialized
mcp_list_tools_public
assert_call_auth_error missing-credentials 4 invalid_token
assert_call_auth_error invalid-token 5 invalid_token "$invalid_token_header"
# The local Keycloak workaround binds the resource audience to the optional mcp:tools scope.
# Omitting that scope therefore also omits the required audience and is correctly invalid_token;
# deterministic Rust/wire tests cover valid-audience tokens that lack only the required scope.
assert_call_auth_error missing-scope 6 invalid_token "$smoke_dir/missing-scope.authorization"
assert_valid_members_call access-token 7 "$access_token_header"
assert_call_auth_error refresh-token-as-bearer 8 invalid_token "$refresh_token_header"

refresh_response="$smoke_dir/refresh.response.json"
refresh_status="$(
  safe_curl --silent --show-error \
    --request POST \
    --output "$refresh_response" \
    --write-out '%{http_code}' \
    --data-urlencode 'grant_type=refresh_token' \
    --data-urlencode "client_id=$client_id" \
    --data-urlencode "refresh_token@$smoke_dir/refresh-token" \
    --data-urlencode "resource=$resource_url" \
    "$token_endpoint"
)"
chmod 600 "$refresh_response"
if [[ "$refresh_status" != "200" ]]; then
  refresh_error="$(jq -r '.error // "unknown_error"' "$refresh_response" 2>/dev/null || printf 'invalid_json')"
  echo "Refresh token exchange failed: HTTP $refresh_status, OAuth error=$refresh_error" >&2
  exit 1
fi
jq -e \
  --arg scope "$required_scope" \
  '.token_type == "Bearer"
   and (.access_token | type == "string" and length > 0)
   and (.refresh_token | type == "string" and length > 0)
   and ((.scope / " ") | index($scope)) != null' \
  "$refresh_response" >/dev/null
jq --exit-status --raw-output --join-output '.access_token' \
  "$refresh_response" >"$smoke_dir/refreshed-access-token"
jq --exit-status --raw-output --join-output '.refresh_token' \
  "$refresh_response" >"$smoke_dir/refreshed-refresh-token"
chmod 600 "$smoke_dir/refreshed-access-token" "$smoke_dir/refreshed-refresh-token"
validate_access_token "$smoke_dir/refreshed-access-token" "$smoke_dir/refreshed-subject"
cmp -s "$smoke_dir/access-subject" "$smoke_dir/refreshed-subject"
refreshed_access_header="$smoke_dir/refreshed-access-token.authorization"
make_authorization_header "$smoke_dir/refreshed-access-token" "$refreshed_access_header"
assert_valid_members_call refreshed-access-token 9 "$refreshed_access_header"

revoke_status="$(
  safe_curl --silent --show-error \
    --request POST \
    --output "$smoke_dir/revoke.response" \
    --write-out '%{http_code}' \
    --data-urlencode "client_id=$client_id" \
    --data-urlencode "token@$smoke_dir/refreshed-refresh-token" \
    --data-urlencode 'token_type_hint=refresh_token' \
    "$revocation_endpoint"
)"
chmod 600 "$smoke_dir/revoke.response"
if [[ "$revoke_status" != "200" ]]; then
  echo "Refresh token revocation failed: HTTP $revoke_status" >&2
  exit 1
fi

revoked_refresh_response="$smoke_dir/revoked-refresh.response.json"
revoked_refresh_status="$(
  safe_curl --silent --show-error \
    --request POST \
    --output "$revoked_refresh_response" \
    --write-out '%{http_code}' \
    --data-urlencode 'grant_type=refresh_token' \
    --data-urlencode "client_id=$client_id" \
    --data-urlencode "refresh_token@$smoke_dir/refreshed-refresh-token" \
    "$token_endpoint"
)"
chmod 600 "$revoked_refresh_response"
assert_invalid_grant "$revoked_refresh_status" "$revoked_refresh_response" 'revoked refresh token'

terminate_mcp_session
delete_test_user
echo "Keycloak PKCE E2E passed: S256 enforced; public discovery and MCP OAuth tool-call challenges verified; JWT and refresh accepted, actor=$actor_id"
