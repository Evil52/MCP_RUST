#!/bin/sh
set -eu

host="${CONTROL_AUTH_JWKS_HOST:-}"
path="${CONTROL_AUTH_JWKS_PATH:-}"

# These values are public deployment metadata, not model input. Still validate
# them before substitution so an operator typo cannot widen the nginx config or
# inject another directive. Ports, IP literals, queries and fragments are
# intentionally unsupported.
if [ "${#host}" -gt 253 ] \
    || ! printf '%s\n' "$host" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])$' \
    || ! printf '%s\n' "$host" | grep -Eq '[A-Za-z]' \
    || ! printf '%s\n' "$host" | grep -Fq '.' \
    || printf '.%s.' "$host" | grep -Fq '..'; then
  echo "CONTROL_AUTH_JWKS_HOST must be one bounded DNS hostname" >&2
  exit 64
fi

if [ "${#path}" -gt 512 ] \
    || ! printf '%s\n' "$path" | grep -Eq '^/[A-Za-z0-9._~/-]+$' \
    || printf '%s' "$path" | grep -Eq '(^|/)\.\.?(/|$)|//'; then
  echo "CONTROL_AUTH_JWKS_PATH must be one bounded absolute path" >&2
  exit 64
fi

export CONTROL_AUTH_JWKS_HOST="$host"
export CONTROL_AUTH_JWKS_PATH="$path"
mkdir -p /tmp/nginx-client /tmp/nginx-proxy
# Pass a literal allowlist of variable names to envsubst; shell expansion here
# would erase the placeholders before nginx configuration generation.
# shellcheck disable=SC2016
envsubst '${CONTROL_AUTH_JWKS_HOST} ${CONTROL_AUTH_JWKS_PATH}' \
  </etc/control-auth-egress/nginx.conf.template >/tmp/nginx.conf
nginx -t -c /tmp/nginx.conf
exec nginx -c /tmp/nginx.conf -g 'daemon off;'
