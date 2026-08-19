#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

if [[ $# -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 64
fi

: "${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required}"
: "${DAILY_REPORT_MAIL_POLICY_HOST:?DAILY_REPORT_MAIL_POLICY_HOST is required}"
: "${REPORT_MAIL_ROUTING_HOST:?REPORT_MAIL_ROUTING_HOST is required}"
: "${REPORT_GMAIL_OAUTH_DIR_HOST:?REPORT_GMAIL_OAUTH_DIR_HOST is required}"

for path in \
  "$MCP_ACCESS_CONFIG_HOST" \
  "$DAILY_REPORT_MAIL_POLICY_HOST" \
  "$REPORT_MAIL_ROUTING_HOST"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required mail-canary file is unavailable or unsafe: $path" >&2
    exit 66
  fi
done
if [[ ! -d "$REPORT_GMAIL_OAUTH_DIR_HOST" || -L "$REPORT_GMAIL_OAUTH_DIR_HOST" ]]; then
  echo "Gmail OAuth directory is unavailable or unsafe" >&2
  exit 66
fi
if [[ ! -f .position.env || -L .position.env ]]; then
  echo ".position.env is unavailable or unsafe" >&2
  exit 66
fi

compose=(
  docker compose --env-file .position.env
  -f compose.position.yaml
  -f compose.reporting-mail-canary.yaml
  --profile reporting-mail-canary
)

# Starting the proxy cannot send a message. The report worker is launched only
# by the explicit one-shot command below and never as a background loop.
cleanup() {
  "${compose[@]}" stop --timeout 10 mail-egress >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up --detach --wait --wait-timeout 30 --no-deps mail-egress
"${compose[@]}" run --rm --no-deps report-worker deliver-one
