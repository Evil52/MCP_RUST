#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

confirmation="--confirm-canary-sent-and-reconciled"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
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
    echo "required scheduled-mail file is unavailable or unsafe: $path" >&2
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
  -f compose.reporting-mail-live.yaml
  --profile reporting-mail-live
)

# Compose recreates the disabled base worker with the opt-in scheduled mode.
# The narrow credentialless proxy is the only component attached to outbound.
"${compose[@]}" config --quiet
"${compose[@]}" up --detach --wait --wait-timeout 60 mail-egress report-worker
