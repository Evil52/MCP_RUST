#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <ozon|wb> <account-id> <YYYY-MM-DD>" >&2
  exit 64
fi

marketplace="$1"
account_id="$2"
business_date="$3"
case "$marketplace" in
  ozon) mode=ozon_dry_run ;;
  wb) mode=wb_dry_run ;;
  *)
    echo "marketplace must be ozon or wb" >&2
    exit 64
    ;;
esac

if [[ ! "$account_id" =~ ^[A-Za-z0-9_-]{1,128}$ ]]; then
  echo "account id is invalid" >&2
  exit 64
fi
if [[ ! "$business_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "business date must use YYYY-MM-DD" >&2
  exit 64
fi

: "${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required}"
: "${REPORT_COLLECTOR_CREDENTIAL_DIR_HOST:?REPORT_COLLECTOR_CREDENTIAL_DIR_HOST is required}"
DAILY_REPORT_CANARY_POLICY_HOST="${DAILY_REPORT_CANARY_POLICY_HOST:-$project_dir/config/daily-report-pilot.example.json}"

for path in "$MCP_ACCESS_CONFIG_HOST" "$DAILY_REPORT_CANARY_POLICY_HOST"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required canary metadata path is not a regular file: $path" >&2
    exit 66
  fi
done
if [[ ! -d "$REPORT_COLLECTOR_CREDENTIAL_DIR_HOST" || -L "$REPORT_COLLECTOR_CREDENTIAL_DIR_HOST" ]]; then
  echo "report credential directory is unavailable or unsafe" >&2
  exit 66
fi
if [[ ! -f .position.env || -L .position.env ]]; then
  echo ".position.env is unavailable or unsafe" >&2
  exit 66
fi

export REPORT_COLLECTOR_CANARY_MODE="$mode"
export DAILY_REPORT_CANARY_POLICY_HOST

exec docker compose --env-file .position.env \
  -f compose.position.yaml \
  -f compose.reporting-canary.yaml \
  --profile reporting-canary \
  run --rm --no-deps report-collector \
  "${marketplace}-dry-run" "$account_id" "$business_date"
