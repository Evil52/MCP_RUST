#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

confirmation="--confirm-canaries-published-and-reconciled"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

: "${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required}"
: "${DAILY_REPORT_POLICY_HOST:?DAILY_REPORT_POLICY_HOST is required}"
: "${REPORT_COLLECTOR_CREDENTIAL_DIR_HOST:?REPORT_COLLECTOR_CREDENTIAL_DIR_HOST is required}"

for path in "$MCP_ACCESS_CONFIG_HOST" "$DAILY_REPORT_POLICY_HOST"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required scheduled-collection file is unavailable or unsafe: $path" >&2
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

release_record="$(
  "$project_dir/scripts/verify-release-images.sh" report-collector ozon-egress
)"
export MCP_RELEASE_GIT_SHA
export MCP_REPORT_COLLECTOR_IMAGE
export MCP_OZON_EGRESS_IMAGE
MCP_RELEASE_GIT_SHA="$(jq -r '.git_sha' <<<"$release_record")"
MCP_REPORT_COLLECTOR_IMAGE="$(jq -r '.images["report-collector"]' <<<"$release_record")"
MCP_OZON_EGRESS_IMAGE="$(jq -r '.images["ozon-egress"]' <<<"$release_record")"

compose=(
  docker compose --env-file .position.env
  -f compose.position.yaml
  -f compose.reporting-live.yaml
  --profile reporting-live
)

# The preflight uses only the private metadata mounts and PostgreSQL proof. It
# performs no marketplace request and fails unless every live-policy target
# shares one successful, fully paginated four-source cutoff from the previous
# 24 hours.
"${compose[@]}" config --quiet
"${compose[@]}" run --rm --no-deps report-collector collection-preflight
"${compose[@]}" up --detach --wait --wait-timeout 60 ozon-egress report-collector
