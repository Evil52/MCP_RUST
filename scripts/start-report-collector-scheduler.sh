#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

confirmation="--confirm-independent-source-collection"
if [[ $# -ne 1 || "$1" != "$confirmation" ]]; then
  echo "usage: $0 $confirmation" >&2
  exit 64
fi

: "${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required}"
: "${REPORT_COLLECTION_POLICY_HOST:=${DAILY_REPORT_POLICY_HOST:-}}"
: "${REPORT_COLLECTION_POLICY_HOST:?REPORT_COLLECTION_POLICY_HOST or DAILY_REPORT_POLICY_HOST is required}"
if [[ -n "${DAILY_REPORT_POLICY_HOST:-}" && "$DAILY_REPORT_POLICY_HOST" != "$REPORT_COLLECTION_POLICY_HOST" ]]; then
  echo "collection and legacy policy paths conflict; configure only one" >&2
  exit 64
fi
export REPORT_COLLECTION_POLICY_HOST
: "${REPORT_COLLECTOR_CREDENTIAL_DIR_HOST:?REPORT_COLLECTOR_CREDENTIAL_DIR_HOST is required}"

for path in "$MCP_ACCESS_CONFIG_HOST" "$REPORT_COLLECTION_POLICY_HOST"; do
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

# Preflight checks the enabled policy and migration/role contract without API
# requests. Each source now publishes independently; full-report completeness
# is verified separately by the existing manifest reader.
"${compose[@]}" config --quiet
"${compose[@]}" run --rm --no-deps report-collector sources-preflight
"${compose[@]}" up --detach --wait --wait-timeout 60 ozon-egress report-collector
