#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <YYYY-MM-DD> <morning|evening>" >&2
  exit 64
fi

business_date="$1"
report_kind="$2"
if [[ ! "$business_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "business date must use YYYY-MM-DD" >&2
  exit 64
fi
if [[ "$report_kind" != morning && "$report_kind" != evening ]]; then
  echo "report kind must be morning or evening" >&2
  exit 64
fi

: "${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required}"
: "${DAILY_REPORT_CANARY_POLICY_HOST:?DAILY_REPORT_CANARY_POLICY_HOST is required}"
: "${DAILY_REPORT_POLICY_HOST:?DAILY_REPORT_POLICY_HOST is required}"
: "${REPORT_COLLECTOR_CREDENTIAL_DIR_HOST:?REPORT_COLLECTOR_CREDENTIAL_DIR_HOST is required}"

accounts=(
  furnitura_dlya_doma
  evromebelkomplekt
  dom_mebelnoy_furnitury
  ofk_komplekt_ozon
  mebelnaya_furniturnaya_kompaniya
  tsentr_mebelnoy_furnitury
  megamarket_ozon
)

run_account_canary() {
  local account_id="$1"
  local attempt_log
  local attempt_status
  attempt_log="$(mktemp "${TMPDIR:-/tmp}/mcp-ozon-report-canary.XXXXXX")"

  if ./scripts/run-report-canary.sh \
    ozon "$account_id" "$business_date" "$report_kind" \
    >"$attempt_log" 2>&1; then
    cat "$attempt_log"
    rm -f "$attempt_log"
    return 0
  else
    attempt_status=$?
  fi

  cat "$attempt_log" >&2
  if ! grep -q 'performance_rate_limited' "$attempt_log"; then
    rm -f "$attempt_log"
    return "$attempt_status"
  fi

  echo "Ozon Performance rate limit for ${account_id}; one retry in 20 seconds" >&2
  sleep 20
  if ./scripts/run-report-canary.sh \
    ozon "$account_id" "$business_date" "$report_kind" \
    >"$attempt_log" 2>&1; then
    cat "$attempt_log"
    rm -f "$attempt_log"
    return 0
  else
    attempt_status=$?
  fi
  cat "$attempt_log" >&2
  rm -f "$attempt_log"
  return "$attempt_status"
}

compose=(
  docker compose --env-file .position.env
  -f compose.position.yaml
)

"${compose[@]}" up --detach --wait --wait-timeout 30 --no-deps ozon-egress

for account_id in "${accounts[@]}"; do
  echo "collecting Ozon report cutoff for ${account_id}" >&2
  run_account_canary "$account_id"
done

./scripts/start-report-collector-scheduler.sh \
  --confirm-canaries-published-and-reconciled

worker_compose=(
  docker compose --env-file .position.env
  -f compose.position.yaml
  -f compose.reporting-worker-dry-run.yaml
)
"${worker_compose[@]}" config --quiet
"${worker_compose[@]}" up --detach --wait --wait-timeout 60 --no-deps report-worker

echo "all seven Ozon canaries published; collection and dry-run report generation are running" >&2
