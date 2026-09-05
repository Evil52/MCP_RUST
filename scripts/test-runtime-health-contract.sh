#!/bin/bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
health_script="$project_root/scripts/check-runtime-health.sh"

assert_contract_rejected() {
  local variable_name="$1"
  local value="$2"
  local status=0

  env "$variable_name=$value" "$health_script" >/dev/null 2>&1 || status=$?
  if [[ "$status" -ne 2 ]]; then
    echo "$variable_name=$value must fail with health-check configuration status 2; got $status" >&2
    exit 1
  fi
}

assert_contract_rejected MCP_HEALTH_REQUIRED_SERVICES ''
assert_contract_rejected MCP_HEALTH_REQUIRED_SERVICES 'position-db,,ozon-egress'
assert_contract_rejected MCP_HEALTH_REQUIRED_SERVICES 'position-db, report-worker'
assert_contract_rejected MCP_HEALTH_REQUIRED_LAUNCH_AGENTS ''
assert_contract_rejected MCP_HEALTH_REQUIRED_LAUNCH_AGENTS 'com.ofk.runtime/unsafe'

for required_contract in \
  "plan.status IN ('creating','adding_products','activating','ambiguous')" \
  "plan.status IN ('approved','created','products_added')" \
  "plan.status='failed' AND plan.campaign_id IS NOT NULL" \
  "plan.status='applied' AND guard.plan_id IS NULL" \
  "status IN ('stopping','incident')" \
  "WHERE status='active'" \
  'Ozon launch requires readback recovery' \
  'Ozon launch outbox is stalled' \
  'Ozon applied campaign has no durable spend guard' \
  'Ozon campaign guard is incident-locked' \
  'Ozon campaign stop is unresolved' \
  'Ozon campaign guard is stale'; do
  if ! grep -Fq "$required_contract" "$health_script"; then
    echo "runtime health check is missing Ozon contract: $required_contract" >&2
    exit 1
  fi
done

test_root="$(mktemp -d)"
cleanup() {
  rm -rf "$test_root"
}
trap cleanup EXIT

fake_docker="$test_root/docker"
# shellcheck disable=SC2016 # The generated mock expands its own positional arguments.
printf '%s\n' \
  '#!/bin/bash' \
  'set -euo pipefail' \
  'case "${1:-}" in' \
  '  info) exit 0 ;;' \
  '  ps) printf "running|Up 1 minute (healthy)\\n" ;;' \
  '  container) printf "running|healthy\\n" ;;' \
  '  run)' \
  '    while IFS= read -r _; do :; done' \
  '    printf "cycle_age|0\\n"' \
  '    exit 9' \
  '    ;;' \
  '  *) exit 2 ;;' \
  'esac' >"$fake_docker"
chmod 700 "$fake_docker"

backup="$test_root/backups/20260904T000000Z"
mkdir -p "$backup"
: >"$backup/offsite-complete.json"
: >"$backup/restore-verified.json"
: >"$test_root/position.env"
: >"$test_root/ready"

status=0
output="$(
  env \
    DOCKER_BIN="$fake_docker" \
    MCP_HEALTH_POSITION_ENV="$test_root/position.env" \
    MCP_BACKUP_DIR="$test_root/backups" \
    MCP_HEALTH_MCP_READY_URL="file://$test_root/ready" \
    MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK=true \
    "$health_script" 2>&1
)" || status=$?
if [[ "$status" -ne 1 ]] \
  || [[ "$output" != *'position database health probe failed before returning complete evidence'* ]] \
  || [[ "$output" == *'health check: clean'* ]]; then
  echo "a partial PostgreSQL result followed by failure must never be reported as clean" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

echo 'runtime health contract validation: OK'
