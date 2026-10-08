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
assert_contract_rejected MCP_HEALTH_DISK_PATHS ''
assert_contract_rejected MCP_HEALTH_DISK_PATHS '/,relative'
assert_contract_rejected MCP_HEALTH_DISK_PATHS '/,,/home'
assert_contract_rejected MCP_HEALTH_DISK_MAX_USED_PERCENT '100'
assert_contract_rejected MCP_HEALTH_DISK_MAX_USED_PERCENT '08'
assert_contract_rejected MCP_HEALTH_DISK_MIN_FREE_GIB '0'

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

mkdir -p "$test_root/bin"
cat >"$test_root/bin/df" <<'SH'
#!/bin/bash
if [[ "${FAKE_DISK_ERROR:-false}" == true ]]; then exit 1; fi
printf '%s\n' 'Filesystem 1024-blocks Used Available Capacity Mounted on' \
  "${FAKE_DISK_ROW:-/dev/test 104857600 10485760 94371840 10% /}"
SH
chmod 700 "$test_root/bin/df"
export PATH="$test_root/bin:$PATH"

fake_docker="$test_root/docker"
# shellcheck disable=SC2016 # The generated mock expands its own positional arguments.
printf '%s\n' \
  '#!/bin/bash' \
  'set -euo pipefail' \
  'case "${1:-}" in' \
  '  info) exit "${FAKE_DOCKER_INFO_STATUS:-0}" ;;' \
  '  ps) printf "running|Up 1 minute (healthy)\\n" ;;' \
  '  container) printf "running|healthy\\n" ;;' \
  '  run)' \
  '    while IFS= read -r _; do :; done' \
  '    printf "%s\\n" "${FAKE_PROBE_ROWS:-cycle_age|0}"' \
  '    exit "${FAKE_PROBE_STATUS:-9}"' \
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

assert_quota_finding() {
  local rows="$1"
  local expected="$2"
  local status=0
  local output
  output="$(
    env \
      DOCKER_BIN="$fake_docker" \
      FAKE_PROBE_ROWS="$rows" \
      FAKE_PROBE_STATUS=0 \
      MCP_HEALTH_POSITION_ENV="$test_root/position.env" \
      MCP_BACKUP_DIR="$test_root/backups" \
      MCP_HEALTH_MCP_READY_URL="file://$test_root/ready" \
      MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK=true \
      "$health_script" 2>&1
  )" || status=$?
  if [[ "$status" -ne 1 || "$output" != *"$expected"* ]]; then
    echo "a long shared marketplace cooldown must be reported: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
}

assert_quota_finding $'cycle_age|0\nquota_cooldown|2|90000' \
  'marketplace quota has 2 cooldown(s) over 1h, the longest ends in 1500 minutes'
assert_quota_finding $'cycle_age|0\nquota_cooldown|1|infinity' \
  'marketplace quota has 1 cooldown(s) over 1h, one without an end'
assert_quota_finding $'cycle_age|0\nquota_cooldown|x|1' \
  'marketplace quota returned invalid cooldown evidence'

assert_disk_result() {
  local row="$1" expected="$2" expected_status="$3"
  local output status=0
  output="$(env \
    DOCKER_BIN="$fake_docker" FAKE_PROBE_STATUS=0 FAKE_DISK_ROW="$row" \
    MCP_HEALTH_POSITION_ENV="$test_root/position.env" \
    MCP_BACKUP_DIR="$test_root/backups" \
    MCP_HEALTH_MCP_READY_URL="file://$test_root/ready" \
    MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK=true \
    "$health_script" 2>&1)" || status=$?
  if [[ "$status" -ne "$expected_status" || "$output" != *"$expected"* ]]; then
    echo "disk health result did not match: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
  if [[ "$expected" == 'disk space is low:' \
    && "$(printf '%s\n' "$output" | grep -Fc 'disk space is low:')" -ne 1 ]]; then
    echo 'the same filesystem must produce only one low-space finding' >&2
    exit 1
  fi
}

# Relative fullness and absolute reserve are independent triggers; equality
# at the reserve is healthy. A large disk can be 85% full with >20 GiB free.
assert_disk_result '/dev/test 1048576000 891289600 157286400 85% /' 'disk space is low:' 1
assert_disk_result '/dev/test 31457280 15728640 15728640 50% /' 'disk space is low:' 1
assert_disk_result '/dev/test 104857600 83886080 20971520 80% /' 'health check: clean' 0
assert_disk_result '/dev/test 100 90 unknown 90% /' 'disk space evidence is invalid:' 1
FAKE_DISK_ERROR=true assert_disk_result '' 'disk space evidence is unavailable:' 1
FAKE_DOCKER_INFO_STATUS=1 assert_disk_result \
  '/dev/test 104857600 104857600 0 100% /' 'disk space is low:' 1

echo 'runtime health contract validation: OK'
