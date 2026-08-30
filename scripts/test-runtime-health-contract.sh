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

echo 'runtime health contract validation: OK'
