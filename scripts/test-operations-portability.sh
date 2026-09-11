#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
cleanup() {
  rm -rf "$test_root"
}
trap cleanup EXIT

# Exercise the installer's actual render/first-backup blocks without executing
# its host installation, LaunchAgent, or production backup side effects.
python3 - "$project_root" "$test_root" <<'PY'
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys

project, temporary = map(Path, sys.argv[1:])
installer = project / 'scripts/install-operations-agents.sh'
source = installer.read_text()
render_start = source.index('render() {')
render_end = source.index('\n}\n', render_start) + 3
render = source[render_start:render_end]
first_start = source.index('echo "==> taking one backup"')
first_end = source.index('echo "==> restoring it into a disposable database"', first_start)
first_backup = source[first_start:first_end]
runner_dir = temporary / 'installer-fixture'
runner_dir.mkdir()
capture = runner_dir / 'selected-volume'
runner = runner_dir / 'backup-position-stack.sh'
runner.write_text('#!/bin/bash\nset -eu\nprintf "%s" "${MCP_BACKUP_GUARD_STATE_VOLUME-UNSET}" >"$TEST_GUARD_CAPTURE"\n')
runner.chmod(0o700)

for volume in ('fixture-guard_state.1', ''):
    environment = os.environ.copy()
    for name in re.findall(r'\$([a-z_][a-z_0-9]*)', render + first_backup):
        environment[name] = 'fixture-value'
    environment.update(guard_volume=volume, libexec_dir=str(runner_dir),
                       TEST_GUARD_CAPTURE=str(capture))
    result = subprocess.run(['bash', '-euc', render + '\nrender "$1" "$2"',
                             'installer-render-test', str(runner),
                             str(project / 'ops/macos/com.ofk.mcp-ozon-backup.plist.in')],
                            env=environment, capture_output=True, check=True)
    rendered = plistlib.loads(result.stdout)
    assert rendered['EnvironmentVariables']['MCP_BACKUP_GUARD_STATE_VOLUME'] == volume
    subprocess.run(['bash', '-euc', first_backup], env=environment,
                   capture_output=True, check=True)
    assert capture.read_text() == volume

for invalid in ('/tmp/guard', '../guard', 'guard:ro', 'guard|other', '<guard>',
                '-guard', 'guard&other', 'guard\nother'):
    result = subprocess.run(['bash', str(installer)], capture_output=True,
                            env={**os.environ, 'MCP_BACKUP_GUARD_STATE_VOLUME': invalid})
    assert result.returncode == 1
    assert result.stderr == b'MCP_BACKUP_GUARD_STATE_VOLUME must be a Docker named volume or empty for discovery\n'
PY

# Reproduce installation from a disposable checkout, then delete that checkout.
# All credentials, archives and command doubles below belong to this test.
staging="$test_root/staging"
installed="$test_root/installed"
mkdir -p "$staging/scripts" "$installed" "$test_root/bin"
cp "$project_root/scripts/check-runtime-health.sh" \
  "$project_root/scripts/backup-position-stack.sh" "$staging/scripts/"
cp "$staging/scripts/"*.sh "$installed/"
cp "$project_root/scripts/reporting-health-contract.py" \
  "$project_root/scripts/reporting-health.sql" \
  "$project_root/scripts/operations_notify.py" \
  "$project_root/scripts/operations_heartbeat.py" "$installed/"
db_image="$(awk '/^FROM postgres:/ { print $2; exit }' "$project_root/position-monitor/Dockerfile")"
rm -rf "$staging"

cat >"$test_root/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  info | volume | network | rm) exit 0 ;;
  logs) printf 'locked\n' ;;
  inspect) printf 'true|test-started-at\n' ;;
  ps) printf 'running|Up 1 minute (healthy)\n' ;;
  container) printf 'running|healthy\n' ;;
  run)
    case "$*" in
      *--detach*) printf 'test-lease-container\n' ;;
      *'cat /guard-state/state.json'*) printf '{"last_static_audit_event_id":1}\n' ;;
      *pg_dump*) head -c 4096 /dev/zero ;;
      *tar*) printf 'synthetic artifact archive' ;;
      *) cat >"${TEST_OPS_SQL_CAPTURE:-/dev/null}"; printf 'cycle_age|0\n' ;;
    esac
    ;;
  *) exit 9 ;;
esac
MOCK
# Encryption itself has separate restore tests. This double isolates whether
# the scheduled job can finish after the installation source disappears.
cat >"$test_root/bin/age" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do
  if [[ "$1" == --output ]]; then
    cat >"$2"
    exit 0
  fi
  shift
done
exit 9
MOCK
chmod 700 "$test_root/bin/"*
printf '%s\n' 'POSITION_DB_ADMIN_PASSWORD=test-only-abcdefghijklmnopqrstuvwxyz' \
  >"$test_root/position.env"
printf '%s\n' 'test-only-recipient' >"$test_root/recipients"
chmod 600 "$test_root/position.env" "$test_root/recipients"

env \
  PATH="$test_root/bin:$PATH" \
  DOCKER_BIN="$test_root/bin/docker" \
  MCP_OPS_PROJECT_DIR="$staging" \
  MCP_OPS_POSTGRES_IMAGE="$db_image" \
  MCP_BACKUP_POSITION_ENV="$test_root/position.env" \
  MCP_BACKUP_AGE_RECIPIENTS_FILE="$test_root/recipients" \
  MCP_BACKUP_DIR="$test_root/backups" \
  MCP_BACKUP_GUARD_STATE_VOLUME=test-guard-state \
  MCP_BACKUP_OFFSITE_COMMAND='' \
  MCP_BACKUP_ALLOW_LOCAL_ONLY=true \
  bash "$installed/backup-position-stack.sh" >"$test_root/backup.log"

backup="$(find "$test_root/backups" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
jq --exit-status --arg image "$db_image" \
  '.postgres_image == $image and .archives["position-db.dump.age"].bytes == 4096' \
  "$backup/manifest.json" >/dev/null
test -s "$backup/report-artifacts.tar.age"
test -s "$backup/ozon-guard-state.tar.age"
test -f "$backup/local-only-risk-accepted.json"
: >"$backup/restore-verified.json"
: >"$test_root/ready"

health_env=(
  "DOCKER_BIN=$test_root/bin/docker"
  "MCP_OPS_PROJECT_DIR=$staging"
  "MCP_OPS_POSTGRES_IMAGE=$db_image"
  "MCP_HEALTH_POSITION_ENV=$test_root/position.env"
  "MCP_BACKUP_DIR=$test_root/backups"
  'MCP_BACKUP_ALLOW_LOCAL_ONLY=true'
  "MCP_HEALTH_MCP_READY_URL=file://$test_root/ready"
  'MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK=true'
  'MCP_HEALTH_REQUIRED_SERVICES=position-db,ozon-egress'
  'MCP_HEALTH_REPORTING_POLICY='
  'MCP_HEALTH_REPORTING_REGISTRY='
  'MCP_HEALTH_NOTIFY_COMMAND='
  'MCP_HEALTH_HEARTBEAT_COMMAND='
  'MCP_HEALTH_CHECK_TUNNEL=false'
  "TEST_OPS_SQL_CAPTURE=$test_root/health.sql"
)
output="$(env "${health_env[@]}" bash "$installed/check-runtime-health.sh")"
if [[ "$output" != *'health check: clean'* ]]; then
  echo 'installed health check failed after staging checkout deletion' >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

# Installed reporting resources must also survive checkout deletion, including
# the enabled path that executes the parser and submits the SQL contract.
printf '%s\n' \
  '{"version":1,"enabled":true,"timezone":"Asia/Yekaterinburg","account_ids":["test_ozon"]}' \
  >"$test_root/policy.json"
printf '%s\n' \
  '{"version":1,"accounts":[{"id":"test_ozon","marketplace":"ozon"}]}' \
  >"$test_root/registry.json"
reporting_env=(
  "MCP_HEALTH_REPORTING_POLICY=$test_root/policy.json"
  "MCP_HEALTH_REPORTING_REGISTRY=$test_root/registry.json"
  'MCP_HEALTH_REQUIRED_SERVICES=position-db,ozon-egress,report-collector'
)
env "${health_env[@]}" "${reporting_env[@]}" \
  bash "$installed/check-runtime-health.sh" >/dev/null
if ! grep -Fq 'PREPARE reporting_health' "$test_root/health.sql"; then
  echo 'installed reporting scope did not execute its SQL contract' >&2
  exit 1
fi
for resource in reporting-health.sql reporting-health-contract.py; do
  mv "$installed/$resource" "$installed/$resource.unavailable"
  probe_status=0
  output="$(env "${health_env[@]}" "${reporting_env[@]}" \
    bash "$installed/check-runtime-health.sh" 2>&1)" || probe_status=$?
  mv "$installed/$resource.unavailable" "$installed/$resource"
  if [[ "$probe_status" -ne 1 || "$output" == *'health check: clean'* ]]; then
    echo "missing installed resource must produce a finding: $resource" >&2
    exit 1
  fi
done

# Portability must not weaken the immutable-image requirement.
probe_status=0
env "${health_env[@]}" MCP_OPS_POSTGRES_IMAGE=postgres:latest \
  bash "$installed/check-runtime-health.sh" >/dev/null 2>&1 || probe_status=$?
if [[ "$probe_status" -ne 2 ]]; then
  echo 'unlocked PostgreSQL image must be rejected' >&2
  exit 1
fi

echo 'operations portability regression: OK'
