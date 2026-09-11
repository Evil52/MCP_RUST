#!/usr/bin/env bash
# Real age/PostgreSQL/tar round trips using only isolated synthetic fixtures.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker_bin="${DOCKER_BIN:-$(command -v docker)}"
image="$(awk '/^FROM postgres:/ { print $2; exit }' "$project_root/position-monitor/Dockerfile")"
fixture_dir="$(mktemp -d)"
suffix="${RANDOM}-$$"
fixture_db="mcp-backup-test-db-$suffix"
fixture_network="mcp-backup-test-net-$suffix"
fixture_guard="mcp-backup-test-guard-$suffix"
fixture_artifacts="mcp-backup-test-artifacts-$suffix"
fixture_holder="mcp-backup-test-holder-$suffix"
cleanup() {
  "$docker_bin" rm -fv "$fixture_holder" "$fixture_db" >/dev/null 2>&1 || true
  "$docker_bin" volume rm "$fixture_guard" "$fixture_artifacts" >/dev/null 2>&1 || true
  "$docker_bin" network rm "$fixture_network" >/dev/null 2>&1 || true
  rm -rf "$fixture_dir"
}
trap cleanup EXIT
umask 077

for dependency in age age-keygen jq python3; do command -v "$dependency" >/dev/null; done
"$docker_bin" info >/dev/null
if ! "$docker_bin" image inspect "$image" >/dev/null 2>&1; then
  "$docker_bin" pull "$image" >/dev/null
fi
"$docker_bin" network create --internal "$fixture_network" >/dev/null
"$docker_bin" volume create "$fixture_guard" >/dev/null
"$docker_bin" volume create "$fixture_artifacts" >/dev/null
"$docker_bin" run --detach --name "$fixture_db" --network "$fixture_network" \
  --env POSTGRES_PASSWORD=backup-test-only-password-with-sufficient-length \
  --env POSTGRES_USER=position_admin --env POSTGRES_DB=ozon_positions \
  "$image" >/dev/null
ready=false
# PostgreSQL's initdb server listens only on its Unix socket and exits before
# the final TCP server starts. Do not initialize fixture data during that gap.
for _attempt in $(seq 1 60); do
  if [[ "$("$docker_bin" exec \
    --env PGPASSWORD=backup-test-only-password-with-sufficient-length "$fixture_db" \
    psql --host 127.0.0.1 --username position_admin --dbname ozon_positions \
    --no-password --no-psqlrc --no-align --tuples-only --set ON_ERROR_STOP=1 \
    --command 'SELECT 1' 2>/dev/null)" == 1 ]]; then
    ready=true
    break
  fi
  sleep 1
done
[[ "$ready" == true ]]
"$docker_bin" exec --interactive "$fixture_db" \
  psql --username position_admin --dbname ozon_positions --quiet --set ON_ERROR_STOP=1 <<'SQL'
CREATE SCHEMA search_position;
CREATE SCHEMA daily_reporting;
CREATE SCHEMA control;
CREATE SCHEMA wb_automation;
CREATE TABLE daily_reporting.delivery_batches (artifact_object_key text);
INSERT INTO daily_reporting.delivery_batches VALUES ('fixture-report.html');
CREATE TABLE control.ozon_static_guard_audit_events (event_id bigint PRIMARY KEY, account_id text);
INSERT INTO control.ozon_static_guard_audit_events VALUES (7, 'test_ozon');
SQL
cat >"$fixture_dir/state.json" <<'JSON'
{"last_static_audit_event_id":7,"incident_campaign_ids":[101],"incidents":{"101":{"error_class":"fixture_unresolved","occurred_at":"2026-01-01T00:00:00Z"}},"last_bid_change_at":{"101":"2026-01-01T00:00:00Z"},"pending_bid_changes":{"101":{"from_microrubles":1000000,"to_microrubles":2000000,"started_at":"2026-01-01T00:00:00Z"}},"pending_campaign_mutations":{}}
JSON
"$docker_bin" run --rm --interactive --network none \
  --volume "$fixture_guard:/guard-state" --volume "$fixture_artifacts:/artifacts" \
  --entrypoint /bin/sh "$image" -ec '
    cat >/guard-state/state.json
    printf "pid=fixture\n" >/guard-state/.state.json.lease
    chmod 700 /guard-state
    chmod 600 /guard-state/state.json /guard-state/.state.json.lease
    chown -R 10001:10001 /guard-state
    printf "fixture report\n" >/artifacts/fixture-report.html
  ' <"$fixture_dir/state.json"
age-keygen --output "$fixture_dir/identity" 2>"$fixture_dir/keygen.log"
age-keygen -y "$fixture_dir/identity" >"$fixture_dir/recipients"
printf '%s\n' POSITION_DB_ADMIN_PASSWORD=backup-test-only-password-with-sufficient-length \
  >"$fixture_dir/position.env"
backup_env=(
  "DOCKER_BIN=$docker_bin" "MCP_OPS_POSTGRES_IMAGE=$image"
  "MCP_BACKUP_POSITION_ENV=$fixture_dir/position.env"
  "MCP_BACKUP_AGE_RECIPIENTS_FILE=$fixture_dir/recipients"
  "MCP_BACKUP_AGE_IDENTITY_FILE=$fixture_dir/identity"
  "MCP_BACKUP_ARTIFACT_VOLUME=$fixture_artifacts"
  "MCP_BACKUP_GUARD_STATE_VOLUME=$fixture_guard"
  "MCP_BACKUP_DB_NETWORK=$fixture_network" "MCP_BACKUP_DB_HOST=$fixture_db"
  'MCP_BACKUP_ALLOW_LOCAL_ONLY=true' 'MCP_BACKUP_OFFSITE_COMMAND='
)
expect_failure() {
  local expected="$1" output
  shift
  if output="$("$@" 2>&1)"; then
    echo "expected backup failure did not occur: $expected" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected"* ]]; then
    printf 'unexpected failure; wanted %s\n%s\n' "$expected" "$output" >&2
    exit 1
  fi
}

# Proves that the pinned Alpine has flock and that a live executor lease stops
# capture before pg_dump. No test command names or mounts a production resource.
"$docker_bin" run --detach --name "$fixture_holder" --network none \
  --read-only --cap-drop ALL --user 10001:10001 \
  --volume "$fixture_guard:/guard-state:ro" --entrypoint /bin/sh "$image" \
  -ec 'exec 9</guard-state/.state.json.lease; flock -n 9; echo locked; exec sleep 120' >/dev/null
for _attempt in 1 2 3 4 5; do
  [[ "$("$docker_bin" logs "$fixture_holder")" != locked ]] || break
  sleep 1
done
[[ "$("$docker_bin" logs "$fixture_holder")" == locked ]]
expect_failure 'lease is busy or unsafe' env "${backup_env[@]}" \
  "MCP_BACKUP_DIR=$fixture_dir/busy" bash "$project_root/scripts/backup-position-stack.sh"
[[ -z "$(find "$fixture_dir/busy" -mindepth 1 -maxdepth 1 -type d)" ]]
"$docker_bin" rm -f "$fixture_holder" >/dev/null

env "${backup_env[@]}" "MCP_BACKUP_DIR=$fixture_dir/backups" \
  bash "$project_root/scripts/backup-position-stack.sh" >"$fixture_dir/backup.log"
backup="$(find "$fixture_dir/backups" -mindepth 1 -maxdepth 1 -type d)"
jq -e '.manifest_version == 3 and .guard_state_required == true' "$backup/manifest.json" >/dev/null
env "${backup_env[@]}" bash "$project_root/scripts/verify-position-backup.sh" "$backup" \
  >"$fixture_dir/verify.log"
jq -e '.guard_state_verified == true and (.archive_identity | split(":") | length) == 3' \
  "$backup/restore-verified.json" >/dev/null
age --decrypt --identity "$fixture_dir/identity" "$backup/ozon-guard-state.tar.age" \
  >"$fixture_dir/guard.tar"
tar -xOf "$fixture_dir/guard.tar" state.json >"$fixture_dir/recovered-state.json"
cmp "$fixture_dir/state.json" "$fixture_dir/recovered-state.json"

# New verifier retains v2 DB/artifact recovery without making a guard claim.
cp -R "$backup" "$fixture_dir/legacy"
jq '.manifest_version = 2 | .capture_order = ["position-db", "report-artifacts"]
    | del(.guard_state, .guard_state_required, .archives["ozon-guard-state.tar.age"])' \
  "$backup/manifest.json" >"$fixture_dir/legacy/manifest.json"
rm "$fixture_dir/legacy/ozon-guard-state.tar.age" "$fixture_dir/legacy/restore-verified.json"
env "${backup_env[@]}" bash "$project_root/scripts/verify-position-backup.sh" "$fixture_dir/legacy" \
  >"$fixture_dir/legacy.log" 2>&1
jq -e '.guard_state_verified == false' "$fixture_dir/legacy/restore-verified.json" >/dev/null

# A validly encrypted but independently captured cursor must fail restored-DB
# comparison; unsafe archive members must fail before extraction.
for scenario in cursor traversal; do
  cp -R "$backup" "$fixture_dir/$scenario"
  rm "$fixture_dir/$scenario/restore-verified.json"
  python3 - "$fixture_dir/guard.tar" "$fixture_dir/changed.tar" "$scenario" <<'PY'
import io
import json
import sys
import tarfile
with tarfile.open(sys.argv[1]) as source:
    member = source.getmembers()[0]
    data = source.extractfile(member).read()
if sys.argv[3] == 'cursor':
    state = json.loads(data)
    state['last_static_audit_event_id'] = 8
    data = json.dumps(state).encode()
else:
    member.name = '../state.json'
member.size = len(data)
with tarfile.open(sys.argv[2], 'w') as target:
    target.addfile(member, io.BytesIO(data))
PY
  rm "$fixture_dir/$scenario/ozon-guard-state.tar.age"
  age --encrypt --recipients-file "$fixture_dir/recipients" \
    --output "$fixture_dir/$scenario/ozon-guard-state.tar.age" "$fixture_dir/changed.tar"
  python3 - "$fixture_dir/$scenario" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
root = Path(sys.argv[1])
manifest = json.loads((root / 'manifest.json').read_text())
data = (root / 'ozon-guard-state.tar.age').read_bytes()
manifest['archives']['ozon-guard-state.tar.age'].update(sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
(root / 'manifest.json').write_text(json.dumps(manifest))
PY
  if [[ "$scenario" == cursor ]]; then expected='does not match the database audit cursor'
  else expected='guard archive is unsafe'; fi
  expect_failure "$expected" env "${backup_env[@]}" \
    bash "$project_root/scripts/verify-position-backup.sh" "$fixture_dir/$scenario"
  [[ ! -e "$fixture_dir/$scenario/restore-verified.json" ]]
done

# Missing local state must never turn existing DB guard history into a base
# backup. Simulate an empty volume inventory, keeping real fixture DB I/O.
cat >"$fixture_dir/docker-no-guards" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == volume && "${2:-}" == ls ]]; then exit 0; fi
exec "$BACKUP_TEST_REAL_DOCKER" "$@"
WRAPPER
chmod 700 "$fixture_dir/docker-no-guards"
guardless_env=("DOCKER_BIN=$fixture_dir/docker-no-guards" "BACKUP_TEST_REAL_DOCKER=$docker_bin" 'MCP_BACKUP_GUARD_STATE_VOLUME=')
expect_failure 'cursor mismatch before dump' env "${backup_env[@]}" "${guardless_env[@]}" \
  "MCP_BACKUP_DIR=$fixture_dir/missing-guard" bash "$project_root/scripts/backup-position-stack.sh"
[[ -z "$(find "$fixture_dir/missing-guard" -mindepth 1 -maxdepth 1 -type d)" ]]

# Capture and restore both reject a multi-account ledger with only one state
# file, even if archives from different authentic backups are combined.
"$docker_bin" exec "$fixture_db" psql --username position_admin --dbname ozon_positions \
  --quiet --set ON_ERROR_STOP=1 --command "INSERT INTO control.ozon_static_guard_audit_events VALUES (8, 'other_fixture')"
expect_failure 'cursor mismatch before dump' env "${backup_env[@]}" \
  "MCP_BACKUP_DIR=$fixture_dir/multiple-guards" bash "$project_root/scripts/backup-position-stack.sh"
cp -R "$backup" "$fixture_dir/multiple-account-restore"
rm "$fixture_dir/multiple-account-restore/restore-verified.json" "$fixture_dir/multiple-account-restore/position-db.dump.age"
"$docker_bin" exec "$fixture_db" pg_dump --username position_admin --dbname ozon_positions --format=custom \
  | age --encrypt --recipients-file "$fixture_dir/recipients" \
      --output "$fixture_dir/multiple-account-restore/position-db.dump.age"
python3 - "$fixture_dir/multiple-account-restore" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
root = Path(sys.argv[1])
manifest = json.loads((root / 'manifest.json').read_text())
data = (root / 'position-db.dump.age').read_bytes()
manifest['archives']['position-db.dump.age'].update(sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
(root / 'manifest.json').write_text(json.dumps(manifest))
PY
expect_failure 'does not match the database audit cursor' env "${backup_env[@]}" \
  bash "$project_root/scripts/verify-position-backup.sh" "$fixture_dir/multiple-account-restore"

# Guardless deployments retain v2 only when the ledger stays empty through
# pg_dump; the manifest makes this narrower coverage explicit.
"$docker_bin" exec "$fixture_db" psql --username position_admin --dbname ozon_positions \
  --quiet --set ON_ERROR_STOP=1 --command 'TRUNCATE control.ozon_static_guard_audit_events'
env "${backup_env[@]}" "${guardless_env[@]}" "MCP_BACKUP_DIR=$fixture_dir/base" \
  bash "$project_root/scripts/backup-position-stack.sh" >"$fixture_dir/base.log"
base_backup="$(find "$fixture_dir/base" -mindepth 1 -maxdepth 1 -type d)"
jq -e '.manifest_version == 2 and .guard_state_required == false
  and (.archives | length) == 2' "$base_backup/manifest.json" >/dev/null

echo 'position backup/restore contract: OK (PostgreSQL, age, flock, cursor, multiple accounts, legacy v2, guardless capture, unsafe tar)'
