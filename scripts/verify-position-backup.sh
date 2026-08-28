#!/bin/bash
# Restore a backup into a disposable PostgreSQL container and prove it is
# usable. A backup that has never been restored is not a backup, so this is
# the half of the recovery story that is worth automating.
#
# Three things are checked, in increasing order of what they would have cost
# to discover during a real incident:
#
#   1. The ciphertext still matches the SHA-256 recorded when it was written.
#   2. `pg_restore` completes into the exact pinned PostgreSQL build, with the
#      application roles present, so ownership and grants apply cleanly.
#   3. Every `artifact_object_key` the restored database references is present
#      in the artifact archive captured alongside it. This is the cross-store
#      consistency that two independently restored volumes silently lose.
#
# Nothing here touches the live stack: the disposable container has no network
# and its volume is removed on exit.

set -euo pipefail

runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
passphrase_file="${MCP_BACKUP_PASSPHRASE_FILE:-$runtime_dir/backup-passphrase}"
backup_root="${MCP_BACKUP_DIR:-$HOME/MCP_OZON-backups}"
readiness_attempts=60

umask 077

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [BACKUP_DIRECTORY]" >&2
  exit 64
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  stat_mode=(/usr/bin/stat -f '%Lp')
  sha256=(shasum -a 256)
else
  stat_mode=(stat -c '%a')
  sha256=(sha256sum)
fi

backup_dir="${1:-}"
if [[ -z "$backup_dir" ]]; then
  backup_dir="$(
    find "$backup_root" -mindepth 1 -maxdepth 1 -type d -name '2*Z' 2>/dev/null \
      | sort -r \
      | head -n 1 \
      || true
  )"
  if [[ -z "$backup_dir" ]]; then
    echo "no backup found under $backup_root" >&2
    exit 1
  fi
fi
if [[ ! -d "$backup_dir" || -L "$backup_dir" ]]; then
  echo "backup directory is unavailable or unsafe: $backup_dir" >&2
  exit 1
fi

manifest="$backup_dir/manifest.json"
database_archive="$backup_dir/position-db.dump.enc"
artifact_archive="$backup_dir/report-artifacts.tar.enc"
for path in "$manifest" "$database_archive" "$artifact_archive" "$passphrase_file"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required restore input is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if [[ "$("${stat_mode[@]}" "$passphrase_file")" != "600" ]]; then
  echo "backup passphrase must have mode 600: $passphrase_file" >&2
  exit 1
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi

if ! jq -e '.manifest_version == 1' "$manifest" >/dev/null; then
  echo "backup manifest version is unsupported" >&2
  exit 1
fi
db_image="$(jq -r '.postgres_image' "$manifest")"
db_name="$(jq -r '.database_name' "$manifest")"
db_owner="$(jq -r '.database_owner' "$manifest")"
iterations="$(jq -r '.key_derivation.iterations' "$manifest")"
expected_db_sha="$(jq -r '.archives["position-db.dump.enc"].sha256' "$manifest")"
expected_artifact_sha="$(jq -r '.archives["report-artifacts.tar.enc"].sha256' "$manifest")"

if [[ ! "$db_image" =~ ^postgres:[0-9]+-alpine@sha256:[0-9a-f]{64}$ ]] \
  || [[ ! "$db_name" =~ ^[A-Za-z0-9_]+$ ]] \
  || [[ ! "$db_owner" =~ ^[A-Za-z0-9_]+$ ]] \
  || [[ ! "$iterations" =~ ^[1-9][0-9]*$ ]]; then
  echo "backup manifest does not describe a restorable archive" >&2
  exit 1
fi

echo "==> verifying archive integrity"
actual_db_sha="$("${sha256[@]}" "$database_archive" | awk '{ print $1 }')"
actual_artifact_sha="$("${sha256[@]}" "$artifact_archive" | awk '{ print $1 }')"
if [[ "$actual_db_sha" != "$expected_db_sha" ]]; then
  echo "database archive does not match its recorded SHA-256" >&2
  exit 1
fi
if [[ "$actual_artifact_sha" != "$expected_artifact_sha" ]]; then
  echo "artifact archive does not match its recorded SHA-256" >&2
  exit 1
fi

# The disposable instance keeps production's scram-sha-256 settings, so even
# its local socket needs this ephemeral password. It lives for one run, in a
# container with no network, and is discarded with the volume.
verify_password="$(openssl rand -hex 24)"
container="mcp-ozon-backup-verify-$$"
volume="mcp-ozon-backup-verify-$$"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/mcp-ozon-verify.XXXXXX")"

# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  "$docker_bin" rm -f "$container" >/dev/null 2>&1 || true
  "$docker_bin" volume rm "$volume" >/dev/null 2>&1 || true
  rm -rf "$work_dir"
}
trap cleanup EXIT

decrypt() {
  openssl enc -d -aes-256-cbc -pbkdf2 -iter "$iterations" \
    -pass "file:$passphrase_file" -in "$1"
}

echo "==> starting disposable PostgreSQL ($db_image)"
"$docker_bin" volume create "$volume" >/dev/null
"$docker_bin" run --detach \
  --name "$container" \
  --network none \
  --volume "$volume:/var/lib/postgresql/data" \
  --env "POSTGRES_USER=$db_owner" \
  --env "POSTGRES_DB=$db_name" \
  --env "POSTGRES_PASSWORD=$verify_password" \
  --env 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=scram-sha-256' \
  --memory 512m \
  --pids-limit 128 \
  "$db_image" >/dev/null

ready=false
for _attempt in $(seq 1 "$readiness_attempts"); do
  if "$docker_bin" exec "$container" pg_isready \
    --username "$db_owner" --dbname "$db_name" --quiet >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "disposable PostgreSQL did not become ready" >&2
  exit 1
fi

psql_exec() {
  "$docker_bin" exec --interactive \
    --env "PGPASSWORD=$verify_password" "$container" \
    psql --username "$db_owner" --dbname "$db_name" \
    --no-password --quiet --set ON_ERROR_STOP=1 "$@"
}

psql_quiet() {
  psql_exec --no-align --tuples-only
}

# The dump carries ownership and grants for the restricted application roles.
# Creating them first is what lets `pg_restore` apply the real privilege
# structure instead of quietly discarding it.
echo "==> creating application roles"
psql_quiet <<'SQL' >/dev/null
DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'position_collector', 'position_reader', 'report_worker',
        'report_collector', 'control_writer', 'wb_automation_writer'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', role_name);
        END IF;
    END LOOP;
END
$$;
SQL

echo "==> restoring the database dump"
restore_log="$work_dir/pg_restore.log"
restore_status=0
decrypt "$database_archive" \
  | "$docker_bin" exec --interactive \
      --env "PGPASSWORD=$verify_password" "$container" \
      pg_restore \
        --username "$db_owner" \
        --dbname "$db_name" \
        --no-password \
        --single-transaction \
    >"$restore_log" 2>&1 || restore_status=$?
if ((restore_status != 0)) || grep -qi 'errors ignored on restore' "$restore_log"; then
  echo "pg_restore did not complete cleanly:" >&2
  tail -n 30 "$restore_log" >&2
  exit 1
fi

echo "==> checking restored schema"
missing_schemas="$(
  psql_quiet <<'SQL'
SELECT string_agg(expected.name, ', ' ORDER BY expected.name)
FROM unnest(ARRAY[
    'search_position', 'daily_reporting', 'control', 'wb_automation'
]) AS expected(name)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_namespace WHERE nspname = expected.name
);
SQL
)"
if [[ -n "$missing_schemas" ]]; then
  echo "restored database is missing schemas: $missing_schemas" >&2
  exit 1
fi

echo "==> restored row counts"
psql_exec <<'SQL'
SELECT
    schema_name AS "schema",
    table_name AS "table",
    (xpath(
        '/row/count/text()',
        query_to_xml(
            format('SELECT count(*) AS count FROM %I.%I', schema_name, table_name),
            false, true, ''
        )
    ))[1]::text::bigint AS "rows"
FROM (
    VALUES
        ('daily_reporting', 'delivery_batches'),
        ('daily_reporting', 'source_snapshots'),
        ('wb_automation', 'cycles'),
        ('wb_automation', 'action_attempts'),
        ('wb_automation', 'execution_state'),
        ('wb_automation', 'audit_events'),
        ('control', 'wb_plans'),
        ('control', 'wb_plan_approvals'),
        ('search_position', 'measurements')
) AS expected(schema_name, table_name)
WHERE to_regclass(format('%I.%I', schema_name, table_name)) IS NOT NULL
ORDER BY 1, 2;
SQL

# Cross-store consistency. Restoring the database and the artifact volume from
# different points in time is the failure this check exists to catch, and it
# is invisible until someone tries to send a report.
echo "==> cross-checking artifact references"
decrypt "$artifact_archive" >"$work_dir/report-artifacts.tar"
tar --list --file "$work_dir/report-artifacts.tar" \
  | sed -e 's#^\./##' -e 's#/$##' \
  | sort -u >"$work_dir/artifact-keys.txt"

psql_quiet <<'SQL' | sed -e 's#^\./##' | sort -u >"$work_dir/referenced-keys.txt"
SELECT DISTINCT artifact_object_key
FROM daily_reporting.delivery_batches
WHERE artifact_object_key IS NOT NULL;
SQL

referenced_count="$(wc -l <"$work_dir/referenced-keys.txt" | tr -d ' ')"
if ((referenced_count == 0)); then
  echo "    no artifact references in this backup; nothing to cross-check"
else
  missing_keys="$(comm -23 "$work_dir/referenced-keys.txt" "$work_dir/artifact-keys.txt")"
  if [[ -n "$missing_keys" ]]; then
    echo "artifact archive is missing keys the database references:" >&2
    printf '%s\n' "$missing_keys" | head -n 20 >&2
    exit 1
  fi
  echo "    $referenced_count referenced artifact keys all present"
fi

printf '\nbackup verified: %s\n' "$backup_dir"
