#!/bin/sh

set -eu

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

mode="${1:-migrate}"
migration_dir="/opt/mcp-ozon/migrations"

# The shipped migration set is derived from the image rather than restated
# here. A hand-maintained list silently skips any migration that reaches
# `initdb/` (which the Dockerfile copies by glob) but is forgotten in this
# file: the image carries the SQL, the ledger never records it, and CI stays
# green. Deriving both the apply order and the known-id set from one directory
# listing removes that failure mode.
#
# `-type f` also rejects symlinks, matching the per-file safety check below.
# C collation reproduces the historical apply order, in which the two 002_*
# migrations run ozon-before-wb.
migrations="$(
  find "$migration_dir" -maxdepth 1 -type f -name '*.sql' |
    sed 's|.*/||' |
    LC_ALL=C sort
)"

if [ -z "$migrations" ]; then
  echo "no migrations found in $migration_dir" >&2
  exit 1
fi

# Enforce the ledger's own identifier grammar at the source. This keeps the
# derived names safe to inline into the SQL id list below and rejects a
# malformed migration before it can reach the database.
for candidate in $migrations; do
  if ! printf '%s' "$candidate" | grep -Eq '^[0-9]{3}_[a-z0-9_]+[.]sql$'; then
    echo "migration file name is not a valid migration id: $candidate" >&2
    exit 1
  fi
done

latest_migration="$(printf '%s\n' "$migrations" | tail -n 1)"

case "$mode" in
  migrate | --baseline-current) ;;
  *)
    echo "usage: migrate-position-db [migrate|--baseline-current]" >&2
    exit 64
    ;;
esac

psql_admin() {
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --no-psqlrc \
    --set ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    "$@"
}

ledger_exists="$(psql_admin --tuples-only --no-align --command \
  "SELECT to_regclass('mcp_runtime.schema_migrations') IS NOT NULL")"
known_schema_exists="$(psql_admin --tuples-only --no-align --command \
  "SELECT to_regnamespace('search_position') IS NOT NULL
       OR to_regnamespace('daily_reporting') IS NOT NULL
       OR to_regnamespace('control') IS NOT NULL
       OR to_regnamespace('wb_automation') IS NOT NULL")"

create_ledger() {
  psql_admin <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(731928461017004201);
CREATE SCHEMA IF NOT EXISTS mcp_runtime;
REVOKE ALL ON SCHEMA mcp_runtime FROM PUBLIC;
CREATE TABLE IF NOT EXISTS mcp_runtime.schema_migrations (
    migration_id text PRIMARY KEY
        CHECK (migration_id ~ '^[0-9]{3}_[a-z0-9_]+[.]sql$'),
    sha256 char(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('applying', 'applied')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    CHECK (
        (state = 'applying' AND applied_at IS NULL)
        OR (state = 'applied' AND applied_at IS NOT NULL)
    )
);
REVOKE ALL ON TABLE mcp_runtime.schema_migrations FROM PUBLIC;
COMMIT;
SQL
}

# Renders the derived migration set as a quoted SQL id list. The names are
# constrained by the ledger's own CHECK to `[0-9]{3}_[a-z0-9_]+[.]sql`, so no
# name that reaches this point can carry a quote.
known_migration_id_list() {
  known_list=""
  for known_file in $migrations; do
    if [ -z "$known_list" ]; then
      known_list="'$known_file'"
    else
      known_list="$known_list, '$known_file'"
    fi
  done
  printf '%s' "$known_list"
}

unexpected_migration_count() {
  psql_admin --tuples-only --no-align --command \
    "SELECT count(*) FROM mcp_runtime.schema_migrations
     WHERE migration_id NOT IN ($(known_migration_id_list))"
}

refuse_unknown_migrations() {
  if [ "$(unexpected_migration_count)" != "0" ]; then
    echo "database contains migrations unknown to this binary; rollback is refused" >&2
    exit 1
  fi
}

if [ "$mode" = "--baseline-current" ]; then
  if [ "$known_schema_exists" != "t" ]; then
    echo "cannot baseline an empty database; run the normal migrator" >&2
    exit 1
  fi
  create_ledger
  refuse_unknown_migrations
  POSITION_DB_REQUIRE_MIGRATION_LEDGER=false \
    /usr/local/bin/position-db-healthcheck
  for file in $migrations; do
    path="$migration_dir/$file"
    if [ ! -f "$path" ] || [ -L "$path" ]; then
      echo "migration file is unavailable or unsafe: $file" >&2
      exit 1
    fi
    checksum="$(sha256sum "$path" | awk '{ print $1 }')"
    psql_admin --set migration_id="$file" --set checksum="$checksum" <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(731928461017004201);
INSERT INTO mcp_runtime.schema_migrations (
    migration_id, sha256, state, started_at, applied_at
) VALUES (
    :'migration_id', :'checksum', 'applied', clock_timestamp(), clock_timestamp()
)
ON CONFLICT (migration_id) DO NOTHING;
SELECT EXISTS (
    SELECT 1 FROM mcp_runtime.schema_migrations
    WHERE migration_id = :'migration_id'
      AND sha256 = :'checksum'
      AND state = 'applied'
) AS ledger_entry_valid \gset
\if :ledger_entry_valid
\else
  \echo 'migration baseline conflicts with existing ledger'
  \quit 1
\endif
COMMIT;
SQL
  done
  "$migration_dir/003_roles.sh"
  echo "migration ledger baselined at $latest_migration"
  exit 0
fi

if [ "$ledger_exists" != "t" ] && [ "$known_schema_exists" = "t" ]; then
  echo "existing schema has no migration ledger; take a verified backup, then run:" >&2
  echo "  migrate-position-db --baseline-current" >&2
  exit 1
fi
create_ledger
refuse_unknown_migrations
if [ "$known_schema_exists" = "t" ]; then
  ledger_count="$(psql_admin --tuples-only --no-align --command \
    "SELECT count(*) FROM mcp_runtime.schema_migrations")"
  if [ "$ledger_count" = "0" ]; then
    echo "existing schema has an empty migration ledger; complete the reviewed baseline" >&2
    echo "  migrate-position-db --baseline-current" >&2
    exit 1
  fi
fi

roles_refreshed=false
for file in $migrations; do
  path="$migration_dir/$file"
  if [ ! -f "$path" ] || [ -L "$path" ]; then
    echo "migration file is unavailable or unsafe: $file" >&2
    exit 1
  fi
  checksum="$(sha256sum "$path" | awk '{ print $1 }')"
  recorded="$(psql_admin --tuples-only --no-align \
    --command "SELECT state || '|' || sha256
               FROM mcp_runtime.schema_migrations
               WHERE migration_id = '$file'")"
  if [ -n "$recorded" ]; then
    if [ "$recorded" != "applied|$checksum" ]; then
      echo "migration ledger mismatch or interrupted migration: $file" >&2
      exit 1
    fi
  else
    psql_admin --set migration_id="$file" --set checksum="$checksum" <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(731928461017004201);
INSERT INTO mcp_runtime.schema_migrations (
    migration_id, sha256, state, started_at, applied_at
) VALUES (:'migration_id', :'checksum', 'applying', clock_timestamp(), NULL);
COMMIT;
SQL
    if ! psql_admin --file "$path"; then
      echo "migration failed and remains marked applying: $file" >&2
      exit 1
    fi
    psql_admin --set migration_id="$file" --set checksum="$checksum" <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(731928461017004201);
UPDATE mcp_runtime.schema_migrations
SET state = 'applied', applied_at = clock_timestamp()
WHERE migration_id = :'migration_id'
  AND sha256 = :'checksum'
  AND state = 'applying';
SELECT EXISTS (
    SELECT 1 FROM mcp_runtime.schema_migrations
    WHERE migration_id = :'migration_id'
      AND sha256 = :'checksum'
      AND state = 'applied'
) AS ledger_entry_valid \gset
\if :ledger_entry_valid
\else
  \echo 'migration ledger finalization failed'
  \quit 1
\endif
COMMIT;
SQL
  fi

  if [ "$file" = "002_wb_official_history.sql" ]; then
    "$migration_dir/003_roles.sh"
    roles_refreshed=true
  fi
done

if [ "$roles_refreshed" != true ]; then
  echo "role bootstrap did not run at the required migration boundary" >&2
  exit 1
fi

echo "database migrations verified through $latest_migration"
