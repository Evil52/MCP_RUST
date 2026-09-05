#!/bin/sh

set -eu

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

mode="${1:-migrate}"
migration_dir="/opt/mcp-ozon/migrations"
migrations="
001_schema.sql
002_ozon_collector_contract.sql
002_wb_official_history.sql
004_ozon_postgres_adapter.sql
005_daily_reporting_outbox.sql
006_daily_report_snapshots.sql
007_daily_reporting_optional_metrics.sql
008_daily_reporting_artifact_identity.sql
009_daily_reporting_generation_backoff.sql
010_daily_reporting_observation_window.sql
011_daily_reporting_collection_claims.sql
012_daily_reporting_delivery_error_classes.sql
013_daily_reporting_delivery_reconciliation.sql
014_daily_reporting_period_preserving_catchup.sql
015_daily_reporting_ozon_finance.sql
016_daily_reporting_unit_economics.sql
017_daily_reporting_advertising_extensions.sql
018_daily_reporting_recovery_observation_window.sql
019_daily_reporting_mcp_read_views.sql
020_wb_control_plans.sql
021_wb_automation_state.sql
022_wb_automation_explicit_resume.sql
023_daily_reporting_ozon_refresh_queue.sql
024_ozon_control_campaign_plans.sql
025_ozon_durable_launch_workflow.sql
026_marketplace_sales_refresh_queue.sql
027_position_latest_lookup.sql
"

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

unexpected_migration_count() {
  psql_admin --tuples-only --no-align --command \
    "SELECT count(*) FROM mcp_runtime.schema_migrations
     WHERE migration_id NOT IN (
       '001_schema.sql',
       '002_ozon_collector_contract.sql',
       '002_wb_official_history.sql',
       '004_ozon_postgres_adapter.sql',
       '005_daily_reporting_outbox.sql',
       '006_daily_report_snapshots.sql',
       '007_daily_reporting_optional_metrics.sql',
       '008_daily_reporting_artifact_identity.sql',
       '009_daily_reporting_generation_backoff.sql',
       '010_daily_reporting_observation_window.sql',
       '011_daily_reporting_collection_claims.sql',
       '012_daily_reporting_delivery_error_classes.sql',
       '013_daily_reporting_delivery_reconciliation.sql',
       '014_daily_reporting_period_preserving_catchup.sql',
       '015_daily_reporting_ozon_finance.sql',
       '016_daily_reporting_unit_economics.sql',
       '017_daily_reporting_advertising_extensions.sql',
       '018_daily_reporting_recovery_observation_window.sql',
       '019_daily_reporting_mcp_read_views.sql',
       '020_wb_control_plans.sql',
       '021_wb_automation_state.sql',
       '022_wb_automation_explicit_resume.sql',
       '023_daily_reporting_ozon_refresh_queue.sql',
       '024_ozon_control_campaign_plans.sql',
       '025_ozon_durable_launch_workflow.sql',
       '026_marketplace_sales_refresh_queue.sql',
       '027_position_latest_lookup.sql'
     )"
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
  echo "migration ledger baselined at 027_position_latest_lookup.sql"
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

echo "database migrations verified through 027_position_latest_lookup.sql"
