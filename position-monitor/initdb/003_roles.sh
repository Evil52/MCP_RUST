#!/bin/sh
set -eu

: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${POSITION_COLLECTOR_DB_PASSWORD:?POSITION_COLLECTOR_DB_PASSWORD is required}"
: "${POSITION_READER_DB_PASSWORD:?POSITION_READER_DB_PASSWORD is required}"

validate_password() {
  label=$1
  value=$2
  case "$value" in
    replace-with-* | *placeholder*)
      echo "$label must not use an example placeholder" >&2
      exit 1
      ;;
  esac
  if [ "${#value}" -lt 24 ]; then
    echo "$label must contain at least 24 characters" >&2
    exit 1
  fi
}

validate_password POSTGRES_PASSWORD "$POSTGRES_PASSWORD"
validate_password POSITION_COLLECTOR_DB_PASSWORD "$POSITION_COLLECTOR_DB_PASSWORD"
validate_password POSITION_READER_DB_PASSWORD "$POSITION_READER_DB_PASSWORD"

if [ "$POSTGRES_USER" = position_collector ] || [ "$POSTGRES_USER" = position_reader ]; then
  echo "POSTGRES_USER must not reuse a restricted application role" >&2
  exit 1
fi
if [ "$POSITION_COLLECTOR_DB_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ]; then
  echo "collector and reader database passwords must be different" >&2
  exit 1
fi
if [ "$POSTGRES_PASSWORD" = "$POSITION_COLLECTOR_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ]; then
  echo "application database passwords must differ from the admin password" >&2
  exit 1
fi

PGPASSWORD="$POSTGRES_PASSWORD" psql --set=ON_ERROR_STOP=1 \
  --no-psqlrc \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=db_name="$POSTGRES_DB" <<'SQL'
\getenv collector_password POSITION_COLLECTOR_DB_PASSWORD
\getenv reader_password POSITION_READER_DB_PASSWORD

BEGIN;

SELECT format('CREATE ROLE position_collector LOGIN PASSWORD %L', :'collector_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_collector')
\gexec

SELECT format('CREATE ROLE position_reader LOGIN PASSWORD %L', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_reader')
\gexec

ALTER ROLE position_collector WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD :'collector_password';
ALTER ROLE position_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 16 PASSWORD :'reader_password';

ALTER ROLE position_collector SET statement_timeout = '60s';
ALTER ROLE position_collector SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE position_reader SET default_transaction_read_only = on;
ALTER ROLE position_reader SET statement_timeout = '15s';
ALTER ROLE position_reader SET idle_in_transaction_session_timeout = '15s';

-- Application roles are cluster-wide. Revoke the default PUBLIC ingress and
-- TEMP privilege from every connectable database before explicitly allowing
-- only this application's database below.
SELECT format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', datname)
FROM pg_database
WHERE datallowconn
\gexec

GRANT CONNECT ON DATABASE :"db_name" TO position_collector, position_reader;
GRANT USAGE ON SCHEMA search_position TO position_collector, position_reader;

-- Make re-running this role bootstrap converge to the exact ACL instead of
-- retaining stale grants from an older schema revision.
REVOKE ALL ON ALL TABLES IN SCHEMA search_position
    FROM position_collector, position_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA search_position
    FROM position_collector, position_reader;

GRANT SELECT ON search_position.monitors TO position_collector;
GRANT SELECT, INSERT, UPDATE ON search_position.collection_runs TO position_collector;
GRANT SELECT, INSERT ON search_position.measurements TO position_collector;
GRANT SELECT, INSERT ON search_position.alerts TO position_collector;
GRANT SELECT ON search_position.wb_search_targets TO position_collector;
GRANT SELECT ON search_position.wb_bid_targets TO position_collector;
GRANT SELECT, INSERT ON search_position.wb_collection_runs TO position_collector;
GRANT UPDATE (
    finished_at,
    source_updated_at,
    status,
    targets_attempted,
    targets_succeeded,
    error_class,
    http_status
) ON search_position.wb_collection_runs TO position_collector;
GRANT INSERT ON search_position.wb_search_snapshots TO position_collector;
GRANT INSERT ON search_position.wb_bid_snapshots TO position_collector;
GRANT USAGE, SELECT ON SEQUENCE search_position.collection_runs_id_seq,
    search_position.measurements_id_seq,
    search_position.alerts_id_seq,
    search_position.wb_collection_runs_id_seq,
    search_position.wb_search_snapshots_id_seq,
    search_position.wb_bid_snapshots_id_seq TO position_collector;

GRANT SELECT ON search_position.monitors,
    search_position.collection_runs,
    search_position.measurements,
    search_position.alerts,
    search_position.latest_measurements,
    search_position.hourly_position_summary,
    search_position.wb_search_targets,
    search_position.wb_bid_targets,
    search_position.latest_wb_search_snapshots,
    search_position.latest_wb_bid_snapshots TO position_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA search_position
    REVOKE SELECT ON TABLES FROM position_reader;

COMMIT;
SQL
