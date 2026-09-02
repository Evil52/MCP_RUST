#!/bin/sh
set -eu

: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${POSITION_COLLECTOR_DB_PASSWORD:?POSITION_COLLECTOR_DB_PASSWORD is required}"
: "${POSITION_READER_DB_PASSWORD:?POSITION_READER_DB_PASSWORD is required}"
: "${REPORT_WORKER_DB_PASSWORD:?REPORT_WORKER_DB_PASSWORD is required}"
: "${REPORT_COLLECTOR_DB_PASSWORD:?REPORT_COLLECTOR_DB_PASSWORD is required}"
: "${REPORT_REFRESH_REQUESTER_DB_PASSWORD:?REPORT_REFRESH_REQUESTER_DB_PASSWORD is required}"
: "${CONTROL_WRITER_DB_PASSWORD:?CONTROL_WRITER_DB_PASSWORD is required}"
: "${WB_AUTOMATION_DB_PASSWORD:?WB_AUTOMATION_DB_PASSWORD is required}"

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
validate_password REPORT_WORKER_DB_PASSWORD "$REPORT_WORKER_DB_PASSWORD"
validate_password REPORT_COLLECTOR_DB_PASSWORD "$REPORT_COLLECTOR_DB_PASSWORD"
validate_password REPORT_REFRESH_REQUESTER_DB_PASSWORD "$REPORT_REFRESH_REQUESTER_DB_PASSWORD"
validate_password CONTROL_WRITER_DB_PASSWORD "$CONTROL_WRITER_DB_PASSWORD"
validate_password WB_AUTOMATION_DB_PASSWORD "$WB_AUTOMATION_DB_PASSWORD"

if [ "$POSTGRES_USER" = position_collector ] ||
   [ "$POSTGRES_USER" = position_reader ] ||
   [ "$POSTGRES_USER" = report_worker ] ||
   [ "$POSTGRES_USER" = report_collector ] ||
   [ "$POSTGRES_USER" = report_refresh_requester ] ||
   [ "$POSTGRES_USER" = control_writer ] ||
   [ "$POSTGRES_USER" = wb_automation_writer ]; then
  echo "POSTGRES_USER must not reuse a restricted application role" >&2
  exit 1
fi
if [ "$POSITION_COLLECTOR_DB_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ] ||
   [ "$POSITION_COLLECTOR_DB_PASSWORD" = "$REPORT_WORKER_DB_PASSWORD" ] ||
   [ "$POSITION_COLLECTOR_DB_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$POSITION_READER_DB_PASSWORD" = "$REPORT_WORKER_DB_PASSWORD" ] ||
   [ "$POSITION_READER_DB_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$REPORT_WORKER_DB_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" = "$POSITION_COLLECTOR_DB_PASSWORD" ] ||
   [ "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ] ||
   [ "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" = "$REPORT_WORKER_DB_PASSWORD" ] ||
   [ "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" = "$CONTROL_WRITER_DB_PASSWORD" ] ||
   [ "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" = "$WB_AUTOMATION_DB_PASSWORD" ] ||
   [ "$CONTROL_WRITER_DB_PASSWORD" = "$POSITION_COLLECTOR_DB_PASSWORD" ] ||
   [ "$CONTROL_WRITER_DB_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ] ||
   [ "$CONTROL_WRITER_DB_PASSWORD" = "$REPORT_WORKER_DB_PASSWORD" ] ||
   [ "$CONTROL_WRITER_DB_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$WB_AUTOMATION_DB_PASSWORD" = "$POSITION_COLLECTOR_DB_PASSWORD" ] ||
   [ "$WB_AUTOMATION_DB_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ] ||
   [ "$WB_AUTOMATION_DB_PASSWORD" = "$REPORT_WORKER_DB_PASSWORD" ] ||
   [ "$WB_AUTOMATION_DB_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$WB_AUTOMATION_DB_PASSWORD" = "$CONTROL_WRITER_DB_PASSWORD" ]; then
  echo "all application database passwords must be different" >&2
  exit 1
fi
if [ "$POSTGRES_PASSWORD" = "$POSITION_COLLECTOR_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$POSITION_READER_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$REPORT_WORKER_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$REPORT_COLLECTOR_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$REPORT_REFRESH_REQUESTER_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$CONTROL_WRITER_DB_PASSWORD" ] ||
   [ "$POSTGRES_PASSWORD" = "$WB_AUTOMATION_DB_PASSWORD" ]; then
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
\getenv report_worker_password REPORT_WORKER_DB_PASSWORD
\getenv report_collector_password REPORT_COLLECTOR_DB_PASSWORD
\getenv report_refresh_requester_password REPORT_REFRESH_REQUESTER_DB_PASSWORD
\getenv control_writer_password CONTROL_WRITER_DB_PASSWORD
\getenv wb_automation_password WB_AUTOMATION_DB_PASSWORD

BEGIN;

SELECT format('CREATE ROLE position_collector LOGIN PASSWORD %L', :'collector_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_collector')
\gexec

SELECT format('CREATE ROLE position_reader LOGIN PASSWORD %L', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_reader')
\gexec

SELECT format('CREATE ROLE report_worker LOGIN PASSWORD %L', :'report_worker_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_worker')
\gexec

SELECT format('CREATE ROLE report_collector LOGIN PASSWORD %L', :'report_collector_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_collector')
\gexec

SELECT format(
    'CREATE ROLE report_refresh_requester LOGIN PASSWORD %L',
    :'report_refresh_requester_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'report_refresh_requester'
)
\gexec

SELECT format('CREATE ROLE control_writer LOGIN PASSWORD %L', :'control_writer_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'control_writer')
\gexec

SELECT format(
    'CREATE ROLE wb_automation_writer LOGIN PASSWORD %L',
    :'wb_automation_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wb_automation_writer')
\gexec

ALTER ROLE position_collector WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD :'collector_password';
ALTER ROLE position_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 16 PASSWORD :'reader_password';
ALTER ROLE report_worker WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD :'report_worker_password';
ALTER ROLE report_collector WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD :'report_collector_password';
ALTER ROLE report_refresh_requester WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4
    PASSWORD :'report_refresh_requester_password';
ALTER ROLE control_writer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD :'control_writer_password';
ALTER ROLE wb_automation_writer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2
    PASSWORD :'wb_automation_password';

-- Membership is an independent cluster-wide privilege path: NOINHERIT still
-- permits SET ROLE. Converge both directions so neither marketplace writer
-- gains another role or becomes a grantable privilege bundle for one.
SELECT format(
    'REVOKE %I FROM %I', granted_role.rolname, member_role.rolname
)
FROM pg_auth_members AS membership
JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid
JOIN pg_roles AS member_role ON member_role.oid = membership.member
WHERE granted_role.rolname IN (
        'report_refresh_requester', 'control_writer', 'wb_automation_writer'
      )
   OR member_role.rolname IN (
        'report_refresh_requester', 'control_writer', 'wb_automation_writer'
      )
ORDER BY granted_role.rolname, member_role.rolname
\gexec

ALTER ROLE position_collector SET statement_timeout = '60s';
ALTER ROLE position_collector SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE position_reader SET default_transaction_read_only = on;
ALTER ROLE position_reader SET statement_timeout = '15s';
ALTER ROLE position_reader SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE report_worker SET statement_timeout = '60s';
ALTER ROLE report_worker SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE report_collector SET statement_timeout = '60s';
ALTER ROLE report_collector SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE report_refresh_requester SET statement_timeout = '15s';
ALTER ROLE report_refresh_requester SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE control_writer SET statement_timeout = '30s';
ALTER ROLE control_writer SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE wb_automation_writer SET statement_timeout = '30s';
ALTER ROLE wb_automation_writer SET idle_in_transaction_session_timeout = '15s';

-- Application roles are cluster-wide. Revoke PUBLIC and stale direct ingress
-- from every database before allowing only this application's database below.
SELECT format(
    'REVOKE CREATE, CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', datname
)
FROM pg_database
\gexec

SELECT format(
    'REVOKE CREATE, CONNECT, TEMPORARY ON DATABASE %I FROM %I',
    database_row.datname, application_role.rolname
)
FROM pg_database AS database_row
CROSS JOIN (
    VALUES
      ('position_collector'),
      ('position_reader'),
      ('report_worker'),
      ('report_collector'),
      ('report_refresh_requester'),
      ('control_writer'),
      ('wb_automation_writer')
) AS application_role(rolname)
ORDER BY database_row.datname, application_role.rolname
\gexec

SELECT format(
    'REVOKE CREATE ON SCHEMA %I FROM PUBLIC', namespace_row.nspname
)
FROM pg_namespace AS namespace_row
WHERE namespace_row.nspname <> 'information_schema'
  AND namespace_row.nspname !~ '^pg_'
ORDER BY namespace_row.nspname
\gexec

SELECT format(
    'REVOKE CREATE ON SCHEMA %I FROM %I',
    namespace_row.nspname, application_role.rolname
)
FROM pg_namespace AS namespace_row
CROSS JOIN (
    VALUES
      ('position_collector'),
      ('position_reader'),
      ('report_worker'),
      ('report_collector'),
      ('report_refresh_requester'),
      ('control_writer'),
      ('wb_automation_writer')
) AS application_role(rolname)
WHERE namespace_row.nspname <> 'information_schema'
  AND namespace_row.nspname !~ '^pg_'
ORDER BY namespace_row.nspname, application_role.rolname
\gexec

GRANT CONNECT ON DATABASE :"db_name" TO position_collector, position_reader, report_worker,
    report_collector, report_refresh_requester, control_writer, wb_automation_writer;
GRANT USAGE ON SCHEMA search_position TO position_collector, position_reader;

-- Make re-running this role bootstrap converge to the exact ACL instead of
-- retaining stale grants from an older schema revision.
REVOKE ALL ON ALL TABLES IN SCHEMA search_position
    FROM position_collector, position_reader, report_worker, report_collector,
    report_refresh_requester, control_writer, wb_automation_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA search_position
    FROM position_collector, position_reader, report_worker, report_collector,
    report_refresh_requester, control_writer, wb_automation_writer;

GRANT SELECT ON search_position.monitors TO position_collector;
GRANT SELECT, INSERT ON search_position.collection_runs TO position_collector;
GRANT UPDATE (
    finished_at,
    status,
    monitors_attempted,
    monitors_succeeded,
    queries_attempted,
    queries_succeeded,
    error_class,
    http_status
) ON search_position.collection_runs TO position_collector;
GRANT INSERT ON search_position.measurements TO position_collector;
GRANT INSERT ON search_position.alerts TO position_collector;
GRANT EXECUTE ON FUNCTION
    search_position.open_ozon_collector_circuit(bigint, text),
    search_position.claim_ozon_request_budget(text, timestamptz)
    TO position_collector;
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
    search_position.published_measurements,
    search_position.published_alerts,
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
