#!/bin/sh
set -eu

: "${POSITION_COLLECTOR_DB_PASSWORD:?POSITION_COLLECTOR_DB_PASSWORD is required}"
: "${POSITION_READER_DB_PASSWORD:?POSITION_READER_DB_PASSWORD is required}"

PGPASSWORD="$POSTGRES_PASSWORD" psql --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=db_name="$POSTGRES_DB" \
  --set=collector_password="$POSITION_COLLECTOR_DB_PASSWORD" \
  --set=reader_password="$POSITION_READER_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE position_collector LOGIN PASSWORD %L', :'collector_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_collector')
\gexec

SELECT format('CREATE ROLE position_reader LOGIN PASSWORD %L', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_reader')
\gexec

ALTER ROLE position_collector SET statement_timeout = '60s';
ALTER ROLE position_collector SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE position_reader SET default_transaction_read_only = on;
ALTER ROLE position_reader SET statement_timeout = '15s';
ALTER ROLE position_reader SET idle_in_transaction_session_timeout = '15s';

GRANT CONNECT ON DATABASE :"db_name" TO position_collector, position_reader;
GRANT USAGE ON SCHEMA search_position TO position_collector, position_reader;

GRANT SELECT ON search_position.monitors TO position_collector;
GRANT SELECT, INSERT, UPDATE ON search_position.collection_runs TO position_collector;
GRANT SELECT, INSERT ON search_position.measurements TO position_collector;
GRANT SELECT, INSERT ON search_position.alerts TO position_collector;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA search_position TO position_collector;

GRANT SELECT ON ALL TABLES IN SCHEMA search_position TO position_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA search_position
    GRANT SELECT ON TABLES TO position_reader;
SQL
