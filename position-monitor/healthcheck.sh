#!/bin/sh
set -eu

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

healthy="$({
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host=127.0.0.1 \
    --username="$POSTGRES_USER" \
    --dbname="$POSTGRES_DB" \
    --no-psqlrc \
    --no-align \
    --tuples-only \
    --set=ON_ERROR_STOP=1 <<'SQL'
SELECT
    to_regclass('search_position.monitors') IS NOT NULL
    AND to_regclass('search_position.collection_runs') IS NOT NULL
    AND to_regclass('search_position.measurements') IS NOT NULL
    AND to_regclass('search_position.latest_measurements') IS NOT NULL
    AND has_database_privilege('position_collector', current_database(), 'CONNECT')
    AND has_database_privilege('position_reader', current_database(), 'CONNECT')
    AND NOT has_database_privilege('position_collector', current_database(), 'TEMP')
    AND NOT has_database_privilege('position_reader', current_database(), 'TEMP')
    AND has_table_privilege(
        'position_collector', 'search_position.monitors', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.monitors', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.monitors', 'DELETE'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.monitors', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.monitors', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.monitors', 'DELETE'
    )
    AND (
        SELECT rolconfig @> ARRAY['default_transaction_read_only=on']
        FROM pg_roles
        WHERE rolname = 'position_reader'
    );
SQL
} 2>/dev/null)"

[ "$healthy" = "t" ]
