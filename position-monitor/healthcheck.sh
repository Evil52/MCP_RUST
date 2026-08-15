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
    AND to_regclass('search_position.published_measurements') IS NOT NULL
    AND to_regclass('search_position.published_alerts') IS NOT NULL
    AND to_regclass('search_position.ozon_collector_circuit') IS NOT NULL
    AND to_regclass('search_position.ozon_region_request_budgets') IS NOT NULL
    AND to_regclass('search_position.ozon_request_budget_usage') IS NOT NULL
    AND to_regclass('search_position.wb_search_targets') IS NOT NULL
    AND to_regclass('search_position.wb_bid_targets') IS NOT NULL
    AND to_regclass('search_position.wb_collection_runs') IS NOT NULL
    AND to_regclass('search_position.wb_search_snapshots') IS NOT NULL
    AND to_regclass('search_position.wb_bid_snapshots') IS NOT NULL
    AND to_regclass('search_position.latest_wb_search_snapshots') IS NOT NULL
    AND to_regclass('search_position.latest_wb_bid_snapshots') IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_wb_search_target_update()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_ozon_collection_run_state()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.require_running_ozon_measurement_run()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.require_running_ozon_alert_run()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.open_ozon_collector_circuit(bigint,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.claim_ozon_request_budget(text,timestamp with time zone)'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_wb_bid_target_update()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_wb_collection_run_state()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.require_running_wb_snapshot_run()'
    ) IS NOT NULL
    AND (
        SELECT count(*) = 5
        FROM pg_trigger
        WHERE tgname IN (
            'wb_search_targets_enforce_update',
            'wb_bid_targets_enforce_update',
            'wb_collection_runs_enforce_state',
            'wb_search_snapshots_require_running_run',
            'wb_bid_snapshots_require_running_run'
        )
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 3
        FROM pg_trigger
        WHERE tgname IN (
            'collection_runs_enforce_state',
            'measurements_require_running_run',
            'alerts_require_running_run'
        )
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 2
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'measurements'
          AND column_name IN ('overall_position', 'placement')
    )
    AND (
        SELECT count(*) = 5
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'collection_runs'
          AND column_name IN (
              'scheduled_for',
              'queries_planned',
              'queries_attempted',
              'queries_succeeded',
              'monitors_planned'
          )
    )
    AND (
        SELECT count(*) = 0
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'wb_search_snapshots'
          AND column_name IN (
              'region',
              'region_code',
              'organic_position',
              'sponsored_position',
              'live_position'
          )
    )
    AND (
        SELECT count(*) = 3
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'latest_wb_search_snapshots'
          AND column_name IN (
              'is_live_position',
              'region',
              'placement_split_available'
          )
    )
    AND (
        SELECT count(*) = 2
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name IN (
              'latest_wb_search_snapshots',
              'latest_wb_bid_snapshots'
          )
          AND column_name = 'run_status'
    )
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
        'position_collector', 'search_position.collection_runs', 'INSERT'
    )
    AND has_column_privilege(
        'position_collector', 'search_position.collection_runs', 'status', 'UPDATE'
    )
    AND NOT has_column_privilege(
        'position_collector', 'search_position.collection_runs', 'scheduled_for', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.collection_runs', 'UPDATE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.measurements', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.measurements', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.measurements', 'DELETE'
    )
    AND has_function_privilege(
        'position_collector',
        'search_position.open_ozon_collector_circuit(bigint,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'position_collector',
        'search_position.claim_ozon_request_budget(text,timestamp with time zone)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.ozon_collector_circuit', 'UPDATE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'DELETE'
    )
    AND has_column_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'status', 'UPDATE'
    )
    AND NOT has_column_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'scheduled_for', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'UPDATE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_search_snapshots', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_snapshots', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_snapshots', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_bid_snapshots', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_snapshots', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_snapshots', 'DELETE'
    )
    AND NOT has_sequence_privilege(
        'position_collector', 'search_position.wb_search_targets_id_seq', 'USAGE'
    )
    AND NOT has_sequence_privilege(
        'position_collector', 'search_position.wb_bid_targets_id_seq', 'USAGE'
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
    AND NOT has_table_privilege(
        'position_reader', 'search_position.collection_runs', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.measurements', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.alerts', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.published_measurements', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.published_alerts', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.latest_measurements', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.ozon_collector_circuit', 'SELECT'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.claim_ozon_request_budget(text,timestamp with time zone)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_search_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.wb_search_targets', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.wb_bid_targets', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_collection_runs', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_bid_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.latest_wb_search_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.latest_wb_bid_snapshots', 'SELECT'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_default_acl AS defaults
        CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS expanded_acl
        WHERE defaults.defaclnamespace = 'search_position'::regnamespace
          AND defaults.defaclobjtype = 'r'
          AND expanded_acl.grantee = 'position_reader'::regrole
          AND expanded_acl.privilege_type = 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_search_snapshots', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_bid_snapshots', 'UPDATE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.enforce_wb_search_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.enforce_wb_search_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.enforce_wb_bid_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.enforce_wb_bid_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.enforce_wb_collection_run_state()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.enforce_wb_collection_run_state()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.require_running_wb_snapshot_run()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.require_running_wb_snapshot_run()',
        'EXECUTE'
    )
    AND (
        SELECT rolconfig @> ARRAY['default_transaction_read_only=on']
        FROM pg_roles
        WHERE rolname = 'position_reader'
    );
SQL
} 2>/dev/null)"

[ "$healthy" = "t" ]
