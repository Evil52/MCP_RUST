#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
suffix="${GITHUB_RUN_ID:-local}-${RANDOM}-$$"
image="${POSITION_SCHEMA_TEST_IMAGE:-mcp-ozon-position-schema-test:${suffix}}"
keep_image="${POSITION_SCHEMA_TEST_KEEP_IMAGE:-false}"
container="mcp-ozon-position-schema-test-${suffix}"
admin_password="position-admin-schema-test"
collector_password="position-collector-schema-test"
reader_password="position-reader-schema-test"
report_worker_password="report-worker-schema-test"
report_collector_password="report-collector-schema-test"
control_writer_password="control-writer-schema-test"
wb_automation_password="wb-automation-schema-test"

case "$keep_image" in
  true | false) ;;
  *)
    echo "POSITION_SCHEMA_TEST_KEEP_IMAGE must be true or false" >&2
    exit 1
    ;;
esac

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  if [[ "$keep_image" == false ]]; then
    docker image rm "$image" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

expect_exact_validation_failure() {
  local description="$1" expected="$2" output
  shift 2
  if output="$("$@" 2>&1)"; then
    echo "$description unexpectedly succeeded" >&2
    exit 1
  fi
  if [[ "$output" != "$expected" ]]; then
    echo "$description returned an unexpected error" >&2
    printf 'expected: %s\nactual:   %s\n' "$expected" "$output" >&2
    exit 1
  fi
}

expect_failure_containing() {
  local description="$1" expected="$2" output
  shift 2
  if output="$("$@" 2>&1)"; then
    echo "$description unexpectedly succeeded" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected"* ]]; then
    echo "$description failed for an unexpected reason" >&2
    printf 'expected error containing: %s\nactual error:\n%s\n' "$expected" "$output" >&2
    exit 1
  fi
}

assert_control_schema_contract() {
  local description="$1" contract
  shift
  contract="$({ "$@" --tuples-only --no-align --command "
    WITH expected_triggers(
        table_name, trigger_name, function_name, trigger_type
    ) AS (
        VALUES
          ('wb_plans', 'wb_plans_transition_guard',
           'enforce_wb_plan_transition', 19),
          ('wb_policy_revisions', 'wb_policy_revisions_validate',
           'validate_wb_policy_revision_insert', 7),
          ('wb_policy_revisions', 'wb_policy_revisions_append_only',
           'reject_wb_append_only_mutation', 27),
          ('wb_prepare_reservations', 'wb_prepare_reservations_validate',
           'validate_wb_prepare_reservation_insert', 7),
          ('wb_prepare_reservations', 'wb_prepare_reservations_append_only',
           'reject_wb_append_only_mutation', 27),
          ('wb_plans', 'wb_plans_validate_insert',
           'validate_wb_plan_insert', 7),
          ('wb_runtime_gates', 'wb_runtime_gates_validate_write',
           'validate_wb_runtime_gate_write', 23),
          ('wb_plan_approvals', 'wb_plan_approvals_validate',
           'validate_wb_approval_insert', 7),
          ('wb_plan_approvals', 'wb_plan_approvals_append_only',
           'reject_wb_append_only_mutation', 27),
          ('wb_action_reservations', 'wb_action_reservations_validate',
           'validate_wb_reservation_insert', 7),
          ('wb_action_reservations', 'wb_action_reservations_append_only',
           'reject_wb_append_only_mutation', 27),
          ('wb_audit_events', 'wb_audit_events_append_only',
           'reject_wb_append_only_mutation', 27)
    ),
    other_app_roles(role_name) AS (
        VALUES
          ('position_collector'),
          ('position_reader'),
          ('report_collector'),
          ('report_worker'),
          ('wb_automation_writer')
    ),
    actual_triggers(
        table_name, trigger_name, function_name, trigger_type, enabled
    ) AS (
        SELECT relation.relname::text, trigger.tgname::text,
               routine.proname::text, trigger.tgtype::integer,
               trigger.tgenabled
        FROM pg_trigger AS trigger
        JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        JOIN pg_proc AS routine ON routine.oid = trigger.tgfoid
        WHERE namespace.nspname = 'control'
          AND NOT trigger.tgisinternal
    )
    SELECT
      (
          SELECT string_agg(relation.relname::text, ',' ORDER BY relation.relname)
                 = 'wb_action_reservations,wb_audit_events,wb_plan_approvals,' ||
                   'wb_plans,wb_policy_revisions,wb_prepare_reservations,' ||
                   'wb_runtime_gates'
          FROM pg_class AS relation
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          WHERE namespace.nspname = 'control'
            AND relation.relkind IN ('r', 'p')
      )
      AND (
          SELECT string_agg(
                     relation.relname::text || ':' || acl.privilege_type,
                     ',' ORDER BY relation.relname, acl.privilege_type
                 ) =
                 'wb_action_reservations:INSERT,wb_action_reservations:SELECT,' ||
                 'wb_audit_events:INSERT,wb_audit_events:SELECT,' ||
                 'wb_plan_approvals:INSERT,wb_plan_approvals:SELECT,' ||
                 'wb_plans:INSERT,wb_plans:SELECT,' ||
                 'wb_policy_revisions:INSERT,wb_policy_revisions:SELECT,' ||
                 'wb_prepare_reservations:INSERT,wb_prepare_reservations:SELECT,' ||
                 'wb_runtime_gates:SELECT'
          FROM pg_class AS relation
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN LATERAL aclexplode(
              coalesce(relation.relacl, acldefault('r', relation.relowner))
          ) AS acl
          WHERE namespace.nspname = 'control'
            AND relation.relkind IN ('r', 'p')
            AND acl.grantee = 'control_writer'::regrole
      )
      AND (
          SELECT string_agg(
                     relation.relname::text || '.' || attribute.attname::text ||
                     ':' || acl.privilege_type,
                     ',' ORDER BY relation.relname, attribute.attname,
                                  acl.privilege_type
                 ) =
                 'wb_plans.apply_started_at:UPDATE,' ||
                 'wb_plans.finished_at:UPDATE,' ||
                 'wb_plans.last_error_class:UPDATE,' ||
                 'wb_plans.readback_json:UPDATE,' ||
                 'wb_plans.status:UPDATE,' ||
                 'wb_plans.write_response_json:UPDATE'
          FROM pg_attribute AS attribute
          JOIN pg_class AS relation ON relation.oid = attribute.attrelid
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl
          WHERE namespace.nspname = 'control'
            AND attribute.attnum > 0
            AND NOT attribute.attisdropped
            AND acl.grantee = 'control_writer'::regrole
      )
      AND (
          SELECT string_agg(acl.privilege_type, ',' ORDER BY acl.privilege_type)
                 = 'USAGE'
          FROM pg_namespace AS namespace
          CROSS JOIN LATERAL aclexplode(
              coalesce(namespace.nspacl, acldefault('n', namespace.nspowner))
          ) AS acl
          WHERE namespace.nspname = 'control'
            AND acl.grantee = 'control_writer'::regrole
      )
      AND (
          SELECT string_agg(acl.privilege_type, ',' ORDER BY acl.privilege_type)
                 = 'SELECT,USAGE'
          FROM pg_class AS relation
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN LATERAL aclexplode(
              coalesce(relation.relacl, acldefault('S', relation.relowner))
          ) AS acl
          WHERE namespace.nspname = 'control'
            AND relation.relkind = 'S'
            AND relation.relname = 'wb_audit_events_id_seq'
            AND acl.grantee = 'control_writer'::regrole
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_namespace AS namespace
          CROSS JOIN LATERAL aclexplode(
              coalesce(namespace.nspacl, acldefault('n', namespace.nspowner))
          ) AS acl
          WHERE namespace.nspname = 'control'
            AND (
                acl.grantee = 0
                OR acl.grantee IN (
                    SELECT role_name::regrole::oid FROM other_app_roles
                )
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM other_app_roles
          WHERE has_schema_privilege(role_name::name, 'control', 'USAGE')
             OR has_schema_privilege(role_name::name, 'control', 'CREATE')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_auth_members AS membership
          WHERE membership.roleid = 'control_writer'::regrole
             OR membership.member = 'control_writer'::regrole
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_namespace AS namespace
          CROSS JOIN unnest(ARRAY[
              'position_collector', 'position_reader', 'report_worker',
              'report_collector', 'control_writer', 'wb_automation_writer'
          ]::name[]) AS application_role(role_name)
          WHERE namespace.nspname <> 'information_schema'
            AND namespace.nspname !~ '^pg_'
            AND has_schema_privilege(
                role_name, namespace.nspname, 'CREATE'
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_class AS relation
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN LATERAL aclexplode(
              coalesce(
                  relation.relacl,
                  acldefault(
                      (CASE WHEN relation.relkind = 'S' THEN 'S' ELSE 'r' END)::\"char\",
                      relation.relowner
                  )
              )
          ) AS acl
          WHERE namespace.nspname = 'control'
            AND (
                acl.grantee = 0
                OR acl.grantee IN (
                    SELECT role_name::regrole::oid FROM other_app_roles
                )
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_proc AS routine
          JOIN pg_namespace AS namespace
            ON namespace.oid = routine.pronamespace
          CROSS JOIN LATERAL aclexplode(
              coalesce(routine.proacl, acldefault('f', routine.proowner))
          ) AS acl
          WHERE namespace.nspname = 'control'
            AND (
                acl.grantee = 0
                OR acl.grantee = 'control_writer'::regrole
                OR acl.grantee IN (
                    SELECT role_name::regrole::oid FROM other_app_roles
                )
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_default_acl AS defaults
          CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS acl
          WHERE defaults.defaclnamespace = 'control'::regnamespace
            AND (
                acl.grantee = 0
                OR acl.grantee = 'control_writer'::regrole
                OR acl.grantee IN (
                    SELECT role_name::regrole::oid FROM other_app_roles
                )
            )
      )
      AND (
          SELECT string_agg(routine.proname::text, ',' ORDER BY routine.proname)
                 = 'enforce_wb_plan_transition,reject_wb_append_only_mutation,' ||
                   'validate_wb_approval_insert,validate_wb_plan_insert,' ||
                   'validate_wb_policy_revision_insert,' ||
                   'validate_wb_prepare_reservation_insert,' ||
                   'validate_wb_reservation_insert,validate_wb_runtime_gate_write'
          FROM pg_proc AS routine
          JOIN pg_namespace AS namespace
            ON namespace.oid = routine.pronamespace
          WHERE namespace.nspname = 'control'
            AND routine.prorettype = 'trigger'::regtype
      )
      AND (
          SELECT string_agg(
                     relation.relname::text || ':' || constraint_row.conname ||
                     ':' || constraint_row.contype::text,
                     ',' ORDER BY relation.relname, constraint_row.conname
                 ) =
                 'wb_plan_approvals:wb_approval_ttl:c,' ||
                 'wb_plans:wb_plan_state_shape:c,' ||
                 'wb_plans:wb_plan_ttl:c,' ||
                 'wb_plans:wb_plans_prepare_reservation_id_fkey:f,' ||
                 'wb_plans:wb_plans_prepare_reservation_id_key:u,' ||
                 'wb_prepare_reservations:wb_prepare_reservation_ttl:c,' ||
                 'wb_prepare_reservations:wb_prepare_reservations_pkey:p,' ||
                 'wb_prepare_reservations:' ||
                 'wb_prepare_reservations_policy_revision_fkey:f,' ||
                 'wb_runtime_gates:wb_runtime_gate_lease_bound:c,' ||
                 'wb_runtime_gates:wb_runtime_gate_scope:c'
          FROM pg_constraint AS constraint_row
          JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          WHERE namespace.nspname = 'control'
            AND constraint_row.conname IN (
                'wb_approval_ttl',
                'wb_plan_state_shape',
                'wb_plan_ttl',
                'wb_plans_prepare_reservation_id_fkey',
                'wb_plans_prepare_reservation_id_key',
                'wb_prepare_reservation_ttl',
                'wb_prepare_reservations_pkey',
                'wb_prepare_reservations_policy_revision_fkey',
                'wb_runtime_gate_lease_bound',
                'wb_runtime_gate_scope'
            )
      )
      AND (
          SELECT count(*) = 12
          FROM actual_triggers
      )
      AND NOT EXISTS (
          SELECT 1
          FROM expected_triggers AS expected
          LEFT JOIN actual_triggers AS actual
            ON actual.table_name = expected.table_name
           AND actual.trigger_name = expected.trigger_name
          WHERE actual.trigger_name IS NULL
             OR actual.function_name <> expected.function_name
             OR actual.trigger_type <> expected.trigger_type
             OR actual.enabled <> 'O'
      )
  "; } | tr -d '[:space:]')"
  if [[ "$contract" != t ]]; then
    echo "$description does not match the exact Control schema/ACL/trigger contract" >&2
    printf '%s\n' "$contract" >&2
    exit 1
  fi
}

expect_exact_validation_failure \
  "example admin-password validation" \
  "POSTGRES_PASSWORD must not use an example placeholder" \
  env \
  POSTGRES_USER=position_admin \
  POSTGRES_PASSWORD=replace-with-a-long-random-admin-password \
  POSTGRES_DB=ozon_positions \
  POSITION_COLLECTOR_DB_PASSWORD="$collector_password" \
  POSITION_READER_DB_PASSWORD="$reader_password" \
  REPORT_WORKER_DB_PASSWORD="$report_worker_password" \
  REPORT_COLLECTOR_DB_PASSWORD="$report_collector_password" \
  CONTROL_WRITER_DB_PASSWORD="$control_writer_password" \
  WB_AUTOMATION_DB_PASSWORD="$wb_automation_password" \
  "$project_root/position-monitor/initdb/003_roles.sh"

expect_exact_validation_failure \
  "admin/application role-collision validation" \
  "POSTGRES_USER must not reuse a restricted application role" \
  env \
  POSTGRES_USER=position_reader \
  POSTGRES_PASSWORD="$admin_password" \
  POSTGRES_DB=ozon_positions \
  POSITION_COLLECTOR_DB_PASSWORD="$collector_password" \
  POSITION_READER_DB_PASSWORD="$reader_password" \
  REPORT_WORKER_DB_PASSWORD="$report_worker_password" \
  REPORT_COLLECTOR_DB_PASSWORD="$report_collector_password" \
  CONTROL_WRITER_DB_PASSWORD="$control_writer_password" \
  WB_AUTOMATION_DB_PASSWORD="$wb_automation_password" \
  "$project_root/position-monitor/initdb/003_roles.sh"

docker build --pull --tag "$image" --file "$project_root/position-monitor/Dockerfile" \
  "$project_root/position-monitor"
image_user="$(docker image inspect --format '{{.Config.User}}' "$image")"
if [[ "$image_user" != postgres ]]; then
  echo "position database image must declare the non-root postgres user" >&2
  printf 'actual image user: %s\n' "$image_user" >&2
  exit 1
fi
docker run --detach --rm --name "$container" \
  --env POSTGRES_DB=ozon_positions \
  --env POSTGRES_USER=position_admin \
  --env POSTGRES_PASSWORD="$admin_password" \
  --env 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=scram-sha-256' \
  --env POSITION_COLLECTOR_DB_PASSWORD="$collector_password" \
  --env POSITION_READER_DB_PASSWORD="$reader_password" \
  --env REPORT_WORKER_DB_PASSWORD="$report_worker_password" \
  --env REPORT_COLLECTOR_DB_PASSWORD="$report_collector_password" \
  --env CONTROL_WRITER_DB_PASSWORD="$control_writer_password" \
  --env WB_AUTOMATION_DB_PASSWORD="$wb_automation_password" \
  "$image" >/dev/null

ready=false
for _ in $(seq 1 30); do
  if docker exec "$container" /usr/local/bin/position-db-healthcheck; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs "$container" >&2
  echo "position schema test database did not become ready" >&2
  exit 1
fi

admin_psql=(
  docker exec --env PGPASSWORD="$admin_password" "$container"
  psql --host 127.0.0.1 --username position_admin --dbname ozon_positions
  --no-psqlrc --set ON_ERROR_STOP=1
)
collector_psql=(
  docker exec --env PGPASSWORD="$collector_password" "$container"
  psql --host 127.0.0.1 --username position_collector --dbname ozon_positions
  --no-psqlrc --set ON_ERROR_STOP=1
)
reader_psql=(
  docker exec --env PGPASSWORD="$reader_password" "$container"
  psql --host 127.0.0.1 --username position_reader --dbname ozon_positions
  --no-psqlrc --set ON_ERROR_STOP=1
)
report_worker_psql=(
  docker exec --env PGPASSWORD="$report_worker_password" "$container"
  psql --host 127.0.0.1 --username report_worker --dbname ozon_positions
  --no-psqlrc --set ON_ERROR_STOP=1
)
report_collector_psql=(
  docker exec --env PGPASSWORD="$report_collector_password" "$container"
  psql --host 127.0.0.1 --username report_collector --dbname ozon_positions
  --no-psqlrc --set ON_ERROR_STOP=1
)
control_writer_psql=(
  docker exec --env PGPASSWORD="$control_writer_password" "$container"
  psql --host 127.0.0.1 --username control_writer --dbname ozon_positions
  --no-psqlrc --set ON_ERROR_STOP=1
)
assert_control_schema_contract \
  "fresh database" \
  "${admin_psql[@]}"

"${admin_psql[@]}" --command "
  GRANT position_reader TO control_writer;
  GRANT control_writer TO report_worker;
  GRANT CREATE, CONNECT, TEMPORARY ON DATABASE postgres TO
      position_collector, position_reader, report_worker,
      report_collector, control_writer, wb_automation_writer;
  GRANT CREATE ON SCHEMA public TO
      position_collector, position_reader, report_worker,
      report_collector, control_writer, wb_automation_writer;
" >/dev/null
seeded_control_memberships="$({ "${admin_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT count(*)
    FROM pg_auth_members AS membership
    WHERE membership.roleid = 'control_writer'::regrole
       OR membership.member = 'control_writer'::regrole
  "; } | tr -d '[:space:]')"
if [[ "$seeded_control_memberships" != 2 ]]; then
  echo "failed to seed both stale control_writer membership directions" >&2
  printf '%s\n' "$seeded_control_memberships" >&2
  exit 1
fi
docker exec \
  --env POSTGRES_USER=position_admin \
  --env POSTGRES_PASSWORD="$admin_password" \
  --env POSTGRES_DB=ozon_positions \
  --env POSITION_COLLECTOR_DB_PASSWORD="$collector_password" \
  --env POSITION_READER_DB_PASSWORD="$reader_password" \
  --env REPORT_WORKER_DB_PASSWORD="$report_worker_password" \
  --env REPORT_COLLECTOR_DB_PASSWORD="$report_collector_password" \
  --env CONTROL_WRITER_DB_PASSWORD="$control_writer_password" \
  --env WB_AUTOMATION_DB_PASSWORD="$wb_automation_password" \
  "$container" \
  /docker-entrypoint-initdb.d/003_roles.sh >/dev/null
assert_control_schema_contract \
  "role-bootstrap membership convergence" \
  "${admin_psql[@]}"
converged_database_acl="$({ "${admin_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT count(*) = 6 AND bool_and(
        NOT has_database_privilege(role_name, 'postgres', 'CONNECT')
        AND NOT has_database_privilege(role_name, 'postgres', 'TEMP')
        AND NOT has_database_privilege(role_name, 'postgres', 'CREATE')
    )
    FROM unnest(ARRAY[
        'position_collector', 'position_reader', 'report_worker',
        'report_collector', 'control_writer', 'wb_automation_writer'
    ]::name[]) AS application_role(role_name)
  "; } | tr -d '[:space:]')"
if [[ "$converged_database_acl" != t ]]; then
  echo "role bootstrap retained stale cross-database application grants" >&2
  printf '%s\n' "$converged_database_acl" >&2
  exit 1
fi
converged_schema_acl="$({ "${admin_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT count(*) = 6 AND bool_and(
        NOT has_schema_privilege(role_name, 'public', 'CREATE')
    )
    FROM unnest(ARRAY[
        'position_collector', 'position_reader', 'report_worker',
        'report_collector', 'control_writer', 'wb_automation_writer'
    ]::name[]) AS application_role(role_name)
  "; } | tr -d '[:space:]')"
if [[ "$converged_schema_acl" != t ]]; then
  echo "role bootstrap retained stale application schema CREATE grants" >&2
  printf '%s\n' "$converged_schema_acl" >&2
  exit 1
fi

# Prove that every additive migration can be applied safely to an existing
# database initialized with the original Ozon-only schema and existing roles.
"${admin_psql[@]}" --command 'CREATE DATABASE migration_probe' >/dev/null
migration_admin_psql=(
  docker exec --env PGPASSWORD="$admin_password" "$container"
  psql --host 127.0.0.1 --username position_admin --dbname migration_probe
  --no-psqlrc --set ON_ERROR_STOP=1
)
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/001_schema.sql >/dev/null
"${migration_admin_psql[@]}" --command "
  INSERT INTO search_position.monitors
      (
          store_id, product_id, search_phrase, region_code, region_name,
          interval_minutes, max_position
      )
  VALUES ('legacy-store', '9001', 'legacy phrase', 'legacy-region',
      'Legacy Region', 30, 100);
  INSERT INTO search_position.collection_runs
      (
          started_at, finished_at, status, monitors_attempted,
          monitors_succeeded, collector_version
      )
  VALUES
      (
          '2026-08-15 00:05:00+00', '2026-08-15 00:07:00+00',
          'succeeded', 1, 1, 'legacy-schema-test'
      );
  INSERT INTO search_position.measurements
      (run_id, monitor_id, observed_at, outcome, organic_position)
  VALUES (1, 1, '2026-08-15 00:06:00+00', 'found', 7);
" >/dev/null
"${admin_psql[@]}" --command '
  GRANT CONNECT ON DATABASE migration_probe
      TO position_collector, position_reader, report_worker, report_collector,
      control_writer, wb_automation_writer
' >/dev/null
"${migration_admin_psql[@]}" --command '
  GRANT USAGE ON SCHEMA search_position
      TO position_collector, position_reader;
  GRANT SELECT ON search_position.monitors TO position_collector;
  GRANT SELECT ON ALL TABLES IN SCHEMA search_position TO position_reader;
  ALTER DEFAULT PRIVILEGES IN SCHEMA search_position
      GRANT SELECT ON TABLES TO position_reader;
' >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/002_ozon_collector_contract.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/002_wb_official_history.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/004_ozon_postgres_adapter.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/005_daily_reporting_outbox.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/006_daily_report_snapshots.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/007_daily_reporting_optional_metrics.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/008_daily_reporting_artifact_identity.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/009_daily_reporting_generation_backoff.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/010_daily_reporting_observation_window.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/011_daily_reporting_collection_claims.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/012_daily_reporting_delivery_error_classes.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/013_daily_reporting_delivery_reconciliation.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/014_daily_reporting_period_preserving_catchup.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/015_daily_reporting_ozon_finance.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/016_daily_reporting_unit_economics.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/017_daily_reporting_advertising_extensions.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/018_daily_reporting_recovery_observation_window.sql >/dev/null
"${migration_admin_psql[@]}" --command '
  GRANT USAGE ON SCHEMA daily_reporting TO position_reader;
  GRANT SELECT ON ALL TABLES IN SCHEMA daily_reporting TO position_reader;
  ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
      GRANT SELECT ON TABLES TO position_reader;
' >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/019_daily_reporting_mcp_read_views.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/020_wb_control_plans.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/021_wb_automation_state.sql >/dev/null
# Reapplying an additive migration is required to converge an existing volume
# without changing the exposed contract or broadening the reader role.
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/019_daily_reporting_mcp_read_views.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/020_wb_control_plans.sql >/dev/null
"${migration_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/021_wb_automation_state.sql >/dev/null
optional_sales_metrics="$({ "${migration_admin_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT string_agg(column_name || ':' || is_nullable, ',' ORDER BY column_name)
    FROM information_schema.columns
    WHERE table_schema = 'daily_reporting'
      AND table_name = 'sales_facts'
      AND column_name IN ('cancelled_units', 'returned_units')
  "; } | tr -d '\r')"
if [[ "$optional_sales_metrics" != "cancelled_units:YES,returned_units:YES" ]]; then
  echo "daily-report unavailable sales metrics are not nullable after migration" >&2
  printf '%s\n' "$optional_sales_metrics" >&2
  exit 1
fi
"${migration_admin_psql[@]}" --command "
  CREATE TABLE search_position.reader_default_acl_probe (id integer);
  CREATE TABLE daily_reporting.reader_default_acl_probe (id integer);
  CREATE FUNCTION daily_reporting.reader_default_acl_probe()
  RETURNS integer
  LANGUAGE sql
  IMMUTABLE
  AS \$function\$SELECT 1\$function\$;
" >/dev/null
migration_acl="$({ "${migration_admin_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT
      to_regclass('search_position.published_measurements') IS NOT NULL,
      has_function_privilege(
          'position_collector',
          'search_position.claim_ozon_request_budget(text,timestamp with time zone)',
          'EXECUTE'
      ),
      has_table_privilege(
          'position_reader',
          'search_position.published_measurements',
          'SELECT'
      ),
      NOT has_table_privilege(
          'position_reader',
          'search_position.measurements',
          'SELECT'
      ),
      (
          SELECT scheduled_for = TIMESTAMPTZ '2026-08-15 00:00:00+00'
          FROM search_position.collection_runs
          WHERE id = 1
      ),
      (
          SELECT overall_position = 7 AND placement = 'organic'
          FROM search_position.measurements
          WHERE id = 1
      ),
      (
          SELECT payload_digest = repeat('0', 64)
          FROM search_position.collection_runs
          WHERE id = 1
      ),
      to_regclass('search_position.wb_search_snapshots') IS NOT NULL,
      has_table_privilege(
          'position_collector',
          'search_position.wb_search_targets',
          'SELECT'
      ),
      NOT has_table_privilege(
          'position_collector',
          'search_position.wb_search_targets',
          'INSERT'
      ),
      has_table_privilege(
          'position_collector',
          'search_position.wb_search_snapshots',
          'INSERT'
      ),
      has_table_privilege(
          'position_reader',
          'search_position.latest_wb_search_snapshots',
          'SELECT'
      ),
      NOT has_table_privilege(
          'position_reader',
          'search_position.wb_collection_runs',
          'SELECT'
      ),
      NOT has_table_privilege(
          'position_reader',
          'search_position.wb_search_snapshots',
          'SELECT'
      ),
      NOT has_table_privilege(
          'position_reader',
          'search_position.reader_default_acl_probe',
          'SELECT'
      ),
      to_regclass('daily_reporting.delivery_batches') IS NOT NULL,
      has_table_privilege(
          'report_worker',
          'daily_reporting.delivery_batches',
          'INSERT'
      ),
      NOT has_table_privilege(
          'position_reader',
          'daily_reporting.delivery_batches',
          'SELECT'
      ),
      to_regclass('daily_reporting.source_snapshots') IS NOT NULL,
      has_table_privilege(
          'report_collector',
          'daily_reporting.source_snapshots',
          'INSERT'
      ),
      has_table_privilege(
          'report_worker',
          'daily_reporting.published_source_snapshots',
          'SELECT'
      ),
      to_regclass('daily_reporting.generation_attempts') IS NOT NULL,
      has_table_privilege(
          'report_worker',
          'daily_reporting.generation_attempts',
          'SELECT,INSERT'
      ),
      NOT has_table_privilege(
          'position_reader',
          'daily_reporting.generation_attempts',
          'SELECT'
      ),
      to_regclass('daily_reporting.collection_claims') IS NOT NULL,
      has_function_privilege(
          'report_collector',
          'daily_reporting.claim_report_collection(text,text,timestamp with time zone,text)',
          'EXECUTE'
      ),
      NOT has_table_privilege(
          'report_collector',
          'daily_reporting.collection_claims',
          'SELECT,INSERT,UPDATE,DELETE'
      )
  "; } | tr -d '\r')"
if [[ "$migration_acl" != "t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t:t" ]]; then
  echo "existing-volume migrations did not install the expected schema/ACL" >&2
  printf '%s\n' "$migration_acl" >&2
  exit 1
fi
migration_mcp_acl="$({ "${migration_admin_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT
      has_schema_privilege('position_reader', 'daily_reporting', 'USAGE')
      AND NOT has_schema_privilege('position_reader', 'daily_reporting', 'CREATE')
      AND (
          SELECT count(*) = 9
          FROM pg_class AS relation
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          WHERE namespace.nspname = 'daily_reporting'
            AND relation.relkind = 'v'
            AND relation.relname IN (
                'mcp_collection_status',
                'mcp_published_source_snapshots',
                'mcp_sales_facts',
                'mcp_advertising_facts',
                'mcp_advertising_expense_facts',
                'mcp_finance_facts',
                'mcp_stock_facts',
                'mcp_price_facts',
                'mcp_ready_reports'
            )
            AND coalesce(relation.reloptions, ARRAY[]::text[])
                @> ARRAY['security_barrier=true']
      )
      AND (
          SELECT bool_and(
              has_table_privilege(
                  'position_reader', readable.object_name, 'SELECT'
              )
              AND NOT has_table_privilege(
                  'position_reader', readable.object_name,
                  'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
              )
          )
          FROM unnest(ARRAY[
              'daily_reporting.mcp_collection_status',
              'daily_reporting.mcp_published_source_snapshots',
              'daily_reporting.mcp_sales_facts',
              'daily_reporting.mcp_advertising_facts',
              'daily_reporting.mcp_advertising_expense_facts',
              'daily_reporting.mcp_finance_facts',
              'daily_reporting.mcp_stock_facts',
              'daily_reporting.mcp_price_facts',
              'daily_reporting.mcp_ready_reports'
          ]::text[]) AS readable(object_name)
      )
      AND NOT EXISTS (
          SELECT 1
          FROM unnest(ARRAY[
              'daily_reporting.source_snapshots',
              'daily_reporting.sales_facts',
              'daily_reporting.advertising_facts',
              'daily_reporting.advertising_expense_facts',
              'daily_reporting.finance_facts',
              'daily_reporting.stock_facts',
              'daily_reporting.price_facts',
              'daily_reporting.unit_economics_inputs',
              'daily_reporting.collection_claims',
              'daily_reporting.delivery_batches',
              'daily_reporting.delivery_coverage',
              'daily_reporting.delivery_attempts',
              'daily_reporting.delivery_reconciliations',
              'daily_reporting.generation_attempts',
              'daily_reporting.claimable_deliveries',
              'daily_reporting.generatable_batches',
              'daily_reporting.stalled_report_work',
              'daily_reporting.published_source_snapshots',
              'daily_reporting.published_sales_facts',
              'daily_reporting.published_advertising_facts',
              'daily_reporting.published_advertising_expense_facts',
              'daily_reporting.published_finance_facts',
              'daily_reporting.published_stock_facts',
              'daily_reporting.published_price_facts',
              'daily_reporting.reader_default_acl_probe'
          ]::text[]) AS protected(object_name)
          WHERE has_table_privilege(
              'position_reader', protected.object_name, 'SELECT'
          )
      )
      AND NOT has_function_privilege(
          'position_reader',
          'daily_reporting.reader_default_acl_probe()',
          'EXECUTE'
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pg_default_acl AS defaults
          CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS expanded_acl
          WHERE defaults.defaclnamespace = 'daily_reporting'::regnamespace
            AND expanded_acl.grantee = 'position_reader'::regrole
      )
      AND (
          SELECT string_agg(column_name, ',' ORDER BY ordinal_position) =
                 'batch_id,recipient_id,report_version,local_date,report_kind,' ||
                 'scheduled_for,deadline_at,status,delayed,created_at,updated_at,sent_at'
          FROM information_schema.columns
          WHERE table_schema = 'daily_reporting'
            AND table_name = 'mcp_ready_reports'
      )
  "; } | tr -d '[:space:]')"
if [[ "$migration_mcp_acl" != t ]]; then
  echo "MCP read views did not converge the existing-volume least-privilege ACL" >&2
  printf '%s\n' "$migration_mcp_acl" >&2
  exit 1
fi
assert_control_schema_contract \
  "existing-volume migration" \
  "${migration_admin_psql[@]}"
"${admin_psql[@]}" --command 'DROP DATABASE migration_probe' >/dev/null

# An incompatible legacy monitor must abort the complete Ozon migration without
# deleting or partially altering the existing schema.
"${admin_psql[@]}" --command 'CREATE DATABASE ozon_migration_reject_probe' >/dev/null
reject_admin_psql=(
  docker exec --env PGPASSWORD="$admin_password" "$container"
  psql --host 127.0.0.1 --username position_admin
  --dbname ozon_migration_reject_probe --no-psqlrc --set ON_ERROR_STOP=1
)
"${reject_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/001_schema.sql >/dev/null
"${reject_admin_psql[@]}" --command "
  INSERT INTO search_position.monitors
      (store_id, product_id, search_phrase, region_code, region_name)
  VALUES ('legacy-store', '9001', 'legacy phrase', 'legacy-region', 'Legacy Region');
" >/dev/null
expect_failure_containing \
  "incompatible existing Ozon monitor migration" \
  "monitors_interval_minutes_check" \
  "${reject_admin_psql[@]}" \
  --file /docker-entrypoint-initdb.d/002_ozon_collector_contract.sql
reject_rollback="$({ "${reject_admin_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT
      count(*) = 1,
      NOT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'search_position'
            AND table_name = 'collection_runs'
            AND column_name = 'scheduled_for'
      )
    FROM search_position.monitors
  "; } | tr -d '\r')"
if [[ "$reject_rollback" != "t:t" ]]; then
  echo "failed Ozon migration did not roll back atomically" >&2
  exit 1
fi
"${admin_psql[@]}" --command 'DROP DATABASE ozon_migration_reject_probe' >/dev/null

# Prove that a failure after password/role mutations rolls the entire bootstrap
# back. Hiding one grant target forces a deterministic mid-transaction error.
rollback_collector_password="position-collector-rollback-test"
rollback_reader_password="position-reader-rollback-test"
"${admin_psql[@]}" \
  --command 'ALTER TABLE search_position.alerts RENAME TO alerts_rollback_probe' \
  >/dev/null
expect_failure_containing \
  "position role bootstrap transaction" \
  'relation "search_position.alerts" does not exist' \
  docker exec \
  --env POSTGRES_USER=position_admin \
  --env POSTGRES_PASSWORD="$admin_password" \
  --env POSTGRES_DB=ozon_positions \
  --env POSITION_COLLECTOR_DB_PASSWORD="$rollback_collector_password" \
  --env POSITION_READER_DB_PASSWORD="$rollback_reader_password" \
  --env REPORT_WORKER_DB_PASSWORD="$report_worker_password" \
  --env REPORT_COLLECTOR_DB_PASSWORD="$report_collector_password" \
  --env CONTROL_WRITER_DB_PASSWORD="$control_writer_password" \
  --env WB_AUTOMATION_DB_PASSWORD="$wb_automation_password" \
  "$container" \
  /docker-entrypoint-initdb.d/003_roles.sh
"${admin_psql[@]}" \
  --command 'ALTER TABLE search_position.alerts_rollback_probe RENAME TO alerts' \
  >/dev/null
expect_failure_containing \
  "rolled-back collector password" \
  'password authentication failed for user "position_collector"' \
  docker exec --env PGPASSWORD="$rollback_collector_password" "$container" \
  psql --host 127.0.0.1 --username position_collector --dbname ozon_positions \
  --no-psqlrc --set ON_ERROR_STOP=1 --command 'SELECT 1'

"${admin_psql[@]}" --command "
  INSERT INTO search_position.monitors
      (store_id, product_id, search_phrase, region_code, region_name)
  VALUES
      ('store-a', '1001', 'phrase-a', 'region', 'Region'),
      ('store-b', '1002', 'phrase-b', 'region', 'Region');
" >/dev/null

"${collector_psql[@]}" --command "
  INSERT INTO search_position.collection_runs
      (
          scheduled_for, started_at, status, monitors_planned,
          queries_planned, collector_version, payload_digest
      )
  VALUES
      (
          '2026-08-16 00:00:00+00', '2026-08-16 00:05:00+00',
          'running', 2, 1, 'schema-test', repeat('1', 64)
      );
  INSERT INTO search_position.measurements
      (run_id, monitor_id, observed_at, outcome, response_ms, error_class, http_status)
  VALUES
      (1, 1, '2026-08-16 00:06:00+00', 'error', 123, 'transport', 502);
  INSERT INTO search_position.alerts
      (monitor_id, measurement_id, kind)
  VALUES (1, 1, 'collector_error');
  UPDATE search_position.collection_runs
  SET finished_at = '2026-08-16 00:07:00+00', status = 'failed',
      monitors_attempted = 1, monitors_succeeded = 0,
      queries_attempted = 1, queries_succeeded = 0,
      error_class = 'upstream', http_status = 502
  WHERE id = 1;

  INSERT INTO search_position.collection_runs
      (
          scheduled_for, started_at, status, monitors_planned,
          queries_planned, collector_version, payload_digest
      )
  VALUES
      (
          '2026-08-16 00:30:00+00', '2026-08-16 00:35:00+00',
          'running', 1, 1, 'schema-test', repeat('2', 64)
      );
  INSERT INTO search_position.measurements
      (
          run_id, monitor_id, observed_at, outcome, overall_position,
          placement, response_ms
      )
  VALUES
      (2, 1, '2026-08-16 00:36:00+00', 'found', 21, 'unknown', 87);
  UPDATE search_position.collection_runs
  SET finished_at = '2026-08-16 00:37:00+00', status = 'succeeded',
      monitors_attempted = 1, monitors_succeeded = 1,
      queries_attempted = 1, queries_succeeded = 1
  WHERE id = 2;

  INSERT INTO search_position.collection_runs
      (
          scheduled_for, started_at, status, monitors_planned,
          queries_planned, collector_version, payload_digest
      )
  VALUES
      (
          '2026-08-16 01:00:00+00', '2026-08-16 01:05:00+00',
          'running', 1, 1, 'schema-test', repeat('3', 64)
      );
  INSERT INTO search_position.measurements
      (run_id, monitor_id, observed_at, outcome, response_ms, error_class)
  VALUES
      (3, 1, '2026-08-16 01:06:00+00', 'error', 91, 'transport');
  UPDATE search_position.collection_runs
  SET finished_at = '2026-08-16 01:07:00+00', status = 'failed',
      monitors_attempted = 1, monitors_succeeded = 0,
      queries_attempted = 1, queries_succeeded = 0,
      error_class = 'transport'
  WHERE id = 3;
" >/dev/null

collector_run="$({ "${collector_psql[@]}" --tuples-only --no-align --field-separator=: \
  --command "
    SELECT status, monitors_attempted, monitors_succeeded, error_class, http_status
    FROM search_position.collection_runs
    WHERE id = 1
  "; } | tr -d '\r')"
if [[ "$collector_run" != "failed:1:0:upstream:502" ]]; then
  echo "position_collector did not persist the intended run INSERT/UPDATE" >&2
  printf '%s\n' "$collector_run" >&2
  exit 1
fi

expect_failure_containing \
  "position_collector monitor UPDATE" \
  "permission denied for table monitors" \
  "${collector_psql[@]}" \
  --command 'UPDATE search_position.monitors SET active = false WHERE id = 2'

expect_failure_containing \
  "position_collector monitor DELETE" \
  "permission denied for table monitors" \
  "${collector_psql[@]}" \
  --command 'DELETE FROM search_position.monitors WHERE id = 2'

"${admin_psql[@]}" --command "
  UPDATE search_position.monitors
  SET active = false, updated_at = to_timestamp(0)
  WHERE id = 1
" >/dev/null
monitor_timestamp_updated="$("${admin_psql[@]}" --tuples-only --no-align \
  --command 'SELECT updated_at >= created_at FROM search_position.monitors WHERE id = 1')"
if [[ "$monitor_timestamp_updated" != t ]]; then
  echo "monitor updated_at was not maintained by the database trigger" >&2
  exit 1
fi

if "${admin_psql[@]}" --command "
  INSERT INTO search_position.alerts
      (monitor_id, measurement_id, kind, current_position)
  VALUES (2, 1, 'not_found', 3);
" >/dev/null 2>&1; then
  echo "an alert was incorrectly linked to another monitor's measurement" >&2
  exit 1
fi

if "${admin_psql[@]}" --command "
  WITH new_run AS (
      INSERT INTO search_position.collection_runs
          (
              scheduled_for, started_at, status, monitors_planned,
              queries_planned, collector_version, payload_digest
          )
      VALUES
          (
              '2026-08-16 01:30:00+00', '2026-08-16 01:35:00+00',
              'running', 1, 1, 'schema-test', repeat('4', 64)
          )
      RETURNING id
  )
  INSERT INTO search_position.measurements
      (run_id, monitor_id, observed_at, outcome, organic_position)
  SELECT id, 2, '2026-08-16 01:36:00+00', 'not_found', 7
  FROM new_run;
" >/dev/null 2>&1; then
  echo "a not_found measurement incorrectly retained a search position" >&2
  exit 1
fi

"${reader_psql[@]}" --tuples-only \
  --command 'SELECT count(*) FROM search_position.latest_measurements' \
  | grep -Eq '^[[:space:]]*1[[:space:]]*$'

latest_diagnostics="$({ "${reader_psql[@]}" --tuples-only --no-align --field-separator=: \
  --command "
    SELECT
        overall_position, placement, response_ms, run_status,
        scheduled_for = TIMESTAMPTZ '2026-08-16 00:30:00+00'
    FROM search_position.latest_measurements
    WHERE monitor_id = 1
  "; } | tr -d '\r')"
if [[ "$latest_diagnostics" != "21:unknown:87:succeeded:t" ]]; then
  echo "latest_measurements did not preserve the last published Ozon fact" >&2
  printf '%s\n' "$latest_diagnostics" >&2
  exit 1
fi

expect_failure_containing \
  "Ozon monitor interval outside the reviewed collector contract" \
  "monitors_interval_minutes_check" \
  "${admin_psql[@]}" \
  --command "
    INSERT INTO search_position.monitors
        (
            store_id, product_id, search_phrase, region_code, region_name,
            interval_minutes
        )
    VALUES ('store-c', '1003', 'phrase-c', 'region', 'Region', 15)
  "

expect_failure_containing \
  "duplicate Ozon logical slot" \
  "collection_runs_source_slot_key" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.collection_runs
        (
            scheduled_for, started_at, status, monitors_planned,
            queries_planned, collector_version, payload_digest
        )
    VALUES
        (
            '2026-08-16 00:30:00+00', '2026-08-16 00:35:30+00',
            'running', 1, 1, 'schema-test', repeat('5', 64)
        )
  "

expect_failure_containing \
  "terminal Ozon run mutation" \
  "terminal Ozon collection run is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.collection_runs
    SET status = 'failed'
    WHERE id = 2
  "

expect_failure_containing \
  "post-terminal Ozon measurement" \
  "Ozon measurements can only be appended to a running run" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.measurements
        (run_id, monitor_id, observed_at, outcome)
    VALUES (2, 2, '2026-08-16 00:38:00+00', 'not_found')
  "

open_run_id="$({ "${collector_psql[@]}" --tuples-only --no-align --quiet --command "
  INSERT INTO search_position.collection_runs
      (
          scheduled_for, started_at, status, monitors_planned,
          queries_planned, collector_version, payload_digest
      )
  VALUES
      (
          '2026-08-16 02:00:00+00', '2026-08-16 02:05:00+00',
          'running', 1, 1, 'schema-test', repeat('6', 64)
      )
  RETURNING id;
"; } | tr -d '\r')"

expect_failure_containing \
  "Ozon payload digest mutation" \
  "Ozon collection payload digest is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.collection_runs
    SET payload_digest = repeat('f', 64)
    WHERE id = $open_run_id
  "

expect_failure_containing \
  "Ozon measurement outside its logical slot" \
  "Ozon measurement is outside its logical slot" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.measurements
        (run_id, monitor_id, observed_at, outcome)
    VALUES ($open_run_id, 2, '2026-08-16 02:30:00+00', 'not_found')
  "

"${admin_psql[@]}" --command "
  INSERT INTO search_position.ozon_region_request_budgets
      (region_code, daily_limit)
  VALUES ('region', 2);
" >/dev/null

first_budget_claim="$({ "${collector_psql[@]}" --tuples-only --no-align \
  --command "SELECT search_position.claim_ozon_request_budget(
      'region', '2026-08-16 02:00:00+00'
  )"; } | tr -d '\r')"
second_budget_claim="$({ "${collector_psql[@]}" --tuples-only --no-align \
  --command "SELECT search_position.claim_ozon_request_budget(
      'region', '2026-08-16 02:00:00+00'
  )"; } | tr -d '\r')"
third_budget_claim="$({ "${collector_psql[@]}" --tuples-only --no-align \
  --command "SELECT search_position.claim_ozon_request_budget(
      'region', '2026-08-16 02:00:00+00'
  )"; } | tr -d '\r')"
if [[ "$first_budget_claim:$second_budget_claim:$third_budget_claim" != "t:t:f" ]]; then
  echo "Ozon daily request budget was not enforced atomically" >&2
  exit 1
fi

"${collector_psql[@]}" --command "
  SELECT search_position.open_ozon_collector_circuit($open_run_id, 'captcha');
" >/dev/null
blocked_budget_claim="$({ "${collector_psql[@]}" --tuples-only --no-align \
  --command "SELECT search_position.claim_ozon_request_budget(
      'region', '2026-08-17 02:00:00+00'
  )"; } | tr -d '\r')"
if [[ "$blocked_budget_claim" != f ]]; then
  echo "an open Ozon circuit did not stop a new request-budget claim" >&2
  exit 1
fi

expect_failure_containing \
  "position_collector direct Ozon circuit UPDATE" \
  "permission denied for table ozon_collector_circuit" \
  "${collector_psql[@]}" \
  --command "
    UPDATE search_position.ozon_collector_circuit
    SET circuit_open = false
    WHERE source = 'ozon_public_search'
  "

"${collector_psql[@]}" --command "
  UPDATE search_position.collection_runs
  SET finished_at = '2026-08-16 02:07:00+00', status = 'blocked',
      monitors_attempted = 1, monitors_succeeded = 0,
      queries_attempted = 1, queries_succeeded = 0,
      error_class = 'captcha'
  WHERE id = $open_run_id;
" >/dev/null

expect_failure_containing \
  "opening the Ozon circuit from a terminal run" \
  "Ozon collector circuit requires a running run" \
  "${collector_psql[@]}" \
  --command "SELECT search_position.open_ozon_collector_circuit(
      $open_run_id, 'captcha'
  )"

"${admin_psql[@]}" --command "
  UPDATE search_position.ozon_collector_circuit
  SET circuit_open = false,
      reset_at = statement_timestamp(),
      reset_by = 'schema-test-admin'
  WHERE source = 'ozon_public_search';
" >/dev/null

expect_failure_containing \
  "position_reader raw Ozon measurements" \
  "permission denied for table measurements" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM search_position.measurements'

published_count="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --command 'SELECT count(*) FROM search_position.published_measurements'; } \
  | tr -d '\r')"
if [[ "$published_count" != 1 ]]; then
  echo "the Ozon publication view exposed a non-published run" >&2
  exit 1
fi

# WB Search Analytics is a separate source-aware aggregate dataset. Exercise its
# provenance, identity, idempotency and append-only role boundary end-to-end.
"${admin_psql[@]}" --command "
  INSERT INTO search_position.wb_search_targets
      (account_id, store_id, nm_id, search_phrase)
  VALUES
      ('wb-account-a', 'wb-store-a', 3411079879, 'ручка мебельная'),
      ('wb-account-a', 'wb-store-a', 3388722638, 'ручка кнопка');

  INSERT INTO search_position.wb_bid_targets
      (
          account_id,
          store_id,
          source,
          campaign_id,
          nm_id,
          payment_type,
          placement
      )
  VALUES
      (
          'wb-account-a', 'wb-store-a', 'promotion_cluster_bids',
          7001, 3411079879, NULL, NULL
      ),
      (
          'wb-account-a', 'wb-store-a', 'promotion_minimum_bids',
          7002, 3388722638, 'cpc', 'search'
      ),
      (
          'wb-account-a', 'wb-store-a', 'promotion_bid_recommendations',
          7003, 3411079879, 'cpm', NULL
      );
" >/dev/null

expect_failure_containing \
  "WB cluster target with invented payment/placement" \
  "violates check constraint" \
  "${admin_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_bid_targets (
        account_id, store_id, source, campaign_id, nm_id,
        payment_type, placement
    ) VALUES (
        'wb-account-a', 'wb-store-a', 'promotion_cluster_bids',
        7991, 3411079879, 'cpm', 'search'
    )
  "

expect_failure_containing \
  "WB recommendation target with CPC" \
  "violates check constraint" \
  "${admin_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_bid_targets (
        account_id, store_id, source, campaign_id, nm_id, payment_type
    ) VALUES (
        'wb-account-a', 'wb-store-a', 'promotion_bid_recommendations',
        7992, 3411079879, 'cpc'
    )
  "

"${collector_psql[@]}" --command "
  INSERT INTO search_position.wb_collection_runs (
      account_id,
      store_id,
      source,
      source_host,
      source_method,
      source_path,
      scheduled_for,
      started_at,
      status,
      collector_version
  ) VALUES (
      'wb-account-a',
      'wb-store-a',
      'search_product_orders',
      'seller-analytics-api.wildberries.ru',
      'POST',
      '/api/v2/search-report/product/orders',
      '2026-08-15 10:00:00+00',
      '2026-08-15 10:00:01+00',
      'running',
      'schema-test'
  );

  INSERT INTO search_position.wb_search_snapshots (
      run_id,
      target_id,
      source,
      account_id,
      store_id,
      nm_id,
      search_phrase,
      period_start,
      period_end,
      observed_at,
      source_updated_at,
      data_granularity,
      average_position,
      orders
  ) VALUES (
      (SELECT id FROM search_position.wb_collection_runs
       WHERE source = 'search_product_orders'),
      1,
      'search_product_orders',
      'wb-account-a',
      'wb-store-a',
      3411079879,
      'ручка мебельная',
      '2026-08-14',
      '2026-08-14',
      '2026-08-15 10:00:05+00',
      '2026-08-15 09:59:00+00',
      'daily',
      32.4,
      4
  );

  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 10:00:10+00',
      source_updated_at = '2026-08-15 09:59:00+00',
      status = 'succeeded',
      targets_attempted = 1,
      targets_succeeded = 1
  WHERE source = 'search_product_orders';

  INSERT INTO search_position.wb_collection_runs (
      account_id,
      store_id,
      source,
      source_host,
      source_method,
      source_path,
      scheduled_for,
      started_at,
      status,
      collector_version
  ) VALUES (
      'wb-account-a',
      'wb-store-a',
      'promotion_cluster_bids',
      'advert-api.wildberries.ru',
      'POST',
      '/adv/v0/normquery/get-bids',
      '2026-08-15 10:00:00+00',
      '2026-08-15 10:00:01+00',
      'running',
      'schema-test'
  );

  INSERT INTO search_position.wb_bid_snapshots (
      run_id,
      target_id,
      source,
      account_id,
      store_id,
      campaign_id,
      nm_id,
      scope,
      query_phrase,
      bid_kind,
      bid_kopecks,
      observed_at
  ) VALUES (
      (SELECT id FROM search_position.wb_collection_runs
       WHERE source = 'promotion_cluster_bids'),
      1,
      'promotion_cluster_bids',
      'wb-account-a',
      'wb-store-a',
      7001,
      3411079879,
      'search_cluster',
      'ручка мебельная',
      'current',
      10500,
      '2026-08-15 10:00:05+00'
  );

  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 10:00:09+00',
      status = 'succeeded',
      targets_attempted = 1,
      targets_succeeded = 1
  WHERE source = 'promotion_cluster_bids';

  INSERT INTO search_position.wb_collection_runs (
      account_id, store_id, source, source_host, source_method, source_path,
      scheduled_for, started_at, status, collector_version
  ) VALUES (
      'wb-account-a', 'wb-store-a', 'promotion_minimum_bids',
      'advert-api.wildberries.ru', 'POST', '/api/advert/v1/bids/min',
      '2026-08-15 10:00:00+00', '2026-08-15 10:00:01+00',
      'running', 'schema-test'
  );
  INSERT INTO search_position.wb_bid_snapshots (
      run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
      payment_type, placement, scope, bid_kind, bid_kopecks, observed_at
  ) VALUES (
      (SELECT id FROM search_position.wb_collection_runs
       WHERE source = 'promotion_minimum_bids'),
      2, 'promotion_minimum_bids', 'wb-account-a', 'wb-store-a',
      7002, 3388722638, 'cpc', 'search', 'product', 'minimum', 250,
      '2026-08-15 10:00:05+00'
  );
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 10:00:09+00', status = 'succeeded',
      targets_attempted = 1, targets_succeeded = 1
  WHERE source = 'promotion_minimum_bids';

  INSERT INTO search_position.wb_collection_runs (
      account_id, store_id, source, source_host, source_method, source_path,
      scheduled_for, started_at, status, collector_version
  ) VALUES (
      'wb-account-a', 'wb-store-a', 'promotion_bid_recommendations',
      'advert-api.wildberries.ru', 'GET',
      '/api/advert/v0/bids/recommendations',
      '2026-08-15 10:00:00+00', '2026-08-15 10:00:01+00',
      'running', 'schema-test'
  );
  INSERT INTO search_position.wb_bid_snapshots (
      run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
      payment_type, scope, bid_kind, bid_kopecks, observed_at
  ) VALUES (
      (SELECT id FROM search_position.wb_collection_runs
       WHERE source = 'promotion_bid_recommendations'),
      3, 'promotion_bid_recommendations', 'wb-account-a', 'wb-store-a',
      7003, 3411079879, 'cpm', 'product', 'competitive', 39500,
      '2026-08-15 10:00:05+00'
  );
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 10:00:09+00', status = 'succeeded',
      targets_attempted = 1, targets_succeeded = 1
  WHERE source = 'promotion_bid_recommendations';
" >/dev/null

wb_search_snapshot="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT source,
           data_granularity,
           is_live_position,
           coalesce(region, 'NULL'),
           placement_split_available,
           average_position,
           coalesce(median_position::text, 'NULL'),
           orders,
           frequency,
           run_status,
           is_partial
    FROM search_position.latest_wb_search_snapshots
    WHERE target_id = 1
  "; } | tr -d '\r')"
if [[ "$wb_search_snapshot" != \
  "search_product_orders:daily:f:NULL:f:32.4000:NULL:4::succeeded:f" ]]; then
  echo "WB Search Analytics snapshot or semantic flags are incorrect" >&2
  printf '%s\n' "$wb_search_snapshot" >&2
  exit 1
fi

wb_bid_snapshot="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT source,
           coalesce(payment_type, 'NULL'),
           coalesce(placement, 'NULL'),
           scope,
           query_phrase,
           bid_kind,
           bid_kopecks,
           run_status,
           is_partial
    FROM search_position.latest_wb_bid_snapshots
    WHERE target_id = 1
  "; } | tr -d '\r')"
if [[ "$wb_bid_snapshot" != \
  "promotion_cluster_bids:NULL:NULL:search_cluster:ручка мебельная:current:10500:succeeded:f" ]]; then
  echo "WB bid snapshot lost source or query-level provenance" >&2
  printf '%s\n' "$wb_bid_snapshot" >&2
  exit 1
fi

wb_other_bid_snapshots="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT target_id,
           source,
           payment_type,
           coalesce(placement, 'NULL'),
           bid_kind,
           bid_kopecks
    FROM search_position.latest_wb_bid_snapshots
    WHERE target_id IN (2, 3)
    ORDER BY target_id
  "; } | tr -d '\r')"
expected_other_bids=$'2:promotion_minimum_bids:cpc:search:minimum:250\n3:promotion_bid_recommendations:cpm:NULL:competitive:39500'
if [[ "$wb_other_bid_snapshots" != "$expected_other_bids" ]]; then
  echo "WB minimum/recommendation snapshots lost source-specific semantics" >&2
  printf '%s\n' "$wb_other_bid_snapshots" >&2
  exit 1
fi

wb_forbidden_position_columns="$({ "${admin_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT count(*)
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
  "; } | tr -d '\r')"
if [[ "$wb_forbidden_position_columns" != 0 ]]; then
  echo "WB search aggregates expose unsupported live/region/placement columns" >&2
  exit 1
fi

"${admin_psql[@]}" --command "
  UPDATE search_position.wb_search_targets
  SET active = false, updated_at = to_timestamp(0)
  WHERE id = 1;
  UPDATE search_position.wb_bid_targets
  SET active = false, updated_at = to_timestamp(0)
  WHERE id = 1;
" >/dev/null
wb_target_timestamps="$({ "${admin_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT
      (SELECT updated_at >= created_at
       FROM search_position.wb_search_targets WHERE id = 1),
      (SELECT updated_at >= created_at
       FROM search_position.wb_bid_targets WHERE id = 1)
  "; } | tr -d '\r')"
if [[ "$wb_target_timestamps" != "t:t" ]]; then
  echo "WB target updated_at is not maintained by its database trigger" >&2
  exit 1
fi

expect_failure_containing \
  "immutable WB search target identity" \
  "WB search target identity is immutable" \
  "${admin_psql[@]}" \
  --command 'UPDATE search_position.wb_search_targets SET nm_id = 1 WHERE id = 1'

expect_failure_containing \
  "immutable WB bid target identity" \
  "WB bid target identity is immutable" \
  "${admin_psql[@]}" \
  --command 'UPDATE search_position.wb_bid_targets SET campaign_id = 1 WHERE id = 1'

expect_failure_containing \
  "WB succeeded run changed to failed" \
  "terminal WB collection run is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET status = 'failed'
    WHERE source = 'search_product_orders'
      AND scheduled_for = '2026-08-15 10:00:00+00'
  "

expect_failure_containing \
  "WB succeeded run reopened" \
  "terminal WB collection run is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET status = 'running', finished_at = NULL
    WHERE source = 'search_product_orders'
      AND scheduled_for = '2026-08-15 10:00:00+00'
  "

expect_failure_containing \
  "hourly WB run idempotency" \
  "duplicate key value violates unique constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_collection_runs (
        account_id, store_id, source, source_host, source_method, source_path,
        scheduled_for, started_at, status, collector_version
    ) VALUES (
        'wb-account-a', 'wb-store-a', 'search_product_orders',
        'seller-analytics-api.wildberries.ru', 'POST',
        '/api/v2/search-report/product/orders',
        '2026-08-15 10:00:00+00', '2026-08-15 10:00:02+00',
        'running', 'schema-test'
    )
  "

expect_failure_containing \
  "WB run scheduled outside an hour boundary" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_collection_runs (
        account_id, store_id, source, source_host, source_method, source_path,
        scheduled_for, started_at, status, collector_version
    ) VALUES (
        'wb-account-a', 'wb-store-a', 'search_product_texts',
        'seller-analytics-api.wildberries.ru', 'POST',
        '/api/v2/search-report/product/search-texts',
        '2026-08-15 10:15:00+00', '2026-08-15 10:15:01+00',
        'running', 'schema-test'
    )
  "

"${collector_psql[@]}" --command "
  INSERT INTO search_position.wb_collection_runs (
      account_id, store_id, source, source_host, source_method, source_path,
      scheduled_for, started_at, status, collector_version
  ) VALUES
      (
          'wb-account-a', 'wb-store-a', 'search_product_orders',
          'seller-analytics-api.wildberries.ru', 'POST',
          '/api/v2/search-report/product/orders',
          '2026-08-15 11:00:00+00', '2026-08-15 11:00:01+00',
          'running', 'schema-test'
      ),
      (
          'wb-account-a', 'wb-store-a', 'search_product_texts',
          'seller-analytics-api.wildberries.ru', 'POST',
          '/api/v2/search-report/product/search-texts',
          '2026-08-15 11:00:00+00', '2026-08-15 11:00:01+00',
          'running', 'schema-test'
      ),
      (
          'wb-account-a', 'wb-store-a', 'promotion_cluster_bids',
          'advert-api.wildberries.ru', 'POST',
          '/adv/v0/normquery/get-bids',
          '2026-08-15 11:00:00+00', '2026-08-15 11:00:01+00',
          'running', 'schema-test'
      ),
      (
          'wb-account-a', 'wb-store-a', 'promotion_minimum_bids',
          'advert-api.wildberries.ru', 'POST',
          '/api/advert/v1/bids/min',
          '2026-08-15 11:00:00+00', '2026-08-15 11:00:01+00',
          'running', 'schema-test'
      ),
      (
          'wb-account-a', 'wb-store-a', 'promotion_bid_recommendations',
          'advert-api.wildberries.ru', 'GET',
          '/api/advert/v0/bids/recommendations',
          '2026-08-15 11:00:00+00', '2026-08-15 11:00:01+00',
          'running', 'schema-test'
      );
" >/dev/null

expect_failure_containing \
  "WB search snapshot source/run mismatch" \
  "violates foreign key constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_orders'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_texts', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-14', '2026-08-14',
        '2026-08-15 11:00:05+00', 'period_aggregate', 20
    )
  "

expect_failure_containing \
  "WB orders snapshot with a multi-day period" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_orders'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_orders', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-13', '2026-08-14',
        '2026-08-15 11:00:05+00', 'daily', 20
    )
  "

expect_failure_containing \
  "WB orders snapshot with period granularity" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_orders'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_orders', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-14', '2026-08-14',
        '2026-08-15 11:00:05+00', 'period_aggregate', 20
    )
  "

expect_failure_containing \
  "WB orders row with period-level frequency" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position, frequency
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_orders'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_orders', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-14', '2026-08-14',
        '2026-08-15 11:00:05+00', 'daily', 20, 100
    )
  "

expect_failure_containing \
  "WB orders row with unsupported daily median" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position, median_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_orders'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_orders', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-14', '2026-08-14',
        '2026-08-15 11:00:05+00', 'daily', 20, 21
    )
  "

expect_failure_containing \
  "WB search-text snapshot beyond the 31-day contract" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_texts'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_texts', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-07-14', '2026-08-14',
        '2026-08-15 11:00:05+00', 'period_aggregate', 20
    )
  "

expect_failure_containing \
  "WB search-text snapshot with daily granularity" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_texts'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_texts', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-14', '2026-08-14',
        '2026-08-15 11:00:05+00', 'daily', 20
    )
  "

expect_failure_containing \
  "WB cluster snapshot with invented payment/placement" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_bid_snapshots (
        run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
        payment_type, placement, scope, query_phrase, bid_kind, bid_kopecks,
        observed_at
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'promotion_cluster_bids'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        1, 'promotion_cluster_bids', 'wb-account-a', 'wb-store-a',
        7001, 3411079879, 'cpm', 'search', 'search_cluster',
        'ручка мебельная', 'current', 100, '2026-08-15 11:00:05+00'
    )
  "

expect_failure_containing \
  "WB minimum-bid target metadata mismatch" \
  "violates foreign key constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_bid_snapshots (
        run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
        payment_type, placement, scope, bid_kind, bid_kopecks, observed_at
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'promotion_minimum_bids'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'promotion_minimum_bids', 'wb-account-a', 'wb-store-a',
        7002, 3388722638, 'cpm', 'search', 'product', 'minimum', 100,
        '2026-08-15 11:00:05+00'
    )
  "

expect_failure_containing \
  "WB recommendation snapshot with CPC" \
  "violates check constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_bid_snapshots (
        run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
        payment_type, scope, bid_kind, bid_kopecks, observed_at
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'promotion_bid_recommendations'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        3, 'promotion_bid_recommendations', 'wb-account-a', 'wb-store-a',
        7003, 3411079879, 'cpc', 'product', 'competitive', 100,
        '2026-08-15 11:00:05+00'
    )
  "

expect_failure_containing \
  "WB bid snapshot cross-source target" \
  "violates foreign key constraint" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_bid_snapshots (
        run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
        scope, query_phrase, bid_kind, bid_kopecks, observed_at
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'promotion_cluster_bids'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'promotion_cluster_bids', 'wb-account-a', 'wb-store-a',
        7002, 3388722638, 'search_cluster', 'ручка кнопка', 'current', 100,
        '2026-08-15 11:00:05+00'
    )
  "

expect_failure_containing \
  "WB run inserted directly as terminal" \
  "WB collection run must start clean in running" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_collection_runs (
        account_id, store_id, source, source_host, source_method, source_path,
        scheduled_for, started_at, finished_at, status, collector_version
    ) VALUES (
        'wb-account-a', 'wb-store-a', 'search_product_texts',
        'seller-analytics-api.wildberries.ru', 'POST',
        '/api/v2/search-report/product/search-texts',
        '2026-08-15 13:00:00+00', '2026-08-15 13:00:01+00',
        '2026-08-15 13:00:02+00', 'failed', 'schema-test'
    )
  "

expect_failure_containing \
  "WB running-run provenance mutation" \
  "WB collection run provenance is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET source_path = '/not-the-requested-endpoint'
    WHERE source = 'promotion_minimum_bids'
      AND scheduled_for = '2026-08-15 11:00:00+00'
  "

expect_failure_containing \
  "WB running-run started_at mutation" \
  "WB collection run provenance is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET started_at = '2026-08-15 11:00:02+00'
    WHERE source = 'promotion_minimum_bids'
      AND scheduled_for = '2026-08-15 11:00:00+00'
  "

"${collector_psql[@]}" --command "
  UPDATE search_position.wb_collection_runs
  SET targets_attempted = 2, targets_succeeded = 1
  WHERE source = 'promotion_minimum_bids'
    AND scheduled_for = '2026-08-15 11:00:00+00';
" >/dev/null
expect_failure_containing \
  "WB running-run counter decrease" \
  "WB collection run counters cannot decrease" \
  "${collector_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET targets_attempted = 1
    WHERE source = 'promotion_minimum_bids'
      AND scheduled_for = '2026-08-15 11:00:00+00'
  "

# A newer failed Search Report run remains forensic-only and cannot displace
# the previous succeeded snapshot in the reader projection.
"${collector_psql[@]}" --command "
  INSERT INTO search_position.wb_search_snapshots (
      run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
      period_start, period_end, observed_at, data_granularity,
      average_position, orders
  ) VALUES (
      (SELECT id FROM search_position.wb_collection_runs
       WHERE source = 'search_product_orders'
         AND scheduled_for = '2026-08-15 11:00:00+00'),
      1, 'search_product_orders', 'wb-account-a', 'wb-store-a', 3411079879,
      'ручка мебельная', '2026-08-14', '2026-08-14',
      '2026-08-15 11:00:05+00', 'daily', 99, 0
  );
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 11:00:10+00', status = 'failed',
      targets_attempted = 1, error_class = 'upstream', http_status = 502
  WHERE source = 'search_product_orders'
    AND scheduled_for = '2026-08-15 11:00:00+00';
" >/dev/null
wb_search_after_failed="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT average_position, run_status
    FROM search_position.latest_wb_search_snapshots
    WHERE target_id = 1
  "; } | tr -d '\r')"
if [[ "$wb_search_after_failed" != "32.4000:succeeded" ]]; then
  echo "a failed WB run displaced the last published search snapshot" >&2
  printf '%s\n' "$wb_search_after_failed" >&2
  exit 1
fi

expect_failure_containing \
  "WB failed run changed to succeeded" \
  "terminal WB collection run is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET status = 'succeeded'
    WHERE source = 'search_product_orders'
      AND scheduled_for = '2026-08-15 11:00:00+00'
  "

expect_failure_containing \
  "WB snapshot appended after terminal run" \
  "WB snapshots can only be appended to a running run" \
  "${collector_psql[@]}" \
  --command "
    INSERT INTO search_position.wb_search_snapshots (
        run_id, target_id, source, account_id, store_id, nm_id, search_phrase,
        period_start, period_end, observed_at, data_granularity,
        average_position
    ) VALUES (
        (SELECT id FROM search_position.wb_collection_runs
         WHERE source = 'search_product_orders'
           AND scheduled_for = '2026-08-15 11:00:00+00'),
        2, 'search_product_orders', 'wb-account-a', 'wb-store-a', 3388722638,
        'ручка кнопка', '2026-08-14', '2026-08-14',
        '2026-08-15 11:00:06+00', 'daily', 20
    )
  "

# Running facts are not published. Once the same run becomes partial, the
# projection exposes both the newer bid and bounded partial diagnostics.
"${collector_psql[@]}" --command "
  INSERT INTO search_position.wb_bid_snapshots (
      run_id, target_id, source, account_id, store_id, campaign_id, nm_id,
      scope, query_phrase, bid_kind, bid_kopecks, observed_at
  ) VALUES (
      (SELECT id FROM search_position.wb_collection_runs
       WHERE source = 'promotion_cluster_bids'
         AND scheduled_for = '2026-08-15 11:00:00+00'),
      1, 'promotion_cluster_bids', 'wb-account-a', 'wb-store-a',
      7001, 3411079879, 'search_cluster', 'ручка мебельная', 'current',
      11000, '2026-08-15 11:00:05+00'
  );
" >/dev/null
wb_bid_while_running="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT bid_kopecks, run_status
    FROM search_position.latest_wb_bid_snapshots
    WHERE target_id = 1
  "; } | tr -d '\r')"
if [[ "$wb_bid_while_running" != "10500:succeeded" ]]; then
  echo "a running WB run displaced the last published bid snapshot" >&2
  exit 1
fi

"${collector_psql[@]}" --command "
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 11:00:10+00', status = 'partial',
      targets_attempted = 2, targets_succeeded = 1,
      error_class = 'upstream', http_status = 502
  WHERE source = 'promotion_cluster_bids'
    AND scheduled_for = '2026-08-15 11:00:00+00';
" >/dev/null
wb_partial_bid="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT bid_kopecks, run_status, is_partial, targets_attempted,
           targets_succeeded, run_error_class, run_http_status
    FROM search_position.latest_wb_bid_snapshots
    WHERE target_id = 1
  "; } | tr -d '\r')"
if [[ "$wb_partial_bid" != "11000:partial:t:2:1:upstream:502" ]]; then
  echo "partial WB publication omitted its bounded diagnostics" >&2
  printf '%s\n' "$wb_partial_bid" >&2
  exit 1
fi

expect_failure_containing \
  "WB partial run changed to succeeded" \
  "terminal WB collection run is immutable" \
  "${admin_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET status = 'succeeded'
    WHERE source = 'promotion_cluster_bids'
      AND scheduled_for = '2026-08-15 11:00:00+00'
  "

"${collector_psql[@]}" --command "
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 11:00:10+00', status = 'failed',
      targets_attempted = 2, targets_succeeded = 1,
      error_class = 'upstream', http_status = 502
  WHERE source = 'promotion_minimum_bids'
    AND scheduled_for = '2026-08-15 11:00:00+00';
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 11:00:10+00', status = 'failed',
      targets_attempted = 1, error_class = 'upstream', http_status = 502
  WHERE source = 'promotion_bid_recommendations'
    AND scheduled_for = '2026-08-15 11:00:00+00';
  UPDATE search_position.wb_collection_runs
  SET finished_at = '2026-08-15 11:00:10+00', status = 'failed',
      targets_attempted = 1, error_class = 'upstream', http_status = 502
  WHERE source = 'search_product_texts'
    AND scheduled_for = '2026-08-15 11:00:00+00';
" >/dev/null

expect_failure_containing \
  "position_collector WB target UPDATE" \
  "permission denied for table wb_search_targets" \
  "${collector_psql[@]}" \
  --command 'UPDATE search_position.wb_search_targets SET active = true WHERE id = 1'

expect_failure_containing \
  "position_collector WB snapshot UPDATE" \
  "permission denied for table wb_search_snapshots" \
  "${collector_psql[@]}" \
  --command 'UPDATE search_position.wb_search_snapshots SET orders = 5 WHERE id = 1'

expect_failure_containing \
  "position_collector WB snapshot DELETE" \
  "permission denied for table wb_bid_snapshots" \
  "${collector_psql[@]}" \
  --command 'DELETE FROM search_position.wb_bid_snapshots WHERE id = 1'

expect_failure_containing \
  "position_collector WB scheduled-hour UPDATE" \
  "permission denied for table wb_collection_runs" \
  "${collector_psql[@]}" \
  --command "
    UPDATE search_position.wb_collection_runs
    SET scheduled_for = '2026-08-15 11:00:00+00'
    WHERE source = 'search_product_orders'
  "

expect_failure_containing \
  "position_reader raw WB run access" \
  "permission denied for table wb_collection_runs" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM search_position.wb_collection_runs'

expect_failure_containing \
  "position_reader WB snapshot write access" \
  "permission denied for table wb_search_snapshots" \
  "${reader_psql[@]}" \
  --command 'SET default_transaction_read_only=off' \
  --command 'UPDATE search_position.wb_search_snapshots SET orders = 5 WHERE id = 1'

# Daily-report source facts are published atomically. A report worker can read
# only terminal succeeded/partial projections; raw/running/failed data stays
# private to the collector boundary.
claim_identity="$({ "${report_collector_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT claim_id, claim_generation
    FROM daily_reporting.claim_report_collection(
        'diana-ozon', 'ozon', '2099-08-16 03:00:00+00', 'schema-test-diana'
    )
  "; } | tr -d '\r')"
same_claim_identity="$({ "${report_collector_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT claim_id, claim_generation
    FROM daily_reporting.claim_report_collection(
        'diana-ozon', 'ozon', '2099-08-16 03:00:00+00', 'schema-test-diana'
    )
  "; } | tr -d '\r')"
busy_claim_count="$({ "${report_collector_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT count(*)
    FROM daily_reporting.claim_report_collection(
        'diana-ozon', 'ozon', '2099-08-16 03:00:00+00', 'competing-owner'
    )
  "; } | tr -d '[:space:]')"
if [[ -z "$claim_identity" || "$same_claim_identity" != "$claim_identity" \
    || "$busy_claim_count" != 0 ]]; then
  echo "daily-report collection claim is not idempotent and exclusive" >&2
  exit 1
fi

expect_failure_containing \
  "daily-report snapshot without collection claim" \
  "new source snapshot requires an active collection claim" \
  "${report_collector_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.source_snapshots
        (
            account_id, marketplace, source, cutoff_at, source_as_of,
            period_start, period_end, collector_version
        )
    VALUES
        (
            'unclaimed', 'ozon', 'stocks', '2099-08-16 03:00:00+00',
            '2099-08-16 02:30:00+00', '2099-08-16 02:30:00+00',
            '2099-08-16 02:30:00+00', 'schema-test'
        )
  "

"${report_collector_psql[@]}" --command "
  INSERT INTO daily_reporting.source_snapshots
      (
          account_id, marketplace, source, cutoff_at, source_as_of,
          period_start, period_end, collector_version,
          claim_id, claim_generation
      )
  SELECT
          'diana-ozon', 'ozon', 'sales', '2099-08-16 03:00:00+00',
          '2099-08-16 02:30:00+00', '2099-08-15 00:00:00+00',
          '2099-08-16 00:00:00+00', 'schema-test',
          claim_id, claim_generation
  FROM daily_reporting.claim_report_collection(
      'diana-ozon', 'ozon', '2099-08-16 03:00:00+00', 'schema-test-diana'
  );
  INSERT INTO daily_reporting.sales_facts
      (
          snapshot_id, business_date, sku, ordered_units,
          operational_gmv_minor, cancelled_units, returned_units
      )
  SELECT id, '2099-08-15', 3411079879, 4, 270000, 0, 1
  FROM daily_reporting.source_snapshots
  WHERE account_id = 'diana-ozon'
    AND marketplace = 'ozon'
    AND source = 'sales'
    AND cutoff_at = '2099-08-16 03:00:00+00';
  UPDATE daily_reporting.source_snapshots
  SET status = 'succeeded', pagination_complete = true, row_count = 1,
      payload_sha256 = repeat('a', 64),
      finished_at = '2099-08-16 02:40:00+00'
  WHERE account_id = 'diana-ozon'
    AND marketplace = 'ozon'
    AND source = 'sales'
    AND cutoff_at = '2099-08-16 03:00:00+00';

  INSERT INTO daily_reporting.source_snapshots
      (
          account_id, marketplace, source, cutoff_at, source_as_of,
          period_start, period_end, collector_version,
          claim_id, claim_generation
      )
  SELECT
          'diana-ozon', 'ozon', 'stocks', '2099-08-16 03:00:00+00',
          '2099-08-16 03:20:00+00', '2099-08-16 03:20:00+00',
          '2099-08-16 03:20:00+00', 'schema-test',
          claim_id, claim_generation
  FROM daily_reporting.claim_report_collection(
      'diana-ozon', 'ozon', '2099-08-16 03:00:00+00', 'schema-test-diana'
  );
  INSERT INTO daily_reporting.stock_facts
      (snapshot_id, sku, warehouse_id, sellable_units)
  SELECT id, 3411079879, 'failed-source-probe', 2
  FROM daily_reporting.source_snapshots
  WHERE account_id = 'diana-ozon'
    AND marketplace = 'ozon'
    AND source = 'stocks'
    AND cutoff_at = '2099-08-16 03:00:00+00';
  UPDATE daily_reporting.source_snapshots
  SET status = 'failed', row_count = 1,
      finished_at = '2099-08-16 02:41:00+00',
      error_class = 'transport', http_status = 502
  WHERE account_id = 'diana-ozon'
    AND marketplace = 'ozon'
    AND source = 'stocks'
    AND cutoff_at = '2099-08-16 03:00:00+00';
" >/dev/null

published_report_facts="$({ "${report_worker_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT snapshot.status, snapshot.row_count, fact.sku,
           fact.ordered_units, fact.returned_units
    FROM daily_reporting.published_source_snapshots AS snapshot
    JOIN daily_reporting.published_sales_facts AS fact
      ON fact.snapshot_id = snapshot.id
  "; } | tr -d '\r')"
if [[ "$published_report_facts" != "succeeded:1:3411079879:4:1" ]]; then
  echo "published daily-report facts differ from the frozen snapshot" >&2
  printf '%s\n' "$published_report_facts" >&2
  exit 1
fi

mcp_collection_status="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT source, status, pagination_complete, row_count,
           coalesce(last_published_status, 'none')
    FROM daily_reporting.mcp_collection_status
    WHERE account_id = 'diana-ozon'
      AND marketplace = 'ozon'
    ORDER BY source
  "; } | tr -d '\r')"
expected_mcp_collection_status=$'sales:succeeded:t:1:succeeded\nstocks:failed:f:1:none'
if [[ "$mcp_collection_status" != "$expected_mcp_collection_status" ]]; then
  echo "MCP collection status did not preserve failed versus published state" >&2
  printf '%s\n' "$mcp_collection_status" >&2
  exit 1
fi

mcp_published_dataset="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT manifest.source, manifest.status, manifest.pagination_complete,
           manifest.row_count, fact.sku, fact.ordered_units,
           fact.returned_units
    FROM daily_reporting.mcp_published_source_snapshots AS manifest
    JOIN daily_reporting.mcp_sales_facts AS fact
      ON fact.snapshot_id = manifest.snapshot_id
    WHERE manifest.account_id = 'diana-ozon'
      AND manifest.marketplace = 'ozon'
  "; } | tr -d '\r')"
if [[ "$mcp_published_dataset" != "sales:succeeded:t:1:3411079879:4:1" ]]; then
  echo "MCP published dataset differs from its frozen snapshot manifest" >&2
  printf '%s\n' "$mcp_published_dataset" >&2
  exit 1
fi

mcp_failed_stock_count="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT count(*)
    FROM daily_reporting.mcp_stock_facts
    WHERE account_id = 'diana-ozon'
      AND marketplace = 'ozon'
  "; } | tr -d '[:space:]')"
if [[ "$mcp_failed_stock_count" != 0 ]]; then
  echo "MCP stock projection published facts from a failed source" >&2
  exit 1
fi

expect_failure_containing \
  "daily-report fact append after publication" \
  "facts may be appended only to a running matching snapshot" \
  "${report_collector_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.sales_facts
        (snapshot_id, business_date, sku, ordered_units, operational_gmv_minor)
    SELECT id, '2099-08-15', 3388722638, 1, 10000
    FROM daily_reporting.source_snapshots
    WHERE account_id = 'diana-ozon'
      AND marketplace = 'ozon'
      AND source = 'sales'
      AND cutoff_at = '2099-08-16 03:00:00+00'
  "

expect_failure_containing \
  "daily-report snapshot row-count mismatch" \
  "source snapshot row_count does not match persisted facts" \
  "${report_collector_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.source_snapshots
        (
            account_id, marketplace, source, cutoff_at, source_as_of,
            period_start, period_end, collector_version,
            claim_id, claim_generation
        )
    SELECT
            'anna-wb', 'wildberries', 'prices', '2099-08-16 03:00:00+00',
            '2099-08-16 02:30:00+00', '2099-08-16 02:30:00+00',
            '2099-08-16 02:30:00+00', 'schema-test',
            claim_id, claim_generation
    FROM daily_reporting.claim_report_collection(
        'anna-wb', 'wildberries', '2099-08-16 03:00:00+00', 'schema-test-anna'
    );
    UPDATE daily_reporting.source_snapshots
    SET status = 'succeeded', pagination_complete = true, row_count = 1,
        payload_sha256 = repeat('b', 64),
        finished_at = '2099-08-16 02:40:00+00'
    WHERE account_id = 'anna-wb'
      AND marketplace = 'wildberries'
      AND source = 'prices'
      AND cutoff_at = '2099-08-16 03:00:00+00'
  "

expect_failure_containing \
  "daily-report observation after the bounded cutoff window" \
  "source_snapshots_observation_window_check" \
  "${report_collector_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.source_snapshots
        (
            account_id, marketplace, source, cutoff_at, source_as_of,
            period_start, period_end, collector_version,
            claim_id, claim_generation
        )
    SELECT
            'anna-wb', 'wildberries', 'stocks', '2099-08-16 03:00:00+00',
            '2099-08-17 03:00:01+00', '2099-08-17 03:00:01+00',
            '2099-08-17 03:00:01+00', 'schema-test',
            claim_id, claim_generation
    FROM daily_reporting.claim_report_collection(
        'anna-wb', 'wildberries', '2099-08-16 03:00:00+00', 'schema-test-anna'
    )
  "

expect_failure_containing \
  "report worker raw snapshot access" \
  "permission denied for table source_snapshots" \
  "${report_worker_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.source_snapshots'

expect_failure_containing \
  "position reader raw daily-report snapshot access" \
  "permission denied for table source_snapshots" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.source_snapshots'

expect_failure_containing \
  "position reader underlying published snapshot access" \
  "permission denied for view published_source_snapshots" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.published_source_snapshots'

expect_failure_containing \
  "report collector outbox access" \
  "permission denied for table delivery_batches" \
  "${report_collector_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.delivery_batches'

expect_failure_containing \
  "report collector raw collection-claim access" \
  "permission denied for table collection_claims" \
  "${report_collector_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.collection_claims'

# Reporting outbox: legacy consolidated coverage remains readable, provider
# attempts are append-only, and terminal delivery is frozen.
"${report_worker_psql[@]}" --command "
  INSERT INTO daily_reporting.delivery_batches
      (recipient_id, report_version, scheduled_for, delayed)
  VALUES ('pilot_owner', 1, '2099-08-16 12:00:00+00', true);
  INSERT INTO daily_reporting.delivery_coverage
      (
          batch_id, recipient_id, report_version, local_date, report_kind,
          scheduled_for, deadline_at
      )
  VALUES
      (
          1, 'pilot_owner', 1, '2099-08-16', 'morning',
          '2099-08-16 03:00:00+00', '2099-08-16 09:00:00+00'
      ),
      (
          1, 'pilot_owner', 1, '2099-08-16', 'evening',
          '2099-08-16 12:00:00+00', '2099-08-16 18:00:00+00'
      );
  UPDATE daily_reporting.delivery_batches
  SET status = 'generating', updated_at = updated_at + interval '1 second'
  WHERE id = 1;
  UPDATE daily_reporting.delivery_batches
  SET status = 'ready',
      artifact_object_key = 'daily-reports/2099/08/16/pilot_owner/v1/evening.xlsx',
      artifact_sha256 = repeat('a', 64),
      artifact_html_sha256 = repeat('b', 64),
      updated_at = updated_at + interval '1 second'
  WHERE id = 1;
  UPDATE daily_reporting.delivery_batches
  SET status = 'sending', attempts = 1,
      updated_at = updated_at + interval '1 second'
  WHERE id = 1;
  INSERT INTO daily_reporting.delivery_attempts
      (
          batch_id, attempt_no, started_at, finished_at, outcome,
          provider_message_id
      )
  VALUES
      (
          1, 1, '2099-08-16 12:01:00+00', '2099-08-16 12:01:01+00',
          'sent', 'gmail-message-1'
      );
  UPDATE daily_reporting.delivery_batches
  SET status = 'sent', provider_message_id = 'gmail-message-1',
      sent_at = '2099-08-16 12:01:01+00',
      updated_at = updated_at + interval '1 second'
  WHERE id = 1;

  INSERT INTO daily_reporting.delivery_batches
      (recipient_id, report_version, scheduled_for)
  VALUES ('ready_owner', 1, '2099-08-18 03:00:00+00');
  INSERT INTO daily_reporting.delivery_coverage
      (
          batch_id, recipient_id, report_version, local_date, report_kind,
          scheduled_for, deadline_at
      )
  SELECT id, recipient_id, report_version, '2099-08-18', 'morning',
         '2099-08-18 03:00:00+00', '2099-08-18 09:00:00+00'
  FROM daily_reporting.delivery_batches
  WHERE recipient_id = 'ready_owner';
  UPDATE daily_reporting.delivery_batches
  SET status = 'generating', updated_at = updated_at + interval '1 second'
  WHERE recipient_id = 'ready_owner';
  UPDATE daily_reporting.delivery_batches
  SET status = 'ready',
      artifact_object_key =
          'daily-reports/2099/08/18/ready_owner/v1/morning.xlsx',
      artifact_sha256 = repeat('c', 64),
      artifact_html_sha256 = repeat('d', 64),
      updated_at = updated_at + interval '1 second'
  WHERE recipient_id = 'ready_owner';
" >/dev/null

report_delivery="$({ "${report_worker_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT batch.status, batch.delayed, batch.attempts,
           count(coverage.report_kind), min(coverage.report_kind),
           max(coverage.report_kind), count(attempt.id)
    FROM daily_reporting.delivery_batches AS batch
    JOIN daily_reporting.delivery_coverage AS coverage
      ON coverage.batch_id = batch.id
    JOIN daily_reporting.delivery_attempts AS attempt
      ON attempt.batch_id = batch.id
    WHERE batch.id = 1
    GROUP BY batch.id
  "; } | tr -d '\r')"
if [[ "$report_delivery" != "sent:t:1:2:evening:morning:2" ]]; then
  echo "legacy consolidated report delivery was not persisted exactly" >&2
  printf '%s\n' "$report_delivery" >&2
  exit 1
fi

mcp_ready_report="$({ "${reader_psql[@]}" --tuples-only --no-align \
  --field-separator=: --command "
    SELECT recipient_id, report_version, local_date, report_kind, status,
           delayed,
           scheduled_for = CASE recipient_id
               WHEN 'pilot_owner' THEN TIMESTAMPTZ '2099-08-16 12:00:00+00'
               ELSE TIMESTAMPTZ '2099-08-18 03:00:00+00'
           END,
           deadline_at = CASE recipient_id
               WHEN 'pilot_owner' THEN TIMESTAMPTZ '2099-08-16 18:00:00+00'
               ELSE TIMESTAMPTZ '2099-08-18 09:00:00+00'
           END
    FROM daily_reporting.mcp_ready_reports
    ORDER BY recipient_id
  "; } | tr -d '\r')"
expected_mcp_ready_report=$'pilot_owner:1:2099-08-16:evening:sent:t:t:t\nready_owner:1:2099-08-18:morning:ready:f:t:t'
if [[ "$mcp_ready_report" != "$expected_mcp_ready_report" ]]; then
  echo "MCP ready-report catalog did not expose the safe batch metadata" >&2
  printf '%s\n' "$mcp_ready_report" >&2
  exit 1
fi

expect_failure_containing \
  "MCP ready-report provider identifier exposure" \
  "column \"provider_message_id\" does not exist" \
  "${reader_psql[@]}" \
  --command 'SELECT provider_message_id FROM daily_reporting.mcp_ready_reports'

# A recovered morning remains its own occurrence and receives only the bounded
# 17:00--23:00 EKB catch-up window.
"${report_worker_psql[@]}" --command "
  INSERT INTO daily_reporting.delivery_batches
      (recipient_id, report_version, scheduled_for, delayed)
  VALUES ('recovered_owner', 1, '2099-08-17 12:00:00+00', true);
  INSERT INTO daily_reporting.delivery_coverage
      (
          batch_id, recipient_id, report_version, local_date, report_kind,
          scheduled_for, deadline_at
      )
  SELECT id, recipient_id, report_version, '2099-08-17', 'morning',
         '2099-08-17 12:00:00+00', '2099-08-17 18:00:00+00'
  FROM daily_reporting.delivery_batches
  WHERE recipient_id = 'recovered_owner';
" >/dev/null

expect_failure_containing \
  "invalid recovered morning deadline" \
  "violates check constraint" \
  "${report_worker_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.delivery_batches
        (recipient_id, report_version, scheduled_for, delayed)
    VALUES ('invalid_recovery', 1, '2099-08-17 12:00:00+00', true);
    INSERT INTO daily_reporting.delivery_coverage
        (
            batch_id, recipient_id, report_version, local_date, report_kind,
            scheduled_for, deadline_at
        )
    SELECT id, recipient_id, report_version, '2099-08-17', 'morning',
           '2099-08-17 12:00:00+00', '2099-08-17 17:59:59+00'
    FROM daily_reporting.delivery_batches
    WHERE recipient_id = 'invalid_recovery';
  "

expect_failure_containing \
  "report artifact identity outside delivery coverage" \
  "report artifact identity does not match delivery coverage" \
  "${report_worker_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.delivery_batches
        (recipient_id, report_version, scheduled_for)
    VALUES ('artifact_probe', 1, '2099-08-17 12:00:00+00');
    INSERT INTO daily_reporting.delivery_coverage
        (
            batch_id, recipient_id, report_version, local_date, report_kind,
            scheduled_for, deadline_at
        )
    SELECT id, recipient_id, report_version, '2099-08-17', 'evening',
           '2099-08-17 12:00:00+00', '2099-08-17 18:00:00+00'
    FROM daily_reporting.delivery_batches
    WHERE recipient_id = 'artifact_probe';
    UPDATE daily_reporting.delivery_batches
    SET status = 'generating', updated_at = updated_at + interval '1 second'
    WHERE recipient_id = 'artifact_probe';
    UPDATE daily_reporting.delivery_batches
    SET status = 'ready',
        artifact_object_key =
            'daily-reports/2099/08/17/artifact_probe/v1/morning.xlsx',
        artifact_sha256 = repeat('a', 64),
        artifact_html_sha256 = repeat('b', 64),
        updated_at = updated_at + interval '1 second'
    WHERE recipient_id = 'artifact_probe';
  "

expect_failure_containing \
  "duplicate report occurrence coverage" \
  "duplicate key value violates unique constraint" \
  "${report_worker_psql[@]}" \
  --command "
    INSERT INTO daily_reporting.delivery_batches
        (recipient_id, report_version, scheduled_for)
    VALUES ('pilot_owner', 1, '2099-08-16 03:00:00+00');
    INSERT INTO daily_reporting.delivery_coverage
        (
            batch_id, recipient_id, report_version, local_date, report_kind,
            scheduled_for, deadline_at
        )
    SELECT id, recipient_id, report_version, '2099-08-16', 'morning',
           '2099-08-16 03:00:00+00', '2099-08-16 09:00:00+00'
    FROM daily_reporting.delivery_batches
    WHERE recipient_id = 'pilot_owner' AND status = 'planned'
  "

expect_failure_containing \
  "terminal report mutation" \
  "terminal report delivery is immutable" \
  "${report_worker_psql[@]}" \
  --command "
    UPDATE daily_reporting.delivery_batches
    SET status = 'expired', updated_at = updated_at + interval '1 second'
    WHERE id = 1
  "

expect_failure_containing \
  "report coverage mutation" \
  "permission denied for table delivery_coverage" \
  "${report_worker_psql[@]}" \
  --command "
    UPDATE daily_reporting.delivery_coverage
    SET deadline_at = deadline_at + interval '1 minute'
    WHERE batch_id = 1
  "

expect_failure_containing \
  "report attempt mutation" \
  "permission denied for table delivery_attempts" \
  "${report_worker_psql[@]}" \
  --command "
    DELETE FROM daily_reporting.delivery_attempts WHERE batch_id = 1
  "

expect_failure_containing \
  "report reconciliation mutation" \
  "permission denied for table delivery_reconciliations" \
  "${report_worker_psql[@]}" \
  --command "
    DELETE FROM daily_reporting.delivery_reconciliations WHERE batch_id = 1
  "

expect_failure_containing \
  "report worker marketplace-history access" \
  "permission denied for schema search_position" \
  "${report_worker_psql[@]}" \
  --command 'SELECT count(*) FROM search_position.monitors'

expect_failure_containing \
  "position reader report-outbox access" \
  "permission denied for table delivery_batches" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.delivery_batches'

expect_failure_containing \
  "position reader report-attempt access" \
  "permission denied for table delivery_attempts" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.delivery_attempts'

expect_failure_containing \
  "position_reader TEMPORARY privilege" \
  "permission denied to create temporary tables in database" \
  "${reader_psql[@]}" \
  --command 'SET default_transaction_read_only=off' \
  --command 'CREATE TEMP TABLE must_be_denied(id integer)'

expect_failure_containing \
  "position_collector TEMPORARY privilege" \
  "permission denied to create temporary tables in database" \
  "${collector_psql[@]}" \
  --command 'CREATE TEMP TABLE must_be_denied(id integer)'

expect_failure_containing \
  "report_worker TEMPORARY privilege" \
  "permission denied to create temporary tables in database" \
  "${report_worker_psql[@]}" \
  --command 'CREATE TEMP TABLE must_be_denied(id integer)'

expect_failure_containing \
  "report_collector TEMPORARY privilege" \
  "permission denied to create temporary tables in database" \
  "${report_collector_psql[@]}" \
  --command 'CREATE TEMP TABLE must_be_denied(id integer)'

expect_failure_containing \
  "control_writer TEMPORARY privilege" \
  "permission denied to create temporary tables in database" \
  "${control_writer_psql[@]}" \
  --command 'CREATE TEMP TABLE must_be_denied(id integer)'

control_runtime_contract="$({ "${control_writer_psql[@]}" --tuples-only --no-align \
  --command "
    SELECT
      current_user = 'control_writer'
      AND has_database_privilege(current_user, current_database(), 'CONNECT')
      AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
      AND NOT has_database_privilege(current_user, current_database(), 'TEMP')
      AND has_schema_privilege(current_user, 'control', 'USAGE')
      AND NOT has_schema_privilege(current_user, 'control', 'CREATE')
      AND has_table_privilege(
          current_user, 'control.wb_policy_revisions', 'SELECT'
      )
      AND has_table_privilege(
          current_user, 'control.wb_policy_revisions', 'INSERT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_policy_revisions', 'UPDATE'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_policy_revisions', 'DELETE'
      )
      AND has_table_privilege(
          current_user, 'control.wb_prepare_reservations', 'SELECT'
      )
      AND has_table_privilege(
          current_user, 'control.wb_prepare_reservations', 'INSERT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_prepare_reservations', 'UPDATE'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_prepare_reservations', 'DELETE'
      )
      AND has_table_privilege(current_user, 'control.wb_plans', 'SELECT')
      AND has_table_privilege(current_user, 'control.wb_plans', 'INSERT')
      AND NOT has_table_privilege(current_user, 'control.wb_plans', 'UPDATE')
      AND NOT has_table_privilege(current_user, 'control.wb_plans', 'DELETE')
      AND has_column_privilege(current_user, 'control.wb_plans', 'status', 'UPDATE')
      AND NOT has_column_privilege(
          current_user, 'control.wb_plans', 'plan_digest', 'UPDATE'
      )
      AND has_table_privilege(
          current_user, 'control.wb_plan_approvals', 'SELECT'
      )
      AND has_table_privilege(
          current_user, 'control.wb_plan_approvals', 'INSERT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_plan_approvals', 'UPDATE'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_plan_approvals', 'DELETE'
      )
      AND has_table_privilege(
          current_user, 'control.wb_runtime_gates', 'SELECT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_runtime_gates', 'INSERT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_runtime_gates', 'UPDATE'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_runtime_gates', 'DELETE'
      )
      AND has_table_privilege(
          current_user, 'control.wb_action_reservations', 'SELECT'
      )
      AND has_table_privilege(
          current_user, 'control.wb_action_reservations', 'INSERT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_action_reservations', 'UPDATE'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_action_reservations', 'DELETE'
      )
      AND has_table_privilege(
          current_user, 'control.wb_audit_events', 'SELECT'
      )
      AND has_table_privilege(
          current_user, 'control.wb_audit_events', 'INSERT'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_audit_events', 'UPDATE'
      )
      AND NOT has_table_privilege(
          current_user, 'control.wb_audit_events', 'DELETE'
      )
      AND has_sequence_privilege(
          current_user, 'control.wb_audit_events_id_seq', 'USAGE'
      )
      AND has_sequence_privilege(
          current_user, 'control.wb_audit_events_id_seq', 'SELECT'
      )
      AND NOT has_sequence_privilege(
          current_user, 'control.wb_audit_events_id_seq', 'UPDATE'
      )
      AND NOT has_schema_privilege(current_user, 'daily_reporting', 'USAGE')
      AND NOT has_schema_privilege(current_user, 'search_position', 'USAGE')
  "; } | tr -d '[:space:]')"
if [[ "$control_runtime_contract" != t ]]; then
  echo "control_writer runtime contract is not least-privilege" >&2
  exit 1
fi

expect_failure_containing \
  "control_writer policy-revision mutation" \
  "permission denied for table wb_policy_revisions" \
  "${control_writer_psql[@]}" \
  --command "UPDATE control.wb_policy_revisions
             SET registered_at = registered_at WHERE false"

expect_failure_containing \
  "control_writer prepare-reservation update" \
  "permission denied for table wb_prepare_reservations" \
  "${control_writer_psql[@]}" \
  --command "UPDATE control.wb_prepare_reservations
             SET reserved_at = reserved_at WHERE false"

expect_failure_containing \
  "control_writer prepare-reservation deletion" \
  "permission denied for table wb_prepare_reservations" \
  "${control_writer_psql[@]}" \
  --command 'DELETE FROM control.wb_prepare_reservations WHERE false'

expect_failure_containing \
  "control_writer plan deletion" \
  "permission denied for table wb_plans" \
  "${control_writer_psql[@]}" \
  --command 'DELETE FROM control.wb_plans WHERE false'

expect_failure_containing \
  "control_writer approval mutation" \
  "permission denied for table wb_plan_approvals" \
  "${control_writer_psql[@]}" \
  --command "UPDATE control.wb_plan_approvals
             SET reason = reason WHERE false"

expect_failure_containing \
  "control_writer runtime-gate mutation" \
  "permission denied for table wb_runtime_gates" \
  "${control_writer_psql[@]}" \
  --command "UPDATE control.wb_runtime_gates
             SET reason = reason WHERE false"

expect_failure_containing \
  "control_writer reservation mutation" \
  "permission denied for table wb_action_reservations" \
  "${control_writer_psql[@]}" \
  --command "UPDATE control.wb_action_reservations
             SET reserved_at = reserved_at WHERE false"

expect_failure_containing \
  "control_writer audit mutation" \
  "permission denied for table wb_audit_events" \
  "${control_writer_psql[@]}" \
  --command "UPDATE control.wb_audit_events
             SET event_type = event_type WHERE false"

expect_failure_containing \
  "position_reader write access" \
  "permission denied for table monitors" \
  "${reader_psql[@]}" \
  --command 'SET default_transaction_read_only=off' \
  --command "
    INSERT INTO search_position.monitors
        (store_id, product_id, search_phrase, region_code, region_name)
    VALUES ('denied', 'denied', 'denied', 'denied', 'denied')
  "

for role in position_collector position_reader report_collector report_worker control_writer wb_automation_writer; do
  case "$role" in
    position_collector) role_password="$collector_password" ;;
    position_reader) role_password="$reader_password" ;;
    report_worker) role_password="$report_worker_password" ;;
    report_collector) role_password="$report_collector_password" ;;
    control_writer) role_password="$control_writer_password" ;;
    wb_automation_writer) role_password="$wb_automation_password" ;;
  esac
  expect_failure_containing \
    "$role connection to the postgres database" \
    'permission denied for database "postgres"' \
    docker exec --env PGPASSWORD="$role_password" "$container" \
    psql --host 127.0.0.1 --username "$role" --dbname postgres \
    --no-psqlrc --set ON_ERROR_STOP=1 --command 'SELECT 1'
done

"${admin_psql[@]}" --command "
  CREATE TABLE search_position.future_table_default_acl_probe (id integer);
  CREATE FUNCTION search_position.default_acl_probe()
  RETURNS integer
  LANGUAGE sql
  IMMUTABLE
  AS 'SELECT 1';
  CREATE TABLE daily_reporting.future_table_default_acl_probe (id integer);
  CREATE FUNCTION daily_reporting.default_acl_probe()
  RETURNS integer
  LANGUAGE sql
  IMMUTABLE
  AS 'SELECT 1';
" >/dev/null

expect_failure_containing \
  "position_reader future-table SELECT privilege" \
  "permission denied for table future_table_default_acl_probe" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM search_position.future_table_default_acl_probe'

expect_failure_containing \
  "position_reader future-function EXECUTE privilege" \
  "permission denied for function default_acl_probe" \
  "${reader_psql[@]}" \
  --command 'SELECT search_position.default_acl_probe()'

expect_failure_containing \
  "position_collector future-function EXECUTE privilege" \
  "permission denied for function default_acl_probe" \
  "${collector_psql[@]}" \
  --command 'SELECT search_position.default_acl_probe()'

expect_failure_containing \
  "position_reader future daily-report table SELECT privilege" \
  "permission denied for table future_table_default_acl_probe" \
  "${reader_psql[@]}" \
  --command 'SELECT count(*) FROM daily_reporting.future_table_default_acl_probe'

expect_failure_containing \
  "position_reader future daily-report function EXECUTE privilege" \
  "permission denied for function default_acl_probe" \
  "${reader_psql[@]}" \
  --command 'SELECT daily_reporting.default_acl_probe()'

role_attributes="$("${admin_psql[@]}" --tuples-only --no-align --field-separator=: \
  --command "
    SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit,
           rolreplication, rolbypassrls, rolconnlimit
    FROM pg_roles
    WHERE rolname IN (
        'position_collector', 'position_reader', 'report_collector', 'report_worker',
        'control_writer', 'wb_automation_writer'
    )
    ORDER BY rolname
  ")"
expected_attributes=$'control_writer:t:f:f:f:f:f:f:4\nposition_collector:t:f:f:f:f:f:f:4\nposition_reader:t:f:f:f:f:f:f:16\nreport_collector:t:f:f:f:f:f:f:4\nreport_worker:t:f:f:f:f:f:f:4\nwb_automation_writer:t:f:f:f:f:f:f:2'
if [[ "$role_attributes" != "$expected_attributes" ]]; then
  echo "restricted database role attributes differ from the expected policy" >&2
  printf '%s\n' "$role_attributes" >&2
  exit 1
fi

echo "Position schema verified: relational invariants and restricted-role ACLs hold."
