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

expect_exact_validation_failure \
  "example admin-password validation" \
  "POSTGRES_PASSWORD must not use an example placeholder" \
  env \
  POSTGRES_USER=position_admin \
  POSTGRES_PASSWORD=replace-with-a-long-random-admin-password \
  POSTGRES_DB=ozon_positions \
  POSITION_COLLECTOR_DB_PASSWORD="$collector_password" \
  POSITION_READER_DB_PASSWORD="$reader_password" \
  "$project_root/position-monitor/initdb/002_roles.sh"

expect_exact_validation_failure \
  "admin/application role-collision validation" \
  "POSTGRES_USER must not reuse a restricted application role" \
  env \
  POSTGRES_USER=position_reader \
  POSTGRES_PASSWORD="$admin_password" \
  POSTGRES_DB=ozon_positions \
  POSITION_COLLECTOR_DB_PASSWORD="$collector_password" \
  POSITION_READER_DB_PASSWORD="$reader_password" \
  "$project_root/position-monitor/initdb/002_roles.sh"

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
  "$container" \
  /docker-entrypoint-initdb.d/002_roles.sh
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
      ('store-a', 'product-a', 'phrase-a', 'region', 'Region'),
      ('store-b', 'product-b', 'phrase-b', 'region', 'Region');
" >/dev/null

"${collector_psql[@]}" --command "
  INSERT INTO search_position.collection_runs (started_at, status, collector_version)
  VALUES (now(), 'running', 'schema-test');
  UPDATE search_position.collection_runs
  SET finished_at = now(), status = 'failed', monitors_attempted = 1,
      monitors_succeeded = 0, error_class = 'upstream', http_status = 502
  WHERE id = 1;
  INSERT INTO search_position.measurements
      (run_id, monitor_id, observed_at, outcome, response_ms, error_class, http_status)
  VALUES (1, 1, now(), 'error', 123, 'transport', 502);
  INSERT INTO search_position.alerts
      (monitor_id, measurement_id, kind)
  VALUES (1, 1, 'collector_error');
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
  INSERT INTO search_position.measurements
      (run_id, monitor_id, observed_at, outcome, organic_position)
  VALUES (1, 2, now(), 'not_found', 7);
" >/dev/null 2>&1; then
  echo "a not_found measurement incorrectly retained a search position" >&2
  exit 1
fi

"${reader_psql[@]}" --tuples-only \
  --command 'SELECT count(*) FROM search_position.latest_measurements' \
  | grep -Eq '^[[:space:]]*1[[:space:]]*$'

latest_diagnostics="$({ "${reader_psql[@]}" --tuples-only --no-align --field-separator=: \
  --command '
    SELECT response_ms, error_class, http_status
    FROM search_position.latest_measurements
    WHERE monitor_id = 1
  '; } | tr -d '\r')"
if [[ "$latest_diagnostics" != "123:transport:502" ]]; then
  echo "latest_measurements omitted collector diagnostics" >&2
  printf '%s\n' "$latest_diagnostics" >&2
  exit 1
fi

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
  "position_reader write access" \
  "permission denied for table monitors" \
  "${reader_psql[@]}" \
  --command 'SET default_transaction_read_only=off' \
  --command "
    INSERT INTO search_position.monitors
        (store_id, product_id, search_phrase, region_code, region_name)
    VALUES ('denied', 'denied', 'denied', 'denied', 'denied')
  "

for role in position_collector position_reader; do
  case "$role" in
    position_collector) role_password="$collector_password" ;;
    position_reader) role_password="$reader_password" ;;
  esac
  expect_failure_containing \
    "$role connection to the postgres database" \
    'permission denied for database "postgres"' \
    docker exec --env PGPASSWORD="$role_password" "$container" \
    psql --host 127.0.0.1 --username "$role" --dbname postgres \
    --no-psqlrc --set ON_ERROR_STOP=1 --command 'SELECT 1'
done

"${admin_psql[@]}" --command "
  CREATE FUNCTION search_position.default_acl_probe()
  RETURNS integer
  LANGUAGE sql
  IMMUTABLE
  AS 'SELECT 1';
" >/dev/null

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

role_attributes="$("${admin_psql[@]}" --tuples-only --no-align --field-separator=: \
  --command "
    SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolinherit,
           rolreplication, rolbypassrls, rolconnlimit
    FROM pg_roles
    WHERE rolname IN ('position_collector', 'position_reader')
    ORDER BY rolname
  ")"
expected_attributes=$'position_collector:f:f:f:f:f:f:4\nposition_reader:f:f:f:f:f:f:16'
if [[ "$role_attributes" != "$expected_attributes" ]]; then
  echo "restricted database role attributes differ from the expected policy" >&2
  printf '%s\n' "$role_attributes" >&2
  exit 1
fi

echo "Position schema verified: relational invariants and restricted-role ACLs hold."
