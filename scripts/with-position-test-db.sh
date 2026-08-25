#!/usr/bin/env bash

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 command [args ...]" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
suffix="${GITHUB_RUN_ID:-local}-${RANDOM}-$$"
image="mcp-ozon-position-repository-test:${suffix}"
container="mcp-ozon-position-repository-test-${suffix}"
data_volume="${container}-data"
admin_password="position-admin-repository-test"
collector_password="position-collector-repository-test"
reader_password="position-reader-repository-test"
report_worker_password="report-worker-repository-test"
report_collector_password="report-collector-repository-test"
control_writer_password="control-writer-repository-test"
wb_automation_password="wb-automation-repository-test"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  docker volume rm "$data_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build --pull --tag "$image" \
  --file "$project_root/position-monitor/Dockerfile" \
  "$project_root/position-monitor" >/dev/null
docker volume create "$data_volume" >/dev/null
docker run --detach --rm --name "$container" \
  --volume "$data_volume:/var/lib/postgresql/data" \
  --publish 127.0.0.1::5432 \
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
  if docker exec "$container" /usr/local/bin/position-db-healthcheck \
      >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs "$container" >&2
  echo "position repository test database did not become ready" >&2
  exit 1
fi

mapped_endpoint="$(docker port "$container" 5432/tcp)"
mapped_port="${mapped_endpoint##*:}"
if [[ ! "$mapped_port" =~ ^[0-9]+$ ]]; then
  echo "Docker returned an invalid PostgreSQL host port" >&2
  exit 1
fi

export POSITION_REPOSITORY_TEST_ADMIN_URL="postgresql://position_admin:${admin_password}@127.0.0.1:${mapped_port}/ozon_positions"
export POSITION_REPOSITORY_TEST_COLLECTOR_URL="postgresql://position_collector:${collector_password}@127.0.0.1:${mapped_port}/ozon_positions"
export POSITION_REPOSITORY_TEST_READER_URL="postgresql://position_reader:${reader_password}@127.0.0.1:${mapped_port}/ozon_positions"
export REPORT_OUTBOX_TEST_WORKER_URL="postgresql://report_worker:${report_worker_password}@127.0.0.1:${mapped_port}/ozon_positions"
export REPORT_SNAPSHOT_TEST_COLLECTOR_URL="postgresql://report_collector:${report_collector_password}@127.0.0.1:${mapped_port}/ozon_positions"
export WB_CONTROL_TEST_DATABASE_URL="postgresql://control_writer:${control_writer_password}@127.0.0.1:${mapped_port}/ozon_positions"
export WB_AUTOMATION_TEST_DATABASE_URL="postgresql://wb_automation_writer:${wb_automation_password}@127.0.0.1:${mapped_port}/ozon_positions"
export POSITION_COLLECTOR_MODE=disabled
export POSITION_COLLECTOR_DATABASE_URL="$POSITION_REPOSITORY_TEST_COLLECTOR_URL"

cd "$project_root"
"$@"
