#!/usr/bin/env bash

set -euo pipefail

: "${POSITION_REPOSITORY_TEST_ADMIN_URL:?run through with-position-test-db.sh}"
: "${POSITION_REPOSITORY_TEST_COLLECTOR_URL:?run through with-position-test-db.sh}"
: "${POSITION_COLLECTOR_DATABASE_URL:?run through with-position-test-db.sh}"
: "${REPORT_OUTBOX_TEST_WORKER_URL:?run through with-position-test-db.sh}"
: "${REPORT_SNAPSHOT_TEST_COLLECTOR_URL:?run through with-position-test-db.sh}"
: "${WB_AUTOMATION_TEST_DATABASE_URL:?run through with-position-test-db.sh}"

# Every suite that needs a live server. Each one silently skips when its URL is
# absent, so a suite left out here does not fail — it simply never runs, which
# is how the reporting and session-hardening suites went unexercised in CI
# while still passing locally.
cargo test --locked \
  --test position_postgres_repository \
  --test postgres_session_hardening \
  --test reporting_postgres_outbox \
  --test reporting_postgres_snapshots \
  --test wb_automation_postgres
cargo run --quiet --locked --bin position-collector -- healthcheck
