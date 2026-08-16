#!/usr/bin/env bash

set -euo pipefail

: "${POSITION_REPOSITORY_TEST_ADMIN_URL:?run through with-position-test-db.sh}"
: "${POSITION_REPOSITORY_TEST_COLLECTOR_URL:?run through with-position-test-db.sh}"
: "${POSITION_COLLECTOR_DATABASE_URL:?run through with-position-test-db.sh}"

cargo test --locked --test position_postgres_repository
cargo run --quiet --locked --bin position-collector -- healthcheck
