#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Every suite that needs a live server, derived from the suites themselves.
# Each one silently skips when its URL is absent, so a suite left out of a
# hand-written list does not fail — it simply never runs, which is how the
# reporting and session-hardening suites went unexercised in CI while still
# passing locally. Selecting by the URLs the tests actually read keeps a newly
# added suite from going dark the same way.
db_suites=()
while IFS= read -r suite; do
  db_suites+=("$suite")
done < <(
  grep -lE '"[A-Z_]*TEST_[A-Z_]*URL"' "$project_root"/tests/*.rs |
    sed 's|.*/||; s|\.rs$||' |
    LC_ALL=C sort
)

if [[ ${#db_suites[@]} -eq 0 ]]; then
  echo "no database-backed test suites found under tests/" >&2
  exit 1
fi

# Every URL those suites read has to be present, or the run degrades into a
# no-op that still reports success.
required_urls=()
while IFS= read -r url; do
  required_urls+=("$url")
done < <(
  grep -ohE '"[A-Z_]*TEST_[A-Z_]*URL"' "$project_root"/tests/*.rs |
    tr -d '"' |
    LC_ALL=C sort -u
)

missing=()
for name in "${required_urls[@]}" POSITION_COLLECTOR_DATABASE_URL; do
  if [[ -z "${!name:-}" ]]; then
    missing+=("$name")
  fi
done
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "run through with-position-test-db.sh; not set: ${missing[*]}" >&2
  exit 1
fi

cargo_args=()
for suite in "${db_suites[@]}"; do
  cargo_args+=(--test "$suite")
done

printf 'running %d database-backed suites: %s\n' \
  "${#db_suites[@]}" "${db_suites[*]}"
cargo test --locked "${cargo_args[@]}"
cargo run --quiet --locked --bin position-collector -- healthcheck
