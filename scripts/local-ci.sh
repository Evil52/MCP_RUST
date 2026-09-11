#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

readonly required_rust_version="1.98.0"
actual_rust_version="$(rustc --version | awk '{ print $2 }')"
if [[ "$actual_rust_version" != "$required_rust_version" ]]; then
  echo "Rust $required_rust_version is required; active rustc is $actual_rust_version" >&2
  exit 1
fi

echo "==> Formatting"
cargo fmt --all -- --check

echo "==> Rust structure budget"
python3 -B scripts/check-rust-structure.py
python3 -B -m unittest discover -s tests -p test_rust_structure.py
python3 -B -m unittest discover -s tests -p test_local_artifact.py

echo "==> Tests"
cargo test --locked --workspace --all-targets --all-features -- --test-threads=1

echo "==> Clippy"
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

echo "==> Documentation"
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features

echo "==> RustSec audit"
cargo audit --deny warnings

echo "==> Dependency policy"
cargo deny check

echo "==> ShellCheck"
if ! command -v shellcheck >/dev/null 2>&1; then
  echo "shellcheck is required: brew install shellcheck" >&2
  exit 1
fi
shellcheck scripts/*.sh position-monitor/*.sh position-monitor/initdb/*.sh
./scripts/test-runtime-health-contract.sh
bash ./scripts/test-operations-portability.sh
bash ./scripts/test-local-runtime-recovery.sh
python3 -B -m unittest discover -s tests -p test_reporting_health_contract.py
python3 -B -m unittest discover -s tests -p test_operations_notifications.py
command -v age >/dev/null
command -v age-keygen >/dev/null
python3 -B -m unittest discover -s tests -p test_recovery_bundle.py
./scripts/test-release-image-lock.sh
./scripts/test-shared-rust-image-builder.sh

echo "==> Library coverage and runtime binary probes"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required: cargo install cargo-llvm-cov --version 0.8.7 --locked" >&2
  exit 1
fi
# Configured baseline after the large inline server test module moved out of
# src/server.rs. Other inline test modules still contribute to this figure;
# it is not production-only coverage. Runtime entrypoints use separate probes.
# Match the ordinary test run: fixtures share connection-limited DB roles.
# Individual concurrency tests still exercise their own parallel tasks.
./scripts/with-position-test-db.sh cargo llvm-cov \
  --locked --workspace \
    --all-targets \
    --all-features \
    --ignore-filename-regex 'src/(main|bin/(mcp-ozon-control|ozon-campaign-guard|position-collector|report-collector|report-worker|wb-automation))\.rs$' \
    --show-missing-lines \
    --fail-under-functions 95.5 \
    --fail-under-lines 95.8 \
    -- --test-threads=1

echo "Local CI passed."
