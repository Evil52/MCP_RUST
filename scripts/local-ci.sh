#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

readonly required_rust_version="1.97.1"
actual_rust_version="$(rustc --version | awk '{ print $2 }')"
if [[ "$actual_rust_version" != "$required_rust_version" ]]; then
  echo "Rust $required_rust_version is required; active rustc is $actual_rust_version" >&2
  exit 1
fi

echo "==> Formatting"
cargo fmt --all -- --check

echo "==> Tests"
cargo test --locked --all-targets --all-features

echo "==> Clippy"
cargo clippy --locked --all-targets --all-features -- -D warnings

echo "==> Documentation"
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features

echo "==> RustSec audit"
cargo audit --deny warnings

echo "==> Dependency policy"
cargo deny check

echo "==> Coverage"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required: cargo install cargo-llvm-cov --version 0.8.7 --locked" >&2
  exit 1
fi
./scripts/with-position-test-db.sh cargo llvm-cov \
  --locked \
  --all-targets \
  --all-features \
  --ignore-filename-regex 'src/(main|bin/mcp-ozon-control)\.rs$' \
  --fail-under-lines 100

echo "Local CI passed."
