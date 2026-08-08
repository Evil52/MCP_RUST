#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

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
cargo llvm-cov \
  --locked \
  --all-targets \
  --all-features \
  --ignore-filename-regex 'src/main\.rs$' \
  --fail-under-lines 100

echo "Local CI passed."
