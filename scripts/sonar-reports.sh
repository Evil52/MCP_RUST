#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
report_dir="$project_root/target/sonar"
test_report="$report_dir/test-executions.xml"
test_list="$report_dir/test-list.txt"

cd "$project_root"
mkdir -p "$report_dir"

echo "==> Formatting"
cargo fmt --check

echo "==> Tests"
cargo test
cargo test -- --list 2>/dev/null | sed -n 's/: test$//p' > "$test_list"

{
  printf '%s\n' '<testExecutions version="1">'
  printf '%s\n' '  <file path="tests/sonar.rs">'
  while IFS= read -r test_name; do
    printf '    <testCase name="%s" duration="0"/>\n' "$test_name"
  done < "$test_list"
  printf '%s\n' '  </file>'
  printf '%s\n' '</testExecutions>'
} > "$test_report"

test_count="$(wc -l < "$test_list" | tr -d ' ')"
if [[ "$test_count" == "0" ]]; then
  echo "No Rust tests were discovered" >&2
  exit 1
fi

echo "==> Clippy"
cargo clippy --all-targets --message-format=json -- -D warnings > "$report_dir/clippy.json"

echo "==> Coverage"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is missing. Install it with: cargo install cargo-llvm-cov --locked" >&2
  exit 1
fi
cargo llvm-cov \
  --all-targets \
  --ignore-filename-regex 'src/main\.rs$' \
  --lcov \
  --output-path "$report_dir/lcov.info"
sed -i.bak "s#SF:$project_root/#SF:#" "$report_dir/lcov.info"
rm -f "$report_dir/lcov.info.bak" "$test_list"

echo "Sonar reports are ready: $test_count tests"
