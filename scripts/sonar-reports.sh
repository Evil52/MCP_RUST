#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
report_dir="$project_root/target/sonar"
test_report="$report_dir/test-executions.xml"
test_list="$report_dir/test-list.txt"
clippy_report="$report_dir/clippy.json"
coverage_report="$report_dir/lcov.info"
test_report_tmp="$test_report.tmp"
clippy_report_tmp="$clippy_report.tmp"
coverage_report_tmp="$coverage_report.tmp"

cleanup() {
  rm -f \
    "$test_list" \
    "$test_report_tmp" \
    "$clippy_report_tmp" \
    "$coverage_report_tmp" \
    "$coverage_report_tmp.bak"
}
trap cleanup EXIT

cd "$project_root"
mkdir -p "$report_dir"
rm -f "$test_report" "$clippy_report" "$coverage_report"

echo "==> Formatting"
cargo fmt --all -- --check

echo "==> Tests"
cargo test --locked --all-targets --all-features -- --test-threads=1
cargo test --locked --all-targets --all-features -- --list 2>/dev/null \
  | sed -n 's/: test$//p' > "$test_list"

{
  printf '%s\n' '<testExecutions version="1">'
  printf '%s\n' '  <file path="tests/sonar.rs">'
  while IFS= read -r test_name; do
    printf '    <testCase name="%s" duration="0"/>\n' "$test_name"
  done < "$test_list"
  printf '%s\n' '  </file>'
  printf '%s\n' '</testExecutions>'
} > "$test_report_tmp"

test_count="$(wc -l < "$test_list" | tr -d ' ')"
if [[ "$test_count" == "0" ]]; then
  echo "No Rust tests were discovered" >&2
  exit 1
fi
mv "$test_report_tmp" "$test_report"

echo "==> Clippy"
cargo clippy --locked --all-targets --all-features --message-format=json -- -D warnings \
  > "$clippy_report_tmp"
mv "$clippy_report_tmp" "$clippy_report"

echo "==> Coverage"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is missing. Install it with: cargo install cargo-llvm-cov --locked" >&2
  exit 1
fi
./scripts/with-position-test-db.sh cargo llvm-cov \
  --locked \
  --all-targets \
  --all-features \
  --ignore-filename-regex 'src/(main|bin/(mcp-ozon-control|ozon-campaign-guard|position-collector|report-collector|report-worker|wb-automation))\.rs$' \
  --lcov \
  --output-path "$coverage_report_tmp"
sed -i.bak "s#SF:$project_root/#SF:#" "$coverage_report_tmp"
mv "$coverage_report_tmp" "$coverage_report"

echo "Sonar reports are ready: $test_count tests"
