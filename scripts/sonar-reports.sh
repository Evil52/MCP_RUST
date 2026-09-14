#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Keep verification artifacts separate from shared worktree build caches.
export CARGO_TARGET_DIR="$project_root/target/verification/cargo"
export CARGO_BUILD_BUILD_DIR="$CARGO_TARGET_DIR"
export CARGO_LLVM_COV_TARGET_DIR="$project_root/target/verification/coverage"
export CARGO_LLVM_COV_BUILD_DIR="$CARGO_LLVM_COV_TARGET_DIR"
report_dir="$project_root/target/sonar"
tools_dir="$project_root/target/verification/sonar-tools"
test_report="$report_dir/test-executions.xml"
test_output="$report_dir/test-output.txt"
clippy_report="$report_dir/clippy.json"
coverage_report="$report_dir/lcov.info"
python_coverage_report="$report_dir/python-coverage.xml"
python_coverage_data="$report_dir/python-coverage"
shellcheck_report="$report_dir/shellcheck-issues.json"
zizmor_report="$report_dir/zizmor.sarif"
test_report_tmp="$test_report.tmp"
clippy_report_tmp="$clippy_report.tmp"
coverage_report_tmp="$coverage_report.tmp"
shellcheck_output_tmp="$report_dir/shellcheck.json1.tmp"
zizmor_report_tmp="$zizmor_report.tmp"

cleanup() {
  rm -f \
    "$test_report_tmp" \
    "$clippy_report_tmp" \
    "$coverage_report_tmp" \
    "$shellcheck_output_tmp" \
    "$zizmor_report_tmp"
}
trap cleanup EXIT

cd "$project_root"
mkdir -p "$report_dir"
rm -rf "$test_report" "$clippy_report" "$coverage_report" "$python_coverage_report" \
  "$python_coverage_data" "$shellcheck_report" "$zizmor_report"

echo "==> Formatting"
cargo fmt --all -- --check

echo "==> Tests"
./scripts/with-position-test-db.sh cargo test \
  --locked --workspace --all-targets --all-features -- \
  --include-ignored --test-threads=1 \
  | tee "$test_output"
python3 "$project_root/scripts/sonar-test-report.py" "$test_output" "$test_report_tmp"
mv "$test_report_tmp" "$test_report"

echo "==> Clippy"
cargo clippy --locked --workspace --all-targets --all-features --message-format=json -- -D warnings \
  > "$clippy_report_tmp"
mv "$clippy_report_tmp" "$clippy_report"

echo "==> Coverage"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is missing. Install it with: cargo install cargo-llvm-cov --locked" >&2
  exit 1
fi
# Every PostgreSQL contract runs against the isolated fixture database.
# The environment also works for binary probes without forwarding harness flags.
# Runtime entrypoints are measured, not filtered out. The default filename
# filter is disabled because it drops `tests/../src/bin/...` instantiations of
# binary modules that integration tests include with #[path]; the normalizer
# merges them back into their source files and drops non-project records.
RUST_TEST_THREADS=1 ./scripts/with-position-test-db.sh cargo llvm-cov \
  --locked --workspace --all-targets --all-features \
  --disable-default-ignore-filename-regex \
  --lcov \
  --output-path "$coverage_report_tmp" \
  -- --include-ignored
python3 -B "$project_root/scripts/sonar-lcov-normalize.py" \
  "$coverage_report_tmp" "$project_root" "$coverage_report"

echo "==> Sonar analysis tools"
if [[ ! -x "$tools_dir/bin/python" ]]; then
  python3 -m venv "$tools_dir"
fi
"$tools_dir/bin/python" -m pip install --quiet --disable-pip-version-check \
  --require-hashes --only-binary=:all: \
  --requirement "$project_root/scripts/sonar-tools-requirements.txt"

echo "==> Python coverage"
# Branch coverage for scripts/, including scripts the tests start as
# subprocesses. Absolute source keeps subprocesses with another cwd measured,
# and the tools venv leads PATH so a `python3` subprocess can load coverage.
python_coverage_rc="$python_coverage_data/coveragerc"
mkdir -p "$python_coverage_data"
cat > "$python_coverage_rc" <<EOF
[run]
branch = true
parallel = true
patch = subprocess
relative_files = true
source = $project_root/scripts
data_file = $python_coverage_data/.coverage
EOF
PATH="$tools_dir/bin:$PATH" COVERAGE_RCFILE="$python_coverage_rc" \
  "$tools_dir/bin/python" -B -m coverage run -m unittest discover -s tests -p 'test_*.py'
COVERAGE_RCFILE="$python_coverage_rc" "$tools_dir/bin/python" -m coverage combine --quiet
COVERAGE_RCFILE="$python_coverage_rc" "$tools_dir/bin/python" -m coverage xml --quiet \
  -o "$python_coverage_report"
COVERAGE_RCFILE="$python_coverage_rc" "$tools_dir/bin/python" -m coverage report

echo "==> ShellCheck"
if ! command -v shellcheck >/dev/null 2>&1; then
  echo "shellcheck is required: brew install shellcheck" >&2
  exit 1
fi
shell_scripts=()
while IFS= read -r -d '' shell_script; do
  shell_scripts+=("$shell_script")
done < <(git ls-files -z -- '*.sh')
# Findings are imported into Sonar (exit 1); only tool failures (>1) abort.
shellcheck_status=0
shellcheck -x --format=json1 "${shell_scripts[@]}" \
  > "$shellcheck_output_tmp" || shellcheck_status=$?
if ((shellcheck_status > 1)); then
  echo "ShellCheck failed with exit status $shellcheck_status" >&2
  exit 1
fi
python3 -B "$project_root/scripts/sonar-external-issues.py" \
  "$shellcheck_output_tmp" "$project_root" "$shellcheck_report"

echo "==> GitHub Actions audit"
# SARIF output never uses zizmor's finding exit codes; offline keeps the
# report reproducible without a GitHub token.
git ls-files -z -- '.github/workflows/*.yml' '.github/workflows/*.yaml' \
  '.github/dependabot.yml' '.github/dependabot.yaml' \
  | xargs -0 "$tools_dir/bin/zizmor" --offline --format=sarif > "$zizmor_report_tmp"
mv "$zizmor_report_tmp" "$zizmor_report"

echo "Sonar reports are ready (executed and skipped test results preserved)."
