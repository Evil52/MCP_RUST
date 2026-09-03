#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/mcp-tested-pr.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

mkdir -p "$work_dir/repo/scripts" "$work_dir/bin" "$work_dir/fixtures"
cp "$project_root/scripts/verify-merged-pr-release.sh" "$work_dir/repo/scripts/"
printf 'release source\n' >"$work_dir/repo/source.txt"
git -C "$work_dir/repo" init --quiet
git -C "$work_dir/repo" config user.name test
git -C "$work_dir/repo" config user.email test@example.invalid
git -C "$work_dir/repo" add scripts/verify-merged-pr-release.sh source.txt
git -C "$work_dir/repo" commit --quiet -m fixture

release_sha="$(git -C "$work_dir/repo" rev-parse HEAD)"
source_tree="$(git -C "$work_dir/repo" rev-parse 'HEAD^{tree}')"
tested_head_sha="$(printf 'b%.0s' {1..40})"

jq -n \
  --arg head_sha "$tested_head_sha" '
  [
    {
      number: 59,
      state: "closed",
      merged_at: "2026-09-03T09:40:52Z",
      base: {ref: "master"},
      head: {sha: $head_sha, repo: {full_name: "Evil52/MCP_RUST"}}
    }
  ]
' >"$work_dir/fixtures/pulls.json"
jq -n --arg tree "$source_tree" '{tree: {sha: $tree}}' \
  >"$work_dir/fixtures/head-commit.json"

required_checks=(
  "Quality"
  "Rust 1.98.0"
  "Core library source lines"
  "Dependency security"
  "Hardened container"
  "Analyze (actions)"
  "Analyze (rust)"
  "Dependency review"
)
required_checks_json="$(printf '%s\n' "${required_checks[@]}" | jq -R . | jq -s .)"
jq -n --argjson names "$required_checks_json" '
  {
    check_runs: [
      $names[]
      | {
          name: .,
          status: "completed",
          conclusion: "success",
          app: {slug: "github-actions"}
        }
    ]
  }
' >"$work_dir/fixtures/checks.json"

cat >"$work_dir/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
endpoint="${*: -1}"
case "$endpoint" in
  */pulls?per_page=100)
    cat "$MOCK_PULLS_JSON"
    ;;
  */check-runs?filter=latest\&per_page=100)
    cat "$MOCK_CHECKS_JSON"
    ;;
  */git/commits/*)
    cat "$MOCK_HEAD_COMMIT_JSON"
    ;;
  *)
    echo "unexpected gh api endpoint: $endpoint" >&2
    exit 1
    ;;
esac
EOF
chmod 700 "$work_dir/bin/gh"

run_verifier() {
  PATH="$work_dir/bin:$PATH" \
    MOCK_PULLS_JSON="$work_dir/fixtures/pulls.json" \
    MOCK_CHECKS_JSON="$work_dir/fixtures/checks.json" \
    MOCK_HEAD_COMMIT_JSON="$work_dir/fixtures/head-commit.json" \
    "$work_dir/repo/scripts/verify-merged-pr-release.sh" \
      "$release_sha" Evil52/MCP_RUST master "$1"
}

run_verifier "$work_dir/tested-pr.json" >/dev/null
jq -e \
  --arg release_sha "$release_sha" \
  --arg source_tree "$source_tree" \
  --arg tested_head_sha "$tested_head_sha" '
    .schema_version == 1
    and .release_sha == $release_sha
    and .source_tree == $source_tree
    and .pull_request_number == 59
    and .tested_head_sha == $tested_head_sha
    and (.required_checks | length == 8)
  ' "$work_dir/tested-pr.json" >/dev/null

jq 'del(.check_runs[] | select(.name == "Quality"))' \
  "$work_dir/fixtures/checks.json" >"$work_dir/fixtures/checks-missing.json"
mv "$work_dir/fixtures/checks-missing.json" "$work_dir/fixtures/checks.json"
if run_verifier "$work_dir/missing-check.json" >/dev/null 2>&1; then
  echo "merged PR verification accepted a missing required check" >&2
  exit 1
fi

jq -n --arg tree "$(printf 'c%.0s' {1..40})" '{tree: {sha: $tree}}' \
  >"$work_dir/fixtures/head-commit.json"
jq -n --argjson names "$required_checks_json" '
  {
    check_runs: [
      $names[]
      | {
          name: .,
          status: "completed",
          conclusion: "success",
          app: {slug: "github-actions"}
        }
    ]
  }
' >"$work_dir/fixtures/checks.json"
if run_verifier "$work_dir/wrong-tree.json" >/dev/null 2>&1; then
  echo "merged PR verification accepted a different source tree" >&2
  exit 1
fi

echo "merged pull request release contract passed"
