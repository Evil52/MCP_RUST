#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 RELEASE_SHA REPOSITORY BASE_REF OUTPUT" >&2
  exit 64
fi

release_sha="$1"
repository="$2"
base_ref="$3"
output="$4"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! "$release_sha" =~ ^[0-9a-f]{40}$ ]] \
  || [[ "$repository" != "Evil52/MCP_RUST" ]] \
  || [[ "$base_ref" != "main" && "$base_ref" != "master" ]]; then
  echo "release source identity is invalid" >&2
  exit 1
fi
for command_name in gh git jq; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "$command_name is required to verify the merged pull request" >&2
    exit 1
  fi
done

cd "$project_root"
if [[ "$(git rev-parse HEAD)" != "$release_sha" ]]; then
  echo "release SHA does not match the checked-out commit" >&2
  exit 1
fi
if ! git diff --quiet --ignore-submodules -- \
  || ! git diff --cached --quiet --ignore-submodules -- \
  || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "merged pull request verification requires a clean checkout" >&2
  exit 1
fi
source_tree="$(git rev-parse "$release_sha^{tree}")"

pulls_json="$(
  gh api \
    -H 'Accept: application/vnd.github+json' \
    "repos/$repository/commits/$release_sha/pulls?per_page=100"
)"
pull_request="$(
  jq -ce \
    --arg base_ref "$base_ref" '
      [
        .[]
        | select(
            .state == "closed"
            and .merged_at != null
            and .base.ref == $base_ref
            and .head.sha != null
            and .head.repo.full_name != null
          )
      ]
      | if length == 1 then .[0]
        else error("release commit must belong to exactly one merged pull request")
        end
    ' <<<"$pulls_json"
)"
pull_request_number="$(jq -er '.number' <<<"$pull_request")"
tested_head_sha="$(jq -er '.head.sha' <<<"$pull_request")"
tested_head_repository="$(jq -er '.head.repo.full_name' <<<"$pull_request")"
if [[ ! "$pull_request_number" =~ ^[1-9][0-9]*$ ]] \
  || [[ ! "$tested_head_sha" =~ ^[0-9a-f]{40}$ ]] \
  || [[ ! "$tested_head_repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
  echo "merged pull request identity is invalid" >&2
  exit 1
fi

head_commit_json="$(
  gh api \
    -H 'Accept: application/vnd.github+json' \
    "repos/$tested_head_repository/git/commits/$tested_head_sha"
)"
tested_head_tree="$(jq -er '.tree.sha' <<<"$head_commit_json")"
if [[ ! "$tested_head_tree" =~ ^[0-9a-f]{40}$ ]] \
  || [[ "$tested_head_tree" != "$source_tree" ]]; then
  echo "merged release tree differs from the pull request tree that passed CI" >&2
  exit 1
fi

checks_json="$(
  gh api \
    -H 'Accept: application/vnd.github+json' \
    "repos/$repository/commits/$tested_head_sha/check-runs?filter=latest&per_page=100"
)"
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
for check_name in "${required_checks[@]}"; do
  if ! jq -e \
    --arg check_name "$check_name" '
      [
        .check_runs[]
        | select(
            .name == $check_name
            and .status == "completed"
            and .conclusion == "success"
            and .app.slug == "github-actions"
          )
      ]
      | length >= 1
    ' <<<"$checks_json" >/dev/null; then
    echo "required pull request check is not successful: $check_name" >&2
    exit 1
  fi
done

output_dir="$(dirname "$output")"
mkdir -p "$output_dir"
temporary="$(mktemp "$output_dir/.tested-pr.XXXXXX")"
trap 'rm -f "$temporary"' EXIT
required_checks_json="$(printf '%s\n' "${required_checks[@]}" | jq -R . | jq -s .)"
jq -n \
  --arg release_sha "$release_sha" \
  --arg source_tree "$source_tree" \
  --arg repository "$repository" \
  --arg base_ref "$base_ref" \
  --argjson pull_request_number "$pull_request_number" \
  --arg tested_head_sha "$tested_head_sha" \
  --argjson required_checks "$required_checks_json" '
    {
      schema_version: 1,
      release_sha: $release_sha,
      source_tree: $source_tree,
      repository: $repository,
      base_ref: $base_ref,
      pull_request_number: $pull_request_number,
      tested_head_sha: $tested_head_sha,
      required_checks: $required_checks
    }
  ' >"$temporary"
chmod 600 "$temporary"
mv -f "$temporary" "$output"
trap - EXIT

printf 'release tree matches tested pull request #%s (%s)\n' \
  "$pull_request_number" "$tested_head_sha"
