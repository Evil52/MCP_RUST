#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
evidence="${MCP_RELEASE_EVIDENCE:?MCP_RELEASE_EVIDENCE must point to CI release.json}"
image_lock="${MCP_RELEASE_IMAGE_LOCK:?MCP_RELEASE_IMAGE_LOCK must point to release-images.json}"

for path in "$evidence" "$image_lock"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "CI release input is unavailable or unsafe: $path" >&2
    exit 1
  fi
done
if ! command -v jq >/dev/null 2>&1 || ! command -v gh >/dev/null 2>&1; then
  echo "jq and gh are required to verify CI release evidence" >&2
  exit 1
fi

if ! jq -e '
  .schema_version == 2
  and .workflow_path == ".github/workflows/ci.yml"
  and (.git_sha | type == "string" and test("^[0-9a-f]{40}$"))
  and (.source_tree | type == "string" and test("^[0-9a-f]{40}$"))
  and (.image_lock_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
  and (.repository | type == "string" and test("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"))
  and (.run_id | type == "number" and . > 0 and floor == .)
  and (keys | sort == [
    "git_sha", "image_lock_sha256", "repository", "run_id",
    "schema_version", "source_tree", "workflow_path"
  ])
' "$evidence" >/dev/null; then
  echo "CI release evidence has an invalid contract" >&2
  exit 1
fi

git_sha="$(jq -r '.git_sha' "$evidence")"
source_tree="$(jq -r '.source_tree' "$evidence")"
repository="$(jq -r '.repository' "$evidence")"
run_id="$(jq -r '.run_id' "$evidence")"
expected_image_lock_sha256="$(jq -r '.image_lock_sha256' "$evidence")"

if [[ "$(uname -s)" == "Darwin" ]]; then
  actual_image_lock_sha256="$(shasum -a 256 "$image_lock" | awk '{ print $1 }')"
else
  actual_image_lock_sha256="$(sha256sum "$image_lock" | awk '{ print $1 }')"
fi
if [[ "$actual_image_lock_sha256" != "$expected_image_lock_sha256" ]]; then
  echo "release image lock does not match CI release evidence" >&2
  exit 1
fi
"$project_root/scripts/validate-release-image-lock.sh" \
  "$image_lock" "$git_sha" "$repository"

cd "$project_root"
head_sha="$(git rev-parse HEAD)"
head_tree="$(git rev-parse 'HEAD^{tree}')"
if [[ "$head_sha" != "$git_sha" || "$head_tree" != "$source_tree" ]]; then
  echo "checkout does not match the CI-verified commit and source tree" >&2
  exit 1
fi
if ! git diff --quiet --ignore-submodules -- \
  || ! git diff --cached --quiet --ignore-submodules -- \
  || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "deployment requires a clean checkout, including no untracked files" >&2
  exit 1
fi

run_json="$(gh api "repos/$repository/actions/runs/$run_id")"
if ! jq -e \
  --arg git_sha "$git_sha" \
  '.name == "Rust CI"
   and .path == ".github/workflows/ci.yml"
   and .head_sha == $git_sha
   and .status == "completed"
   and .conclusion == "success"' \
  <<<"$run_json" >/dev/null; then
  echo "GitHub does not confirm a successful Rust CI run for this SHA" >&2
  exit 1
fi

printf '%s\n' "$git_sha"
