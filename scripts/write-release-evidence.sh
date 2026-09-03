#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 GIT_SHA REPOSITORY RUN_ID IMAGE_LOCK OUTPUT" >&2
  exit 64
fi

git_sha="$1"
repository="$2"
run_id="$3"
image_lock="$4"
output="$5"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! "$git_sha" =~ ^[0-9a-f]{40}$ ]] \
  || [[ ! "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] \
  || [[ ! "$run_id" =~ ^[1-9][0-9]*$ ]]; then
  echo "release identity is invalid" >&2
  exit 1
fi

cd "$project_root"
head_sha="$(git rev-parse HEAD)"
if [[ "$head_sha" != "$git_sha" ]]; then
  echo "release SHA does not match the checked-out commit" >&2
  exit 1
fi
if ! git diff --quiet --ignore-submodules -- \
  || ! git diff --cached --quiet --ignore-submodules -- \
  || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "release evidence requires a clean checkout" >&2
  exit 1
fi

source_tree="$(git rev-parse "$git_sha^{tree}")"
"$project_root/scripts/validate-release-image-lock.sh" \
  "$image_lock" "$git_sha" "$repository"
if [[ "$(uname -s)" == "Darwin" ]]; then
  image_lock_sha256="$(shasum -a 256 "$image_lock" | awk '{ print $1 }')"
else
  image_lock_sha256="$(sha256sum "$image_lock" | awk '{ print $1 }')"
fi
output_dir="$(dirname "$output")"
mkdir -p "$output_dir"
temporary="$(mktemp "$output_dir/.release-evidence.XXXXXX")"
trap 'rm -f "$temporary"' EXIT

jq -n \
  --arg git_sha "$git_sha" \
  --arg source_tree "$source_tree" \
  --arg image_lock_sha256 "$image_lock_sha256" \
  --arg repository "$repository" \
  --argjson run_id "$run_id" \
  '{
    schema_version: 2,
    workflow_path: ".github/workflows/release.yml",
    git_sha: $git_sha,
    source_tree: $source_tree,
    image_lock_sha256: $image_lock_sha256,
    repository: $repository,
    run_id: $run_id
  }' >"$temporary"
chmod 600 "$temporary"
mv -f "$temporary" "$output"
trap - EXIT
