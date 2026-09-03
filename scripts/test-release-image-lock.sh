#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/mcp-release-lock.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

git_sha="$(printf 'a%.0s' {1..40})"
repository="Evil52/MCP_RUST"
ids=(
  control control-auth-egress control-ingress control-ozon-write-egress
  control-write-egress
  mail-egress ozon-egress position-collector position-db report-collector
  report-worker server wb-automation
)
if [[ "$(uname -s)" == "Darwin" ]]; then
  sha256=(shasum -a 256)
else
  sha256=(sha256sum)
fi

mkdir "$work_dir/fragments"
for image_id in "${ids[@]}"; do
  digest="$(printf '%s' "$image_id" | "${sha256[@]}" | awk '{ print $1 }')"
  jq -n \
    --arg id "$image_id" \
    --arg git_sha "$git_sha" \
    --arg repository "$repository" \
    --arg reference "ghcr.io/evil52/mcp-rust-runtime@sha256:$digest" '
    {
      schema_version: 1,
      id: $id,
      git_sha: $git_sha,
      repository: $repository,
      platforms: ["linux/amd64", "linux/arm64"],
      reference: $reference
    }
  ' >"$work_dir/fragments/$image_id.json"
done

"$project_root/scripts/create-release-image-lock.sh" \
  "$work_dir/fragments" "$git_sha" "$repository" "$work_dir/release-images.json"
"$project_root/scripts/validate-release-image-lock.sh" \
  "$work_dir/release-images.json" "$git_sha" "$repository"

jq '.images.server.reference = "ghcr.io/evil52/mcp-rust-runtime:mutable"' \
  "$work_dir/release-images.json" >"$work_dir/tampered.json"
if "$project_root/scripts/validate-release-image-lock.sh" \
  "$work_dir/tampered.json" "$git_sha" "$repository" 2>/dev/null; then
  echo "release image lock accepted a mutable tag" >&2
  exit 1
fi

echo "release image lock contract passed"
