#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 IMAGE_ID [IMAGE_ID ...]" >&2
  exit 64
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image_lock="${MCP_RELEASE_IMAGE_LOCK:?MCP_RELEASE_IMAGE_LOCK must point to release-images.json}"
evidence="${MCP_RELEASE_EVIDENCE:?MCP_RELEASE_EVIDENCE must point to release.json}"
release_sha="$("$project_root/scripts/verify-release-source.sh")"
repository="$(jq -r '.repository' "$evidence")"

"$project_root/scripts/validate-release-image-lock.sh" \
  "$image_lock" "$release_sha" "$repository"

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi
if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is required to verify image provenance" >&2
  exit 1
fi

selected='{}'
for image_id in "$@"; do
  if ! jq -e --arg image_id "$image_id" '.images[$image_id] != null' \
    "$image_lock" >/dev/null; then
    echo "release image id is unknown: $image_id" >&2
    exit 1
  fi
  reference="$(jq -r --arg image_id "$image_id" '.images[$image_id].reference' "$image_lock")"

  echo "==> verifying attestation for $image_id" >&2
  gh attestation verify "oci://$reference" \
    --repo "$repository" \
    --signer-workflow "$repository/.github/workflows/ci.yml" \
    --source-digest "$release_sha" \
    --deny-self-hosted-runners >&2

  echo "==> pulling immutable $image_id image" >&2
  if ! "$docker_bin" pull "$reference" >&2; then
    echo "public GHCR pull failed; verify package visibility and registry availability" >&2
    exit 1
  fi
  actual_release_sha="$(
    "$docker_bin" image inspect \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
      "$reference"
  )"
  if [[ "$actual_release_sha" != "$release_sha" ]]; then
    echo "release image revision does not match CI evidence: $image_id" >&2
    exit 1
  fi

  selected="$(
    jq -cn \
      --argjson current "$selected" \
      --arg image_id "$image_id" \
      --arg reference "$reference" \
      '$current + {($image_id): $reference}'
  )"
done

jq -cn --arg git_sha "$release_sha" --argjson images "$selected" \
  '{git_sha: $git_sha, images: $images}'
