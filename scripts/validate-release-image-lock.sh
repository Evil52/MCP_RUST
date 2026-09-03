#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 IMAGE_LOCK GIT_SHA REPOSITORY" >&2
  exit 64
fi

image_lock="$1"
git_sha="$2"
repository="$3"

if [[ ! -f "$image_lock" || -L "$image_lock" ]]; then
  echo "release image lock is unavailable or unsafe: $image_lock" >&2
  exit 1
fi
if [[ ! "$git_sha" =~ ^[0-9a-f]{40}$ ]] \
  || [[ "$repository" != "Evil52/MCP_RUST" ]]; then
  echo "release image lock identity is invalid" >&2
  exit 1
fi

jq -e \
  --arg git_sha "$git_sha" \
  --arg repository "$repository" '
  .schema_version == 1
  and .git_sha == $git_sha
  and .repository == $repository
  and .registry == "ghcr.io"
  and .package == "ghcr.io/evil52/mcp-rust-runtime"
  and .platforms == ["linux/amd64", "linux/arm64"]
  and (keys | sort == [
    "git_sha", "images", "package", "platforms", "registry",
    "repository", "schema_version"
  ])
  and (.images | keys | sort == [
    "control", "control-auth-egress", "control-ingress",
    "control-ozon-write-egress", "control-write-egress", "mail-egress",
    "ozon-egress",
    "position-collector", "position-db", "report-collector",
    "report-worker", "server", "wb-automation"
  ])
  and ([.images[]] | all(
    (keys | sort == ["reference"])
    and (.reference | type == "string")
    and (.reference | test(
      "^ghcr\\.io/evil52/mcp-rust-runtime@sha256:[0-9a-f]{64}$"
    ))
  ))
' "$image_lock" >/dev/null
