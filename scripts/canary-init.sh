#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
environment_file="$project_root/.env"
source_registry="$project_root/config/access.json"
canary_registry="$project_root/config/access.canary.json"
temporary_registry="$canary_registry.tmp"

# This is intentionally a metadata-only check. Docker Compose remains the only
# consumer of .env; this script never reads or copies its contents.
if [[ -L "$environment_file" ]]; then
  echo ".env must not be a symbolic link" >&2
  exit 1
fi

if [[ ! -e "$environment_file" ]]; then
  echo ".env must exist before initializing the canary" >&2
  exit 1
fi

if [[ ! -f "$environment_file" ]]; then
  echo ".env must be a regular file" >&2
  exit 1
fi

if [[ ! -f "$source_registry" || -L "$source_registry" ]]; then
  echo "config/access.json must be an existing regular file" >&2
  exit 1
fi

cleanup() {
  rm -f "$temporary_registry"
}
trap cleanup EXIT

umask 077
install -m 600 "$source_registry" "$temporary_registry"
mv -f "$temporary_registry" "$canary_registry"

echo "Created isolated canary registry: config/access.canary.json"
