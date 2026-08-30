#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dockerfiles=(
  Dockerfile
  Dockerfile.control
  Dockerfile.position-collector
  Dockerfile.report-collector
  Dockerfile.report-worker
  Dockerfile.wb-automation-shadow
)
runtime_bins=(
  mcp-ozon
  mcp-ozon-control
  position-collector
  report-collector
  report-worker
  wb-automation
)

builder_stage() {
  sed '/^FROM alpine:/,$d' "$1"
}

baseline="$(builder_stage "$project_root/${dockerfiles[0]}")"
if [[ "$(grep -c 'cargo build --locked --release --bins' <<<"$baseline")" -ne 1 ]]; then
  echo "the shared Rust builder must compile every binary exactly once" >&2
  exit 1
fi

for runtime_bin in "${runtime_bins[@]}"; do
  if ! grep -Fq "/build/target/release/$runtime_bin" <<<"$baseline"; then
    echo "the shared Rust builder does not retain $runtime_bin" >&2
    exit 1
  fi
done

for index in "${!dockerfiles[@]}"; do
  dockerfile="$project_root/${dockerfiles[$index]}"
  runtime_bin="${runtime_bins[$index]}"
  if [[ "$(builder_stage "$dockerfile")" != "$baseline" ]]; then
    echo "${dockerfiles[$index]} has drifted from the shared Rust builder stage" >&2
    exit 1
  fi
  if ! grep -Fqx \
    "COPY --from=builder /build/runtime-bin/$runtime_bin /usr/local/bin/$runtime_bin" \
    "$dockerfile"; then
    echo "${dockerfiles[$index]} does not copy the expected $runtime_bin artifact" >&2
    exit 1
  fi
done

echo "shared Rust image builder contract passed"
