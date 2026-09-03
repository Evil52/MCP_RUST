#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow="$project_root/.github/workflows/ci.yml"
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

prime_job="$(sed -n \
  '/^  prime-rust-release-cache:/,/^  publish-images:/p' \
  "$workflow")"
publish_job="$(sed -n \
  '/^  publish-images:/,/^  release-evidence:/p' \
  "$workflow")"

for required_line in \
  '            runner: ubuntu-24.04' \
  '            runner: ubuntu-24.04-arm' \
  '            platform: linux/amd64' \
  '            platform: linux/arm64' \
  '            cache_scope: release-rust-binaries-amd64' \
  '            cache_scope: release-rust-binaries-arm64' \
  "          platforms: \${{ matrix.platform }}"; do
  if ! grep -Fqx "$required_line" <<<"$prime_job"; then
    echo "the shared Rust cache job is missing: $required_line" >&2
    exit 1
  fi
done

if grep -Fq 'docker/setup-qemu-action@' <<<"$prime_job"; then
  echo "the shared Rust cache job must compile on native runners without QEMU" >&2
  exit 1
fi

for image_id in \
  server \
  control \
  position-collector \
  report-collector \
  report-worker \
  wb-automation; do
  image_config="$(awk -v marker="          - id: $image_id" '
    $0 == marker { capture = 1 }
    capture && $0 != marker && /^          - id: / { exit }
    capture { print }
  ' <<<"$publish_job")"
  for cache_scope in \
    release-rust-binaries-amd64 \
    release-rust-binaries-arm64; do
    if ! grep -Fq "type=gha,scope=$cache_scope" <<<"$image_config"; then
      echo "$image_id does not consume $cache_scope" >&2
      exit 1
    fi
  done
done

echo "shared Rust image builder contract passed"
