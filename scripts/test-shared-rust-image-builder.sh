#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ci_workflow="$project_root/.github/workflows/ci.yml"
codeql_workflow="$project_root/.github/workflows/codeql.yml"
release_workflow="$project_root/.github/workflows/release.yml"
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
  '/^  prime-rust-release-cache:/,/^  publish-platform-images:/p' \
  "$release_workflow")"
platform_job="$(sed -n \
  '/^  publish-platform-images:/,/^  assemble-images:/p' \
  "$release_workflow")"
assembly_job="$(sed -n \
  '/^  assemble-images:/,/^  release-evidence:/p' \
  "$release_workflow")"

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

if grep -Fq 'docker/setup-qemu-action@' "$release_workflow"; then
  echo "the release workflow must not build through QEMU" >&2
  exit 1
fi

# shellcheck disable=SC2016 # Literal GitHub expressions are the contract under test.
for required_line in \
  '    runs-on: ${{ matrix.platform.runner }}' \
  '          - arch: amd64' \
  '            name: linux/amd64' \
  '            runner: ubuntu-24.04' \
  '          - arch: arm64' \
  '            name: linux/arm64' \
  '            runner: ubuntu-24.04-arm' \
  '          platforms: ${{ matrix.platform.name }}' \
  '            type=gha,scope=release-${{ matrix.image.id }}-${{ matrix.platform.arch }}' \
  '            type=gha,scope=release-rust-binaries-${{ matrix.platform.arch }}'; do
  if ! grep -Fqx "$required_line" <<<"$platform_job"; then
    echo "native platform publication is missing: $required_line" >&2
    exit 1
  fi
done

for image_id in \
  server \
  control \
  position-db \
  position-collector \
  report-collector \
  report-worker \
  wb-automation \
  ozon-egress \
  control-ingress \
  control-auth-egress \
  control-ozon-write-egress \
  control-write-egress \
  mail-egress; do
  if ! grep -Fqx "          - id: $image_id" <<<"$platform_job"; then
    echo "the native platform matrix is missing $image_id" >&2
    exit 1
  fi
done

# shellcheck disable=SC2016 # Literal shell snippets are the contract under test.
for required_text in \
  'docker buildx imagetools create' \
  '--metadata-file target/release-image-fragment/manifest-metadata.json' \
  'amd64_reference="$(jq -r '\''.reference'\'' "$amd64_fragment")"' \
  'arm64_reference="$(jq -r '\''.reference'\'' "$arm64_fragment")"' \
  'and (.manifests | length == 2)'; do
  if ! grep -Fq -- "$required_text" <<<"$assembly_job"; then
    echo "multi-platform manifest assembly is missing: $required_text" >&2
    exit 1
  fi
done

if grep -Eq '^[[:space:]]+push:' "$ci_workflow" \
  || grep -Eq '^[[:space:]]+push:' "$codeql_workflow"; then
  echo "PR CI and CodeQL must not repeat on a push to the protected branch" >&2
  exit 1
fi
if grep -Eq '^  (prime-rust-release-cache|publish-images|release-evidence):' \
  "$ci_workflow"; then
  echo "release jobs must live only in release.yml" >&2
  exit 1
fi
for required_text in \
  'name: Release CD' \
  '  push:' \
  '  verify-tested-tree:' \
  '  publish-platform-images:' \
  '  assemble-images:' \
  '  release-evidence:'; do
  if ! grep -Fqx "$required_text" "$release_workflow"; then
    echo "release workflow is missing: $required_text" >&2
    exit 1
  fi
done

echo "shared Rust image builder contract passed"
