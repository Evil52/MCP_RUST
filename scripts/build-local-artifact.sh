#!/usr/bin/env bash

# Local, commit-bound Linux binaries. Never publishes or produces CI evidence.
set -euo pipefail
umask 077

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
if [[ $# -ne 0 ]]; then
  echo "usage: bash scripts/build-local-artifact.sh" >&2
  exit 64
fi
if ! git diff --quiet --ignore-submodules -- \
  || ! git diff --cached --quiet --ignore-submodules -- \
  || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "local artifacts require a clean committed checkout" >&2
  exit 1
fi
git_sha="$(git rev-parse HEAD)"
source_tree="$(git rev-parse "$git_sha^{tree}")"
case "$(docker info --format '{{.Architecture}}')" in
  aarch64|arm64) arch=arm64 ;;
  x86_64|amd64) arch=amd64 ;;
  *) echo "only native Linux arm64/amd64 Docker builders are supported" >&2; exit 1 ;;
esac
platform="linux/$arch"
output_dir="$project_root/target/local-artifacts"
mkdir -p "$output_dir"
archive_name="mcp-ozon-${git_sha}-linux-${arch}.tar.gz"
archive="$output_dir/$archive_name"
if [[ -e "$archive" || -e "$archive.sha256" ]]; then
  echo "artifact already exists; refusing to overwrite $archive" >&2
  exit 1
fi

stage="$(mktemp -d "$output_dir/.build-${git_sha:0:12}.XXXXXX")"
mkdir "$stage/source" "$stage/bundle"
# Only committed files enter Docker; ignored credentials/config never enter.
git archive "$git_sha" | tar -x -C "$stage/source"
metadata="$(cd "$stage/source" && cargo metadata --locked --offline --no-deps --format-version 1)"
package="$(jq -ec '.packages[] | select(.name == "mcp-ozon")' <<<"$metadata")"
version="$(jq -er '.version' <<<"$package")"
rust_version="$(jq -er '.rust_version' <<<"$package")"
bins=()
while IFS= read -r binary; do
  if [[ ! "$binary" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
    echo "invalid binary target name" >&2
    exit 1
  fi
  bins+=("$binary")
done < <(jq -r '.targets[] | select(.kind | index("bin")) | .name' <<<"$package" | LC_ALL=C sort)
if [[ "${#bins[@]}" -eq 0 ]]; then
  echo "no runtime binary targets were discovered" >&2
  exit 1
fi

image="mcp-ozon-local-builder:${git_sha}-${arch}"
docker build --platform "$platform" --target builder --tag "$image" \
  --file "$stage/source/Dockerfile" "$stage/source"
image_id="$(docker image inspect --format '{{.Id}}' "$image")"
container=""
cleanup() {
  if [[ -n "$container" ]]; then
    docker rm "$container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
container="$(docker create "$image_id")"
docker cp "$container:/build/runtime-bin" "$stage/extracted-bin"
docker rm "$container" >/dev/null
container=""
mkdir "$stage/bundle/bin"
chmod 755 "$stage/bundle" "$stage/bundle/bin"
checks='[]'
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{ print $1 }'
  else
    shasum -a 256 "$1" | awk '{ print $1 }'
  fi
}
for binary in "${bins[@]}"; do
  source_binary="$stage/extracted-bin/$binary"
  if [[ ! -f "$source_binary" || -L "$source_binary" || ! -x "$source_binary" ]]; then
    echo "missing or unsafe binary: $binary" >&2
    exit 1
  fi
  probe="$(docker run --rm --platform "$platform" --network none --read-only \
    --cap-drop ALL --security-opt no-new-privileges:true --pids-limit 32 \
    --memory 128m --user 65534:65534 --entrypoint "/build/runtime-bin/$binary" \
    "$image_id" --version 2>"$stage/probe.stderr")"
  if [[ "$probe" != "$binary $version" || -s "$stage/probe.stderr" ]]; then
    echo "version probe failed for $binary" >&2
    exit 1
  fi
  cp "$source_binary" "$stage/bundle/bin/$binary"
  chmod 755 "$stage/bundle/bin/$binary"
  digest="$(sha256 "$stage/bundle/bin/$binary")"
  checks="$(jq -c --arg name "$binary" --arg sha256 "$digest" \
    '. + [{name: $name, sha256: $sha256, version_probe: "passed"}]' <<<"$checks")"
done
cp "$stage/source/docs/local-artifacts.md" "$stage/bundle/README.md"
jq -n --arg git_sha "$git_sha" --arg source_tree "$source_tree" \
  --arg platform "$platform" --arg version "$version" --arg rust_version "$rust_version" \
  --arg builder_image_id "$image_id" --argjson binaries "$checks" \
  '{schema_version: 1, kind: "local-rust-binaries", git_sha: $git_sha,
    source_tree: $source_tree, platform: $platform, version: $version,
    declared_rust_version: $rust_version, builder_image_id: $builder_image_id,
    production_release: false, binaries: $binaries}' >"$stage/bundle/manifest.json"
chmod 644 "$stage/bundle/manifest.json" "$stage/bundle/README.md"
COPYFILE_DISABLE=1 tar -czf "$stage/archive.tar.gz" -C "$stage/bundle" .
# Hard-link publication is atomic and refuses a concurrently created target.
ln "$stage/archive.tar.gz" "$archive"
printf '%s  %s\n' "$(sha256 "$archive")" "$archive_name" >"$stage/archive.sha256"
ln "$stage/archive.sha256" "$archive.sha256"
printf 'Local artifact: %s\nChecksum: %s.sha256\n' "$archive" "$archive"
