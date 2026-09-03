#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/mcp-release-evidence.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/target/fragments" "$work_dir/bin"
cp \
  "$project_root/scripts/create-release-image-lock.sh" \
  "$project_root/scripts/validate-release-image-lock.sh" \
  "$project_root/scripts/verify-release-source.sh" \
  "$project_root/scripts/write-release-evidence.sh" \
  "$work_dir/repo/scripts/"
printf 'target/\n' >"$work_dir/repo/.gitignore"
git -C "$work_dir/repo" init --quiet
git -C "$work_dir/repo" config user.name test
git -C "$work_dir/repo" config user.email test@example.invalid
git -C "$work_dir/repo" add .gitignore scripts
git -C "$work_dir/repo" commit --quiet -m fixture

git_sha="$(git -C "$work_dir/repo" rev-parse HEAD)"
repository="Evil52/MCP_RUST"
ids=(
  control control-auth-egress control-ingress control-ozon-write-egress
  control-write-egress mail-egress ozon-egress position-collector position-db
  report-collector report-worker server wb-automation
)
if [[ "$(uname -s)" == "Darwin" ]]; then
  sha256=(shasum -a 256)
else
  sha256=(sha256sum)
fi
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
    ' >"$work_dir/repo/target/fragments/$image_id.json"
done

"$work_dir/repo/scripts/create-release-image-lock.sh" \
  "$work_dir/repo/target/fragments" \
  "$git_sha" \
  "$repository" \
  "$work_dir/repo/target/release-images.json"
"$work_dir/repo/scripts/write-release-evidence.sh" \
  "$git_sha" \
  "$repository" \
  12345 \
  "$work_dir/repo/target/release-images.json" \
  "$work_dir/repo/target/release.json"

if [[ "$(jq -r '.workflow_path' "$work_dir/repo/target/release.json")" \
  != ".github/workflows/release.yml" ]]; then
  echo "new release evidence does not identify Release CD" >&2
  exit 1
fi

cat >"$work_dir/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat "$MOCK_RUN_JSON"
EOF
chmod 700 "$work_dir/bin/gh"

verify() {
  PATH="$work_dir/bin:$PATH" \
    MOCK_RUN_JSON="$work_dir/run.json" \
    MCP_RELEASE_EVIDENCE="$work_dir/repo/target/release.json" \
    MCP_RELEASE_IMAGE_LOCK="$work_dir/repo/target/release-images.json" \
    "$work_dir/repo/scripts/verify-release-source.sh"
}

jq -n --arg git_sha "$git_sha" '
  {
    name: "Release CD",
    path: ".github/workflows/release.yml",
    head_sha: $git_sha,
    status: "completed",
    conclusion: "success"
  }
' >"$work_dir/run.json"
verify >/dev/null

jq '.workflow_path = ".github/workflows/ci.yml"' \
  "$work_dir/repo/target/release.json" \
  >"$work_dir/repo/target/legacy-release.json"
mv "$work_dir/repo/target/legacy-release.json" "$work_dir/repo/target/release.json"
jq '.name = "Rust CI" | .path = ".github/workflows/ci.yml"' \
  "$work_dir/run.json" >"$work_dir/legacy-run.json"
mv "$work_dir/legacy-run.json" "$work_dir/run.json"
verify >/dev/null

jq '.name = "Release CD"' "$work_dir/run.json" >"$work_dir/wrong-run.json"
mv "$work_dir/wrong-run.json" "$work_dir/run.json"
if verify >/dev/null 2>&1; then
  echo "release verification accepted a workflow name/path mismatch" >&2
  exit 1
fi

echo "release evidence contract passed"
