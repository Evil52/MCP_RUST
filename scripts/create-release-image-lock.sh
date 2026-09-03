#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 FRAGMENT_DIR GIT_SHA REPOSITORY OUTPUT" >&2
  exit 64
fi

fragment_dir="$1"
git_sha="$2"
repository="$3"
output="$4"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -d "$fragment_dir" || -L "$fragment_dir" ]]; then
  echo "release image fragment directory is unavailable or unsafe" >&2
  exit 1
fi
if [[ ! "$git_sha" =~ ^[0-9a-f]{40}$ ]] \
  || [[ "$repository" != "Evil52/MCP_RUST" ]]; then
  echo "release image identity is invalid" >&2
  exit 1
fi

expected_ids=(
  control
  control-auth-egress
  control-ingress
  control-ozon-write-egress
  control-write-egress
  mail-egress
  ozon-egress
  position-collector
  position-db
  report-collector
  report-worker
  server
  wb-automation
)
fragments=()
for image_id in "${expected_ids[@]}"; do
  fragment="$fragment_dir/$image_id.json"
  if [[ ! -f "$fragment" || -L "$fragment" ]]; then
    echo "release image fragment is missing or unsafe: $image_id" >&2
    exit 1
  fi
  fragments+=("$fragment")
done

fragment_count="$(find "$fragment_dir" -maxdepth 1 -type f -name '*.json' | wc -l | tr -d ' ')"
if [[ "$fragment_count" != "${#expected_ids[@]}" ]]; then
  echo "release image fragment set contains unexpected files" >&2
  exit 1
fi

output_dir="$(dirname "$output")"
mkdir -p "$output_dir"
temporary="$(mktemp "$output_dir/.release-images.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary"
}
trap cleanup EXIT

jq -n \
  --arg git_sha "$git_sha" \
  --arg repository "$repository" \
  --slurpfile control "$fragment_dir/control.json" \
  --slurpfile control_auth_egress "$fragment_dir/control-auth-egress.json" \
  --slurpfile control_ingress "$fragment_dir/control-ingress.json" \
  --slurpfile control_ozon_write_egress "$fragment_dir/control-ozon-write-egress.json" \
  --slurpfile control_write_egress "$fragment_dir/control-write-egress.json" \
  --slurpfile mail_egress "$fragment_dir/mail-egress.json" \
  --slurpfile ozon_egress "$fragment_dir/ozon-egress.json" \
  --slurpfile position_collector "$fragment_dir/position-collector.json" \
  --slurpfile position_db "$fragment_dir/position-db.json" \
  --slurpfile report_collector "$fragment_dir/report-collector.json" \
  --slurpfile report_worker "$fragment_dir/report-worker.json" \
  --slurpfile server "$fragment_dir/server.json" \
  --slurpfile wb_automation "$fragment_dir/wb-automation.json" '
  def checked($entry; $id):
    if (
        ($entry | length) == 1
        and ($entry[0]
        | (
        .schema_version == 1
        and .id == $id
        and .git_sha == $git_sha
        and .repository == $repository
        and .platforms == ["linux/amd64", "linux/arm64"]
        and (.reference | test(
          "^ghcr\\.io/evil52/mcp-rust-runtime@sha256:[0-9a-f]{64}$"
        ))
        and (keys | sort == [
          "git_sha", "id", "platforms", "reference", "repository",
          "schema_version"
        ])
        ))
      ) then {reference: $entry[0].reference}
      else error("invalid release fragment: " + $id)
      end;
  {
    schema_version: 1,
    git_sha: $git_sha,
    repository: $repository,
    registry: "ghcr.io",
    package: "ghcr.io/evil52/mcp-rust-runtime",
    platforms: ["linux/amd64", "linux/arm64"],
    images: {
      "control": checked($control; "control"),
      "control-auth-egress": checked($control_auth_egress; "control-auth-egress"),
      "control-ingress": checked($control_ingress; "control-ingress"),
      "control-ozon-write-egress": checked($control_ozon_write_egress; "control-ozon-write-egress"),
      "control-write-egress": checked($control_write_egress; "control-write-egress"),
      "mail-egress": checked($mail_egress; "mail-egress"),
      "ozon-egress": checked($ozon_egress; "ozon-egress"),
      "position-collector": checked($position_collector; "position-collector"),
      "position-db": checked($position_db; "position-db"),
      "report-collector": checked($report_collector; "report-collector"),
      "report-worker": checked($report_worker; "report-worker"),
      "server": checked($server; "server"),
      "wb-automation": checked($wb_automation; "wb-automation")
    }
  }
' >"$temporary"

"$project_root/scripts/validate-release-image-lock.sh" \
  "$temporary" "$git_sha" "$repository"
chmod 600 "$temporary"
mv -f "$temporary" "$output"
trap - EXIT
