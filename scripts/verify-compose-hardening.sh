#!/bin/bash

# Verifies the hardening declared by the Compose files that actually ship.
#
# The CI container job asserts the flags it passes to `docker run` itself,
# which cannot catch a regression in compose.yaml — and compose.yaml is what
# operators deploy. This checks the rendered configuration instead, so removing
# read_only, widening the published port, or dropping a resource limit fails.

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for dependency in docker jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing dependency: $dependency" >&2
    exit 1
  fi
done

# `docker compose config` needs the referenced env files to exist, but never
# needs their contents: only the rendered service definition is inspected.
scratch="$(mktemp -d "${TMPDIR:-/tmp}/mcp-ozon-compose-verify.XXXXXX")"
chmod 700 "$scratch"
created_env_files=()
cleanup() {
  local created
  if (( ${#created_env_files[@]} > 0 )); then
    for created in "${created_env_files[@]}"; do
      rm -f "$created"
    done
  fi
  rm -rf "$scratch"
}
trap cleanup EXIT

ensure_placeholder() {
  local path="$1"
  if [[ ! -e "$path" ]]; then
    : >"$path"
    chmod 600 "$path"
    created_env_files+=("$path")
  fi
}

ensure_placeholder "$project_dir/.env"

failures=0
check() {
  local description="$1" filter="$2" document="$3"
  if jq -e "$filter" <<<"$document" >/dev/null; then
    printf 'ok   %s\n' "$description"
  else
    printf 'FAIL %s\n' "$description" >&2
    failures=$((failures + 1))
  fi
}

rendered="$(cd "$project_dir" && docker compose -f compose.yaml config --format json)"
server="$(jq -c '.services.server' <<<"$rendered")"

check "root filesystem is read-only" '.read_only == true' "$server"
check "all Linux capabilities are dropped" '.cap_drop == ["ALL"]' "$server"
check "privilege escalation is blocked" \
  '(.security_opt // []) | index("no-new-privileges:true") != null' "$server"
check "the container is never privileged" '(.privileged // false) == false' "$server"
check "writable state is a bounded tmpfs" \
  '(.tmpfs // []) | length == 1 and (.[0] | startswith("/tmp:") and contains("size="))' "$server"
check "memory is bounded" '(.mem_limit // "") | test("^[0-9]+[kmg]?$"; "i")' "$server"
check "process count is bounded" '(.pids_limit // 0) > 0' "$server"
check "logs cannot fill the disk" \
  '.logging.driver == "json-file"
   and (.logging.options["max-size"] // "") != ""
   and (.logging.options["max-file"] // "") != ""' "$server"
check "a healthcheck is defined" '(.healthcheck.test // []) | length > 0' "$server"

# Every published port must stay on loopback: the MCP trusts its network
# position, so binding 0.0.0.0 on the host would expose it directly.
check "published ports are loopback-only" \
  '[.ports[]? | select((.host_ip // "0.0.0.0") | test("^(127\\.|::1$)") | not)] | length == 0' \
  "$server"

# The access registry must never be writable from inside the container.
check "the access registry is mounted read-only" \
  '[.volumes[]? | select(.target == "/etc/mcp-ozon/access.json")]
   | length == 1 and .[0].read_only == true' "$server"

# The image must not be able to reach a marketplace with a rewritten base URL.
check "no environment override redirects Ozon egress" \
  '(.environment.OZON_API_BASE_URL // "https://api-seller.ozon.ru") == "https://api-seller.ozon.ru"' \
  "$server"
check "experimental preview contracts stay off" \
  '(.environment.OZON_POSTINGS_VNEXT // "false") == "false"
   and (.environment.OZON_FINANCE_ACCRUALS_PREVIEW // "false") == "false"' "$server"

if (( failures > 0 )); then
  echo "compose hardening verification failed with $failures problem(s)" >&2
  exit 1
fi

echo "Compose hardening verified: read-only rootfs, no capabilities, bounded memory/pids/logs, loopback-only ports, read-only registry mount."
