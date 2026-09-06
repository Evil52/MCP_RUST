#!/bin/bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$project_root/ops/macos/com.ofk.mcp-ozon-runtime.plist.in"
compose_file="$project_root/compose.yaml"
reporting_compose_file="$project_root/compose.reporting-reader.yaml"
environment_file="$project_root/.env"
position_environment_file="$project_root/.position.env"
source_registry="$project_root/config/access.json"
label="com.ofk.mcp-ozon-runtime"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
watchdog_dir="$HOME/.local/libexec/mcp-ozon"
watchdog="$watchdog_dir/ensure-local-runtime.sh"
runtime_dir="$HOME/.local/share/mcp-ozon-runtime"
runtime_registry="$runtime_dir/access.json"
plist="$agent_dir/$label.plist"
domain="gui/$(id -u)"
temporary_plist="$(mktemp "${TMPDIR:-/tmp}/mcp-ozon-launch-agent.XXXXXX")"
runtime_registry_tmp=""
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_plist"
  if [[ -n "$runtime_registry_tmp" ]]; then
    rm -f "$runtime_registry_tmp"
  fi
}
trap cleanup EXIT

if [[ ! -f "$compose_file" || -L "$compose_file" ]]; then
  echo "compose.yaml must be an existing regular file" >&2
  exit 1
fi
if [[ ! -f "$reporting_compose_file" || -L "$reporting_compose_file" ]]; then
  echo "compose.reporting-reader.yaml must be an existing regular file" >&2
  exit 1
fi
if [[ ! -f "$environment_file" || -L "$environment_file" ]]; then
  echo ".env must be an existing regular file" >&2
  exit 1
fi
if [[ ! -f "$position_environment_file" || -L "$position_environment_file" ]]; then
  echo ".position.env must be an existing regular file" >&2
  exit 1
fi
if [[ ! -f "$source_registry" || -L "$source_registry" ]]; then
  echo "config/access.json must be an existing regular file" >&2
  exit 1
fi

release_record="$("$project_root/scripts/verify-release-images.sh" server)"
release_sha="$(jq -r '.git_sha' <<<"$release_record")"
server_image="$(jq -r '.images.server' <<<"$release_record")"

if [[ "$(uname -s)" == "Darwin" ]]; then
  environment_mode="$(/usr/bin/stat -f '%Lp' "$environment_file")"
  position_environment_mode="$(/usr/bin/stat -f '%Lp' "$position_environment_file")"
  source_mode="$(/usr/bin/stat -f '%Lp' "$source_registry")"
  runtime_owner_command=(/usr/bin/stat -f '%u')
else
  environment_mode="$(stat -c '%a' "$environment_file")"
  position_environment_mode="$(stat -c '%a' "$position_environment_file")"
  source_mode="$(stat -c '%a' "$source_registry")"
  runtime_owner_command=(stat -c '%u')
fi
if [[ "$environment_mode" != "600" \
   || "$position_environment_mode" != "600" \
   || "$source_mode" != "600" ]]; then
  echo ".env, .position.env and config/access.json must have mode 600; refusing to change source files" >&2
  exit 1
fi
for required_key in POSITION_READER_DB_PASSWORD REPORT_REFRESH_REQUESTER_DB_PASSWORD; do
  if ! grep -Eq "^${required_key}=.{24,}$" "$position_environment_file"; then
    echo "$required_key is missing or invalid in .position.env" >&2
    exit 1
  fi
done

if [[ -L "$runtime_dir" || ( -e "$runtime_dir" && ! -d "$runtime_dir" ) ]]; then
  echo "runtime directory is not a safe directory: $runtime_dir" >&2
  exit 1
fi
mkdir -p "$runtime_dir"
if [[ "$("${runtime_owner_command[@]}" "$runtime_dir")" != "$(id -u)" ]]; then
  echo "runtime directory is not owned by the current user" >&2
  exit 1
fi
chmod 700 "$runtime_dir"
if [[ -e "$runtime_registry" && ( -L "$runtime_registry" || ! -f "$runtime_registry" ) ]]; then
  echo "runtime registry path is not a regular file: $runtime_registry" >&2
  exit 1
fi

# Only this interactive installer reads files in Documents. The directory is
# private to the host user; 0644 on the nested copy lets container UID 10001
# read the bind mount without exposing it to other host users.
runtime_registry_tmp="$(mktemp "$runtime_dir/.access.json.XXXXXX")"
install -m 644 "$source_registry" "$runtime_registry_tmp"
mv -f "$runtime_registry_tmp" "$runtime_registry"
runtime_registry_tmp=""

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" network inspect mcp-ozon-position-internal >/dev/null 2>&1; then
  echo "reporting database network is unavailable; start and migrate the position stack first" >&2
  exit 1
fi
position_containers="$(
  "$docker_bin" ps \
    --filter label=com.docker.compose.project=mcp-ozon-position \
    --filter label=com.docker.compose.service=position-db \
    --format '{{.ID}}'
)"
if [[ -z "$position_containers" ]]; then
  echo "reporting database container is unavailable; start and migrate the position stack first" >&2
  exit 1
fi
if [[ "$position_containers" == *$'\n'* ]]; then
  echo "multiple reporting database containers are running; refusing an ambiguous deployment" >&2
  exit 1
fi
if [[ "$(
  "$docker_bin" container inspect --format '{{.State.Health.Status}}' "$position_containers" \
    2>/dev/null || true
)" != "healthy" ]]; then
  echo "reporting database is not healthy; start and migrate the position stack first" >&2
  exit 1
fi

compose=(
  "$docker_bin" compose
  --env-file "$position_environment_file"
  --project-directory "$project_root"
  -f "$compose_file"
  -f "$reporting_compose_file"
)

MCP_RELEASE_GIT_SHA="$release_sha" \
MCP_SERVER_IMAGE="$server_image" \
MCP_ACCESS_CONFIG_HOST="$runtime_registry" \
  "${compose[@]}" \
    up -d --no-build --force-recreate --wait --wait-timeout 300 server

actual_release_sha="$(
  "$docker_bin" container inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    mcp-ozon-server
)"
if [[ "$actual_release_sha" != "$release_sha" ]]; then
  "${compose[@]}" \
    stop server >/dev/null 2>&1 || true
  echo "production image revision does not match CI release evidence" >&2
  exit 1
fi

mkdir -p "$agent_dir" "$log_dir" "$watchdog_dir"
install -m 700 "$project_root/scripts/ensure-local-runtime.sh" "$watchdog"
sed \
  -e "s|__WATCHDOG__|$watchdog|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$log_dir|g" \
  -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
  "$template" >"$temporary_plist"
plutil -lint "$temporary_plist" >/dev/null

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
install -m 600 "$temporary_plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl kickstart -k "$domain/$label"

echo "Installed and started $label at verified revision $release_sha"
