#!/bin/bash

set -euo pipefail

profile_name="${TUNNEL_CLIENT_PROFILE:-ozon-local}"
profile_dir="${TUNNEL_CLIENT_PROFILE_DIR:-$HOME/.config/tunnel-client}"
profile_path="$profile_dir/$profile_name.yaml"
runtime_key_path="$profile_dir/runtime-api-key"
health_url_file="${TUNNEL_CLIENT_HEALTH_URL_FILE:-$HOME/Library/Application Support/tunnel-client/health/$profile_name.url}"
mcp_health_url="${MCP_HEALTH_URL:-http://127.0.0.1:8787/health}"
mcp_server_url="${MCP_SERVER_URL:-http://127.0.0.1:8787/mcp}"
tunnel_client="${TUNNEL_CLIENT_BIN:-$HOME/.local/bin/tunnel-client}"
mcp_container_name="${MCP_CONTAINER_NAME:-mcp-ozon-server}"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
runtime_registry="$runtime_dir/access.json"
lock_dir="${TMPDIR:-/tmp}/mcp-ozon-runtime-agent.lock"

if ! mkdir "$lock_dir" 2>/dev/null; then
  exit 0
fi
# shellcheck disable=SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT

if [[ "$(uname -s)" == "Darwin" ]]; then
  runtime_mode_command=(/usr/bin/stat -f '%Lp')
  runtime_owner_command=(/usr/bin/stat -f '%u')
else
  runtime_mode_command=(stat -c '%a')
  runtime_owner_command=(stat -c '%u')
fi

if [[ -L "$runtime_dir" || ! -d "$runtime_dir" ]]; then
  echo "persistent runtime directory is unavailable; rerun the installer" >&2
  exit 1
fi
if [[ "$("${runtime_owner_command[@]}" "$runtime_dir")" != "$(id -u)" ]]; then
  echo "persistent runtime directory is not owned by the current user" >&2
  exit 1
fi
if [[ "$("${runtime_mode_command[@]}" "$runtime_dir")" != "700" ]]; then
  echo "persistent runtime directory must have mode 700; rerun the installer" >&2
  exit 1
fi
if [[ -L "$runtime_registry" || ! -f "$runtime_registry" ]]; then
  echo "persistent runtime registry is unavailable; rerun the installer" >&2
  exit 1
fi
if [[ "$("${runtime_mode_command[@]}" "$runtime_registry")" != "644" ]]; then
  echo "persistent runtime registry must have mode 644 inside its private directory" >&2
  exit 1
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" ]]; then
  for docker_candidate in /usr/local/bin/docker /opt/homebrew/bin/docker; do
    if [[ -x "$docker_candidate" ]]; then
      docker_bin="$docker_candidate"
      break
    fi
  done
fi
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi

if ! "$docker_bin" info >/dev/null 2>&1; then
  if [[ -d /Applications/Docker.app ]]; then
    /usr/bin/open -g -a Docker >/dev/null 2>&1 || true
  fi

  for _attempt in $(seq 1 60); do
    if "$docker_bin" info >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi
if [[ ! -x "$tunnel_client" ]]; then
  echo "tunnel-client is unavailable: $tunnel_client" >&2
  exit 1
fi
if [[ ! -r "$profile_path" || ! -r "$runtime_key_path" ]]; then
  echo "tunnel-client profile or runtime key is unavailable" >&2
  exit 1
fi

if ! "$docker_bin" container inspect "$mcp_container_name" >/dev/null 2>&1; then
  echo "managed MCP container is unavailable; rerun the installer" >&2
  exit 1
fi
mounted_registry="$(
  "$docker_bin" container inspect \
    --format '{{range .Mounts}}{{if eq .Destination "/etc/mcp-ozon/access.json"}}{{.Source}}{{end}}{{end}}' \
    "$mcp_container_name" 2>/dev/null || true
)"
if [[ "$mounted_registry" != "$runtime_registry" ]]; then
  echo "MCP container uses an unmanaged registry mount; rerun the installer" >&2
  exit 1
fi
if ! /usr/bin/curl --fail --silent --show-error "$mcp_health_url" >/dev/null 2>&1; then
  container_running="$(
    "$docker_bin" container inspect --format '{{.State.Running}}' "$mcp_container_name"
  )"
  if [[ "$container_running" == "true" ]]; then
    "$docker_bin" restart "$mcp_container_name" >/dev/null
  else
    "$docker_bin" start "$mcp_container_name" >/dev/null
  fi

  for _attempt in $(seq 1 60); do
    if /usr/bin/curl --fail --silent --show-error "$mcp_health_url" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

if ! /usr/bin/curl --fail --silent --show-error "$mcp_health_url" >/dev/null 2>&1; then
  echo "MCP server is not healthy: $mcp_health_url" >&2
  exit 1
fi

if [[ -r "$health_url_file" ]]; then
  health_base_url="$(tr -d '[:space:]' <"$health_url_file")"
  if [[ "$health_base_url" =~ ^http://127\.0\.0\.1:[0-9]+$ ]] \
    && /usr/bin/curl --fail --silent --show-error "$health_base_url/healthz" >/dev/null 2>&1 \
    && /usr/bin/curl --fail --silent --show-error "$health_base_url/readyz" >/dev/null 2>&1; then
    exit 0
  fi
fi

tunnel_id="$(awk -F'"' '/"tunnel_id"[[:space:]]*:/ { print $4; exit }' "$profile_path")"
if [[ ! "$tunnel_id" =~ ^tunnel_[0-9a-f]{32}$ ]]; then
  echo "invalid tunnel_id in $profile_path" >&2
  exit 1
fi

"$tunnel_client" runtimes stop "$profile_name" >/dev/null 2>&1 || true
"$tunnel_client" doctor \
  --profile "$profile_name" \
  --profile-dir "$profile_dir" >/dev/null
"$tunnel_client" runtimes connect \
  --alias "$profile_name" \
  --profile "$profile_name" \
  --profile-dir "$profile_dir" \
  --tunnel-id "$tunnel_id" \
  --runtime-api-key "file:$runtime_key_path" \
  --mcp-server-url "$mcp_server_url" >/dev/null

for _attempt in $(seq 1 60); do
  if [[ -r "$health_url_file" ]]; then
    health_base_url="$(tr -d '[:space:]' <"$health_url_file")"
    if [[ "$health_base_url" =~ ^http://127\.0\.0\.1:[0-9]+$ ]] \
      && /usr/bin/curl --fail --silent --show-error "$health_base_url/healthz" >/dev/null 2>&1 \
      && /usr/bin/curl --fail --silent --show-error "$health_base_url/readyz" >/dev/null 2>&1; then
      exit 0
    fi
  fi
  sleep 1
done

echo "tunnel-client did not become ready" >&2
exit 1
