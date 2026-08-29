#!/bin/bash

set -euo pipefail

profile_name="${TUNNEL_CLIENT_PROFILE:-ozon-local}"
profile_dir="${TUNNEL_CLIENT_PROFILE_DIR:-$HOME/.config/tunnel-client}"
profile_path="$profile_dir/$profile_name.yaml"
runtime_key_path="$profile_dir/runtime-api-key"
health_url_file="${TUNNEL_CLIENT_HEALTH_URL_FILE:-$HOME/Library/Application Support/tunnel-client/health/$profile_name.url}"
mcp_health_url="${MCP_HEALTH_URL:-http://127.0.0.1:8787/readyz}"
mcp_server_url="${MCP_SERVER_URL:-http://127.0.0.1:8787/mcp}"
tunnel_client="${TUNNEL_CLIENT_BIN:-$HOME/.local/bin/tunnel-client}"
mcp_container_name="${MCP_CONTAINER_NAME:-mcp-ozon-server}"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
runtime_registry="$runtime_dir/access.json"
tunnel_restart_state="$runtime_dir/tunnel-restart.state"
tunnel_poll_stale_after_seconds="${TUNNEL_POLL_STALE_AFTER_SECONDS:-90}"
tunnel_poll_startup_grace_seconds="${TUNNEL_POLL_STARTUP_GRACE_SECONDS:-75}"
tunnel_restart_cooldown_seconds="${TUNNEL_RESTART_COOLDOWN_SECONDS:-300}"
lock_dir="${TMPDIR:-/tmp}/mcp-ozon-runtime-agent.lock"

if [[ ! "$tunnel_poll_stale_after_seconds" =~ ^[0-9]+$ ]] \
  || ((tunnel_poll_stale_after_seconds < 45 || tunnel_poll_stale_after_seconds > 600)); then
  echo "TUNNEL_POLL_STALE_AFTER_SECONDS must be an integer from 45 to 600" >&2
  exit 1
fi
if [[ ! "$tunnel_poll_startup_grace_seconds" =~ ^[0-9]+$ ]] \
  || ((tunnel_poll_startup_grace_seconds < 30 || tunnel_poll_startup_grace_seconds > 180)); then
  echo "TUNNEL_POLL_STARTUP_GRACE_SECONDS must be an integer from 30 to 180" >&2
  exit 1
fi
if [[ ! "$tunnel_restart_cooldown_seconds" =~ ^[0-9]+$ ]] \
  || ((tunnel_restart_cooldown_seconds < 60 || tunnel_restart_cooldown_seconds > 3600)); then
  echo "TUNNEL_RESTART_COOLDOWN_SECONDS must be an integer from 60 to 3600" >&2
  exit 1
fi

if ! mkdir "$lock_dir" 2>/dev/null; then
  exit 0
fi
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
# SC2317 is the pre-0.11 code for the same finding; both are listed so the
# directive works on the shellcheck shipped by Ubuntu and on newer releases.
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

tunnel_health_base_url() {
  local health_base_url

  [[ -r "$health_url_file" ]] || return 1
  health_base_url="$(tr -d '[:space:]' <"$health_url_file")"
  [[ "$health_base_url" =~ ^http://127\.0\.0\.1:[0-9]+$ ]] || return 1
  printf '%s\n' "$health_base_url"
}

tunnel_local_ready() {
  local health_base_url

  health_base_url="$(tunnel_health_base_url)" || return 1
  /usr/bin/curl --max-time 3 --fail --silent --show-error \
    "$health_base_url/healthz" >/dev/null 2>&1 \
    && /usr/bin/curl --max-time 3 --fail --silent --show-error \
      "$health_base_url/readyz" >/dev/null 2>&1
}

tunnel_poll_is_fresh() {
  local health_base_url last_success now poll_age

  tunnel_local_ready || return 1
  health_base_url="$(tunnel_health_base_url)" || return 1
  if ! last_success="$(
    /usr/bin/curl --max-time 3 --fail --silent --show-error \
      "$health_base_url/metrics" 2>/dev/null \
      | awk '/^commands_poll_last_successful_timestamp_seconds[{ ]/ {
          printf "%.0f\n", $2
        }'
  )"; then
    return 1
  fi
  [[ "$last_success" =~ ^[0-9]+$ ]] || return 1

  now="$(date +%s)"
  ((last_success > 0 && now >= last_success)) || return 1
  poll_age=$((now - last_success))
  ((poll_age <= tunnel_poll_stale_after_seconds))
}

tunnel_poll_is_fresh_or_starting() {
  local health_base_url process_started now uptime

  if tunnel_poll_is_fresh; then
    return 0
  fi
  tunnel_local_ready || return 1
  health_base_url="$(tunnel_health_base_url)" || return 1
  if ! process_started="$(
    /usr/bin/curl --max-time 3 --fail --silent --show-error \
      "$health_base_url/metrics" 2>/dev/null \
      | awk '/^process_start_time_seconds[ {]/ {
          printf "%.0f\n", $2
        }'
  )"; then
    return 1
  fi
  [[ "$process_started" =~ ^[0-9]+$ ]] || return 1
  now="$(date +%s)"
  ((process_started > 0 && now >= process_started)) || return 1
  uptime=$((now - process_started))
  ((uptime <= tunnel_poll_startup_grace_seconds))
}

openai_control_plane_preflight() {
  local status

  status="$(
    /usr/bin/curl --noproxy '*' --connect-timeout 3 --max-time 6 \
      --silent --output /dev/null --write-out '%{http_code}' \
      https://api.openai.com/v1/models 2>/dev/null || true
  )"
  case "$status" in
    401)
      return 0
      ;;
    403)
      echo "OpenAI control plane returned HTTP 403; check VPN route, region, and kill-switch" >&2
      return 1
      ;;
    000 | "")
      echo "OpenAI control plane is unreachable; check VPN and VPN-provided DNS" >&2
      return 1
      ;;
    *)
      echo "OpenAI control-plane preflight returned HTTP $status; tunnel restart suppressed" >&2
      return 1
      ;;
  esac
}

tunnel_restart_is_allowed() {
  local last_restart now elapsed

  [[ -e "$tunnel_restart_state" ]] || return 0
  [[ ! -L "$tunnel_restart_state" && -f "$tunnel_restart_state" ]] || return 1
  last_restart="$(tr -d '[:space:]' <"$tunnel_restart_state")"
  [[ "$last_restart" =~ ^[0-9]+$ ]] || return 1
  now="$(date +%s)"
  ((now >= last_restart)) || return 1
  elapsed=$((now - last_restart))
  ((elapsed >= tunnel_restart_cooldown_seconds))
}

record_tunnel_restart() {
  local temporary_state

  temporary_state="$runtime_dir/.tunnel-restart.$$.tmp"
  umask 077
  printf '%s\n' "$(date +%s)" >"$temporary_state"
  chmod 600 "$temporary_state"
  mv -f "$temporary_state" "$tunnel_restart_state"
}

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

if tunnel_poll_is_fresh_or_starting; then
  exit 0
fi

if ! openai_control_plane_preflight; then
  if tunnel_poll_is_fresh; then
    exit 0
  fi
  exit 1
fi
if tunnel_poll_is_fresh; then
  exit 0
fi
if ! tunnel_restart_is_allowed; then
  echo "Tunnel poll is stale, but restart cooldown is active" >&2
  exit 1
fi

tunnel_id="$(awk -F'"' '/"tunnel_id"[[:space:]]*:/ { print $4; exit }' "$profile_path")"
if [[ ! "$tunnel_id" =~ ^tunnel_[0-9a-f]{32}$ ]]; then
  echo "invalid tunnel_id in $profile_path" >&2
  exit 1
fi

record_tunnel_restart
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
  if tunnel_poll_is_fresh; then
    exit 0
  fi
  sleep 1
done

echo "tunnel-client did not establish a fresh control-plane poll" >&2
exit 1
