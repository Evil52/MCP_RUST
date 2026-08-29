#!/bin/bash
# Operator-facing health probe for the whole deployment, not just the MCP
# server that `ensure-local-runtime.sh` already supervises.
#
# Every protective state in this system is deliberately fail-closed and
# deliberately silent: an incident-locked robot stops changing bids but leaves
# the campaign running, an ambiguous delivery stays in `sending` forever, and a
# stack that never came up after a reboot looks exactly like a quiet morning.
# This script is the detection channel those designs assume exists.
#
# Exit status: 0 clean, 1 findings reported, 2 the check itself could not run.
# A findings report is written to stdout and, when
# `MCP_HEALTH_NOTIFY_COMMAND` is set, piped to that command as well.

set -euo pipefail

# A LaunchAgent runs an installed copy from ~/.local/libexec, where the
# path relative to this file no longer points at the project. The agent
# therefore passes the project directory explicitly, exactly as the WB
# automation runner already does.
project_root="${MCP_OPS_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
if [[ ! -d "$project_root" || -L "$project_root" ]]; then
  echo "project directory is unavailable or unsafe: $project_root" >&2
  exit 2
fi
position_env="${MCP_HEALTH_POSITION_ENV:-$project_root/.position.env}"
backup_root="${MCP_BACKUP_DIR:-$HOME/MCP_OZON-backups}"
db_network="${MCP_HEALTH_DB_NETWORK:-mcp-ozon-position-internal}"
db_host="${MCP_HEALTH_DB_HOST:-position-db}"
compose_project="${MCP_HEALTH_COMPOSE_PROJECT:-mcp-ozon-position}"
required_services="${MCP_HEALTH_REQUIRED_SERVICES:-position-db,position-collector,ozon-egress,report-collector,report-worker}"
mcp_container_name="${MCP_HEALTH_MCP_CONTAINER_NAME:-mcp-ozon-server}"
mcp_ready_url="${MCP_HEALTH_MCP_READY_URL:-http://127.0.0.1:8787/readyz}"
required_launch_agents="${MCP_HEALTH_REQUIRED_LAUNCH_AGENTS:-com.ofk.mcp-ozon-runtime,com.ofk.mcp-ozon-backup,com.ofk.mcp-ozon-health,com.ofk.mcp-ozon-restore-verify}"
skip_launch_agent_check="${MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK:-false}"
cycle_stale_seconds="${MCP_HEALTH_CYCLE_STALE_SECONDS:-1800}"
backup_stale_seconds="${MCP_HEALTH_BACKUP_STALE_SECONDS:-129600}"
restore_stale_seconds="${MCP_HEALTH_RESTORE_STALE_SECONDS:-691200}"
allow_local_only="${MCP_BACKUP_ALLOW_LOCAL_ONLY:-false}"
notify_command="${MCP_HEALTH_NOTIFY_COMMAND:-}"

umask 077

for value in "$cycle_stale_seconds" "$backup_stale_seconds" "$restore_stale_seconds"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "health thresholds must be positive integers" >&2
    exit 2
  fi
done
case "$allow_local_only" in
  true | false) ;;
  *)
    echo "MCP_BACKUP_ALLOW_LOCAL_ONLY must be true or false" >&2
    exit 2
    ;;
esac
case "$skip_launch_agent_check" in
  true | false) ;;
  *)
    echo "MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK must be true or false" >&2
    exit 2
    ;;
esac

findings=()
add_finding() {
  findings+=("$1")
}

report_and_exit() {
  local report
  if ((${#findings[@]} == 0)); then
    report="$(printf 'MCP_OZON health check: clean (%s)\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')")"
    printf '%s\n' "$report"
    exit 0
  fi
  report="$(
    printf 'MCP_OZON health check: %s finding(s) at %s\n\n' \
      "${#findings[@]}" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf -- '- %s\n' "${findings[@]}"
  )"
  printf '%s\n' "$report"
  if [[ -n "$notify_command" ]]; then
    printf '%s\n' "$report" | "$notify_command" || true
  fi
  exit 1
}

# The hook is executed directly, never through a shell, so it must be one
# executable file. Wrap `mail`, `curl` or `osascript` in a small script rather
# than passing a command line here.
if [[ -n "$notify_command" && ! -x "$notify_command" ]]; then
  echo "MCP_HEALTH_NOTIFY_COMMAND must be one executable file: $notify_command" >&2
  exit 2
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 2
fi

# A stopped Docker Engine is the finding, not a reason to skip the check.
# This is the "the Mac rebooted and Docker Desktop never came back" case.
if ! "$docker_bin" info >/dev/null 2>&1; then
  add_finding "Docker Engine is unavailable: the entire data plane is down"
  report_and_exit
fi

# ---------------------------------------------------------------- containers

old_ifs="$IFS"
IFS=','
read -r -a service_list <<<"$required_services"
IFS="$old_ifs"

# `docker ps` renders labels as one flat string, so each service is matched by
# its own label filter rather than by parsing a combined listing. `--all` is
# deliberate: an exited container is a more useful finding than a missing one.
for service in "${service_list[@]}"; do
  [[ -z "$service" ]] && continue
  line="$(
    "$docker_bin" ps --all \
      --filter "label=com.docker.compose.project=$compose_project" \
      --filter "label=com.docker.compose.service=$service" \
      --format '{{.State}}|{{.Status}}' \
      | head -n 1
  )"
  if [[ -z "$line" ]]; then
    add_finding "compose service does not exist: $compose_project/$service"
    continue
  fi
  state="${line%%|*}"
  status="${line#*|}"
  if [[ "$state" != "running" ]]; then
    add_finding "compose service is $state: $compose_project/$service"
    continue
  fi
  case "$status" in
    *unhealthy*)
      add_finding "compose service reports unhealthy: $compose_project/$service"
      ;;
  esac
done

mcp_state="$(
  "$docker_bin" container inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' \
    "$mcp_container_name" 2>/dev/null || true
)"
case "$mcp_state" in
  running\|healthy) ;;
  "") add_finding "main MCP container does not exist: $mcp_container_name" ;;
  *) add_finding "main MCP container is not ready: $mcp_container_name ($mcp_state)" ;;
esac
if ! /usr/bin/curl --noproxy '*' --connect-timeout 2 --max-time 4 \
  --fail --silent --show-error "$mcp_ready_url" >/dev/null 2>&1; then
  add_finding "main MCP readiness endpoint is unavailable: $mcp_ready_url"
fi

if [[ "$(uname -s)" == "Darwin" && "$skip_launch_agent_check" != true ]]; then
  old_ifs="$IFS"
  IFS=','
  read -r -a launch_agent_list <<<"$required_launch_agents"
  IFS="$old_ifs"
  launch_domain="gui/$(id -u)"
  for launch_label in "${launch_agent_list[@]}"; do
    [[ -z "$launch_label" ]] && continue
    if ! launchctl print "$launch_domain/$launch_label" >/dev/null 2>&1; then
      add_finding "required LaunchAgent is not loaded: $launch_label"
    fi
  done
fi

# ------------------------------------------------------------------- backups

# A missing backup root must become a finding, not a silent exit: under
# `pipefail` an unguarded `find` over a nonexistent path fails the whole
# assignment and `set -e` would end the check before it reports anything.
newest_backup="$(
  find "$backup_root" -mindepth 1 -maxdepth 1 -type d -name '2*Z' 2>/dev/null \
    | sort -r \
    | head -n 1 \
    || true
)"
if [[ -z "$newest_backup" ]]; then
  add_finding "no backup found under $backup_root"
else
  if [[ "$(uname -s)" == "Darwin" ]]; then
    backup_epoch="$(/usr/bin/stat -f '%m' "$newest_backup")"
  else
    backup_epoch="$(stat -c '%Y' "$newest_backup")"
  fi
  backup_age=$(($(date '+%s') - backup_epoch))
  if ((backup_age > backup_stale_seconds)); then
    add_finding "newest backup is $((backup_age / 3600))h old: $(basename "$newest_backup")"
  fi
  offsite_marker="$newest_backup/offsite-complete.json"
  local_only_marker="$newest_backup/local-only-risk-accepted.json"
  if [[ -f "$offsite_marker" && ! -L "$offsite_marker" ]]; then
    :
  elif [[ "$allow_local_only" == true \
    && -f "$local_only_marker" && ! -L "$local_only_marker" ]]; then
    :
  else
    add_finding "newest backup has no confirmed offsite copy: $(basename "$newest_backup")"
  fi
fi

newest_restore_marker="$(
  find "$backup_root" -mindepth 2 -maxdepth 2 -type f \
    -name restore-verified.json 2>/dev/null \
    | sort -r \
    | head -n 1 \
    || true
)"
if [[ -z "$newest_restore_marker" ]]; then
  add_finding "no successful scheduled restore verification was recorded"
else
  if [[ "$(uname -s)" == "Darwin" ]]; then
    restore_epoch="$(/usr/bin/stat -f '%m' "$newest_restore_marker")"
  else
    restore_epoch="$(stat -c '%Y' "$newest_restore_marker")"
  fi
  restore_age=$(($(date '+%s') - restore_epoch))
  if ((restore_age > restore_stale_seconds)); then
    add_finding "last successful restore verification is $((restore_age / 3600))h old"
  fi
fi

# ------------------------------------------------------------------ database

if [[ ! -f "$position_env" || -L "$position_env" ]]; then
  add_finding "position database credentials are unavailable: $position_env"
  report_and_exit
fi

db_image="$(
  awk '/^FROM postgres:/ { print $2; exit }' \
    "$project_root/position-monitor/Dockerfile"
)"
if [[ ! "$db_image" =~ ^postgres:[0-9]+-alpine@sha256:[0-9a-f]{64}$ ]]; then
  echo "pinned PostgreSQL image could not be resolved" >&2
  exit 2
fi

# `default_transaction_read_only` is set on the session rather than trusted to
# the queries below: this probe must never be able to change state, even if a
# future edit to it is careless.
# shellcheck disable=SC2016 # The password expands inside the container, from
# --env-file, so it never appears in this host's environment or process list.
probe_sql() {
  "$docker_bin" run --rm --interactive \
    --network "$db_network" \
    --env-file "$position_env" \
    --env 'PGOPTIONS=-c default_transaction_read_only=on' \
    --entrypoint /bin/sh \
    "$db_image" \
    -ec '
      PGPASSWORD="$POSITION_DB_ADMIN_PASSWORD" \
      exec psql \
        --host="'"$db_host"'" \
        --port=5432 \
        --username="${POSITION_DB_ADMIN_USER:-position_admin}" \
        --dbname="${POSITION_DB_NAME:-ozon_positions}" \
        --no-password --quiet --no-align --tuples-only \
        --set ON_ERROR_STOP=1
    ' 2>/dev/null
}

probe_output="$(
  probe_sql <<'SQL' || true
SELECT 'incident|' || account_id || '|' || advert_id || '|' || incident_class
FROM wb_automation.execution_state
WHERE incident_class IS NOT NULL;

SELECT 'unresolved|' || idempotency_key || '|' || status
FROM wb_automation.action_attempts
WHERE status NOT IN ('applied', 'cancelled');

SELECT 'cycle_age|' || COALESCE(
    EXTRACT(EPOCH FROM (now() - max(observed_at)))::bigint::text, 'none'
)
FROM wb_automation.cycles;

SELECT 'stalled|' || stall_kind || '|' || reference
FROM daily_reporting.stalled_report_work;
SQL
)"

if [[ -z "$probe_output" ]]; then
  add_finding "position database did not answer the health probe"
  report_and_exit
fi

while IFS= read -r row; do
  [[ -z "$row" ]] && continue
  case "$row" in
    incident\|*)
      add_finding "WB robot is incident-locked, bids are frozen at their last value: ${row#incident|}"
      ;;
    unresolved\|*)
      add_finding "WB robot has an unresolved marketplace action: ${row#unresolved|}"
      ;;
    stalled\|*)
      add_finding "daily report work is stalled and needs an operator: ${row#stalled|}"
      ;;
    cycle_age\|none)
      add_finding "WB robot has never recorded a cycle"
      ;;
    cycle_age\|*)
      age="${row#cycle_age|}"
      if [[ "$age" =~ ^[0-9]+$ ]] && ((age > cycle_stale_seconds)); then
        add_finding "WB robot last ran $((age / 60)) minutes ago; the daily spend cap is only enforced while it polls"
      fi
      ;;
  esac
done <<<"$probe_output"

report_and_exit
