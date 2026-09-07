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

# Installed agents receive the pinned image and private env path directly.
# The checkout is only a fallback for manual developer invocations; a deleted
# staging checkout must never disable the permanent operations jobs.
project_root="${MCP_OPS_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
position_env="${MCP_HEALTH_POSITION_ENV:-$project_root/.position.env}"
backup_root="${MCP_BACKUP_DIR:-$HOME/MCP_OZON-backups}"
db_network="${MCP_HEALTH_DB_NETWORK:-mcp-ozon-position-internal}"
db_host="${MCP_HEALTH_DB_HOST:-position-db}"
compose_project="${MCP_HEALTH_COMPOSE_PROJECT:-mcp-ozon-position}"
# Only the always-on base data plane is required by default. The collectors and
# report worker are deliberately disabled until their guarded operator
# cutovers complete, so treating them as baseline services makes a healthy
# fail-closed deployment permanently noisy. Operators that enable optional
# runtimes must list them explicitly in MCP_HEALTH_REQUIRED_SERVICES.
required_services="${MCP_HEALTH_REQUIRED_SERVICES-position-db,ozon-egress}"
mcp_container_name="${MCP_HEALTH_MCP_CONTAINER_NAME:-mcp-ozon-server}"
mcp_ready_url="${MCP_HEALTH_MCP_READY_URL:-http://127.0.0.1:8787/readyz}"
required_launch_agents="${MCP_HEALTH_REQUIRED_LAUNCH_AGENTS-com.ofk.mcp-ozon-runtime,com.ofk.mcp-ozon-backup,com.ofk.mcp-ozon-health,com.ofk.mcp-ozon-restore-verify}"
skip_launch_agent_check="${MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK:-false}"
cycle_stale_seconds="${MCP_HEALTH_CYCLE_STALE_SECONDS:-1800}"
backup_stale_seconds="${MCP_HEALTH_BACKUP_STALE_SECONDS:-129600}"
restore_stale_seconds="${MCP_HEALTH_RESTORE_STALE_SECONDS:-691200}"
allow_local_only="${MCP_BACKUP_ALLOW_LOCAL_ONLY:-false}"
notify_command="${MCP_HEALTH_NOTIFY_COMMAND:-}"
heartbeat_command="${MCP_HEALTH_HEARTBEAT_COMMAND:-}"
hook_timeout_seconds="${MCP_HEALTH_HOOK_TIMEOUT_SECONDS:-10}"
event_state_dir="${MCP_HEALTH_EVENT_STATE_DIR:-${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}/ops/health-events}"
check_tunnel="${MCP_HEALTH_CHECK_TUNNEL:-false}"
tunnel_url_file="${MCP_HEALTH_TUNNEL_URL_FILE:-$HOME/Library/Application Support/tunnel-client/health/ozon-local.url}"
tunnel_poll_stale_seconds="${MCP_HEALTH_TUNNEL_POLL_STALE_SECONDS:-90}"
operations_resources="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
if [[ "$check_tunnel" != true && "$check_tunnel" != false ]] \
  || [[ ! "$hook_timeout_seconds" =~ ^[0-9]+$ ]] \
  || ((hook_timeout_seconds < 1 || hook_timeout_seconds > 30)) \
  || [[ ! "$tunnel_poll_stale_seconds" =~ ^[0-9]+$ ]] \
  || ((tunnel_poll_stale_seconds < 1 || tunnel_poll_stale_seconds > 600)); then
  echo "health tunnel flag or bounded hook/poll timeout is invalid" >&2
  exit 2
fi
for csv_contract in "$required_services" "$required_launch_agents"; do
  if [[ ! "$csv_contract" =~ ^[A-Za-z0-9_.-]+(,[A-Za-z0-9_.-]+)*$ ]]; then
    echo "health required-service and LaunchAgent lists must be non-empty comma-separated identifiers" >&2
    exit 2
  fi
done

findings=()
finding_keys=()
core_available=true
database_evidence=false
add_finding() {
  findings+=("$1")
  finding_keys+=("${2:-$1}")
}

add_core_finding() {
  core_available=false
  add_finding "$@"
}

render_report() {
  if ((${#findings[@]} == 0)); then
    printf 'MCP_OZON health check: clean (%s)\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  else
    printf 'MCP_OZON health check: %s finding(s) at %s\n\n' \
      "${#findings[@]}" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf -- '- %s\n' "${findings[@]}"
  fi
}

report_and_exit() {
  local key
  local -a notification_args
  # Observe the poll immediately before delivery, after potentially slow SQL,
  # so a previously fresh sample cannot turn stale while database checks run.
  if [[ "$check_tunnel" == true ]]; then
    if ! python3 -B "$operations_resources/operations_heartbeat.py" probe-tunnel \
      --url-file "$tunnel_url_file" --stale-seconds "$tunnel_poll_stale_seconds"; then
      add_core_finding "MCP tunnel health or fresh control-plane poll is unavailable" "tunnel_unavailable"
    fi
  fi
  # Guard locks, stale reports and backup findings are independent of core
  # reachability. A deadman receives a success only after core + DB evidence;
  # alert delivery failure remains a separate visible monitoring failure.
  if [[ -n "$heartbeat_command" && "$core_available" == true && "$database_evidence" == true ]]; then
    if ! python3 -B "$operations_resources/operations_heartbeat.py" send \
      --hook "$heartbeat_command" --timeout "$hook_timeout_seconds"; then
      add_finding "external core heartbeat delivery failed" "heartbeat_delivery"
    fi
  fi
  if [[ -n "$notify_command" ]]; then
    notification_args=(--hook "$notify_command" --state-dir "$event_state_dir" --timeout "$hook_timeout_seconds")
    for key in "${finding_keys[@]+"${finding_keys[@]}"}"; do
      notification_args+=(--finding "$key")
    done
    if ! render_report | python3 -B "$operations_resources/operations_notify.py" "${notification_args[@]}"; then
      add_finding "health notification delivery failed; the event will be retried" "notification_delivery"
    fi
  fi
  render_report
  if ((${#findings[@]} == 0)); then
    exit 0
  fi
  exit 1
}

# The hook is executed directly, never through a shell, so it must be one
# executable file. Wrap `mail`, `curl` or `osascript` in a small script rather
# than passing a command line here.
for hook in "$notify_command" "$heartbeat_command"; do
  if [[ -n "$hook" && (! -f "$hook" || ! -x "$hook" || "$hook" != /*) ]]; then
    echo "health hooks must each be one absolute executable file" >&2
    exit 2
  fi
done

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 2
fi

# A stopped Docker Engine is the finding, not a reason to skip the check.
# This is the "the Mac rebooted and Docker Desktop never came back" case.
if ! "$docker_bin" info >/dev/null 2>&1; then
  add_core_finding "Docker Engine is unavailable: the entire data plane is down"
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
  service_finding=add_finding
  if [[ "$service" == position-db || "$service" == ozon-egress ]]; then
    service_finding=add_core_finding
  fi
  line="$(
    "$docker_bin" ps --all \
      --filter "label=com.docker.compose.project=$compose_project" \
      --filter "label=com.docker.compose.service=$service" \
      --format '{{.State}}|{{.Status}}' \
      | head -n 1
  )"
  if [[ -z "$line" ]]; then
    "$service_finding" "compose service does not exist: $compose_project/$service"
    continue
  fi
  state="${line%%|*}"
  status="${line#*|}"
  if [[ "$state" != "running" ]]; then
    "$service_finding" "compose service is $state: $compose_project/$service"
    continue
  fi
  case "$status" in
    *unhealthy*)
      "$service_finding" "compose service reports unhealthy: $compose_project/$service"
      ;;
    *\(healthy\)*) ;;
    *)
      "$service_finding" "compose service has no healthy readiness evidence: $compose_project/$service"
      ;;
  esac
done

mcp_state="$(
  "$docker_bin" container inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' \
    "$mcp_container_name" 2>/dev/null || true
)"
case "$mcp_state" in
  running\|healthy) ;;
  "") add_core_finding "main MCP container does not exist: $mcp_container_name" ;;
  *) add_core_finding "main MCP container is not ready: $mcp_container_name ($mcp_state)" ;;
esac
if ! /usr/bin/curl --noproxy '*' --connect-timeout 2 --max-time 4 \
  --fail --silent --show-error "$mcp_ready_url" >/dev/null 2>&1; then
  add_core_finding "main MCP readiness endpoint is unavailable: $mcp_ready_url"
fi

if [[ "$(uname -s)" == "Darwin" && "$skip_launch_agent_check" != true ]]; then
  old_ifs="$IFS"
  IFS=','
  read -r -a launch_agent_list <<<"$required_launch_agents"
  IFS="$old_ifs"
  launch_domain="gui/$(id -u)"
  for launch_label in "${launch_agent_list[@]}"; do
    [[ -z "$launch_label" ]] && continue
    if ! launch_output="$(launchctl print "$launch_domain/$launch_label" 2>/dev/null)"; then
      add_finding "required LaunchAgent is not loaded: $launch_label"
      continue
    fi
    case "$launch_label" in
      com.ofk.mcp-ozon-backup | com.ofk.mcp-ozon-restore-verify)
        if ! launch_result="$(printf '%s\n' "$launch_output" \
          | python3 -B "$operations_resources/operations_heartbeat.py" agent-result)"; then
          add_finding "scheduled backup exit evidence is unavailable: $launch_label" "backup_exit_evidence/$launch_label"
        else
          case "$launch_result" in
            unknown | exit:0) ;;
            exit:*)
              add_finding "scheduled backup or restore verification failed: $launch_label (${launch_result#exit:})" "backup_job_failed/$launch_label"
              ;;
          esac
        fi
        ;;
    esac
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
    add_finding "newest backup is $((backup_age / 3600))h old: $(basename "$newest_backup")" "backup_stale"
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
    add_finding "last successful restore verification is $((restore_age / 3600))h old" "restore_stale"
  fi
fi

# ------------------------------------------------------------------ database

if [[ ! -f "$position_env" || -L "$position_env" ]]; then
  add_finding "position database credentials are unavailable: $position_env"
  report_and_exit
fi

db_image="${MCP_OPS_POSTGRES_IMAGE:-}"
if [[ -z "$db_image" && -f "$project_root/position-monitor/Dockerfile" \
  && ! -L "$project_root/position-monitor/Dockerfile" ]]; then
  db_image="$(awk '/^FROM postgres:/ { print $2; exit }' \
    "$project_root/position-monitor/Dockerfile")"
fi
if [[ ! "$db_image" =~ ^postgres:[0-9]+-alpine([0-9]+\.[0-9]+)?@sha256:[0-9a-f]{64}$ ]]; then
  echo "pinned PostgreSQL image could not be resolved" >&2
  exit 2
fi

# `default_transaction_read_only` is set on the session rather than trusted to
# the queries below: this probe must never be able to change state, even if a
# future edit to it is careless.
reporting_scope='[]'
reporting_policy="${MCP_HEALTH_REPORTING_POLICY:-}"
reporting_registry="${MCP_HEALTH_REPORTING_REGISTRY:-}"
reporting_resources="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "$reporting_policy" || -n "$reporting_registry" ]]; then
  if [[ -z "$reporting_policy" || -z "$reporting_registry" ]] \
    || [[ ! -f "$reporting_resources/reporting-health.sql" ]] \
    || ! reporting_scope="$(python3 "$reporting_resources/reporting-health-contract.py" \
      "$reporting_policy" "$reporting_registry")"; then
    add_finding "reporting health policy or registry is unavailable or invalid"
    reporting_scope='[]'
  fi
fi
if [[ ",$required_services," == *,report-collector,* && "$reporting_scope" == '[]' ]]; then
  add_finding "required report-collector has no enabled reporting health scope"
fi

# shellcheck disable=SC2016 # The password expands inside the container, from
# --env-file, so it never appears in this host's environment or process list.
probe_sql() {
  "$docker_bin" run --rm --interactive \
    --network "$db_network" \
    --env-file "$position_env" \
    --env 'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=3000' \
    --env PGCONNECT_TIMEOUT=5 \
    --env "MCP_HEALTH_REPORTING_SCOPE=$reporting_scope" \
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
        --set reporting_accounts="$MCP_HEALTH_REPORTING_SCOPE" \
        --set ON_ERROR_STOP=1
    ' 2>/dev/null
}

probe_status=0
probe_output="$(
  {
    cat <<'SQL'
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

SELECT 'ozon_launch_recovery|' || plan.plan_id || '|' || plan.status || '|' ||
       workflow.action || '|' || GREATEST(
           0,
           EXTRACT(EPOCH FROM (
               now() - COALESCE(plan.operation_started_at,plan.created_at)
           ))
       )::bigint::text
FROM control.ozon_campaign_plans plan
JOIN control.ozon_campaign_launch_workflows workflow
  ON workflow.plan_id=plan.plan_id
WHERE plan.status IN ('creating','adding_products','activating','ambiguous');

SELECT 'ozon_launch_pending|' || plan.plan_id || '|' || plan.status || '|' ||
       workflow.action || '|' || GREATEST(
           0,
           EXTRACT(EPOCH FROM (now() - workflow.requested_at))
       )::bigint::text
FROM control.ozon_campaign_plans plan
JOIN control.ozon_campaign_launch_workflows workflow
  ON workflow.plan_id=plan.plan_id
WHERE workflow.requested_at IS NOT NULL
  AND plan.status IN ('approved','created','products_added');

SELECT 'ozon_launch_failed|' || plan.plan_id || '|' ||
       COALESCE(plan.campaign_id::text,'none') || '|' ||
       COALESCE(plan.last_error_class,'unknown')
FROM control.ozon_campaign_plans plan
WHERE plan.status='failed' AND plan.campaign_id IS NOT NULL;

SELECT 'ozon_applied_without_guard|' || plan.plan_id || '|' ||
       COALESCE(plan.campaign_id::text,'none')
FROM control.ozon_campaign_plans plan
LEFT JOIN control.ozon_campaign_guards guard ON guard.plan_id=plan.plan_id
WHERE plan.status='applied' AND guard.plan_id IS NULL;

SELECT 'ozon_guard_state|' || plan_id || '|' || campaign_id::text || '|' ||
       status || '|' || COALESCE(incident_error_class,stop_reason,'unknown') || '|' ||
       GREATEST(
           0,
           EXTRACT(EPOCH FROM (now() - COALESCE(last_checked_at,created_at)))
       )::bigint::text
FROM control.ozon_campaign_guards
WHERE status IN ('stopping','incident');

SELECT 'ozon_guard_cycle|' || plan_id || '|' || campaign_id::text || '|' ||
       GREATEST(
           0,
           EXTRACT(EPOCH FROM (now() - COALESCE(last_checked_at,created_at)))
       )::bigint::text
FROM control.ozon_campaign_guards
WHERE status='active';

SELECT 'stalled|' || stall_kind || '|' || reference
FROM daily_reporting.stalled_report_work;
SQL
    if [[ "$reporting_scope" != '[]' ]]; then
      printf 'PREPARE reporting_health(text, timestamptz) AS\n'
      cat "$reporting_resources/reporting-health.sql"
      printf '\n;\n'
      printf "EXECUTE reporting_health(:'reporting_accounts', NULL);\n"
    fi
  } | probe_sql
)" || probe_status=$?

if ((probe_status != 0)); then
  add_finding "position database health probe failed before returning complete evidence"
  report_and_exit
fi
if [[ -z "$probe_output" ]]; then
  add_finding "position database returned no health evidence"
  report_and_exit
fi
database_evidence=true

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
    reporting\|*)
      add_finding "daily report collection requires attention: ${row#reporting|}"
      ;;
    cycle_age\|none)
      add_finding "WB robot has never recorded a cycle"
      ;;
    cycle_age\|*)
      age="${row#cycle_age|}"
      if [[ "$age" =~ ^[0-9]+$ ]] && ((age > cycle_stale_seconds)); then
        add_finding "WB robot last ran $((age / 60)) minutes ago; the daily spend cap is only enforced while it polls" "wb_cycle_stale"
      elif [[ ! "$age" =~ ^[0-9]+$ ]]; then
        add_finding "WB robot returned an invalid cycle age"
      fi
      ;;
    ozon_launch_recovery\|*)
      IFS='|' read -r _ plan_id status action age <<<"$row"
      if [[ ! "$age" =~ ^[0-9]+$ ]]; then
        add_finding "Ozon launch recovery returned an invalid age: $plan_id/$status/$action"
      elif ((age > cycle_stale_seconds)); then
        add_finding "Ozon launch requires readback recovery: $plan_id/$status/$action ($((age / 60)) minutes)" "ozon_recovery/$plan_id/$status/$action"
      fi
      ;;
    ozon_launch_pending\|*)
      IFS='|' read -r _ plan_id status action age <<<"$row"
      if [[ ! "$age" =~ ^[0-9]+$ ]]; then
        add_finding "Ozon launch outbox returned an invalid age: $plan_id/$status/$action"
      elif ((age > cycle_stale_seconds)); then
        add_finding "Ozon launch outbox is stalled: $plan_id/$status/$action ($((age / 60)) minutes)" "ozon_outbox/$plan_id/$status/$action"
      fi
      ;;
    ozon_launch_failed\|*)
      add_finding "Ozon launch failed after obtaining a campaign identity: ${row#ozon_launch_failed|}"
      ;;
    ozon_applied_without_guard\|*)
      add_finding "Ozon applied campaign has no durable spend guard: ${row#ozon_applied_without_guard|}"
      ;;
    ozon_guard_state\|*)
      IFS='|' read -r _ plan_id campaign_id status reason age <<<"$row"
      if [[ "$status" == incident ]]; then
        add_finding "Ozon campaign guard is incident-locked: $plan_id/$campaign_id/$reason"
      elif [[ ! "$age" =~ ^[0-9]+$ ]]; then
        add_finding "Ozon campaign guard returned an invalid stopping age: $plan_id/$campaign_id"
      elif ((age > cycle_stale_seconds)); then
        add_finding "Ozon campaign stop is unresolved: $plan_id/$campaign_id/$reason ($((age / 60)) minutes)" "ozon_stop/$plan_id/$campaign_id/$reason"
      fi
      ;;
    ozon_guard_cycle\|*)
      IFS='|' read -r _ plan_id campaign_id age <<<"$row"
      if [[ ! "$age" =~ ^[0-9]+$ ]]; then
        add_finding "Ozon campaign guard returned an invalid cycle age: $plan_id/$campaign_id"
      elif ((age > cycle_stale_seconds)); then
        add_finding "Ozon campaign guard is stale: $plan_id/$campaign_id ($((age / 60)) minutes)" "ozon_guard_stale/$plan_id/$campaign_id"
      fi
      ;;
  esac
done <<<"$probe_output"

report_and_exit
