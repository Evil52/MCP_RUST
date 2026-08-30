#!/bin/bash
# Install the three protective LaunchAgents this deployment needs: a scheduled
# encrypted backup, a scheduled restore verification and a health probe.
#
# Both are proven before they are scheduled. The installer takes one real
# backup, restores it into a disposable PostgreSQL container, and runs one
# health check. An agent that has never completed successfully is not
# installed, because the failure mode this closes is precisely a protective
# job that everyone assumes is running.

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
recipients_file="${MCP_BACKUP_AGE_RECIPIENTS_FILE:-$runtime_dir/backup-age-recipients.txt}"
identity_file="${MCP_BACKUP_AGE_IDENTITY_FILE:-$runtime_dir/backup-age-identity.txt}"
backup_dir="${MCP_BACKUP_DIR:-$HOME/MCP_OZON-backups}"
notify_command="${MCP_HEALTH_NOTIFY_COMMAND:-}"
offsite_command="${MCP_BACKUP_OFFSITE_COMMAND:-}"
allow_local_only="${MCP_BACKUP_ALLOW_LOCAL_ONLY:-false}"
health_required_services="${MCP_HEALTH_REQUIRED_SERVICES-position-db,ozon-egress}"
health_required_launch_agents="${MCP_HEALTH_REQUIRED_LAUNCH_AGENTS-com.ofk.mcp-ozon-runtime,com.ofk.mcp-ozon-backup,com.ofk.mcp-ozon-health,com.ofk.mcp-ozon-restore-verify}"
position_env_source="$project_root/.position.env"
position_env_target="$runtime_dir/position.env"
libexec_dir="$HOME/.local/libexec/mcp-ozon"
agent_dir="$HOME/Library/LaunchAgents"
log_dir="$HOME/Library/Logs/MCP_OZON"
domain="gui/$(id -u)"

backup_source="$project_root/scripts/backup-position-stack.sh"
verify_source="$project_root/scripts/verify-position-backup.sh"
health_source="$project_root/scripts/check-runtime-health.sh"
backup_template="$project_root/ops/macos/com.ofk.mcp-ozon-backup.plist.in"
health_template="$project_root/ops/macos/com.ofk.mcp-ozon-health.plist.in"
restore_template="$project_root/ops/macos/com.ofk.mcp-ozon-restore-verify.plist.in"

umask 077

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "operations LaunchAgent installer supports only macOS" >&2
  exit 1
fi

for path in \
  "$backup_source" "$verify_source" "$health_source" \
  "$backup_template" "$health_template" "$restore_template" "$position_env_source"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required installer input is unavailable or unsafe: $path" >&2
    exit 1
  fi
done

case "$allow_local_only" in
  true | false) ;;
  *)
    echo "MCP_BACKUP_ALLOW_LOCAL_ONLY must be true or false" >&2
    exit 1
    ;;
esac
for csv_contract in "$health_required_services" "$health_required_launch_agents"; do
  if [[ ! "$csv_contract" =~ ^[A-Za-z0-9_.-]+(,[A-Za-z0-9_.-]+)*$ ]]; then
    echo "MCP_HEALTH_REQUIRED_SERVICES and MCP_HEALTH_REQUIRED_LAUNCH_AGENTS must be non-empty comma-separated identifiers" >&2
    exit 1
  fi
done
if [[ -z "$offsite_command" && "$allow_local_only" != true ]]; then
  echo "MCP_BACKUP_OFFSITE_COMMAND is required for scheduled production backups" >&2
  echo "set MCP_BACKUP_ALLOW_LOCAL_ONLY=true only to record an explicit accepted risk" >&2
  exit 1
fi

if [[ "$(/usr/bin/stat -f '%Lp' "$position_env_source")" != "600" ]]; then
  echo "database env must have mode 600: $position_env_source" >&2
  exit 1
fi

for path in "$recipients_file" "$identity_file"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "backup age key file is missing: $path" >&2
    echo "create both first with: ./scripts/bootstrap-backup-age-key.sh" >&2
    exit 1
  fi
  if [[ "$(/usr/bin/stat -f '%Lp' "$path")" != "600" ]]; then
    echo "backup age key file must have mode 600: $path" >&2
    exit 1
  fi
done

for command in age age-keygen openssl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required for the authenticated backup round trip" >&2
    exit 1
  fi
done

for hook in "$notify_command" "$offsite_command"; do
  if [[ -n "$hook" && ! -x "$hook" ]]; then
    echo "notify and offsite hooks must each be one executable file: $hook" >&2
    exit 1
  fi
done

if [[ -z "$notify_command" ]]; then
  echo "warning: MCP_HEALTH_NOTIFY_COMMAND is unset." >&2
  echo "warning: findings will only reach $log_dir/health.log." >&2
fi

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi

mkdir -p "$runtime_dir" "$libexec_dir" "$agent_dir" "$log_dir" "$backup_dir"
chmod 700 "$runtime_dir" "$libexec_dir" "$backup_dir"
install -m 700 "$backup_source" "$libexec_dir/backup-position-stack.sh"
install -m 700 "$verify_source" "$libexec_dir/verify-position-backup.sh"
install -m 700 "$health_source" "$libexec_dir/check-runtime-health.sh"
install -m 600 "$position_env_source" "$position_env_target"

render() {
  sed \
    -e "s|__RUNNER__|$1|g" \
    -e "s|__HOME__|$HOME|g" \
    -e "s|__LOG_DIR__|$log_dir|g" \
    -e "s|__RUNTIME_DIR__|$runtime_dir|g" \
    -e "s|__PROJECT_DIR__|$project_root|g" \
    -e "s|__POSITION_ENV__|$position_env_target|g" \
    -e "s|__BACKUP_DIR__|$backup_dir|g" \
    -e "s|__AGE_RECIPIENTS_FILE__|$recipients_file|g" \
    -e "s|__AGE_IDENTITY_FILE__|$identity_file|g" \
    -e "s|__NOTIFY_COMMAND__|$notify_command|g" \
    -e "s|__OFFSITE_COMMAND__|$offsite_command|g" \
    -e "s|__ALLOW_LOCAL_ONLY__|$allow_local_only|g" \
    -e "s|__HEALTH_REQUIRED_SERVICES__|$health_required_services|g" \
    -e "s|__HEALTH_REQUIRED_LAUNCH_AGENTS__|$health_required_launch_agents|g" \
    "$2"
}

temporary_backup_plist="$(mktemp "${TMPDIR:-/tmp}/mcp-ozon-backup.XXXXXX")"
temporary_health_plist="$(mktemp "${TMPDIR:-/tmp}/mcp-ozon-health.XXXXXX")"
temporary_restore_plist="$(mktemp "${TMPDIR:-/tmp}/mcp-ozon-restore.XXXXXX")"
# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  rm -f "$temporary_backup_plist" "$temporary_health_plist" "$temporary_restore_plist"
}
trap cleanup EXIT

render "$libexec_dir/backup-position-stack.sh" "$backup_template" \
  >"$temporary_backup_plist"
render "$libexec_dir/check-runtime-health.sh" "$health_template" \
  >"$temporary_health_plist"
render "$libexec_dir/verify-position-backup.sh" "$restore_template" \
  >"$temporary_restore_plist"
plutil -lint "$temporary_backup_plist" "$temporary_health_plist" \
  "$temporary_restore_plist" >/dev/null

# Prove one full round trip before scheduling anything. A backup that has never
# been restored is not yet a backup.
echo "==> taking one backup"
MCP_OPS_PROJECT_DIR="$project_root" \
MCP_BACKUP_POSITION_ENV="$position_env_target" \
MCP_BACKUP_AGE_RECIPIENTS_FILE="$recipients_file" \
MCP_BACKUP_DIR="$backup_dir" \
MCP_BACKUP_OFFSITE_COMMAND="$offsite_command" \
MCP_BACKUP_ALLOW_LOCAL_ONLY="$allow_local_only" \
  "$libexec_dir/backup-position-stack.sh"

echo "==> restoring it into a disposable database"
MCP_BACKUP_AGE_IDENTITY_FILE="$identity_file" \
MCP_BACKUP_DIR="$backup_dir" \
  "$libexec_dir/verify-position-backup.sh"

echo "==> running one health check"
health_status=0
MCP_OPS_PROJECT_DIR="$project_root" \
MCP_HEALTH_POSITION_ENV="$position_env_target" \
MCP_BACKUP_DIR="$backup_dir" \
MCP_BACKUP_ALLOW_LOCAL_ONLY="$allow_local_only" \
MCP_HEALTH_REQUIRED_SERVICES="$health_required_services" \
MCP_HEALTH_REQUIRED_LAUNCH_AGENTS="$health_required_launch_agents" \
MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK=true \
  "$libexec_dir/check-runtime-health.sh" || health_status=$?
if ((health_status > 1)); then
  echo "health check could not run; refusing to schedule it" >&2
  exit 1
fi

for label in \
  com.ofk.mcp-ozon-backup \
  com.ofk.mcp-ozon-health \
  com.ofk.mcp-ozon-restore-verify; do
  launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
done
install -m 600 "$temporary_backup_plist" "$agent_dir/com.ofk.mcp-ozon-backup.plist"
install -m 600 "$temporary_health_plist" "$agent_dir/com.ofk.mcp-ozon-health.plist"
install -m 600 "$temporary_restore_plist" \
  "$agent_dir/com.ofk.mcp-ozon-restore-verify.plist"
launchctl bootstrap "$domain" "$agent_dir/com.ofk.mcp-ozon-backup.plist"
launchctl bootstrap "$domain" "$agent_dir/com.ofk.mcp-ozon-health.plist"
launchctl bootstrap "$domain" \
  "$agent_dir/com.ofk.mcp-ozon-restore-verify.plist"

echo
echo "Installed backup (daily 03:30), restore verification (Sunday 04:30),"
echo "and health monitoring (every 15 minutes)."
echo "Backups: $backup_dir"
echo "Logs:    $log_dir/backup.log, $log_dir/health.log"
if [[ "$health_status" -eq 1 ]]; then
  echo
  echo "The health check reported findings above; they are real and unresolved."
fi
