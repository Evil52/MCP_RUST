#!/bin/bash
# Encrypted, self-consistent backup of PostgreSQL, durable Ozon guard state
# and report artifacts. The guard must be quiesced by the operator: this script
# never stops an executor and refuses to copy its state while its lease is held.
#
# Ordering is a correctness property, not a convenience. `persist_and_mark_ready`
# writes artifact bytes before the database row that references them becomes
# ready, so a database snapshot taken at T1 can only reference artifacts that
# already existed before T1. Capturing the database first and the artifacts
# second therefore yields an artifact set that is a superset of what the dump
# references. The reverse order would produce dangling `artifact_object_key`
# values for anything published between the two captures.
#
# The archive is encrypted to an age recipient. The age v1 file format is
# authenticated, so a modified ciphertext is rejected even if an attacker can
# also rewrite the adjacent manifest. The manifest still records SHA-256 for
# early corruption detection and stable archive identity.

set -euo pipefail

# Installed agents receive the pinned image and private env path directly.
# Keep checkout lookup only for manual invocations, never as a dependency of
# the permanent backup schedule.
project_root="${MCP_OPS_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
position_env="${MCP_BACKUP_POSITION_ENV:-$project_root/.position.env}"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
recipients_file="${MCP_BACKUP_AGE_RECIPIENTS_FILE:-$runtime_dir/backup-age-recipients.txt}"
backup_root="${MCP_BACKUP_DIR:-$HOME/MCP_OZON-backups}"
retain="${MCP_BACKUP_RETAIN:-14}"
artifact_volume="${MCP_BACKUP_ARTIFACT_VOLUME:-mcp-ozon-report-artifacts}"
guard_volume="${MCP_BACKUP_GUARD_STATE_VOLUME:-}"
db_network="${MCP_BACKUP_DB_NETWORK:-mcp-ozon-position-internal}"
db_host="${MCP_BACKUP_DB_HOST:-position-db}"
offsite_command="${MCP_BACKUP_OFFSITE_COMMAND:-}"
allow_local_only="${MCP_BACKUP_ALLOW_LOCAL_ONLY:-false}"

umask 077

if [[ "$(uname -s)" == "Darwin" ]]; then
  stat_mode=(/usr/bin/stat -f '%Lp')
  sha256=(shasum -a 256)
else
  stat_mode=(stat -c '%a')
  sha256=(sha256sum)
fi

if [[ ! "$retain" =~ ^[1-9][0-9]*$ ]] || ((retain < 3 || retain > 365)); then
  echo "MCP_BACKUP_RETAIN must be an integer from 3 to 365" >&2
  exit 1
fi
case "$allow_local_only" in
  true | false) ;;
  *)
    echo "MCP_BACKUP_ALLOW_LOCAL_ONLY must be true or false" >&2
    exit 1
    ;;
esac

for path in "$position_env" "$recipients_file"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required backup input is unavailable or unsafe: $path" >&2
    exit 1
  fi
  if [[ "$("${stat_mode[@]}" "$path")" != "600" ]]; then
    echo "backup input must have mode 600: $path" >&2
    exit 1
  fi
done

if ! grep -Eq '^POSITION_DB_ADMIN_PASSWORD=.{24,}$' "$position_env"; then
  echo "position database admin password is unavailable" >&2
  exit 1
fi

# Same contract as the health hook: one executable file, invoked with the new
# backup directory as its only argument.
if [[ -n "$offsite_command" && ! -x "$offsite_command" ]]; then
  echo "MCP_BACKUP_OFFSITE_COMMAND must be one executable file: $offsite_command" >&2
  exit 1
fi
if [[ -z "$offsite_command" && "$allow_local_only" != true ]]; then
  echo "an executable MCP_BACKUP_OFFSITE_COMMAND is required" >&2
  echo "set MCP_BACKUP_ALLOW_LOCAL_ONLY=true only as an explicit accepted-risk exception" >&2
  exit 1
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
if ! command -v age >/dev/null 2>&1; then
  echo "age is required to encrypt the backup" >&2
  echo "install age and run: ./scripts/bootstrap-backup-age-key.sh" >&2
  exit 1
fi

# Restoring with a different PostgreSQL build is the classic way to discover
# that a backup was never restorable. Pin the dump and the future restore to
# the exact digest the running database was built from.
db_image="${MCP_OPS_POSTGRES_IMAGE:-}"
if [[ -z "$db_image" && -f "$project_root/position-monitor/Dockerfile" \
  && ! -L "$project_root/position-monitor/Dockerfile" ]]; then
  db_image="$(awk '/^FROM postgres:/ { print $2; exit }' \
    "$project_root/position-monitor/Dockerfile")"
fi
if [[ ! "$db_image" =~ ^postgres:[0-9]+-alpine([0-9]+\.[0-9]+)?@sha256:[0-9a-f]{64}$ ]]; then
  echo "a pinned PostgreSQL image is required via MCP_OPS_POSTGRES_IMAGE or position-monitor/Dockerfile" >&2
  exit 1
fi

if ! "$docker_bin" volume inspect "$artifact_volume" >/dev/null 2>&1; then
  echo "report artifact volume is unavailable: $artifact_volume" >&2
  exit 1
fi
# Base read-only deployments may have no guard. Discover both Compose-labelled
# volumes and conventional names; never silently pick one of several guards.
if [[ -z "$guard_volume" ]]; then
  guard_volumes="$(
    {
      "$docker_bin" volume ls --filter label=com.docker.compose.volume=guard-state --format '{{.Name}}' || exit 1
      "$docker_bin" volume ls --format '{{.Name}}' | sed -n '/-guard-state$/p' || exit 1
    } | sort -u
  )"
  if [[ -n "$guard_volumes" ]]; then
    if [[ "$(printf '%s\n' "$guard_volumes" | wc -l | tr -d ' ')" != 1 ]]; then
      echo "multiple guard state volumes found; a reviewed multi-guard recovery procedure is required" >&2
      exit 1
    fi
    guard_volume="$guard_volumes"
  fi
fi
if [[ -n "$guard_volume" ]] && ! "$docker_bin" volume inspect "$guard_volume" >/dev/null 2>&1; then
  echo "Ozon guard state volume is unavailable: $guard_volume; refusing an incomplete backup" >&2
  exit 1
fi
if ! "$docker_bin" network inspect "$db_network" >/dev/null 2>&1; then
  echo "position database network is unavailable: $db_network" >&2
  exit 1
fi

# The manifest carries everything the restore path needs, so verification and
# recovery never depend on a `.position.env` that may itself have been lost.
db_name="$(sed -n 's/^POSITION_DB_NAME=//p' "$position_env" | head -n 1)"
db_owner="$(sed -n 's/^POSITION_DB_ADMIN_USER=//p' "$position_env" | head -n 1)"
db_name="${db_name:-ozon_positions}"
db_owner="${db_owner:-position_admin}"
if [[ ! "$db_name" =~ ^[A-Za-z0-9_]+$ || ! "$db_owner" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "position database name or owner is not a plain identifier" >&2
  exit 1
fi

started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
stamp="$(date -u '+%Y%m%dT%H%M%SZ')"
target_dir="$backup_root/$stamp"
staging_dir="$target_dir.partial"

mkdir -p "$backup_root"
chmod 700 "$backup_root"
if [[ -e "$target_dir" || -e "$staging_dir" ]]; then
  echo "backup destination already exists: $target_dir" >&2
  exit 1
fi
mkdir "$staging_dir"
chmod 700 "$staging_dir"

# Keep the lease container until both captures and cursor checks finish.
guard_lock="mcp-ozon-backup-lease-$$-$stamp"
guard_lock_created=false

# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  if [[ "$guard_lock_created" == true ]]; then
    "$docker_bin" rm -f "$guard_lock" >/dev/null 2>&1 || true
  fi
  if [[ -d "$staging_dir" ]]; then
    rm -rf "$staging_dir"
  fi
}
trap cleanup EXIT

encrypt_to() {
  age --encrypt --recipients-file "$recipients_file" --output "$1"
}

database_archive="$staging_dir/position-db.dump.age"
artifact_archive="$staging_dir/report-artifacts.tar.age"
guard_archive="$staging_dir/ozon-guard-state.tar.age"

guard_cursor=0
if [[ -n "$guard_volume" ]]; then
# Rust File::try_lock and Linux flock use the same advisory lease. Opening the
# existing inode read-only leaves its PID heartbeat and all source bytes intact.
# A detached holder lets the two archives stream directly to host-side age.
# It has no restart policy; checking StartedAt and running state again after
# capture proves this exact holder did not exit/restart and release its lock.
guard_lock_created=true
# shellcheck disable=SC2016 # Source permissions and lease are checked in Docker.
"$docker_bin" run --detach --name "$guard_lock" \
  --network none --read-only --cap-drop ALL --user 10001:10001 \
  --volume "$guard_volume:/guard-state:ro" --entrypoint /bin/sh "$db_image" \
  -ec '
    test -f /guard-state/.state.json.lease && test ! -L /guard-state/.state.json.lease
    exec 9</guard-state/.state.json.lease
    flock -n 9
    for path in /guard-state/state.json /guard-state/.state.json.lease; do
      test -f "$path" && test ! -L "$path"
      test "$(stat -c %a "$path")" = 600
      test "$(stat -c %u:%g "$path")" = 10001:10001
    done
    test "$(stat -c %a /guard-state)" = 700
    test "$(stat -c %u:%g /guard-state)" = 10001:10001
    test "$(wc -c </guard-state/state.json)" -le 262144
    echo locked
    exec sleep 3600
  ' >/dev/null
locked=false
for _attempt in 1 2 3 4 5; do
  if [[ "$("$docker_bin" logs "$guard_lock" 2>/dev/null)" == locked ]]; then
    locked=true
    break
  fi
  if [[ "$("$docker_bin" inspect --format '{{.State.Running}}' "$guard_lock")" != true ]]; then
    break
  fi
  sleep 1
done
if [[ "$locked" != true ]]; then
  echo "guard state lease is busy or unsafe; quiesce the Ozon guard in an approved maintenance window before backup" >&2
  exit 1
fi
guard_lock_identity="$("$docker_bin" inspect --format '{{.State.Running}}|{{.State.StartedAt}}' "$guard_lock")"
if [[ "$guard_lock_identity" != true\|* ]]; then
  echo "guard state backup lease was lost" >&2
  exit 1
fi
guard_cursor="$("$docker_bin" run --rm --network none --user 10001:10001 \
  --volume "$guard_volume:/guard-state:ro" --entrypoint /bin/sh "$db_image" \
  -ec 'exec cat /guard-state/state.json' \
  | jq -er '.last_static_audit_event_id | select(type == "number" and . > 0 and . <= 9007199254740991 and . == floor)')" || {
  echo "guard state has no valid initialized audit cursor" >&2
  exit 1
}
if [[ ! "$guard_cursor" =~ ^[1-9][0-9]*$ ]]; then
  echo "guard state has no valid initialized audit cursor" >&2
  exit 1
fi

fi

# `pg_dump --format=custom` is what `pg_restore` consumes selectively, and it
# takes one consistent snapshot without blocking the collectors.
# shellcheck disable=SC2016 # The password expands inside the container, from
# --env-file, so it never appears in this host's environment or process list.
"$docker_bin" run --rm \
  --network "$db_network" \
  --env-file "$position_env" \
  --entrypoint /bin/sh \
  "$db_image" \
  -ec '
    export PGPASSWORD="$POSITION_DB_ADMIN_PASSWORD"
    guard_psql() {
      psql --host="'"$db_host"'" --username="'"$db_owner"'" \
        --dbname="'"$db_name"'" --no-password --no-psqlrc --tuples-only --no-align \
        --set ON_ERROR_STOP=1 --command="$1"
    }
    check_guard_cursor() {
      if test '"$guard_cursor"' = 0; then
        if test "$(guard_psql "SELECT to_regclass('\''control.ozon_static_guard_audit_events'\'') IS NULL")" = t; then
          return 0
        fi
        test "$(guard_psql "SELECT count(*) = 0 FROM control.ozon_static_guard_audit_events")" = t
      else
        test "$(guard_psql "SELECT count(*) = 1 FROM control.ozon_static_guard_audit_events e WHERE e.event_id = '"$guard_cursor"' AND e.event_id = (SELECT max(a.event_id) FROM control.ozon_static_guard_audit_events a WHERE a.account_id = e.account_id) AND (SELECT count(DISTINCT account_id) FROM control.ozon_static_guard_audit_events) = 1")" = t
      fi
    }
    check_guard_cursor || { echo "guard state/database cursor mismatch before dump" >&2; exit 1; }
    pg_dump \
      --host="'"$db_host"'" \
      --port=5432 \
      --username="'"$db_owner"'" \
      --dbname="'"$db_name"'" \
      --format=custom \
      --compress=9 \
      --no-password
    check_guard_cursor || { echo "guard audit changed during dump; refusing inconsistent backup" >&2; exit 1; }
  ' | encrypt_to "$database_archive"

if [[ -n "$guard_volume" ]]; then
# Only state.json is durable. PID lease and abandoned temporary files must not
# be reinstated as executor state. tar retains the state file UID/GID and mode.
"$docker_bin" run --rm --network none --user 10001:10001 \
  --volume "$guard_volume:/guard-state:ro" --entrypoint /bin/sh "$db_image" \
  -ec 'exec tar --create --file - --directory /guard-state state.json' \
  | encrypt_to "$guard_archive"
if [[ "$("$docker_bin" inspect --format '{{.State.Running}}|{{.State.StartedAt}}' "$guard_lock")" != "$guard_lock_identity" ]]; then
  echo "guard state backup lease was lost during capture" >&2
  exit 1
fi
"$docker_bin" rm -f "$guard_lock" >/dev/null
guard_lock_created=false

fi

# Artifacts must follow the database: see the ordering note at the top.
"$docker_bin" run --rm \
  --network none \
  --volume "$artifact_volume:/artifacts:ro" \
  --entrypoint /bin/sh \
  "$db_image" \
  -ec 'exec tar --create --file - --directory /artifacts .' \
  | encrypt_to "$artifact_archive"

finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

database_sha256="$("${sha256[@]}" "$database_archive" | awk '{ print $1 }')"
artifact_sha256="$("${sha256[@]}" "$artifact_archive" | awk '{ print $1 }')"
guard_sha256=''
if [[ -n "$guard_volume" ]]; then
  guard_sha256="$("${sha256[@]}" "$guard_archive" | awk '{ print $1 }')"
fi
recipients_sha256="$("${sha256[@]}" "$recipients_file" | awk '{ print $1 }')"
database_bytes="$(wc -c <"$database_archive" | tr -d ' ')"
artifact_bytes="$(wc -c <"$artifact_archive" | tr -d ' ')"
guard_bytes=0
if [[ -n "$guard_volume" ]]; then
  guard_bytes="$(wc -c <"$guard_archive" | tr -d ' ')"
fi

if ((database_bytes < 1024)); then
  echo "database archive is implausibly small; refusing to publish this backup" >&2
  exit 1
fi

jq -n \
  --arg started_at "$started_at" \
  --arg finished_at "$finished_at" \
  --arg db_image "$db_image" \
  --arg db_name "$db_name" \
  --arg db_owner "$db_owner" \
  --arg database_sha256 "$database_sha256" \
  --arg artifact_sha256 "$artifact_sha256" \
  --arg guard_sha256 "$guard_sha256" \
  --arg recipients_sha256 "$recipients_sha256" \
  --argjson database_bytes "$database_bytes" \
  --argjson artifact_bytes "$artifact_bytes" \
  --argjson guard_bytes "$guard_bytes" \
  '{
     manifest_version: (if $guard_bytes > 0 then 3 else 2 end),
     started_at: $started_at,
     finished_at: $finished_at,
     capture_order: (["position-db"] + (if $guard_bytes > 0 then ["ozon-guard-state"] else [] end) + ["report-artifacts"]),
     guard_state_required: ($guard_bytes > 0),
     postgres_image: $db_image,
     database_name: $db_name,
     database_owner: $db_owner,
     encryption: {
       format: "age",
       specification: "v1",
       recipients_sha256: $recipients_sha256
     },
     archives: {
       "position-db.dump.age": {
         format: "pg_dump --format=custom",
         sha256: $database_sha256,
         bytes: $database_bytes
       },
       "report-artifacts.tar.age": {
         format: "tar",
         sha256: $artifact_sha256,
         bytes: $artifact_bytes
       }
     }
   } + (if $guard_bytes > 0 then {
     guard_state: {file: "state.json", consistency: "exclusive-state-lease", root_mode: "700", uid: 10001, gid: 10001}
   } else {} end)
   | if $guard_bytes > 0 then .archives["ozon-guard-state.tar.age"] = {
       format: "tar", sha256: $guard_sha256, bytes: $guard_bytes
     } else . end' >"$staging_dir/manifest.json"
chmod 600 "$staging_dir"/*
chmod 700 "$staging_dir"

mv "$staging_dir" "$target_dir"
trap - EXIT

# Retention runs after the new backup is durable, so a failed run never
# consumes one of the copies that are still good.
existing=()
while IFS= read -r candidate; do
  existing+=("$candidate")
done < <(
  find "$backup_root" -mindepth 1 -maxdepth 1 -type d -name '2*Z' \
    | sort -r
)
if ((${#existing[@]} > retain)); then
  for stale in "${existing[@]:retain}"; do
    rm -rf "$stale"
  done
fi

if [[ -n "$offsite_command" ]]; then
  "$offsite_command" "$target_dir"
  marker="$target_dir/offsite-complete.json"
  jq -n \
    --arg completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --arg backup "$(basename "$target_dir")" \
    '{schema_version: 1, completed_at: $completed_at, backup: $backup}' \
    >"$marker"
  chmod 600 "$marker"
else
  marker="$target_dir/local-only-risk-accepted.json"
  jq -n \
    --arg accepted_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    '{schema_version: 1, accepted_at: $accepted_at}' >"$marker"
  chmod 600 "$marker"
fi

printf 'backup complete: %s (db %s bytes, guard %s bytes, artifacts %s bytes)\n' \
  "$target_dir" "$database_bytes" "$guard_bytes" "$artifact_bytes"
printf 'verify it with: ./scripts/verify-position-backup.sh %s\n' "$target_dir"
