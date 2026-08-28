#!/bin/bash
# Encrypted, self-consistent backup of the two stores that hold every
# irreplaceable byte in this deployment: the PostgreSQL database and the
# report artifact volume.
#
# Ordering is a correctness property, not a convenience. `persist_and_mark_ready`
# writes artifact bytes before the database row that references them becomes
# ready, so a database snapshot taken at T1 can only reference artifacts that
# already existed before T1. Capturing the database first and the artifacts
# second therefore yields an artifact set that is a superset of what the dump
# references. The reverse order would produce dangling `artifact_object_key`
# values for anything published between the two captures.
#
# The archive is encrypted with AES-256-CBC. CBC is not authenticated, so the
# manifest records the SHA-256 of each ciphertext and the restore path verifies
# it before decrypting. That detects corruption and truncation; it is not a
# defence against an attacker who can rewrite both the archive and its manifest,
# which is why the backup directory belongs on separate, protected storage.

set -euo pipefail

# A LaunchAgent runs an installed copy from ~/.local/libexec, where the
# path relative to this file no longer points at the project. The agent
# therefore passes the project directory explicitly, exactly as the WB
# automation runner already does.
project_root="${MCP_OPS_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
if [[ ! -d "$project_root" || -L "$project_root" ]]; then
  echo "project directory is unavailable or unsafe: $project_root" >&2
  exit 1
fi
position_env="${MCP_BACKUP_POSITION_ENV:-$project_root/.position.env}"
runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
passphrase_file="${MCP_BACKUP_PASSPHRASE_FILE:-$runtime_dir/backup-passphrase}"
backup_root="${MCP_BACKUP_DIR:-$HOME/MCP_OZON-backups}"
retain="${MCP_BACKUP_RETAIN:-14}"
artifact_volume="${MCP_BACKUP_ARTIFACT_VOLUME:-mcp-ozon-report-artifacts}"
db_network="${MCP_BACKUP_DB_NETWORK:-mcp-ozon-position-internal}"
db_host="${MCP_BACKUP_DB_HOST:-position-db}"
offsite_command="${MCP_BACKUP_OFFSITE_COMMAND:-}"
pbkdf2_iterations=600000

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

for path in "$position_env" "$passphrase_file"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required backup input is unavailable or unsafe: $path" >&2
    exit 1
  fi
  if [[ "$("${stat_mode[@]}" "$path")" != "600" ]]; then
    echo "backup input must have mode 600: $path" >&2
    exit 1
  fi
done

passphrase_length="$(head -c 4096 "$passphrase_file" | head -n 1 | tr -d '\n' | wc -c | tr -d ' ')"
if ((passphrase_length < 32)); then
  echo "backup passphrase must be at least 32 characters" >&2
  echo "generate one with: ./scripts/bootstrap-backup-passphrase.sh $passphrase_file" >&2
  exit 1
fi

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

docker_bin="${DOCKER_BIN:-$(command -v docker || true)}"
if [[ -z "$docker_bin" || ! -x "$docker_bin" ]]; then
  echo "docker CLI is unavailable" >&2
  exit 1
fi
if ! "$docker_bin" info >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to encrypt the backup" >&2
  exit 1
fi

# Restoring with a different PostgreSQL build is the classic way to discover
# that a backup was never restorable. Pin the dump and the future restore to
# the exact digest the running database was built from.
db_image="$(
  awk '/^FROM postgres:/ { print $2; exit }' \
    "$project_root/position-monitor/Dockerfile"
)"
if [[ ! "$db_image" =~ ^postgres:[0-9]+-alpine@sha256:[0-9a-f]{64}$ ]]; then
  echo "pinned PostgreSQL image could not be resolved from position-monitor/Dockerfile" >&2
  exit 1
fi

if ! "$docker_bin" volume inspect "$artifact_volume" >/dev/null 2>&1; then
  echo "report artifact volume is unavailable: $artifact_volume" >&2
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

# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
  if [[ -d "$staging_dir" ]]; then
    rm -rf "$staging_dir"
  fi
}
trap cleanup EXIT

encrypt_to() {
  openssl enc -aes-256-cbc -salt -pbkdf2 -iter "$pbkdf2_iterations" \
    -pass "file:$passphrase_file" -out "$1"
}

database_archive="$staging_dir/position-db.dump.enc"
artifact_archive="$staging_dir/report-artifacts.tar.enc"

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
    PGPASSWORD="$POSITION_DB_ADMIN_PASSWORD" \
    exec pg_dump \
      --host="'"$db_host"'" \
      --port=5432 \
      --username="'"$db_owner"'" \
      --dbname="'"$db_name"'" \
      --format=custom \
      --compress=9 \
      --no-password
  ' | encrypt_to "$database_archive"

# Second, and only second: see the ordering note at the top of this file.
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
database_bytes="$(wc -c <"$database_archive" | tr -d ' ')"
artifact_bytes="$(wc -c <"$artifact_archive" | tr -d ' ')"

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
  --argjson database_bytes "$database_bytes" \
  --argjson artifact_bytes "$artifact_bytes" \
  --argjson pbkdf2_iterations "$pbkdf2_iterations" \
  '{
     manifest_version: 1,
     started_at: $started_at,
     finished_at: $finished_at,
     capture_order: ["position-db", "report-artifacts"],
     postgres_image: $db_image,
     database_name: $db_name,
     database_owner: $db_owner,
     cipher: "aes-256-cbc",
     key_derivation: { function: "pbkdf2", iterations: $pbkdf2_iterations },
     archives: {
       "position-db.dump.enc": {
         format: "pg_dump --format=custom",
         sha256: $database_sha256,
         bytes: $database_bytes
       },
       "report-artifacts.tar.enc": {
         format: "tar",
         sha256: $artifact_sha256,
         bytes: $artifact_bytes
       }
     }
   }' >"$staging_dir/manifest.json"
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
fi

printf 'backup complete: %s (db %s bytes, artifacts %s bytes)\n' \
  "$target_dir" "$database_bytes" "$artifact_bytes"
printf 'verify it with: ./scripts/verify-position-backup.sh %s\n' "$target_dir"
