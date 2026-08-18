#!/bin/sh
# Generate the local, ignored credentials required by the internal reporting DB.
# The values are intentionally never printed: Docker Compose receives them only
# from the output file passed by the operator.
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 OUTPUT_PATH" >&2
    exit 64
fi

output_path=$1
if [ -e "$output_path" ]; then
    echo "refusing to overwrite existing secret file: $output_path" >&2
    exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate database passwords" >&2
    exit 1
fi

generate_password() {
    openssl rand -hex 32
}

umask 077
{
    printf '%s\n' 'POSITION_DB_NAME=ozon_positions'
    printf '%s\n' 'POSITION_DB_ADMIN_USER=position_admin'
    printf 'POSITION_DB_ADMIN_PASSWORD=%s\n' "$(generate_password)"
    printf 'POSITION_COLLECTOR_DB_PASSWORD=%s\n' "$(generate_password)"
    printf 'POSITION_READER_DB_PASSWORD=%s\n' "$(generate_password)"
    printf 'REPORT_WORKER_DB_PASSWORD=%s\n' "$(generate_password)"
    printf 'REPORT_COLLECTOR_DB_PASSWORD=%s\n' "$(generate_password)"
} >"$output_path"

chmod 600 "$output_path"
echo "created protected reporting database secret file: $output_path"
