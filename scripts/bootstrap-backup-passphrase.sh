#!/bin/sh
# Generate the passphrase needed only to restore legacy backup format v1.
# The value is never printed: only the backup and restore scripts read it,
# and they take it from this file by path.
set -eu

if [ "$#" -gt 1 ]; then
    echo "usage: $0 [OUTPUT_PATH]" >&2
    exit 64
fi

runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
output_path="${1:-$runtime_dir/backup-passphrase}"

if [ -e "$output_path" ]; then
    echo "refusing to overwrite existing passphrase file: $output_path" >&2
    exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate the backup passphrase" >&2
    exit 1
fi

umask 077
mkdir -p "$(dirname "$output_path")"
openssl rand -hex 32 >"$output_path"
chmod 600 "$output_path"

echo "created protected backup passphrase file: $output_path"
echo
echo "This passphrase is for legacy manifest_version 1 backups only."
echo "New backups use age; run ./scripts/bootstrap-backup-age-key.sh."
echo "Store a copy outside this host now. A backup whose passphrase exists"
echo "only on the machine the backup protects cannot be restored after that"
echo "machine is lost, which is the case the backup exists for."
