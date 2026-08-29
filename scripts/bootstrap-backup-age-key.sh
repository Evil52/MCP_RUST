#!/bin/sh
# Generate the dedicated age identity and public recipient used by backup v2.
# The secret identity is never printed. The backup writer needs only the public
# recipients file; restore verification receives the identity separately.
set -eu

if [ "$#" -gt 2 ]; then
    echo "usage: $0 [IDENTITY_PATH] [RECIPIENTS_PATH]" >&2
    exit 64
fi

runtime_dir="${MCP_RUNTIME_DIR:-$HOME/.local/share/mcp-ozon-runtime}"
identity_path="${1:-$runtime_dir/backup-age-identity.txt}"
recipients_path="${2:-$runtime_dir/backup-age-recipients.txt}"

for path in "$identity_path" "$recipients_path"; do
    if [ -e "$path" ]; then
        echo "refusing to overwrite existing backup key file: $path" >&2
        exit 1
    fi
done

if ! command -v age-keygen >/dev/null 2>&1; then
    echo "age-keygen is required; install the age package first" >&2
    exit 1
fi

umask 077
mkdir -p "$(dirname "$identity_path")" "$(dirname "$recipients_path")"

identity_target="$(cd "$(dirname "$identity_path")" && pwd -P)/$(basename "$identity_path")"
recipients_target="$(cd "$(dirname "$recipients_path")" && pwd -P)/$(basename "$recipients_path")"
if [ "$identity_target" = "$recipients_target" ]; then
    echo "identity and recipients paths must be different" >&2
    exit 1
fi

# shellcheck disable=SC2317,SC2329 # Called indirectly by the EXIT trap.
cleanup() {
    rm -f "$identity_target" "$recipients_target"
}
trap cleanup EXIT HUP INT TERM

age-keygen -o "$identity_path"
age-keygen -y "$identity_path" >"$recipients_path"
chmod 600 "$identity_path" "$recipients_path"
trap - EXIT HUP INT TERM

echo "created protected age identity: $identity_path"
echo "created age recipients file:   $recipients_path"
echo
echo "Store an identity copy outside this host now. The backup writer needs"
echo "only the recipients file; recovery requires the secret identity."
