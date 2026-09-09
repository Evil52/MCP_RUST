#!/bin/bash
# Install independently of the server release; never restart Docker or the VPN.
set -euo pipefail
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-}"
label="com.ofk.mcp-ozon-marketplace-routing"
runtime_dir="/Library/Application Support/MCP_OZON/marketplace-routing"
state_dir="/private/var/db/mcp-ozon-marketplace-routing"
plist="/Library/LaunchDaemons/$label.plist"

if [[ "$(uname -s)" != Darwin || "$EUID" != 0 ]]; then
  echo "Run on macOS with sudo; route changes require administrator access." >&2
  exit 1
fi
if [[ "$mode" != install && "$mode" != uninstall ]]; then
  echo "Usage: sudo bash $0 install CONFIG TUNNEL_URL_FILE | uninstall" >&2
  exit 1
fi

# Reject symlinks and user-writable ancestors before a privileged file operation.
/usr/bin/python3 -I -B - "$runtime_dir" "$state_dir" "$plist" <<'PY'
import pathlib
import stat
import sys
for raw in sys.argv[1:]:
    path = pathlib.Path(raw)
    for entry in (path, *path.parents):
        if not entry.exists() and not entry.is_symlink():
            continue
        metadata = entry.lstat()
        if stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_mode & 0o022:
            raise SystemExit('Installation path must be root-owned, non-writable, and not a symlink')
PY

if [[ "$mode" == uninstall ]]; then
  if launchctl print "system/$label" >/dev/null 2>&1; then
    launchctl bootout "system/$label"
  fi
  /usr/bin/python3 -I -B "$runtime_dir/marketplace_routes.py" rollback \
    --config "$runtime_dir/config.json"
  # Retain root-owned code, journal and logs for diagnosis/reinstallation.
  rm -f "$plist" "$runtime_dir/config.json"
  echo "Stopped $label; only journaled and still-owned routes were removed."
  exit 0
fi

config="${2:?Specify the reviewed interface/gateway configuration}"
tunnel_url_file="${3:?Specify the local tunnel health URL file}"
if [[ -e "$plist" || -e "$runtime_dir/config.json" ]]; then
  echo "Existing installation detected; uninstall first and preserve its state for review." >&2
  exit 1
fi

# Require the requested split to work BEFORE enabling automatic reconciliation.
# This validates routing and unauthenticated HTTPS, never marketplace completeness.
/usr/bin/python3 -B "$project_root/scripts/marketplace_routes.py" check \
  --config "$config" --probe --tunnel-url-file "$tunnel_url_file" >/dev/null

# Any installation failure stops the new job and rolls back its own routes.
# If rollback cannot finish, preserve the config and journal for a retry.
# shellcheck disable=SC2317,SC2329 # Invoked by the EXIT trap.
cleanup_failed_install() {
  local status="$?"
  if ((status == 0)); then return; fi
  trap - EXIT
  if launchctl print "system/$label" >/dev/null 2>&1; then
    launchctl bootout "system/$label" || return "$status"
  fi
  if [[ -f "$runtime_dir/config.json" && -f "$runtime_dir/marketplace_routes.py" ]]; then
    if ! /usr/bin/python3 -I -B "$runtime_dir/marketplace_routes.py" rollback \
      --config "$runtime_dir/config.json"; then
      echo "Installation failed; rollback needs a retry. Config and journal retained." >&2
      return "$status"
    fi
  fi
  rm -f "$plist" "$runtime_dir/config.json"
  return "$status"
}
trap cleanup_failed_install EXIT

install -d -m 755 "/Library/Application Support/MCP_OZON" "$runtime_dir"
install -d -m 700 "$state_dir"
install -m 644 "$project_root/scripts/marketplace_routes.py" "$runtime_dir/marketplace_routes.py"
install -m 644 "$config" "$runtime_dir/config.json"
install -m 644 "$project_root/ops/macos/$label.plist" "$plist"
plutil -lint "$plist" >/dev/null

# On today's working split this is a no-op. Future drift adds only /32 routes.
/usr/bin/python3 -I -B "$runtime_dir/marketplace_routes.py" apply \
  --config "$runtime_dir/config.json"
/usr/bin/python3 -B "$project_root/scripts/marketplace_routes.py" check \
  --config "$runtime_dir/config.json" --probe --tunnel-url-file "$tunnel_url_file" >/dev/null
launchctl bootstrap system "$plist"
launchctl kickstart "system/$label"
echo "Installed $label. Verify the tunnel and marketplace reads after VPN reconnect."
