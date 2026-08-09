#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

test -f .env || {
  echo "Нет .env. Создайте его из .env.example и заполните секреты." >&2
  exit 1
}
test -f config/access.json || {
  echo "Нет config/access.json. Создайте его из config/access.example.json." >&2
  exit 1
}
command -v cargo-watch >/dev/null 2>&1 || {
  echo "Нет cargo-watch. Установите: cargo install cargo-watch --locked" >&2
  exit 1
}

# Keep host development isolated from the Docker/Tunnel endpoint on 8787.
export MCP_BIND="${MCP_DEV_BIND:-127.0.0.1:8789}"

exec cargo watch \
  --watch src \
  --watch Cargo.toml \
  --watch Cargo.lock \
  --watch .env \
  --why \
  --exec run
