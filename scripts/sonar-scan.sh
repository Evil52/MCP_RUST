#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scanner_image="sonarsource/sonar-scanner-cli:12.1.0.3233_8.0.1@sha256:23ca0f137965d9dff2198074043fd48d386280bc5d0ccac8c8349cea4cf096a9"
scanner_container="mcp-ozon-sonar-scan-$$"
sonar_env_file="$project_root/.sonar.env"
sonar_token_source="environment"

load_sonar_env() {
  local replace_token="${1:-false}"
  local key value
  [[ -f "$sonar_env_file" ]] || return 0
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    value="${value%$'\r'}"
    case "$key" in
      SONAR_HOST_URL)
        if [[ -z "${SONAR_HOST_URL:-}" ]]; then
          export SONAR_HOST_URL="$value"
        fi
        ;;
      SONAR_TOKEN)
        if [[ "$replace_token" == "true" || -z "${SONAR_TOKEN:-}" ]]; then
          export SONAR_TOKEN="$value"
          sonar_token_source=".sonar.env"
        fi
        ;;
      ''|'#'*) ;;
      *) echo "Ignoring unsupported variable '$key' in .sonar.env." >&2 ;;
    esac
  done < "$sonar_env_file"
}

bearer_http_status() {
  local token="$1"
  local url="$2"
  printf 'header = "Authorization: Bearer %s"\n' "$token" \
    | curl --config - \
      --silent \
      --show-error \
      --output /dev/null \
      --write-out '%{http_code}' \
      "$url"
}

load_sonar_env

SONAR_HOST_URL="${SONAR_HOST_URL:-http://127.0.0.1:9000}"

if [[ "$SONAR_HOST_URL" == "http://127.0.0.1:9000" ]] \
  || [[ "$SONAR_HOST_URL" == "http://localhost:9000" ]]; then
  "$project_root/scripts/sonar-up.sh"
  if [[ "$sonar_token_source" == ".sonar.env" || -z "${SONAR_TOKEN:-}" ]]; then
    load_sonar_env true
  fi
fi

for report in test-executions.xml lcov.info clippy.json python-coverage.xml \
  shellcheck-issues.json zizmor.sarif; do
  if [[ ! -s "$project_root/target/sonar/$report" ]]; then
    echo "Sonar report $report is missing. Run ./scripts/sonar-reports.sh first." >&2
    exit 1
  fi
done

if [[ -z "${SONAR_TOKEN:-}" ]]; then
  read -r -s -p "Sonar token: " SONAR_TOKEN
  printf '\n'
  export SONAR_TOKEN
  sonar_token_source="interactive input"
fi

# Browser clipboard contents can occasionally include a carriage return.
SONAR_TOKEN="${SONAR_TOKEN//$'\r'/}"
if [[ -z "$SONAR_TOKEN" ]]; then
  echo "Sonar token is empty." >&2
  exit 1
fi
if [[ ! "$SONAR_TOKEN" =~ ^[[:alnum:]_.-]+$ ]]; then
  echo "Sonar token has an unexpected format." >&2
  exit 1
fi

echo "Using SONAR_TOKEN from $sonar_token_source (value hidden)."

scanner_host_url="${SONAR_HOST_URL/127.0.0.1/host.docker.internal}"
scanner_host_url="${scanner_host_url/localhost/host.docker.internal}"

token_status="$(bearer_http_status "$SONAR_TOKEN" "$SONAR_HOST_URL/api/v2/analysis/version")"
if [[ "$token_status" != "200" ]]; then
  echo "SonarQube rejected the token (HTTP $token_status). Create a new analysis token and try again." >&2
  exit 1
fi

snapshot_dir="$(mktemp -d)"

cleanup() {
  docker rm -f "$scanner_container" >/dev/null 2>&1 || true
  rm -rf "$snapshot_dir"
}
trap cleanup EXIT

# The text and secrets sensor applies sonar.text.inclusions (shell, SQL, conf)
# only inside a git repository, and the container runs as another uid.
docker create \
  --name "$scanner_container" \
  --platform linux/amd64 \
  --env SONAR_HOST_URL="$scanner_host_url" \
  --env SONAR_TOKEN \
  --env GIT_CONFIG_COUNT=1 \
  --env GIT_CONFIG_KEY_0=safe.directory \
  --env GIT_CONFIG_VALUE_0=/usr/src \
  --workdir /usr/src \
  "$scanner_image" >/dev/null

echo "Copying tracked project files and Sonar reports..."
# Only git-tracked paths enter the scanner. Untracked local secrets such as
# .env or report-credentials/ can never be indexed or uploaded.
# `git ls-files` lists the working-tree state, so uncommitted edits to tracked
# files are analysed as well.
# COPYFILE_DISABLE stops macOS tar from adding AppleDouble ._* entries, and
# --no-xattrs/--no-acls keep host metadata such as com.apple.provenance out of
# the stream; docker cp cannot apply it inside the Linux container.
(cd "$project_root" && git ls-files -z --cached \
  | while IFS= read -r -d '' path; do
      if [[ -f "$path" && ! -L "$path" ]]; then
        printf '%s\0' "$path"
      fi
    done \
  | COPYFILE_DISABLE=1 tar --create --no-xattrs --no-acls --null --files-from - --file -) \
  | tar --extract --file - --directory "$snapshot_dir"
# A throwaway one-commit repository for the text sensor. Blame stays disabled
# (sonar.scm.disabled), so this history never affects the new-code period.
# Hooks and signing from the operator's global git configuration are bypassed.
git -C "$snapshot_dir" -c init.defaultBranch=snapshot init --quiet
git -C "$snapshot_dir" add --all --force
git -C "$snapshot_dir" -c core.hooksPath=/dev/null \
  -c user.name=sonar-scan -c user.email=sonar-scan@localhost \
  commit --quiet --no-gpg-sign --no-verify --message "tracked tree snapshot"
COPYFILE_DISABLE=1 tar --create --no-xattrs --no-acls --directory "$snapshot_dir" --file - . \
  | docker cp - "$scanner_container:/usr/src" >/dev/null
docker cp "$project_root/target/sonar" "$scanner_container:/usr/src/reports" >/dev/null

docker start --attach "$scanner_container"
