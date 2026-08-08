#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scanner_image="sonarsource/sonar-scanner-cli:12.1.0.3233_8.0.1@sha256:23ca0f137965d9dff2198074043fd48d386280bc5d0ccac8c8349cea4cf096a9"
scanner_container="mcp-ozon-sonar-scan-$$"
sonar_env_file="$project_root/.sonar.env"
sonar_token_source="environment"

if [[ -f "$sonar_env_file" ]]; then
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    value="${value%$'\r'}"
    case "$key" in
      SONAR_HOST_URL)
        if [[ -z "${SONAR_HOST_URL:-}" ]]; then
          export SONAR_HOST_URL="$value"
        fi
        ;;
      SONAR_TOKEN)
        if [[ -z "${SONAR_TOKEN:-}" ]]; then
          export SONAR_TOKEN="$value"
          sonar_token_source=".sonar.env"
        fi
        ;;
      ''|'#'*) ;;
      *) echo "Ignoring unsupported variable '$key' in .sonar.env." >&2 ;;
    esac
  done < "$sonar_env_file"
fi

SONAR_HOST_URL="${SONAR_HOST_URL:-http://127.0.0.1:9000}"

if [[ "$SONAR_HOST_URL" == "http://127.0.0.1:9000" ]] \
  || [[ "$SONAR_HOST_URL" == "http://localhost:9000" ]]; then
  "$project_root/scripts/sonar-up.sh"
  if [[ "$sonar_token_source" == ".sonar.env" ]]; then
    while IFS='=' read -r key value || [[ -n "$key" ]]; do
      value="${value%$'\r'}"
      if [[ "$key" == "SONAR_TOKEN" ]]; then
        export SONAR_TOKEN="$value"
      fi
    done < "$sonar_env_file"
  fi
fi

if [[ ! -s "$project_root/target/sonar/test-executions.xml" ]] \
  || [[ ! -s "$project_root/target/sonar/lcov.info" ]] \
  || [[ ! -s "$project_root/target/sonar/clippy.json" ]]; then
  echo "Sonar reports are missing. Run ./scripts/sonar-reports.sh first." >&2
  exit 1
fi

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

echo "Using SONAR_TOKEN from $sonar_token_source (value hidden)."

scanner_host_url="${SONAR_HOST_URL/127.0.0.1/host.docker.internal}"
scanner_host_url="${scanner_host_url/localhost/host.docker.internal}"

token_status="$(
  curl --silent --show-error \
    --output /dev/null \
    --write-out '%{http_code}' \
    --header "Authorization: Bearer $SONAR_TOKEN" \
    "$SONAR_HOST_URL/api/v2/analysis/version"
)"
if [[ "$token_status" != "200" ]]; then
  echo "SonarQube rejected the token (HTTP $token_status). Create a new analysis token and try again." >&2
  exit 1
fi

cleanup() {
  docker rm -f "$scanner_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker create \
  --name "$scanner_container" \
  --platform linux/amd64 \
  --env SONAR_HOST_URL="$scanner_host_url" \
  --env SONAR_TOKEN \
  --workdir /usr/src \
  "$scanner_image" \
  -Dsonar.scm.disabled=true \
  -Dsonar.rust.clippyReport.reportPaths=reports/clippy.json \
  -Dsonar.rust.lcov.reportPaths=reports/lcov.info \
  -Dsonar.testExecutionReportPaths=reports/test-executions.xml >/dev/null

echo "Copying project files and Sonar reports..."
docker cp "$project_root/Cargo.toml" "$scanner_container:/usr/src/Cargo.toml" >/dev/null
docker cp "$project_root/Cargo.lock" "$scanner_container:/usr/src/Cargo.lock" >/dev/null
docker cp "$project_root/sonar-project.properties" "$scanner_container:/usr/src/sonar-project.properties" >/dev/null
docker cp "$project_root/src" "$scanner_container:/usr/src/src" >/dev/null
docker cp "$project_root/tests" "$scanner_container:/usr/src/tests" >/dev/null
docker cp "$project_root/target/sonar" "$scanner_container:/usr/src/reports" >/dev/null

docker start --attach "$scanner_container"
