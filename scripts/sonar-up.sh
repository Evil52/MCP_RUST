#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/compose.sonar.yaml"
stack_env="$project_root/.sonar-stack.env"
sonar_url="http://127.0.0.1:9000"

if [[ ! -f "$stack_env" ]]; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate the local SonarQube database password." >&2
    exit 1
  fi
  password="$(openssl rand -hex 32)"
  previous_umask="$(umask)"
  umask 077
  printf 'SONAR_DB_USER=sonar\nSONAR_DB_NAME=sonar\nSONAR_DB_PASSWORD=%s\n' \
    "$password" > "$stack_env"
  umask "$previous_umask"
  echo "Created private SonarQube stack credentials in .sonar-stack.env."
fi

docker compose \
  --env-file "$stack_env" \
  --file "$compose_file" \
  up --detach postgres sonarqube

echo "Waiting for MCP_OZON SonarQube at $sonar_url ..."
deadline=$((SECONDS + 300))
while true; do
  status="$(curl --fail --silent --show-error "$sonar_url/api/system/status" 2>/dev/null || true)"
  if [[ "$status" == *'"status":"UP"'* ]]; then
    echo "MCP_OZON SonarQube is ready: $sonar_url"
    break
  fi
  if ((SECONDS >= deadline)); then
    echo "MCP_OZON SonarQube did not become ready within 300 seconds." >&2
    docker compose \
      --env-file "$stack_env" \
      --file "$compose_file" \
      logs --tail 100 sonarqube >&2
    exit 1
  fi
  sleep 5
done

"$project_root/scripts/sonar-bootstrap.sh"
