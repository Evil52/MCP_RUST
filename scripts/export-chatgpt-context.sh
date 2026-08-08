#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_path="${1:-$project_root/target/chatgpt/MCP_OZON_PROJECT_CONTEXT.md}"

mkdir -p "$(dirname "$output_path")"

files=(
  .dockerignore
  .env.example
  .gitignore
  .keycloak.env.example
  .sonar.env.example
  .sonar-stack.env.example
  Cargo.lock
  Cargo.toml
  Dockerfile
  Dockerfile.keycloak
  README.md
  SECURITY.md
  compose.auth.yaml
  compose.dev.yaml
  compose.yaml
  config/access.example.json
  config/keycloak/ofk-realm.json
  deny.toml
  methods.json
  sonar-project.properties
  compose.sonar.yaml
  .github/dependabot.yml
  .github/workflows/ci.yml
  .github/workflows/codeql.yml
  .github/workflows/dependency-review.yml
  scripts/local-ci.sh
  scripts/sonar-reports.sh
  scripts/sonar-bootstrap.sh
  scripts/sonar-scan.sh
  scripts/sonar-up.sh
  src/auth.rs
  src/config.rs
  src/lib.rs
  src/main.rs
  src/ozon.rs
  src/server.rs
  src/test_support.rs
  tests/sonar.rs
)

{
  printf '# MCP_OZON project snapshot\n\n'
  printf 'Generated from the local project. Secret files and build artifacts are intentionally excluded.\n'

  for relative_path in "${files[@]}"; do
    source_path="$project_root/$relative_path"
    if [[ ! -f "$source_path" ]]; then
      printf '\n> Missing expected file: `%s`\n' "$relative_path"
      continue
    fi

    printf '\n## File: `%s`\n\n````text\n' "$relative_path"
    sed -e 's/\r$//' "$source_path"
    printf '\n````\n'
  done
} >"$output_path"

printf 'ChatGPT project snapshot created: %s\n' "$output_path"
