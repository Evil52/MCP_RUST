#!/usr/bin/env bash

keycloak_load_env_file() {
  local env_path="$1"
  local line
  local line_number=0
  local name
  local value
  local seen_names=' '

  while IFS= read -r line || [[ -n "$line" ]]; do
    line_number=$((line_number + 1))
    if [[ -z "$line" || "$line" == \#* ]]; then
      continue
    fi
    if [[ "$line" != *=* ]]; then
      echo "Invalid Keycloak env entry at line $line_number" >&2
      return 1
    fi
    name="${line%%=*}"
    value="${line#*=}"
    case "$name" in
      KEYCLOAK_DB_NAME | \
        KEYCLOAK_DB_USER | \
        KEYCLOAK_DB_PASSWORD | \
        KEYCLOAK_ADMIN_USER | \
        KEYCLOAK_ADMIN_PASSWORD | \
        KEYCLOAK_TEST_USER_PASSWORD) ;;
      *)
        echo "Unexpected variable in Keycloak env at line $line_number" >&2
        return 1
        ;;
    esac
    if [[ "$seen_names" == *" $name "* ]]; then
      echo "Duplicate variable in Keycloak env at line $line_number" >&2
      return 1
    fi
    if [[ -z "$value" || ${#value} -gt 512 || ! "$value" =~ ^[A-Za-z0-9._~:/@%+,=-]+$ ]]; then
      echo "Unsafe or empty value in Keycloak env at line $line_number" >&2
      return 1
    fi
    printf -v "$name" '%s' "$value"
    seen_names+="$name "
  done <"$env_path"
}
