#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

if [[ $# -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 64
fi

: "${MCP_ACCESS_CONFIG_HOST:?MCP_ACCESS_CONFIG_HOST is required}"
: "${DAILY_REPORT_MAIL_POLICY_HOST:?DAILY_REPORT_MAIL_POLICY_HOST is required}"
: "${REPORT_MAIL_ROUTING_HOST:?REPORT_MAIL_ROUTING_HOST is required}"
: "${REPORT_GMAIL_OAUTH_DIR_HOST:?REPORT_GMAIL_OAUTH_DIR_HOST is required}"

audiences=(
  ozon_diana_serafimovich
  ozon_yulia_rogova
  ozon_ekaterina_karpova
  ozon_yulia_laptova
  ozon_artem_skripov
  ozon_maksim_kremnev
  ozon_natalya_kazakova
)

# This makes exactly one bounded Gmail attempt. An ambiguous result stays in
# `sending` and the script stops; it never attempts another recipient.
./scripts/run-report-mail-canary.sh

# The canary worker chooses the oldest ready batch. Resolve its audience only
# through database-backed preflight; recipient addresses never enter output.
for audience_id in "${audiences[@]}"; do
  if REPORT_MAIL_CANARY_AUDIENCE_ID="$audience_id" \
    ./scripts/start-report-mail-scheduler.sh \
      --confirm-canary-sent-and-reconciled; then
    echo "Gmail canary verified; scheduled delivery started for all seven Ozon audiences" >&2
    exit 0
  fi
done

echo "no verified Gmail canary receipt matched the seven-audience policy; scheduled delivery remains disabled" >&2
exit 1
