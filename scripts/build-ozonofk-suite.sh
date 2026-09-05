#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
mode="${1:---build}"
if [[ $# -gt 1 || ( "$mode" != --build && "$mode" != --check ) ]]; then
  echo "usage: $0 [--build|--check]" >&2
  exit 64
fi
suite=plugins/ozonofk-suite
sources=(
  skills/ozon-daily-manager-report/SKILL.md
  skills/ozon-daily-manager-report/references/data-quality.md
  skills/ozon-daily-manager-report/references/report-contract.md
  skills/ozon-daily-manager-report/references/invocation-prompt.md
  skills/ozon-daily-manager-report/references/spreadsheet-schema.md
  skills/ozonofk-marketplace-analytics/SKILL.md
)
# Reject links before copying or comparing: the archive must contain only
# local regular files, never references to credentials outside the package.
if [[ -L "$suite" ]] || [[ -n "$(find "$suite" -type l -print -quit)" ]]; then
  echo "suite must not contain symlinks" >&2
  exit 1
fi
for source in "${sources[@]}"; do
  if [[ ! -f "$source" || -L "$source" ]]; then
    echo "missing or unsafe suite source: $source" >&2
    exit 1
  fi
  if [[ "$mode" == --check ]]; then
    cmp "$source" "$suite/$source"
  else
    mkdir -p "$(dirname "$suite/$source")"
    cp "$source" "$suite/$source"
  fi
done
while IFS= read -r -d '' file; do
  relative="${file#"$suite/"}"
  case "$relative" in
    .codex-plugin/plugin.json|README.md|evals/scenarios.json) continue ;;
  esac
  allowed=false
  for source in "${sources[@]}"; do
    if [[ "$source" == "$relative" ]]; then allowed=true; break; fi
  done
  if [[ "$allowed" != true ]]; then
    echo "unexpected suite file: $relative" >&2
    exit 1
  fi
done < <(find "$suite" -type f -print0)
echo "OzonOFK Suite source contract passed ($mode)"
