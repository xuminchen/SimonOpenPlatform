#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

TARGET_FIND=(
  -type d -name '__pycache__' -o
  -type d -name '.pytest_cache' -o
  -type d -name '.mypy_cache' -o
  -type d -name '.ruff_cache' -o
  -type f -name '*.pyc' -o
  -type f -name '*.pyo' -o
  -type f -name '*.log' -o
  -type f -name '*.out' -o
  -type f -name '.DS_Store'
)

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] candidates (excluding .git):"
  find . \( -path './.git' -o -path './.git/*' \) -prune -o \( "${TARGET_FIND[@]}" \) -print | sed -n '1,300p'
  echo "[dry-run] total:"
  find . \( -path './.git' -o -path './.git/*' \) -prune -o \( "${TARGET_FIND[@]}" \) -print | wc -l
  exit 0
fi

# Delete directories first
find . \( -path './.git' -o -path './.git/*' \) -prune -o -type d \( -name '__pycache__' -o -name '.pytest_cache' -o -name '.mypy_cache' -o -name '.ruff_cache' \) -exec rm -rf {} +
# Delete files
find . \( -path './.git' -o -path './.git/*' \) -prune -o -type f \( -name '*.pyc' -o -name '*.pyo' -o -name '*.log' -o -name '*.out' -o -name '.DS_Store' \) -delete

echo "cleanup finished"
