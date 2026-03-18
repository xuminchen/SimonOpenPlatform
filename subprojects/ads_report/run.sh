#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

python3 subprojects/_shared/module_cli.py ads_report "${1:-list}" "${2:-}"
