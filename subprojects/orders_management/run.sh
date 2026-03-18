#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

python3 subprojects/_shared/module_cli.py orders_management "${1:-list}" "${2:-}"
