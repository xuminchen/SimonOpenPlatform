# subprojects/_shared

Shared runtime utilities for module-level wrappers.

## Files
- `unified_runner_core.py`: common list/profile/task/skip/dry-run/stop-on-error execution logic.
- `module_registry.py`: single source of truth for module ids and path resolution.
- `module_cli.py`: compatibility dispatcher used by `subprojects/*/run.sh`.
- `core/`: shared API/runtime foundation for all modules.
  - `core/http_client.py`: unified timeout/retry/request-result wrapper.
  - `core/auth/`: reusable auth header providers.
  - `core/pagination.py`: common pagination helpers.
  - `core/observability.py`: structured request logging helpers.
  - `core/settings.py`: environment-based secrets/config loading.
  - `core/api_credentials.py`: centralized JSON credential loader.

## Purpose
Keep `subprojects/*/project/unified_runner.py` focused on module task configuration
while centralizing runner behavior in one place.

## Config Convention
- Each module stores runner config in `subprojects/<module>/project/tasks.toml`.
- Core validates:
  - `tasks.<task_id>.script/group/description`
  - `profiles.<profile_name> = [task_id, ...]`
  - mandatory `profiles.all`
- Batch validation command:
  - `python3 subprojects/check_configs.py`
  - `python3 subprojects/check_configs.py --module orders_management`
  - `python3 subprojects/check_configs.py --format json`
  - `python3 subprojects/check_configs.py --list-modules`

## Unified Run Entry
- Every module wrapper calls shared CLI:
  - `bash subprojects/<module>/run.sh list`
  - `bash subprojects/<module>/run.sh dry-run <profile>`
  - `bash subprojects/<module>/run.sh dry-run-task <task_id>`
  - `bash subprojects/<module>/run.sh <profile>`
  - `bash subprojects/<module>/run.sh <task_id>`

`<task_id>` mode is internally mapped to `--single-task`, which bypasses profile preset and runs only that task.

## API Credentials
- Local credential file path: `config/api_credentials.json` (ignored by git).
- Example template: `config/api_credentials.example.json`.
- Optional override:
  - set environment variable `API_CREDENTIALS_FILE=/abs/path/to/credentials.json`
- Python usage:
  - `from subprojects._shared.core.api_credentials import get_credentials`
  - `get_credentials("wechat_shop", "shops", required=True)`
