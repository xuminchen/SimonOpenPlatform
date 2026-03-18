# Subprojects Workspace Guide

## Goal
Turn functional module boundaries into practical, independently runnable workspaces
without moving legacy source files yet.

## Layout
- `subprojects/<module>/README.md`: module scope and entry scripts.
- `subprojects/<module>/FILES.txt`: mapped legacy files.
- `subprojects/<module>/run.sh`: module-level runner wrapper.
- `subprojects/<module>/requirements.txt`: minimal dependency baseline for that module.
- `subprojects/_shared/unified_runner_core.py`: shared execution core used by all module runners.
- `subprojects/_shared/module_registry.py`: shared module registry and path helper.
- `subprojects/_shared/module_cli.py`: shared compatibility CLI behind all `run.sh`.
- `subprojects/_shared/core/`: shared API client foundation (http/auth/pagination/observability/settings).
- `subprojects/<module>/project/tasks.toml`: module task/profile config (editable without Python code changes).

## Usage
From repository root:

```bash
bash subprojects/orders_management/run.sh list
bash subprojects/orders_management/run.sh dry-run core
bash subprojects/orders_management/run.sh tiktokshop_orders
bash subprojects/orders_management/run.sh external
bash subprojects/orders_management/run.sh dry-run-task tiktokshop_orders

bash subprojects/ads_report/run.sh list
bash subprojects/ads_report/run.sh dry-run core

python3 subprojects/check_configs.py
python3 subprojects/check_configs.py --module orders_management
python3 subprojects/check_configs.py --module orders_management --module ads_report --format json
python3 subprojects/check_configs.py --list-modules
```

## Notes
- `run.sh` wrappers call existing legacy scripts; no business logic is duplicated.
- All module wrappers dispatch through `subprojects/_shared/module_cli.py`, so CLI behavior stays consistent.
- These wrappers are designed for gradual migration and safer ops handover.
- Dependency files are intentionally minimal baselines, not lock files.
- Each module provides cron examples in `subprojects/<module>/ops/cron_template.txt`.
- Full folder governance:
  - `subprojects/FOLDER_GOVERNANCE.md`
