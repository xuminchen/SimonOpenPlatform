# WonderLab API Project Framework (Optimized)

## Core Principles
- Keep business code and runtime artifacts separated.
- Keep subprojects as the canonical orchestration entry layer.
- Keep `modules` + `subprojects` as canonical callable layers.

## Recommended Structure
- `modules/`: external-facing module packages (stable import layer for services/scripts).
- `subprojects/`: module boundaries and unified runners.
- `subprojects/_shared/`: shared runner core, module registry, and shared CLI dispatcher.
- `subprojects/_shared/core/`: shared API foundation (http/auth/retry/pagination/observability/settings).
- `subprojects/gateway/`: callable service wrapper for all modules.
- `runtime/`: logs/cache/reports generated at runtime.
- `scripts/maintenance/`: cleanup and maintenance scripts.
- `ops/`: cron templates and operational runbooks.

## Full Repository Coverage
All top-level folders are now governed in one framework:
- `application`: `modules/`, `subprojects/`, `api_modules/`
- `infrastructure`: `query_mc_push_bi/`, `subprojects/_shared/core/`, `subprojects/_shared/db/`
- `ops`: `ops/`, `scripts/`, `runtime/`
- `domain`: all active API business directories mapped to subprojects `ads_report`, `orders_management`
- `archive`: legacy folders after migration completion

Directory governance reference:
- `subprojects/MODULE_ARCHITECTURE.md`

## Runtime Artifacts Policy
Generated artifacts should go to:
- `runtime/logs/`
- `runtime/cache/`
- `runtime/reports/`

Do not store generated artifacts in business source directories.

## Cleanup Standard
Use:
```bash
bash scripts/maintenance/cleanup_artifacts.sh --dry-run
bash scripts/maintenance/cleanup_artifacts.sh
```

This removes:
- `__pycache__/`
- `*.pyc`, `*.pyo`
- `*.log`, `*.out`
- `.DS_Store`
(excluding `.git` internals)

## Service Invocation Layer
- Service wrapper: `subprojects/gateway/module_gateway.py`
- Shared registry: `subprojects/_shared/module_registry.py`
- External Python callers should import from `modules/` packages first, then call `run_profile/run_task`.

## Configuration Layer
Each module keeps task config in:
- `subprojects/<module>/project/tasks.toml`

## Root Consolidation
Top-level active surface has been consolidated to:
- Active business/runtime: `subprojects/`, `ops/`, `scripts/`, `runtime/`
- Module runtime dependencies: directories referenced by `tasks.toml`

Validate all configs:
```bash
python3 subprojects/check_configs.py
```
