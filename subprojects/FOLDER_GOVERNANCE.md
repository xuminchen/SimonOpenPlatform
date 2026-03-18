# Full Repository Folder Governance

## Goal
Keep only active API modules and shared infrastructure layers.

## Layer Mapping
- `application`: `subprojects`, `modules`, `api_modules`
- `infrastructure`: `subprojects/_shared/core`, `subprojects/_shared/db`
- `utility`: `scripts`
- `domain`:
  - `ads_report`: `subprojects/ads_report/*`
  - `orders_management`: `subprojects/orders_management/*`
- `archive`: removed legacy/non-API domains

## Governance Actions
1. New business code must belong to `ads_report` or `orders_management`.
2. HTTP/API request logic should use `subprojects/_shared/core/http_client.py` or `api_modules/common.py`.
3. DB access should use `subprojects/_shared/db`.
4. Module ownership must be reflected in `subprojects/<module>/FILES.txt` and `tasks.toml`.
