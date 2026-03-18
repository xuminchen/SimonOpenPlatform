# WonderLab Web Backend (Phase 1)

This is the Phase 1 backend scaffold for website-driven platform integrations.

## What is included
- FastAPI service skeleton
- Account config CRUD APIs
- Task table and task execution record
- Adapter interface and registry
- One live adapter example: `wechat_shop` order sync

## Run locally
```bash
pip install -r webapp/requirements.txt
uvicorn webapp.main:app --reload --host 0.0.0.0 --port 8000
```

## Web UI
- Console Home: `http://127.0.0.1:8000/`
- UI Alias: `http://127.0.0.1:8000/ui`
- Swagger Docs: `http://127.0.0.1:8000/docs`

### Frontend stack
- React + Vite + TailwindCSS
- Source folder: `frontend/`
- Build output folder: `webapp/static/` (served by FastAPI `/static`)

### Frontend develop/build
```bash
cd frontend
npm install
npm run lint
npm run build
```

After `npm run build`, open:
- `http://127.0.0.1:8000/`

## Environment variables
- `WONDERLAB_WEB_DB_URL` (optional)
  - default: sqlite file at `webapp_data/app.db`
- `WONDERLAB_WEB_SECRET_KEY` (optional)
  - used for account config field encryption in DB
- `API_CREDENTIALS_FILE` (optional)
  - default: `config/api_credentials.json`
  - used by "应用与凭证" JSON 同步接口读取底层 `app_id / secret_key / access_token`
  - web console create/update and token auto-refresh will also write back to this file (`webapp_accounts` namespace + provider mirror)
- `WONDERLAB_TOKEN_REFRESH_ENABLED` (optional)
  - default: `true`
  - enable background token refresh scheduler for `oceanengine / red_juguang / red_chengfeng`
- `WONDERLAB_TOKEN_REFRESH_INTERVAL_SECONDS` (optional)
  - default: `900` (15 minutes)
  - scheduler polling interval; minimum 60 seconds

## API endpoints
- `GET /api/v1/health`
- `POST /api/v1/accounts`
- `GET /api/v1/accounts`
- `GET /api/v1/accounts/{account_id}`
- `PATCH /api/v1/accounts/{account_id}`
- `POST /api/v1/accounts/{account_id}/disable`
- `GET /api/v1/accounts/{account_id}/credentials`
- `POST /api/v1/accounts/{account_id}/credentials/reset`
- `PUT /api/v1/accounts/{account_id}/ip-whitelist`
- `GET /api/v1/accounts/credentials/source`
- `POST /api/v1/accounts/credentials/source/sync`
- `POST /api/v1/tasks/wechat-orders`
- `POST /api/v1/tasks/wechat-orders/submit`
- `POST /api/v1/tasks/meta-report/submit`
- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`

## Quick start example
1. Create account (wechat)
```json
{
  "name": "WONDERLAB Wechat Shop",
  "platform": "wechat_shop",
  "status": "active",
  "config": {
    "app_id": "wx_xxx",
    "secret": "xxx"
  }
}
```

2. Trigger wechat order sync task
```json
{
  "account_id": 1,
  "start_date": "2026-03-17",
  "end_date": "2026-03-17",
  "time_type": "create_time",
  "page_size": 50
}
```

3. Submit async wechat task
```json
{
  "account_id": 1,
  "start_date": "2026-03-17",
  "end_date": "2026-03-17",
  "time_type": "create_time",
  "page_size": 50
}
```

4. Submit async meta report task (dry-run)
```json
{
  "account_id": 2,
  "start_date": "2026-03-10",
  "end_date": "2026-03-17",
  "level": "ad",
  "dry_run": true
}
```
