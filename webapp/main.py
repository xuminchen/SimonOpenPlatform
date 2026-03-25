from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from webapp.db import Base, DB_ENABLED, engine, ensure_schema_upgrade
from webapp.routers.accounts import router as accounts_router
from webapp.routers.alerts import router as alerts_router
from webapp.routers.builder import router as builder_router
from webapp.routers.connections import router as connections_router
from webapp.routers.destinations import router as destinations_router
from webapp.routers.health import router as health_router
from webapp.routers.platform_configs import router as platform_configs_router
from webapp.routers.settings import router as settings_router
from webapp.routers.tasks import router as tasks_router
from webapp.routers.ui import router as ui_router
from webapp.services.storage_retention_scheduler import (
    start_storage_retention_scheduler,
    stop_storage_retention_scheduler,
)
from webapp.services.connections import shutdown_connections_executor
from webapp.services.task_runner import shutdown_runner
from webapp.services.token_scheduler import start_token_scheduler, stop_token_scheduler


app = FastAPI(title="SimonOpenPlatform", version="0.1.0")
STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.on_event("startup")
def _startup() -> None:
    if DB_ENABLED and engine is not None:
        Base.metadata.create_all(bind=engine)
        ensure_schema_upgrade()
        start_token_scheduler()
    start_storage_retention_scheduler()


@app.on_event("shutdown")
def _shutdown() -> None:
    stop_storage_retention_scheduler()
    if DB_ENABLED:
        stop_token_scheduler()
    shutdown_connections_executor(wait=False)
    shutdown_runner()


app.include_router(health_router, prefix="/api/v1")
app.include_router(accounts_router, prefix="/api/v1")
app.include_router(alerts_router, prefix="/api/v1")
app.include_router(platform_configs_router, prefix="/api/v1")
app.include_router(settings_router, prefix="/api/v1")
app.include_router(connections_router, prefix="/api/v1")
app.include_router(destinations_router, prefix="/api/v1")
app.include_router(builder_router, prefix="/api/v1")
app.include_router(tasks_router, prefix="/api/v1")
app.include_router(ui_router)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
