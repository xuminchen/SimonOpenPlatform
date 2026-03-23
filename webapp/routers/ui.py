from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse


router = APIRouter(tags=["ui"])
STATIC_DIR = Path(__file__).resolve().parents[1] / "static"


@router.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/ui")
def ui_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/dashboard")
def dashboard_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/iam")
def iam_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/appauth")
def appauth_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/platform/management")
def platform_management_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application/credentials")
def application_credentials_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application")
def application_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application/connection")
def application_connection_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application/connection/{connection_id}")
def application_connection_detail_entry(connection_id: int) -> FileResponse:
    _ = connection_id
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application/connection/create")
def application_connection_create_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application/transformation")
def application_transformation_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/application/destination")
def application_destination_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/apihub")
def apihub_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/apihub/builder")
def apihub_builder_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/monitor")
def monitor_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/settings")
def settings_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
