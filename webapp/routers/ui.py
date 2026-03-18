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


@router.get("/apihub")
def apihub_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/monitor")
def monitor_entry() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
