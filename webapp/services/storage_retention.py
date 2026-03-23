from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import threading

from webapp.config import get_storage_retention_settings_path, get_storage_root
from webapp.schemas import (
    StorageRetentionRunSummary,
    StorageRetentionSettingsUpdateRequest,
    StorageRetentionSettingsView,
)

_LOCK = threading.Lock()


@dataclass
class _RetentionSettings:
    enabled: bool = False
    retention_days: int = 30


def _settings_default() -> _RetentionSettings:
    return _RetentionSettings(enabled=False, retention_days=30)


def _settings_path() -> Path:
    return get_storage_retention_settings_path()


def _read_settings() -> _RetentionSettings:
    path = _settings_path()
    if not path.exists():
        return _settings_default()
    try:
        with path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return _settings_default()
    if not isinstance(payload, dict):
        return _settings_default()
    enabled = bool(payload.get("enabled", False))
    try:
        retention_days = int(payload.get("retention_days", 30))
    except (TypeError, ValueError):
        retention_days = 30
    retention_days = max(1, min(retention_days, 3650))
    return _RetentionSettings(enabled=enabled, retention_days=retention_days)


def _write_settings(settings: _RetentionSettings) -> None:
    path = _settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "enabled": bool(settings.enabled),
        "retention_days": int(settings.retention_days),
        "updated_at": datetime.utcnow().isoformat(),
    }
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def get_storage_retention_settings() -> StorageRetentionSettingsView:
    with _LOCK:
        settings = _read_settings()
    return StorageRetentionSettingsView(enabled=settings.enabled, retention_days=settings.retention_days)


def update_storage_retention_settings(request: StorageRetentionSettingsUpdateRequest) -> StorageRetentionSettingsView:
    settings = _RetentionSettings(enabled=bool(request.enabled), retention_days=int(request.retention_days))
    with _LOCK:
        _write_settings(settings)
    return StorageRetentionSettingsView(enabled=settings.enabled, retention_days=settings.retention_days)


def run_storage_retention_once(*, force: bool = False) -> StorageRetentionRunSummary:
    with _LOCK:
        settings = _read_settings()
    if not settings.enabled and not force:
        return StorageRetentionRunSummary(
            ok=True,
            enabled=False,
            retention_days=settings.retention_days,
            message="retention is disabled",
        )

    root = get_storage_root().resolve()
    base = (root / "destinations").resolve()
    if not base.exists():
        return StorageRetentionRunSummary(
            ok=True,
            enabled=settings.enabled,
            retention_days=settings.retention_days,
            message="destination storage path does not exist",
        )
    if not base.is_relative_to(root):
        return StorageRetentionRunSummary(
            ok=False,
            enabled=settings.enabled,
            retention_days=settings.retention_days,
            message="invalid destination storage path",
        )

    cutoff = datetime.now() - timedelta(days=settings.retention_days)
    scanned_files = 0
    deleted_files = 0
    deleted_dirs = 0

    for item in base.rglob("*"):
        if not item.is_file():
            continue
        scanned_files += 1
        try:
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
        except OSError:
            continue
        if mtime > cutoff:
            continue
        try:
            item.unlink(missing_ok=True)
            deleted_files += 1
        except OSError:
            continue

    for dir_path in sorted([p for p in base.rglob("*") if p.is_dir()], key=lambda x: len(x.parts), reverse=True):
        try:
            if any(dir_path.iterdir()):
                continue
            dir_path.rmdir()
            deleted_dirs += 1
        except OSError:
            continue

    return StorageRetentionRunSummary(
        ok=True,
        enabled=settings.enabled,
        retention_days=settings.retention_days,
        scanned_files=scanned_files,
        deleted_files=deleted_files,
        deleted_dirs=deleted_dirs,
        message="retention cleanup completed",
    )


def retention_interval_seconds() -> int:
    raw = os.environ.get("SIMON_STORAGE_RETENTION_INTERVAL_SECONDS", "3600")
    try:
        value = int(raw)
    except ValueError:
        value = 3600
    return max(value, 300)
