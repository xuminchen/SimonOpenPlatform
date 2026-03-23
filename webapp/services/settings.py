from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import threading

from webapp.config import DEFAULT_DB_PATH, get_app_settings_path
from webapp.db import DATABASE_URL, DB_ENABLED
from webapp.schemas import AppSettingsUpdateRequest, AppSettingsView

_LOCK = threading.Lock()


def _default_database_url() -> str:
    return "sqlite:///{0}".format(DEFAULT_DB_PATH)


def _read_settings(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_settings(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def _resolve_db_enabled() -> tuple[bool, str]:
    env_raw = os.environ.get("WONDERLAB_WEB_DB_ENABLED")
    if env_raw is not None:
        raw = str(env_raw).strip().lower()
        return (raw in {"1", "true", "yes", "on"}, "env")

    path = get_app_settings_path()
    settings = _read_settings(path)
    raw = str(settings.get("db_enabled", "false")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True, "settings"
    if raw in {"0", "false", "no", "off"}:
        return False, "settings"
    return False, "default"


def _resolve_database_url() -> tuple[str, str]:
    env_raw = str(os.environ.get("WONDERLAB_WEB_DB_URL", "")).strip()
    if env_raw:
        return env_raw, "env"

    path = get_app_settings_path()
    settings = _read_settings(path)
    file_url = str(settings.get("database_url", "")).strip()
    if file_url:
        return file_url, "settings"

    return _default_database_url(), "default"


def get_app_settings() -> AppSettingsView:
    db_enabled_next, db_enabled_source = _resolve_db_enabled()
    database_url_next, database_url_source = _resolve_database_url()
    return AppSettingsView(
        db_enabled_runtime=bool(DB_ENABLED),
        db_enabled_next=db_enabled_next,
        database_url_runtime=str(DATABASE_URL or ""),
        database_url_next=database_url_next,
        db_enabled_source=db_enabled_source,
        database_url_source=database_url_source,
        restart_required=True,
    )


def update_app_settings(request: AppSettingsUpdateRequest) -> AppSettingsView:
    path = get_app_settings_path()
    with _LOCK:
        payload = _read_settings(path)
        payload["db_enabled"] = bool(request.db_enabled)
        next_url = str(request.database_url or "").strip()
        payload["database_url"] = next_url
        payload["updated_at"] = datetime.utcnow().isoformat()
        _write_settings(path, payload)
    return get_app_settings()
