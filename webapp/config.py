from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "webapp_data" / "app.db"
DEFAULT_STORAGE_ROOT = ROOT_DIR / "storage"
DEFAULT_RETENTION_SETTINGS_PATH = ROOT_DIR / "webapp_data" / "storage_retention.json"
DEFAULT_APP_SETTINGS_PATH = ROOT_DIR / "webapp_data" / "app_settings.json"


def get_app_settings_path() -> Path:
    configured = os.environ.get("SIMON_APP_SETTINGS_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_APP_SETTINGS_PATH


def _read_app_settings() -> dict:
    path = get_app_settings_path()
    if not path.exists():
        return {}
    try:
        import json

        with path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def get_database_url() -> str:
    configured = os.environ.get("WONDERLAB_WEB_DB_URL", "").strip()
    if configured:
        return configured
    file_config = str(_read_app_settings().get("database_url", "")).strip()
    if file_config:
        return file_config
    return "sqlite:///{0}".format(DEFAULT_DB_PATH)


def get_secret_key() -> str:
    return os.environ.get("WONDERLAB_WEB_SECRET_KEY", "wonderlab-phase1-key")


def is_db_enabled() -> bool:
    env_raw = os.environ.get("WONDERLAB_WEB_DB_ENABLED")
    if env_raw is not None:
        raw = str(env_raw).strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
    file_raw = str(_read_app_settings().get("db_enabled", "false")).strip().lower()
    return file_raw in {"1", "true", "yes", "on"}


def get_storage_root() -> Path:
    configured = os.environ.get("SIMON_DATA_STORAGE_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_STORAGE_ROOT


def get_storage_retention_settings_path() -> Path:
    configured = os.environ.get("SIMON_STORAGE_RETENTION_SETTINGS_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_RETENTION_SETTINGS_PATH
