from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "webapp_data" / "app.db"


def get_database_url() -> str:
    configured = os.environ.get("WONDERLAB_WEB_DB_URL", "").strip()
    if configured:
        return configured
    return "sqlite:///{0}".format(DEFAULT_DB_PATH)


def get_secret_key() -> str:
    return os.environ.get("WONDERLAB_WEB_SECRET_KEY", "wonderlab-phase1-key")


def is_db_enabled() -> bool:
    raw = str(os.environ.get("WONDERLAB_WEB_DB_ENABLED", "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}
