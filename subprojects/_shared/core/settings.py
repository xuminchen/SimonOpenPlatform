from __future__ import annotations

import os
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def get_required_env(name: str) -> str:
    value = get_env(name)
    if not value:
        raise ValueError("Missing required environment variable: {0}".format(name))
    return value


def get_int_env(name: str, default: int) -> int:
    raw = get_env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError("Environment variable {0} must be an integer, got: {1}".format(name, raw)) from exc
