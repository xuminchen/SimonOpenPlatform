"""TikTok API aggregation facade."""

from __future__ import annotations

import importlib.util
import os
from types import ModuleType
from typing import Any

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_module(name: str, rel_path: str) -> ModuleType:
    path = os.path.join(_ROOT, rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load module: {0}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_tiktok_app() -> Any:
    mod = _load_module("tiktok_app_module", "subprojects/orders_management/tiktok_package/Tiktok.py")
    return mod.TiktokApp()


def create_tiktok_auth() -> Any:
    mod = _load_module("tiktok_auth_module", "subprojects/orders_management/tiktok_package/tiktok_auth.py")
    return mod.TiktokAuth()


__all__ = [
    "create_tiktok_app",
    "create_tiktok_auth",
]
