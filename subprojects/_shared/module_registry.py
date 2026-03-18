#!/usr/bin/env python3
"""Shared module registry and path helpers."""

from __future__ import annotations

import os
from typing import Dict, List

MODULES: List[str] = [
    "orders_management",
    "ads_report",
]


def list_modules() -> List[str]:
    return list(MODULES)


def validate_module(module: str) -> None:
    if module not in MODULES:
        raise ValueError("Unknown module: {0}".format(module))


def module_paths(repo_root: str, module: str) -> Dict[str, str]:
    validate_module(module)
    base = os.path.join(repo_root, "subprojects", module)
    return {
        "base": base,
        "config": os.path.join(base, "project", "tasks.toml"),
        "runner": os.path.join(base, "project", "unified_runner.py"),
    }
