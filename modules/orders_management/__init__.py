#!/usr/bin/env python3
"""External wrapper package for orders_management."""

from __future__ import annotations

from modules.registry import get_module

module = get_module("orders_management")

info = module.info
list_profiles = module.list_profiles
list_tasks = module.list_tasks
run = module.run
run_profile = module.run_profile
run_task = module.run_task

__all__ = [
    "module",
    "info",
    "list_profiles",
    "list_tasks",
    "run",
    "run_profile",
    "run_task",
]
