#!/usr/bin/env python3
"""Service helpers for website/API callers based on modules package."""

from __future__ import annotations

from typing import Dict, List, Optional

from modules.registry import get_alias, get_module, list_aliases, list_entries


def list_module_aliases() -> List[str]:
    return list_aliases()


def list_module_entries() -> List[Dict[str, str]]:
    return list_entries()


def get_module_info(name_or_id: str) -> Dict[str, object]:
    module = get_module(name_or_id)
    info = module.info()
    alias = get_alias(module.module_id)
    return {
        "alias": alias,
        "name": module.name,
        "module_id": module.module_id,
        **info,
    }


def run_module(
    name_or_id: str,
    *,
    profile: str = "all",
    tasks: Optional[List[str]] = None,
    single_tasks: Optional[List[str]] = None,
    skip: Optional[List[str]] = None,
    dry_run: bool = True,
    stop_on_error: bool = False,
    timeout_seconds: int = 1800,
) -> Dict[str, object]:
    module = get_module(name_or_id)
    result = module.run(
        profile=profile,
        tasks=tasks or [],
        single_tasks=single_tasks or [],
        skip=skip or [],
        dry_run=dry_run,
        stop_on_error=stop_on_error,
        timeout_seconds=timeout_seconds,
    )
    result["alias"] = get_alias(module.module_id)
    result["name"] = module.name
    return result
