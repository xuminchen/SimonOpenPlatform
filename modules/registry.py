#!/usr/bin/env python3
"""Registry of externally callable module packages."""

from __future__ import annotations

from typing import Dict, List

from modules._core import ToolModule

MODULE_BY_ALIAS: Dict[str, ToolModule] = {
    "orders_management": ToolModule(module_id="orders_management", name="Orders Management"),
    "ads_report": ToolModule(module_id="ads_report", name="Ads Report"),
}


MODULE_BY_ID: Dict[str, ToolModule] = {module.module_id: module for module in MODULE_BY_ALIAS.values()}
ALIAS_BY_ID: Dict[str, str] = {module.module_id: alias for alias, module in MODULE_BY_ALIAS.items()}


def list_aliases() -> List[str]:
    return list(MODULE_BY_ALIAS.keys())


def list_module_ids() -> List[str]:
    return list(MODULE_BY_ID.keys())


def get_alias(module_id: str) -> str:
    if module_id not in ALIAS_BY_ID:
        raise ValueError("Unknown module id: {0}".format(module_id))
    return ALIAS_BY_ID[module_id]


def list_entries() -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    for alias, module in MODULE_BY_ALIAS.items():
        entries.append(
            {
                "alias": alias,
                "module_id": module.module_id,
                "name": module.name,
            }
        )
    return entries


def get_module(name_or_id: str) -> ToolModule:
    if name_or_id in MODULE_BY_ALIAS:
        return MODULE_BY_ALIAS[name_or_id]
    if name_or_id in MODULE_BY_ID:
        return MODULE_BY_ID[name_or_id]
    raise ValueError("Unknown module alias or id: {0}".format(name_or_id))
