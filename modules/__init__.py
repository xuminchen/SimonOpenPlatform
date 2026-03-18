#!/usr/bin/env python3
"""Unified external module package entry."""

from __future__ import annotations

from modules._core import ToolModule
from modules import ads_report, orders_management
from modules.registry import get_module, list_aliases, list_module_ids
from modules.service import get_module_info, list_module_aliases, list_module_entries, run_module

__all__ = [
    "ToolModule",
    "get_module",
    "list_aliases",
    "list_module_ids",
    "list_module_aliases",
    "list_module_entries",
    "get_module_info",
    "run_module",
    "orders_management",
    "ads_report",
]
