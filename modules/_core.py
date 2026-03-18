#!/usr/bin/env python3
"""Public module wrappers for external callers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from subprojects.gateway.module_gateway import RunRequest, get_module_info, run_module


@dataclass(frozen=True)
class ToolModule:
    """Small facade around one subproject module id."""

    module_id: str
    name: str

    def info(self) -> Dict[str, object]:
        return get_module_info(self.module_id)

    def list_profiles(self) -> List[str]:
        info = self.info()
        return list(info["profiles"].keys())

    def list_tasks(self) -> List[str]:
        info = self.info()
        return list(info["tasks"].keys())

    def run(
        self,
        *,
        profile: str = "all",
        tasks: Optional[List[str]] = None,
        single_tasks: Optional[List[str]] = None,
        skip: Optional[List[str]] = None,
        dry_run: bool = True,
        stop_on_error: bool = False,
        timeout_seconds: int = 1800,
    ) -> Dict[str, object]:
        request = RunRequest(
            profile=profile,
            tasks=tasks or [],
            single_tasks=single_tasks or [],
            skip=skip or [],
            dry_run=dry_run,
            stop_on_error=stop_on_error,
            timeout_seconds=timeout_seconds,
        )
        return run_module(self.module_id, request)

    def run_profile(self, profile: str = "all", *, dry_run: bool = True, timeout_seconds: int = 1800) -> Dict[str, object]:
        return self.run(profile=profile, dry_run=dry_run, timeout_seconds=timeout_seconds)

    def run_task(self, task_id: str, *, dry_run: bool = True, timeout_seconds: int = 1800) -> Dict[str, object]:
        return self.run(single_tasks=[task_id], dry_run=dry_run, timeout_seconds=timeout_seconds)
