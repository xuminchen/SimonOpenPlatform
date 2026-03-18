#!/usr/bin/env python3
"""Service layer to expose subproject tool modules as callable APIs."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from subprojects._shared.module_registry import list_modules as shared_list_modules
from subprojects._shared.module_registry import module_paths, validate_module
from subprojects._shared.unified_runner_core import load_runner_config


@dataclass
class RunRequest:
    profile: str = "all"
    tasks: Optional[List[str]] = None
    single_tasks: Optional[List[str]] = None
    skip: Optional[List[str]] = None
    dry_run: bool = True
    stop_on_error: bool = False
    timeout_seconds: int = 1800


def _normalize_unique(items: Optional[List[str]]) -> List[str]:
    if not items:
        return []
    result: List[str] = []
    seen = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _normalized_request_fields(request: RunRequest) -> Tuple[List[str], List[str], List[str]]:
    tasks = _normalize_unique(request.tasks)
    single_tasks = _normalize_unique(request.single_tasks)
    skip = _normalize_unique(request.skip)
    return tasks, single_tasks, skip


@lru_cache(maxsize=64)
def _cached_config_for_mtime(module: str, config_mtime: float):
    del config_mtime
    paths = module_paths(REPO_ROOT, module)
    return load_runner_config(paths["config"])


def _load_module_config(module: str):
    paths = module_paths(REPO_ROOT, module)
    config_mtime = os.path.getmtime(paths["config"])
    return _cached_config_for_mtime(module, config_mtime)


def list_modules() -> List[str]:
    return shared_list_modules()


def get_module_info(module: str) -> Dict[str, object]:
    validate_module(module)
    paths = module_paths(REPO_ROOT, module)
    tasks, profiles = _load_module_config(module)

    return {
        "module": module,
        "runner": os.path.relpath(paths["runner"], REPO_ROOT),
        "config": os.path.relpath(paths["config"], REPO_ROOT),
        "task_count": len(tasks),
        "profile_count": len(profiles),
        "tasks": tasks,
        "profiles": profiles,
    }


def _build_command(module: str, request: RunRequest) -> List[str]:
    paths = module_paths(REPO_ROOT, module)
    cmd = [sys.executable, paths["runner"]]
    tasks, single_tasks, skip = _normalized_request_fields(request)

    if single_tasks:
        for task_id in single_tasks:
            cmd.extend(["--single-task", task_id])
    else:
        cmd.extend(["--profile", request.profile])

    for task_id in tasks:
        cmd.extend(["--task", task_id])

    for task_id in skip:
        cmd.extend(["--skip", task_id])

    if request.dry_run:
        cmd.append("--dry-run")

    if request.stop_on_error:
        cmd.append("--stop-on-error")

    return cmd


def run_module(module: str, request: RunRequest) -> Dict[str, object]:
    validate_module(module)

    info = get_module_info(module)
    tasks = info["tasks"]
    profiles = info["profiles"]

    tasks_list, single_tasks_list, skip_list = _normalized_request_fields(request)

    if request.timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")

    if (not single_tasks_list) and request.profile not in profiles:
        raise ValueError("Unknown profile for module {0}: {1}".format(module, request.profile))

    unknown_tasks = [task_id for task_id in tasks_list if task_id not in tasks]
    if unknown_tasks:
        raise ValueError("Unknown task ids for module {0}: {1}".format(module, ", ".join(unknown_tasks)))

    unknown_single_tasks = [task_id for task_id in single_tasks_list if task_id not in tasks]
    if unknown_single_tasks:
        raise ValueError("Unknown single_tasks for module {0}: {1}".format(module, ", ".join(unknown_single_tasks)))

    unknown_skip_tasks = [task_id for task_id in skip_list if task_id not in tasks]
    if unknown_skip_tasks:
        raise ValueError("Unknown skip task ids for module {0}: {1}".format(module, ", ".join(unknown_skip_tasks)))

    cmd = _build_command(module, request)

    start = time.time()
    timed_out = False
    timeout_error = ""
    try:
        completed = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=request.timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        timeout_error = "Command timed out after {0} seconds".format(request.timeout_seconds)
        completed = subprocess.CompletedProcess(
            args=cmd,
            returncode=124,
            stdout=exc.stdout or "",
            stderr=(exc.stderr or "") + ("\n" if exc.stderr else "") + timeout_error,
        )
    duration = time.time() - start

    return {
        "module": module,
        "ok": (completed.returncode == 0) and (not timed_out),
        "return_code": completed.returncode,
        "duration_seconds": round(duration, 3),
        "command": cmd,
        "dry_run": request.dry_run,
        "timed_out": timed_out,
        "timeout_seconds": request.timeout_seconds,
        "error": timeout_error,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
