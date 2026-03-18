#!/usr/bin/env python3
"""Shared core for subproject unified runners."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import tomllib
from typing import Dict, List, Tuple

TaskMap = Dict[str, Dict[str, str]]
ProfileMap = Dict[str, List[str]]


def _validate_task_map(tasks: TaskMap) -> None:
    required_keys = {"script", "group", "description"}
    for task_id, cfg in tasks.items():
        if not isinstance(task_id, str) or not task_id:
            raise ValueError("Invalid task id in config")
        if not isinstance(cfg, dict):
            raise ValueError("Task config for {0} must be a dict".format(task_id))
        missing = required_keys - set(cfg.keys())
        if missing:
            raise ValueError("Task {0} missing fields: {1}".format(task_id, ", ".join(sorted(missing))))
        for k in required_keys:
            if not isinstance(cfg[k], str) or not cfg[k]:
                raise ValueError("Task {0} field {1} must be non-empty string".format(task_id, k))


def _validate_profile_map(profiles: ProfileMap, tasks: TaskMap) -> None:
    if "all" not in profiles:
        raise ValueError("profiles.all is required")
    for profile, task_ids in profiles.items():
        if not isinstance(profile, str) or not profile:
            raise ValueError("Invalid profile name")
        if not isinstance(task_ids, list):
            raise ValueError("Profile {0} must be a list".format(profile))
        for task_id in task_ids:
            if task_id not in tasks:
                raise ValueError("Profile {0} references unknown task: {1}".format(profile, task_id))


def load_runner_config(config_path: str) -> Tuple[TaskMap, ProfileMap]:
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config not found: {0}".format(config_path))

    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    tasks = data.get("tasks", {})
    profiles = data.get("profiles", {})

    if not isinstance(tasks, dict) or not tasks:
        raise ValueError("tasks section is required and cannot be empty")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError("profiles section is required and cannot be empty")

    _validate_task_map(tasks)
    _validate_profile_map(profiles, tasks)
    return tasks, profiles


def list_tasks(tasks: TaskMap, profiles: ProfileMap) -> None:
    print("Available profiles:")
    for profile, task_ids in profiles.items():
        print("  - {0}: {1} tasks".format(profile, len(task_ids)))
    print("")
    print("Available tasks:")
    for task_id, cfg in tasks.items():
        print("  - {0}".format(task_id))
        print("    group: {0}".format(cfg["group"]))
        print("    script: {0}".format(cfg["script"]))
        print("    desc: {0}".format(cfg["description"]))


def unique_preserve_order(items: List[str]) -> List[str]:
    result = []
    seen = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _validate_task_ids(task_ids: List[str], tasks: TaskMap) -> None:
    for task_id in task_ids:
        if task_id not in tasks:
            raise ValueError("Unknown task id: {0}".format(task_id))


def build_run_list(profile: str, include_tasks: List[str], skip_tasks: List[str], tasks: TaskMap, profiles: ProfileMap) -> List[str]:
    if profile not in profiles:
        raise ValueError("Unknown profile: {0}".format(profile))

    run_list = list(profiles[profile])
    if include_tasks:
        run_list.extend(include_tasks)

    run_list = unique_preserve_order(run_list)

    _validate_task_ids(run_list, tasks)

    if skip_tasks:
        skip_set = set(skip_tasks)
        run_list = [task_id for task_id in run_list if task_id not in skip_set]

    return run_list


def run_task(task_id: str, tasks: TaskMap, repo_root: str, dry_run: bool = False):
    cfg = tasks[task_id]
    script_rel = cfg["script"]
    script_abs = os.path.join(repo_root, script_rel)

    if not os.path.exists(script_abs):
        return {
            "task_id": task_id,
            "script": script_rel,
            "ok": False,
            "code": 127,
            "duration": 0.0,
            "message": "script not found",
        }

    cmd = [sys.executable, script_abs]
    print("")
    print("[RUN] {0}".format(task_id))
    print("      script: {0}".format(script_rel))
    print("      cmd: {0}".format(" ".join(cmd)))

    if dry_run:
        return {
            "task_id": task_id,
            "script": script_rel,
            "ok": True,
            "code": 0,
            "duration": 0.0,
            "message": "dry-run",
        }

    # Ensure subproject-local packages remain importable after directory consolidation.
    env = os.environ.copy()
    extra_paths = [repo_root]
    script_rel_parts = script_rel.split("/")
    if len(script_rel_parts) >= 3 and script_rel_parts[0] == "subprojects":
        module_root = os.path.join(repo_root, script_rel_parts[0], script_rel_parts[1])
        extra_paths.append(module_root)
    current_pythonpath = env.get("PYTHONPATH")
    if current_pythonpath:
        extra_paths.append(current_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(extra_paths)

    start = time.perf_counter()
    completed = subprocess.run(cmd, cwd=repo_root, env=env)
    duration = time.perf_counter() - start
    ok = completed.returncode == 0

    return {
        "task_id": task_id,
        "script": script_rel,
        "ok": ok,
        "code": completed.returncode,
        "duration": duration,
        "message": "ok" if ok else "failed",
    }


def run_pipeline(*, runner_title: str, parser_description: str, tasks: TaskMap, profiles: ProfileMap, repo_root: str) -> int:
    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument(
        "--profile",
        default="all",
        choices=sorted(profiles.keys()),
        help="Profile preset of tasks to run.",
    )
    parser.add_argument(
        "--task",
        action="append",
        dest="tasks",
        default=[],
        help="Extra task id to include. Can be repeated.",
    )
    parser.add_argument(
        "--single-task",
        action="append",
        dest="single_tasks",
        default=[],
        help="Run exactly these task ids (plus --task), bypassing profile preset.",
    )
    parser.add_argument(
        "--skip",
        action="append",
        dest="skip_tasks",
        default=[],
        help="Task id to skip. Can be repeated.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print available profiles/tasks and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print run plan without executing scripts.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately when one task fails.",
    )
    args = parser.parse_args()

    if args.list:
        list_tasks(tasks, profiles)
        return 0

    if args.single_tasks:
        run_list = unique_preserve_order(list(args.single_tasks) + list(args.tasks))
        _validate_task_ids(run_list, tasks)
        if args.skip_tasks:
            skip_set = set(args.skip_tasks)
            run_list = [task_id for task_id in run_list if task_id not in skip_set]
        display_profile = "single-task"
    else:
        run_list = build_run_list(args.profile, args.tasks, args.skip_tasks, tasks, profiles)
        display_profile = args.profile
    print(runner_title)
    print("Profile: {0}".format(display_profile))
    print("Task count: {0}".format(len(run_list)))
    print("Dry-run: {0}".format(args.dry_run))
    print("Stop-on-error: {0}".format(args.stop_on_error))

    results = []
    for task_id in run_list:
        result = run_task(task_id, tasks, repo_root, dry_run=args.dry_run)
        results.append(result)
        if (not result["ok"]) and args.stop_on_error:
            print("\n[STOP] stop-on-error triggered by: {0}".format(task_id))
            break

    failed = [r for r in results if not r["ok"]]
    print("\nSummary:")
    for r in results:
        status = "OK" if r["ok"] else "FAIL"
        print("  [{0}] {1} ({2:.1f}s, code={3}, {4})".format(status, r["task_id"], r["duration"], r["code"], r["message"]))

    print("\nDone: {0} total, {1} failed".format(len(results), len(failed)))
    return 1 if failed else 0


def run_pipeline_from_config(*, runner_title: str, parser_description: str, config_path: str, repo_root: str) -> int:
    tasks, profiles = load_runner_config(config_path)
    return run_pipeline(
        runner_title=runner_title,
        parser_description=parser_description,
        tasks=tasks,
        profiles=profiles,
        repo_root=repo_root,
    )
