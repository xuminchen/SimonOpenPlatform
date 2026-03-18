#!/usr/bin/env python3
"""Unified compatibility CLI for subproject module runners."""

from __future__ import annotations

import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from module_registry import module_paths, validate_module
from unified_runner_core import load_runner_config


def usage(module: str, profiles, tasks) -> str:
    return (
        "Usage:\n"
        "  bash subprojects/{m}/run.sh list\n"
        "  bash subprojects/{m}/run.sh dry-run [profile]\n"
        "  bash subprojects/{m}/run.sh dry-run-task <task_id>\n"
        "  bash subprojects/{m}/run.sh <profile|task_id>\n"
        "\n"
        "Profiles: {profiles}\n"
        "Tasks: {tasks}\n"
    ).format(m=module, profiles=", ".join(sorted(profiles.keys())), tasks=", ".join(sorted(tasks.keys())))


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python3 subprojects/_shared/module_cli.py <module> [mode] [arg]", file=sys.stderr)
        return 2

    module = sys.argv[1]
    try:
        validate_module(module)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    mode = sys.argv[2] if len(sys.argv) >= 3 else "list"
    mode_arg = sys.argv[3] if len(sys.argv) >= 4 else None

    paths = module_paths(REPO_ROOT, module)
    tasks, profiles = load_runner_config(paths["config"])

    runner = [sys.executable, paths["runner"]]

    if mode == "list":
        cmd = runner + ["--list"]
    elif mode == "dry-run":
        profile = mode_arg or "all"
        if profile not in profiles:
            print("Unknown profile: {0}".format(profile), file=sys.stderr)
            print(usage(module, profiles, tasks), file=sys.stderr)
            return 2
        cmd = runner + ["--profile", profile, "--dry-run"]
    elif mode == "dry-run-task":
        task_id = mode_arg
        if not task_id:
            print("Missing task_id for dry-run-task mode", file=sys.stderr)
            print(usage(module, profiles, tasks), file=sys.stderr)
            return 2
        if task_id not in tasks:
            print("Unknown task: {0}".format(task_id), file=sys.stderr)
            print(usage(module, profiles, tasks), file=sys.stderr)
            return 2
        cmd = runner + ["--single-task", task_id, "--dry-run"]
    elif mode in profiles:
        cmd = runner + ["--profile", mode]
    elif mode in tasks:
        cmd = runner + ["--single-task", mode]
    else:
        print("Unknown mode/profile/task: {0}".format(mode), file=sys.stderr)
        print(usage(module, profiles, tasks), file=sys.stderr)
        return 2

    env = os.environ.copy()
    prev_python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = REPO_ROOT if not prev_python_path else "{0}:{1}".format(REPO_ROOT, prev_python_path)
    completed = subprocess.run(cmd, cwd=REPO_ROOT, env=env)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
