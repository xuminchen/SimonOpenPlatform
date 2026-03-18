#!/usr/bin/env python3
"""Validate subproject tasks.toml configs and referenced scripts."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from _shared.module_registry import list_modules
from _shared.unified_runner_core import load_runner_config

MODULES = list_modules()


def validate_module(module: str) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    config_path = os.path.join(REPO_ROOT, "subprojects", module, "project", "tasks.toml")

    try:
        tasks, profiles = load_runner_config(config_path)
    except Exception as exc:  # noqa: BLE001
        return False, ["config parse/validation failed: {0}".format(exc)]

    if "all" not in profiles:
        errors.append("profiles.all missing")

    all_tasks = set(tasks.keys())
    for profile, profile_tasks in profiles.items():
        for task_id in profile_tasks:
            if task_id not in all_tasks:
                errors.append("profile {0} references unknown task {1}".format(profile, task_id))

    for task_id, cfg in tasks.items():
        script_rel = cfg.get("script", "")
        script_abs = os.path.join(REPO_ROOT, script_rel)
        if not os.path.exists(script_abs):
            errors.append("task {0} script missing: {1}".format(task_id, script_rel))

    return len(errors) == 0, errors


def normalize_modules(selected_modules: List[str]) -> List[str]:
    if not selected_modules:
        return list(MODULES)

    invalid = [m for m in selected_modules if m not in MODULES]
    if invalid:
        raise ValueError("Unknown module(s): {0}".format(", ".join(invalid)))

    seen = set()
    normalized = []
    for module in selected_modules:
        if module not in seen:
            seen.add(module)
            normalized.append(module)
    return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate subprojects tasks.toml configs.")
    parser.add_argument(
        "--module",
        action="append",
        dest="modules",
        default=[],
        help="Module id to validate (repeatable), e.g. --module orders_management",
    )
    parser.add_argument(
        "--list-modules",
        action="store_true",
        help="Print available module ids and exit.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first failed module.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.list_modules:
        for module in MODULES:
            print(module)
        return 0

    try:
        modules = normalize_modules(args.modules)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    failed_modules: Dict[str, List[str]] = {}
    ok_modules: List[str] = []

    for module in modules:
        ok, errors = validate_module(module)
        if ok:
            ok_modules.append(module)
        else:
            failed_modules[module] = errors
            if args.fail_fast:
                break

    if args.format == "json":
        print(
            json.dumps(
                {
                    "repo": REPO_ROOT,
                    "checked_modules": modules,
                    "ok_modules": ok_modules,
                    "failed_modules": failed_modules,
                    "summary": {
                        "ok": len(ok_modules),
                        "failed": len(failed_modules),
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1 if failed_modules else 0

    print("Subproject Config Check")
    print("Repo: {0}".format(REPO_ROOT))
    print("Checked: {0}".format(", ".join(modules)))

    for module in modules:
        if module in failed_modules:
            print("[FAIL] {0}".format(module))
        else:
            print("[OK]   {0}".format(module))

    if failed_modules:
        print("\nDetails:")
        for module, errors in failed_modules.items():
            print("- {0}".format(module))
            for err in errors:
                print("  * {0}".format(err))

    print("\nSummary: {0} ok, {1} failed".format(len(ok_modules), len(failed_modules)))
    return 1 if failed_modules else 0


if __name__ == "__main__":
    raise SystemExit(main())
