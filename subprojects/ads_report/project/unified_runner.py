#!/usr/bin/env python3
"""Unified orchestrator module entrypoint."""

import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from subprojects._shared.unified_runner_core import run_pipeline_from_config


MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(MODULE_DIR, "tasks.toml")


def main():
    return run_pipeline_from_config(
        runner_title="Unified Ads Report Runner",
        parser_description="Unified runner for ads report tasks.",
        config_path=CONFIG_PATH,
        repo_root=REPO_ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
