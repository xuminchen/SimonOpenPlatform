from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

from webapp.db import SessionLocal
from webapp.models import SyncTask
from webapp.services.tasks import execute_task


_MAX_WORKERS = int(os.environ.get("WONDERLAB_TASK_WORKERS", "4"))
_EXECUTOR = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="wl-task")


def submit_task(task_id: int) -> None:
    _EXECUTOR.submit(_run_task, task_id)


def _run_task(task_id: int) -> None:
    if SessionLocal is None:
        return
    db = SessionLocal()
    try:
        task = db.get(SyncTask, task_id)
        if task is None:
            return
        execute_task(db, task)
    finally:
        db.close()


def shutdown_runner() -> None:
    _EXECUTOR.shutdown(wait=False, cancel_futures=False)
