from __future__ import annotations

import logging
import os
import threading

from webapp.services.storage_retention import retention_interval_seconds, run_storage_retention_once


_THREAD: threading.Thread | None = None
_STOP_EVENT = threading.Event()
_LOGGER = logging.getLogger(__name__)


def _enabled() -> bool:
    raw = str(os.environ.get("SIMON_STORAGE_RETENTION_SCHEDULER_ENABLED", "true")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _run_loop() -> None:
    interval = retention_interval_seconds()
    _LOGGER.info("storage-retention scheduler started interval_seconds=%s", interval)
    while not _STOP_EVENT.is_set():
        try:
            summary = run_storage_retention_once(force=False)
            _LOGGER.info("storage-retention run summary=%s", summary)
        except Exception as exc:
            _LOGGER.exception("storage-retention scheduler error=%s", exc)
        _STOP_EVENT.wait(interval)
    _LOGGER.info("storage-retention scheduler stopped")


def start_storage_retention_scheduler() -> None:
    global _THREAD
    if not _enabled():
        _LOGGER.info("storage-retention scheduler disabled by SIMON_STORAGE_RETENTION_SCHEDULER_ENABLED")
        return
    if _THREAD is not None and _THREAD.is_alive():
        return
    _STOP_EVENT.clear()
    _THREAD = threading.Thread(target=_run_loop, name="wl-storage-retention", daemon=True)
    _THREAD.start()


def stop_storage_retention_scheduler() -> None:
    global _THREAD
    _STOP_EVENT.set()
    thread = _THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=3)
    _THREAD = None
