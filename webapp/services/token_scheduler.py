from __future__ import annotations

import logging
import os
import threading

from webapp.db import SessionLocal
from webapp.services.token_refresh import refresh_managed_tokens_once


_THREAD: threading.Thread | None = None
_STOP_EVENT = threading.Event()
_LOGGER = logging.getLogger(__name__)


def _interval_seconds() -> int:
    raw = os.environ.get("WONDERLAB_TOKEN_REFRESH_INTERVAL_SECONDS", "900")
    try:
        value = int(raw)
    except ValueError:
        value = 900
    return max(value, 60)


def _enabled() -> bool:
    raw = str(os.environ.get("WONDERLAB_TOKEN_REFRESH_ENABLED", "true")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _run_loop() -> None:
    interval = _interval_seconds()
    _LOGGER.info("token-refresh scheduler started interval_seconds=%s", interval)
    while not _STOP_EVENT.is_set():
        if SessionLocal is None:
            _STOP_EVENT.wait(interval)
            continue
        db = SessionLocal()
        try:
            summary = refresh_managed_tokens_once(db)
            _LOGGER.info("token-refresh run summary=%s", summary)
        except Exception as exc:
            _LOGGER.exception("token-refresh scheduler error=%s", exc)
        finally:
            db.close()
        _STOP_EVENT.wait(interval)
    _LOGGER.info("token-refresh scheduler stopped")


def start_token_scheduler() -> None:
    global _THREAD
    if not _enabled():
        _LOGGER.info("token-refresh scheduler disabled by WONDERLAB_TOKEN_REFRESH_ENABLED")
        return
    if _THREAD is not None and _THREAD.is_alive():
        return

    _STOP_EVENT.clear()
    _THREAD = threading.Thread(target=_run_loop, name="wl-token-refresh", daemon=True)
    _THREAD.start()


def stop_token_scheduler() -> None:
    global _THREAD
    _STOP_EVENT.set()
    thread = _THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=3)
    _THREAD = None
