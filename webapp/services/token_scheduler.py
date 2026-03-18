from __future__ import annotations

import os
import threading

from webapp.db import SessionLocal
from webapp.services.token_refresh import refresh_managed_tokens_once


_THREAD: threading.Thread | None = None
_STOP_EVENT = threading.Event()


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
    print("token-refresh scheduler started interval_seconds={0}".format(interval))
    while not _STOP_EVENT.is_set():
        if SessionLocal is None:
            _STOP_EVENT.wait(interval)
            continue
        db = SessionLocal()
        try:
            summary = refresh_managed_tokens_once(db)
            print("token-refresh run summary={0}".format(summary))
        except Exception as exc:
            print("token-refresh scheduler error={0}".format(exc))
        finally:
            db.close()
        _STOP_EVENT.wait(interval)
    print("token-refresh scheduler stopped")


def start_token_scheduler() -> None:
    global _THREAD
    if not _enabled():
        print("token-refresh scheduler disabled by WONDERLAB_TOKEN_REFRESH_ENABLED")
        return
    if _THREAD is not None and _THREAD.is_alive():
        return

    _STOP_EVENT.clear()
    _THREAD = threading.Thread(target=_run_loop, name="wl-token-refresh", daemon=True)
    _THREAD.start()


def stop_token_scheduler() -> None:
    _STOP_EVENT.set()
