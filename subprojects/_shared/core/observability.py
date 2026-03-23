from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict


LOGGER = logging.getLogger("wonderlab.api")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]


def now_ms() -> int:
    return int(time.time() * 1000)


def log_event(event: str, **fields: Any) -> None:
    payload: Dict[str, Any] = {"event": event, **fields}
    LOGGER.info(json.dumps(payload, ensure_ascii=True, default=str))
