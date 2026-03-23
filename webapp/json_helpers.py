from __future__ import annotations

import json
from typing import Any


def safe_json_loads(raw: str | None, *, default: Any) -> Any:
    try:
        payload = json.loads(raw or "")
    except json.JSONDecodeError:
        return default
    return payload


def safe_json_dict(raw: str | None) -> dict[str, Any]:
    payload = safe_json_loads(raw, default={})
    return payload if isinstance(payload, dict) else {}


def safe_json_list(raw: str | None) -> list[Any]:
    payload = safe_json_loads(raw, default=[])
    return payload if isinstance(payload, list) else []
