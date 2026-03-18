from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_CREDENTIALS_FILE = ROOT_DIR / "config" / "api_credentials.json"


def _credentials_file() -> Path:
    configured = os.environ.get("API_CREDENTIALS_FILE")
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_CREDENTIALS_FILE


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON in credentials file: {0}".format(path)) from exc

    if not isinstance(data, dict):
        raise ValueError("Credentials file must contain a JSON object: {0}".format(path))
    return data


@lru_cache(maxsize=1)
def load_api_credentials() -> Dict[str, Any]:
    return _read_json(_credentials_file())


def reload_api_credentials() -> None:
    load_api_credentials.cache_clear()


def get_credentials(*keys: str, default: Any = None, required: bool = False) -> Any:
    data: Any = load_api_credentials()
    path = []

    for key in keys:
        path.append(key)
        if not isinstance(data, dict) or key not in data:
            if required:
                raise ValueError(
                    "Missing credentials path: {0} (file: {1})".format(".".join(path), _credentials_file())
                )
            return default
        data = data[key]

    if required and data in (None, "", {}, []):
        raise ValueError(
            "Credentials value is empty for path: {0} (file: {1})".format(".".join(path), _credentials_file())
        )

    return data
