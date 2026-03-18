from __future__ import annotations

import json
import os
from pathlib import Path
import threading
from typing import Any

from webapp.services.platform_alias import normalize_platform

_FILE_LOCK = threading.Lock()
_DEDUP_CONTAINER_KEYS = {"accounts", "accounts_by_app_id", "apps", "shops", "credentials"}


def _credentials_file_path() -> Path:
    configured = os.environ.get("API_CREDENTIALS_FILE")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[2] / "config" / "api_credentials.json"


def _read_credentials(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError("Credentials file must be a JSON object: {0}".format(path))
    return data


def _write_credentials(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def _build_provider_payload(config: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in ("app_id", "secret_key", "secret", "auth_code", "remark"):
        value = config.get(key)
        if value is None:
            continue
        text = str(value).strip() if isinstance(value, str) else value
        if text in ("", None):
            continue
        payload[key] = text

    token_policy = config.get("token_policy")
    if isinstance(token_policy, dict):
        payload["token_policy"] = token_policy

    token = config.get("token")
    if isinstance(token, dict):
        kept = {}
        for key in (
            "access_token",
            "refresh_token",
            "access_token_expires_in",
            "refresh_token_expires_in",
            "access_token_expires_at",
            "refresh_token_expires_at",
            "last_refresh_at",
            "token_status",
            "approval_advertisers",
        ):
            value = token.get(key)
            if value in (None, ""):
                continue
            kept[key] = value
        if kept:
            payload["token"] = kept

    # Compatibility: many legacy scripts read `secret`.
    if "secret_key" in payload and "secret" not in payload:
        payload["secret"] = payload["secret_key"]
    return payload


def _ensure_dict(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        value = {}
        obj[key] = value
    return value


def _remove_app_id_from_bucket(bucket: dict[str, Any], app_id: str) -> None:
    for key in list(bucket.keys()):
        value = bucket.get(key)
        if not isinstance(value, dict):
            continue
        record_app_id = str(value.get("app_id", "")).strip()
        if record_app_id == app_id:
            bucket.pop(key, None)


def _dedup_app_id_records(node: Any, app_id: str, parent_key: str = "") -> None:
    if isinstance(node, dict):
        if parent_key in _DEDUP_CONTAINER_KEYS:
            _remove_app_id_from_bucket(node, app_id)
        for key, value in list(node.items()):
            _dedup_app_id_records(value, app_id, parent_key=str(key))
        return
    if isinstance(node, list):
        for value in node:
            _dedup_app_id_records(value, app_id, parent_key=parent_key)


def _delete_app_id_records(node: Any, app_ids: set[str], parent_key: str = "") -> int:
    deleted = 0
    if isinstance(node, dict):
        if parent_key in _DEDUP_CONTAINER_KEYS:
            for key in list(node.keys()):
                value = node.get(key)
                if not isinstance(value, dict):
                    continue
                record_app_id = str(value.get("app_id", "")).strip()
                if record_app_id in app_ids:
                    node.pop(key, None)
                    deleted += 1
        for key, value in list(node.items()):
            deleted += _delete_app_id_records(value, app_ids, parent_key=str(key))
        return deleted
    if isinstance(node, list):
        for value in node:
            deleted += _delete_app_id_records(value, app_ids, parent_key=parent_key)
    return deleted


def _clear_credential_source_cache() -> None:
    try:
        from webapp.services import credential_source

        credential_source._CREDENTIALS_CACHE = None  # type: ignore[attr-defined]
    except Exception:
        pass


def sync_account_to_credentials_file(*, platform: str, account_name: str, config: dict[str, Any]) -> None:
    normalized_platform = normalize_platform(platform)
    normalized_name = str(account_name or "").strip()
    if not normalized_platform or not normalized_name:
        return

    payload = _build_provider_payload(config if isinstance(config, dict) else {})
    if not payload:
        return
    app_id = str(payload.get("app_id", "")).strip()
    if not app_id:
        raise ValueError("app_id is required and must be unique in credentials file")
    payload["name"] = normalized_name

    path = _credentials_file_path()
    with _FILE_LOCK:
        root = _read_credentials(path)
        _dedup_app_id_records(root, app_id)

        # Canonical storage for webapp-created accounts.
        webapp_accounts = _ensure_dict(root, "webapp_accounts")
        by_platform = _ensure_dict(webapp_accounts, normalized_platform)
        account_bucket = _ensure_dict(by_platform, "accounts_by_app_id")
        account_bucket[app_id] = payload

        _write_credentials(path, root)
    _clear_credential_source_cache()


def delete_accounts_from_credentials_file(*, app_ids: list[str]) -> int:
    normalized = {str(x).strip() for x in app_ids if str(x).strip()}
    if not normalized:
        return 0

    path = _credentials_file_path()
    with _FILE_LOCK:
        root = _read_credentials(path)
        deleted = _delete_app_id_records(root, normalized)
        _write_credentials(path, root)

    _clear_credential_source_cache()
    return deleted
