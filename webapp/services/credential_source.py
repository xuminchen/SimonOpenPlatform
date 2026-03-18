from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from webapp.models import PlatformAccount
from webapp.security import encrypt_text
from webapp.services.platform_alias import normalize_platform


_APP_ID_KEYS = ("app_id", "app_key", "client_id")
_SECRET_KEYS = ("secret_key", "secret", "app_secret", "client_secret", "sign_secret")
_ACCESS_TOKEN_KEYS = ("access_token", "advertiser_access_token")
_REFRESH_TOKEN_KEYS = ("refresh_token",)
_CONTAINER_NAMES = {"shops", "apps", "accounts", "credentials", "tokens"}
_META_CONTAINERS = {"star_accounts", "dou_plus_accounts"}
_CREDENTIALS_CACHE: dict[str, Any] | None = None


@dataclass
class CredentialEntry:
    source_path: str
    platform: str
    name: str
    app_id: str | None
    secret_key: str | None
    access_token: str | None
    config: dict[str, Any]


def _pick_first_non_empty(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _guess_name(path: list[str], platform: str) -> str:
    for segment in reversed(path):
        if segment not in _CONTAINER_NAMES and segment != platform:
            return segment
    return platform


def _extract_entry_name(node: dict[str, Any], path: list[str], platform: str) -> str:
    explicit_name = str(node.get("name", "")).strip()
    if explicit_name:
        return explicit_name
    return _guess_name(path, platform)


def _walk_credentials(node: Any, path: list[str], out: list[CredentialEntry]) -> None:
    if isinstance(node, dict):
        platform = "unknown"
        if path:
            if path[0] == "webapp_accounts" and len(path) >= 2:
                platform = normalize_platform(path[1])
            else:
                platform = normalize_platform(path[0])
        app_id = _pick_first_non_empty(node, _APP_ID_KEYS)
        secret_key = _pick_first_non_empty(node, _SECRET_KEYS)
        access_token = _pick_first_non_empty(node, _ACCESS_TOKEN_KEYS)

        if app_id or secret_key or access_token:
            out.append(
                CredentialEntry(
                    source_path=".".join(path),
                    platform=platform,
                    name=_extract_entry_name(node, path, platform),
                    app_id=app_id,
                    secret_key=secret_key,
                    access_token=access_token,
                    config=node,
                )
            )

        for key, value in node.items():
            if key == "token":
                # token is a nested credential payload, not an account entry.
                continue
            if (not path and key in _META_CONTAINERS) or (
                bool(path) and normalize_platform(path[0]) in {"red_juguang"} and key in _META_CONTAINERS
            ):
                continue
            _walk_credentials(value, path + [str(key)], out)
        return

    if isinstance(node, list):
        for idx, value in enumerate(node):
            _walk_credentials(value, path + [str(idx)], out)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _credentials_file() -> Path:
    configured = os.environ.get("API_CREDENTIALS_FILE")
    if configured:
        return Path(configured).expanduser().resolve()
    return _project_root() / "config" / "api_credentials.json"


def _load_api_credentials(refresh: bool = False) -> dict[str, Any]:
    global _CREDENTIALS_CACHE

    if refresh:
        _CREDENTIALS_CACHE = None

    if _CREDENTIALS_CACHE is not None:
        return _CREDENTIALS_CACHE

    path = _credentials_file()
    if not path.exists():
        _CREDENTIALS_CACHE = {}
        return _CREDENTIALS_CACHE

    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)

    if not isinstance(data, dict):
        raise ValueError("Credentials file must be a JSON object: {0}".format(path))

    _CREDENTIALS_CACHE = data
    return _CREDENTIALS_CACHE


def list_credential_entries(refresh: bool = False) -> list[CredentialEntry]:
    root = _load_api_credentials(refresh=refresh)
    entries: list[CredentialEntry] = []
    _walk_credentials(root, [], entries)

    dedup: dict[str, CredentialEntry] = {}
    for item in entries:
        key = str(item.app_id or "").strip() or "{0}::{1}".format(item.source_path, item.name)
        existing = dedup.get(key)
        if existing is None:
            dedup[key] = item
            continue

        old_score = 1 if "accounts_by_app_id" in existing.source_path else 0
        new_score = 1 if "accounts_by_app_id" in item.source_path else 0
        if new_score >= old_score:
            dedup[key] = item
    return list(dedup.values())


def find_credential_entry_by_app_id(app_id: str, refresh: bool = False) -> CredentialEntry | None:
    normalized_app_id = str(app_id or "").strip()
    if not normalized_app_id:
        return None

    best_item: CredentialEntry | None = None
    best_score = -1
    for item in list_credential_entries(refresh=refresh):
        if str(item.app_id or "").strip() != normalized_app_id:
            continue
        score = 1 if "accounts_by_app_id" in item.source_path else 0
        if score >= best_score:
            best_item = item
            best_score = score
    return best_item


def find_credential_entry(
    *,
    platform: str,
    app_id: str,
    refresh: bool = False,
) -> CredentialEntry | None:
    normalized_platform = normalize_platform(platform)
    normalized_app_id = str(app_id or "").strip()
    if not normalized_platform or not normalized_app_id:
        return None

    for item in list_credential_entries(refresh=refresh):
        if item.platform != normalized_platform:
            continue
        if str(item.app_id or "").strip() != normalized_app_id:
            continue
        return item
    return None


def extract_token_bundle(config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    access_token = _pick_first_non_empty(config, _ACCESS_TOKEN_KEYS)
    refresh_token = _pick_first_non_empty(config, _REFRESH_TOKEN_KEYS)
    if not access_token and not refresh_token:
        return {}

    bundle = {
        "access_token": access_token or "",
        "refresh_token": refresh_token or "",
    }
    for key in ("access_token_expires_in", "refresh_token_expires_in", "access_token_expires_at", "refresh_token_expires_at"):
        value = config.get(key)
        if value in (None, ""):
            continue
        bundle[key] = value
    return bundle


def sync_accounts_from_credential_source(db: Session, refresh: bool = False) -> tuple[int, int, list[PlatformAccount]]:
    entries = list_credential_entries(refresh=refresh)
    created = 0
    updated = 0
    synced_accounts: list[PlatformAccount] = []

    for item in entries:
        account = (
            db.query(PlatformAccount)
            .filter(PlatformAccount.platform == item.platform, PlatformAccount.name == item.name)
            .first()
        )

        encrypted_config = encrypt_text(json.dumps(item.config, ensure_ascii=False))
        now = datetime.utcnow()

        if account is None:
            account = PlatformAccount(
                name=item.name,
                platform=item.platform,
                status="active",
                config_encrypted=encrypted_config,
                app_id=item.app_id,
                secret_key_encrypted=encrypt_text(item.secret_key) if item.secret_key else None,
                ip_whitelist_json="[]",
                credential_updated_at=now if item.secret_key else None,
            )
            db.add(account)
            db.flush()
            created += 1
        else:
            account.config_encrypted = encrypted_config
            if item.app_id:
                account.app_id = item.app_id
            if item.secret_key:
                account.secret_key_encrypted = encrypt_text(item.secret_key)
                account.credential_updated_at = now
            updated += 1
            db.add(account)

        synced_accounts.append(account)

    db.commit()
    for account in synced_accounts:
        db.refresh(account)
    return created, updated, synced_accounts
