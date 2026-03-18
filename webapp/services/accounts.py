from __future__ import annotations

from datetime import datetime
import json
import secrets
from typing import Any, Dict

from sqlalchemy.orm import Session

from webapp.models import PlatformAccount
from webapp.security import decrypt_text, encrypt_text
from webapp.services.credentials_store import sync_account_to_credentials_file
from webapp.services.credential_source import extract_token_bundle, find_credential_entry
from webapp.services.platform_alias import normalize_platform


_TOKEN_MUTABLE_KEYS = {
    "access_token",
    "refresh_token",
    "advertiser_access_token",
    "access_token_expires_in",
    "refresh_token_expires_in",
    "access_token_expires_at",
    "refresh_token_expires_at",
}


def _generate_app_id(platform: str) -> str:
    suffix = secrets.token_hex(4)
    return "wl_{0}_{1}".format(platform.lower(), suffix)


def _generate_secret_key() -> str:
    return "wl_sk_{0}".format(secrets.token_urlsafe(24))


def _mask_secret_key(secret_key: str) -> str:
    if len(secret_key) <= 8:
        return "*" * len(secret_key)
    return "{0}{1}".format(secret_key[:6], "*" * (len(secret_key) - 10) + secret_key[-4:])


def _remove_user_supplied_tokens(config: Dict[str, Any]) -> Dict[str, Any]:
    safe = dict(config)
    for key in list(safe.keys()):
        if key in _TOKEN_MUTABLE_KEYS:
            safe.pop(key, None)

    # token 块由系统维护，用户输入会被忽略。
    token_block = safe.get("token")
    if isinstance(token_block, dict):
        cleaned = {k: v for k, v in token_block.items() if k not in _TOKEN_MUTABLE_KEYS}
        if cleaned:
            safe["token"] = cleaned
        else:
            safe.pop("token", None)
    return safe


def _hydrate_system_token_bundle(config: Dict[str, Any], *, platform: str, app_id: str) -> Dict[str, Any]:
    merged = _remove_user_supplied_tokens(config)
    source_entry = find_credential_entry(platform=platform, app_id=app_id, refresh=False)
    if source_entry is None:
        return merged

    token_bundle = extract_token_bundle(source_entry.config)
    if not token_bundle:
        return merged

    merged["token"] = {
        **token_bundle,
        "managed_by": "system",
        "source": "api_credentials_json",
        "source_path": source_entry.source_path,
        "synced_at": datetime.utcnow().isoformat(),
    }
    return merged


def decode_ip_whitelist(account: PlatformAccount) -> list[str]:
    raw = account.ip_whitelist_json or "[]"
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("ip_whitelist_json must be a JSON array")
    return [str(item).strip() for item in data if str(item).strip()]


def ensure_account_credentials(db: Session, account: PlatformAccount) -> PlatformAccount:
    changed = False
    if not account.app_id:
        account.app_id = _generate_app_id(account.platform)
        changed = True

    if not account.secret_key_encrypted:
        secret_key = _generate_secret_key()
        account.secret_key_encrypted = encrypt_text(secret_key)
        account.credential_updated_at = datetime.utcnow()
        changed = True

    if not account.ip_whitelist_json:
        account.ip_whitelist_json = "[]"
        changed = True

    if changed:
        db.add(account)
        db.commit()
        db.refresh(account)
    return account


def create_account(
    db: Session,
    *,
    name: str,
    platform: str,
    status: str,
    config: Dict[str, Any],
) -> tuple[PlatformAccount, str]:
    normalized_platform = normalize_platform(platform)
    app_id_from_config = str(config.get("app_id", "")).strip() if isinstance(config, dict) else ""
    assigned_app_id = app_id_from_config or _generate_app_id(normalized_platform)
    sanitized_config = _hydrate_system_token_bundle(
        config if isinstance(config, dict) else {},
        platform=normalized_platform,
        app_id=assigned_app_id,
    )
    sanitized_config["app_id"] = assigned_app_id
    encrypted = encrypt_text(json.dumps(sanitized_config, ensure_ascii=False))
    secret_key = _generate_secret_key()
    account = PlatformAccount(
        name=name,
        platform=normalized_platform,
        status=status,
        config_encrypted=encrypted,
        app_id=assigned_app_id,
        secret_key_encrypted=encrypt_text(secret_key),
        ip_whitelist_json="[]",
        credential_updated_at=datetime.utcnow(),
    )
    db.add(account)
    db.commit()
    db.refresh(account)
    sync_account_to_credentials_file(
        platform=normalized_platform,
        account_name=name,
        config=sanitized_config,
    )
    return account, secret_key


def decode_account_config(account: PlatformAccount) -> Dict[str, Any]:
    plain_text = decrypt_text(account.config_encrypted)
    data = json.loads(plain_text)
    if not isinstance(data, dict):
        raise ValueError("Account config must be a JSON object")
    return data


def update_account(
    db: Session,
    account: PlatformAccount,
    *,
    name: str | None = None,
    status: str | None = None,
    config: Dict[str, Any] | None = None,
) -> PlatformAccount:
    if name is not None:
        account.name = name
    if status is not None:
        account.status = status
    if config is not None:
        config_app_id = str(config.get("app_id", "")).strip()
        hydrated = _hydrate_system_token_bundle(
            config,
            platform=account.platform,
            app_id=config_app_id,
        )
        account.config_encrypted = encrypt_text(json.dumps(hydrated, ensure_ascii=False))
    else:
        hydrated = decode_account_config(account)

    db.add(account)
    db.commit()
    db.refresh(account)
    sync_account_to_credentials_file(
        platform=account.platform,
        account_name=account.name,
        config=hydrated,
    )
    return account


def get_secret_key_masked(account: PlatformAccount) -> str:
    if not account.secret_key_encrypted:
        return ""
    plain = decrypt_text(account.secret_key_encrypted)
    return _mask_secret_key(plain)


def reset_account_secret_key(db: Session, account: PlatformAccount) -> tuple[PlatformAccount, str]:
    secret_key = _generate_secret_key()
    account.secret_key_encrypted = encrypt_text(secret_key)
    account.credential_updated_at = datetime.utcnow()
    db.add(account)
    db.commit()
    db.refresh(account)
    return account, secret_key


def update_ip_whitelist(db: Session, account: PlatformAccount, ip_whitelist: list[str]) -> PlatformAccount:
    cleaned = []
    seen = set()
    for item in ip_whitelist:
        value = str(item).strip()
        if not value or value in seen:
            continue
        cleaned.append(value)
        seen.add(value)

    account.ip_whitelist_json = json.dumps(cleaned, ensure_ascii=False)
    db.add(account)
    db.commit()
    db.refresh(account)
    return account
