from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any

from sqlalchemy.orm import Session

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig
from webapp.models import PlatformAccount
from webapp.security import encrypt_text
from webapp.services.accounts import decode_account_config
from webapp.services.credentials_store import sync_account_to_credentials_file
from webapp.services.platform_alias import normalize_platform


HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=30,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)

SUPPORTED_TOKEN_PLATFORMS = {"oceanengine", "red_juguang", "red_chengfeng"}
RED_ACCESS_TOKEN_EXPIRES_IN_SECONDS = 24 * 60 * 60
RED_REFRESH_TOKEN_EXPIRES_IN_SECONDS = 30 * 24 * 60 * 60


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(text: Any) -> datetime | None:
    if not text:
        return None
    value = str(text).strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _pick_str(config: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_payload_data(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Token API payload is not JSON object")
    code = payload.get("code")
    if code not in (None, 0, "0"):
        message = payload.get("message") or payload.get("msg") or "token api error"
        raise ValueError(str(message))
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _extract_red_payload_data(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Red token payload is not JSON object")
    code = payload.get("code")
    if code not in (None, 0, "0"):
        message = payload.get("msg") or payload.get("message") or "red token api error"
        raise ValueError(str(message))
    success = payload.get("success")
    if success is False:
        message = payload.get("msg") or payload.get("message") or "red token api returned success=false"
        raise ValueError(str(message))
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("Red token api data field is missing")
    return data


def _validate_red_token_fields(*, payload: dict[str, Any], flow: str) -> None:
    access_token = _pick_str(payload, "access_token", "advertiser_access_token")
    refresh_token = _pick_str(payload, "refresh_token")
    if not access_token:
        raise ValueError("Red token api missing access_token")
    if flow == "refresh_token" and not refresh_token:
        raise ValueError("Red refresh_token api missing new refresh_token")


def _calc_expire_at(expires_in: Any, *, now: datetime) -> str | None:
    try:
        seconds = int(expires_in)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return (now + timedelta(seconds=seconds)).isoformat()


def _get_token_config(config: dict[str, Any]) -> dict[str, Any]:
    token_cfg = config.get("token")
    if isinstance(token_cfg, dict):
        return token_cfg
    return {}


def _should_refresh(
    *,
    token_cfg: dict[str, Any],
    auth_code: str,
    advance_minutes: int,
) -> bool:
    now = _now_utc()
    access_token = _pick_str(token_cfg, "access_token")
    refresh_token = _pick_str(token_cfg, "refresh_token")

    if not access_token:
        return bool(auth_code or refresh_token)

    expires_at = _parse_iso_datetime(token_cfg.get("access_token_expires_at"))
    if expires_at is not None:
        return now + timedelta(minutes=advance_minutes) >= expires_at

    # Fallback: unknown expiry. Refresh daily to reduce token invalidation risk.
    last_refresh_at = _parse_iso_datetime(token_cfg.get("last_refresh_at") or token_cfg.get("synced_at"))
    if last_refresh_at is None:
        return True
    return now - last_refresh_at >= timedelta(hours=20)


def _refresh_oceanengine_token(*, app_id: str, secret: str, auth_code: str, refresh_token: str) -> dict[str, Any]:
    if refresh_token:
        req_data = {
            "app_id": app_id,
            "secret": secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        endpoint = "https://ad.oceanengine.com/open_api/oauth2/refresh_token/"
    elif auth_code:
        req_data = {
            "app_id": app_id,
            "secret": secret,
            "grant_type": "auth_code",
            "auth_code": auth_code,
        }
        endpoint = "https://ad.oceanengine.com/open_api/oauth2/access_token/"
    else:
        raise ValueError("Missing both refresh_token and auth_code")

    result = HTTP_CLIENT.request_json(
        method="post",
        url=endpoint,
        data=req_data,
        success_checker=lambda payload: isinstance(payload, dict),
        event_name="webapp_oceanengine_token_refresh",
    )
    if not result.ok:
        raise RuntimeError(result.error or result.message)
    return _extract_payload_data(result.data)


def _validate_red_platform_type(*, platform: str, payload: dict[str, Any], flow: str) -> None:
    allowed_by_platform_and_flow: dict[tuple[str, str], set[int]] = {
        ("red_juguang", "auth_code"): {1},
        ("red_juguang", "refresh_token"): {1},
        ("red_chengfeng", "auth_code"): {4},
        # 文档侧对 refresh_token 返回值描述可能存在平台差异，兼容 1/4，避免误拒绝。
        ("red_chengfeng", "refresh_token"): {1, 4},
    }
    allowed = allowed_by_platform_and_flow.get((platform, flow))
    if not allowed:
        return
    raw = payload.get("platform_type")
    if raw in (None, ""):
        return
    try:
        actual = int(raw)
    except (TypeError, ValueError):
        return
    if actual not in allowed:
        raise ValueError(
            "platform_type mismatch for {0}: expected one of {1}, got {2}".format(platform, sorted(allowed), actual)
        )


def refresh_red_token(*, app_id: str, secret: str, auth_code: str, refresh_token: str, platform: str = "") -> dict[str, Any]:
    flow = "refresh_token" if refresh_token else "auth_code"
    if refresh_token:
        req_data = {"app_id": app_id, "secret": secret, "refresh_token": refresh_token}
        endpoint = "https://adapi.xiaohongshu.com/api/open/oauth2/refresh_token"
    elif auth_code:
        req_data = {"app_id": app_id, "secret": secret, "auth_code": auth_code}
        endpoint = "https://adapi.xiaohongshu.com/api/open/oauth2/access_token"
    else:
        raise ValueError("Missing both refresh_token and auth_code")

    result = HTTP_CLIENT.request_json(
        method="post",
        url=endpoint,
        headers={"content-type": "application/json"},
        data=json.dumps(req_data, ensure_ascii=False),
        success_checker=lambda payload: isinstance(payload, dict),
        event_name="webapp_red_token_refresh",
    )
    if not result.ok:
        raise RuntimeError(result.error or result.message)
    payload = _extract_red_payload_data(result.data)
    _validate_red_token_fields(payload=payload, flow=flow)
    normalized_platform = normalize_platform(platform)
    if normalized_platform:
        _validate_red_platform_type(platform=normalized_platform, payload=payload, flow=flow)
    return payload


def _merge_token_payload(
    *,
    current_token: dict[str, Any],
    payload: dict[str, Any],
    platform: str,
) -> dict[str, Any]:
    now = _now_utc()
    next_token = dict(current_token)
    previous_access_token = _pick_str(current_token, "access_token")
    previous_refresh_token = _pick_str(current_token, "refresh_token")
    access_token = _pick_str(payload, "access_token", "advertiser_access_token")
    refresh_token = _pick_str(payload, "refresh_token")

    if access_token:
        next_token["access_token"] = access_token
    if refresh_token:
        next_token["refresh_token"] = refresh_token

    # 平台规则：刷新后旧 token 存在约 5 分钟失效窗口，记录元数据便于排障和幂等切换。
    if access_token and previous_access_token and previous_access_token != access_token:
        next_token["previous_access_token"] = previous_access_token
        next_token["previous_access_token_invalid_after"] = (now + timedelta(minutes=5)).isoformat()
    if refresh_token and previous_refresh_token and previous_refresh_token != refresh_token:
        next_token["previous_refresh_token"] = previous_refresh_token
        next_token["previous_refresh_token_invalid_after"] = (now + timedelta(minutes=5)).isoformat()

    access_expires_in = payload.get("access_token_expires_in") or payload.get("expires_in")
    refresh_expires_in = payload.get("refresh_token_expires_in")
    # 小红书聚光/乘风规则补充：
    # 1) access_token 默认 1 天
    # 2) refresh_token 默认 30 天
    # 3) 刷新后以最新 token 为准（旧 token 的具体失效窗口由平台侧控制）
    if platform in {"red_juguang", "red_chengfeng"}:
        if access_expires_in in (None, ""):
            access_expires_in = RED_ACCESS_TOKEN_EXPIRES_IN_SECONDS
        if refresh_expires_in in (None, ""):
            refresh_expires_in = RED_REFRESH_TOKEN_EXPIRES_IN_SECONDS
    if access_expires_in not in (None, ""):
        next_token["access_token_expires_in"] = access_expires_in
        expire_at = _calc_expire_at(access_expires_in, now=now)
        if expire_at:
            next_token["access_token_expires_at"] = expire_at
    if refresh_expires_in not in (None, ""):
        next_token["refresh_token_expires_in"] = refresh_expires_in
        expire_at = _calc_expire_at(refresh_expires_in, now=now)
        if expire_at:
            next_token["refresh_token_expires_at"] = expire_at

    if platform in {"red_juguang", "red_chengfeng"} and isinstance(payload.get("approval_advertisers"), list):
        next_token["approval_advertisers"] = payload.get("approval_advertisers")
    if platform in {"red_juguang", "red_chengfeng"}:
        next_token["app_id"] = _pick_str(payload, "app_id")
        next_token["user_id"] = _pick_str(payload, "user_id")
        next_token["advertiser_id"] = _pick_str(payload, "advertiser_id")
        next_token["platform_type"] = payload.get("platform_type")
        next_token["role_type"] = payload.get("role_type")
        next_token["approval_role_type"] = payload.get("approval_role_type")
        next_token["corporation_name"] = _pick_str(payload, "corporation_name")
        next_token["scope"] = _pick_str(payload, "scope")
        next_token["virtual_seller_id"] = _pick_str(payload, "virtual_seller_id")
        next_token["create_time"] = payload.get("create_time")
        next_token["update_time"] = payload.get("update_time")

    next_token["managed_by"] = "system"
    next_token["last_refresh_at"] = now.isoformat()
    next_token["token_status"] = "ready"
    return next_token


def bootstrap_tokens_for_config(platform: str, config: dict[str, Any]) -> tuple[dict[str, Any], str]:
    normalized_platform = normalize_platform(platform)
    if normalized_platform not in SUPPORTED_TOKEN_PLATFORMS:
        return config, "skip_unsupported"
    if not isinstance(config, dict):
        return {}, "skip_bad_config"

    next_config = dict(config)
    token_cfg = _get_token_config(next_config)

    app_id = _pick_str(next_config, "app_id", "app_key", "client_id")
    secret = _pick_str(next_config, "secret_key", "secret", "app_secret", "client_secret")
    auth_code = _pick_str(next_config, "auth_code")
    refresh_token = _pick_str(token_cfg, "refresh_token") or _pick_str(next_config, "refresh_token")

    if not app_id or not secret:
        return next_config, "skip_missing_app_credentials"
    if not auth_code and not refresh_token:
        return next_config, "skip_missing_auth_code_or_refresh_token"

    if normalized_platform == "oceanengine":
        payload = _refresh_oceanengine_token(
            app_id=app_id,
            secret=secret,
            auth_code=auth_code,
            refresh_token=refresh_token,
        )
    else:
        payload = refresh_red_token(
            app_id=app_id,
            secret=secret,
            auth_code=auth_code,
            refresh_token=refresh_token,
            platform=normalized_platform,
        )

    next_config["token"] = _merge_token_payload(
        current_token=token_cfg,
        payload=payload,
        platform=normalized_platform,
    )
    return next_config, "refreshed"


def refresh_account_token_if_needed(db: Session, account: PlatformAccount) -> tuple[bool, str]:
    if account.status != "active":
        return False, "skip_inactive"
    normalized_platform = normalize_platform(account.platform)
    if normalized_platform not in SUPPORTED_TOKEN_PLATFORMS:
        return False, "skip_unsupported"

    config = decode_account_config(account)
    if not isinstance(config, dict):
        return False, "skip_bad_config"

    policy = config.get("token_policy")
    if not isinstance(policy, dict):
        policy = {}
    auto_refresh = bool(policy.get("auto_refresh_token", True))
    if not auto_refresh:
        return False, "skip_policy_disabled"

    advance_minutes_raw = policy.get("token_expire_advance_minutes", 30)
    try:
        advance_minutes = int(advance_minutes_raw)
    except (TypeError, ValueError):
        advance_minutes = 30
    advance_minutes = max(5, min(180, advance_minutes))

    app_id = _pick_str(config, "app_id", "app_key", "client_id")
    secret = _pick_str(config, "secret_key", "secret", "app_secret", "client_secret")
    auth_code = _pick_str(config, "auth_code")
    token_cfg = _get_token_config(config)
    refresh_token = _pick_str(token_cfg, "refresh_token")

    if not app_id or not secret:
        return False, "skip_missing_app_credentials"
    if not _should_refresh(token_cfg=token_cfg, auth_code=auth_code, advance_minutes=advance_minutes):
        return False, "skip_not_due"

    if normalized_platform == "oceanengine":
        payload = _refresh_oceanengine_token(
            app_id=app_id,
            secret=secret,
            auth_code=auth_code,
            refresh_token=refresh_token,
        )
    else:
        payload = refresh_red_token(
            app_id=app_id,
            secret=secret,
            auth_code=auth_code,
            refresh_token=refresh_token,
            platform=normalized_platform,
        )

    config["token"] = _merge_token_payload(
        current_token=token_cfg,
        payload=payload,
        platform=normalized_platform,
    )
    account.platform = normalized_platform
    account.config_encrypted = encrypt_text(json.dumps(config, ensure_ascii=False))
    db.add(account)
    db.commit()
    db.refresh(account)
    sync_account_to_credentials_file(
        platform=account.platform,
        account_name=account.name,
        config=config,
    )
    return True, "refreshed"


def refresh_managed_tokens_once(db: Session) -> dict[str, int]:
    summary = {"total": 0, "refreshed": 0, "skipped": 0, "failed": 0}
    all_accounts = db.query(PlatformAccount).order_by(PlatformAccount.id.asc()).all()
    accounts = [x for x in all_accounts if normalize_platform(x.platform) in SUPPORTED_TOKEN_PLATFORMS]
    summary["total"] = len(accounts)

    for account in accounts:
        try:
            changed, status = refresh_account_token_if_needed(db, account)
            if changed:
                summary["refreshed"] += 1
            else:
                summary["skipped"] += 1
                if status == "skip_not_due":
                    continue
                print("token-refresh skip account_id={0} status={1}".format(account.id, status))
        except Exception as exc:
            summary["failed"] += 1
            print("token-refresh failed account_id={0} err={1}".format(account.id, exc))
            db.rollback()

    return summary
