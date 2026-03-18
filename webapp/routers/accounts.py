from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from webapp.db import get_db
from webapp.models import PlatformAccount
from webapp.schemas import (
    AccountCreateRequest,
    AccountCreateResponse,
    AccountCredentialsResetResponse,
    AccountCredentialsView,
    AccountDetail,
    AccountIPWhitelistUpdateRequest,
    AccountSummary,
    AccountUpdateRequest,
    CredentialSourceEntry,
    CredentialSourceBatchDeleteRequest,
    CredentialSourceBatchDeleteResponse,
    CredentialSourceItemDetailResponse,
    CredentialSourceScanResponse,
    CredentialSourceSyncResponse,
    CredentialSourceTokenRefreshRequest,
    CredentialSourceTokenRefreshResponse,
    CredentialSourceUpsertRequest,
    CredentialSourceUpsertResponse,
)
from webapp.services.accounts import (
    create_account,
    decode_account_config,
    decode_ip_whitelist,
    ensure_account_credentials,
    get_secret_key_masked,
    reset_account_secret_key,
    update_account,
    update_ip_whitelist,
)
from webapp.services.credential_source import list_credential_entries, sync_accounts_from_credential_source
from webapp.services.credential_source import find_credential_entry_by_app_id
from webapp.services.credentials_store import delete_accounts_from_credentials_file, sync_account_to_credentials_file
from webapp.services.platform_alias import normalize_platform
from webapp.services.platform_configs import list_platform_configs
from webapp.services.token_refresh import SUPPORTED_TOKEN_PLATFORMS, bootstrap_tokens_for_config


router = APIRouter(prefix="/accounts", tags=["accounts"])


def _require_db(db: Session | None) -> Session:
    if db is None:
        raise HTTPException(status_code=503, detail="Database is disabled. JSON mode is active.")
    return db


@router.post("", response_model=AccountCreateResponse)
def create_account_api(request: AccountCreateRequest, db: Session | None = Depends(get_db)) -> AccountCreateResponse:
    db = _require_db(db)
    account, issued_secret_key = create_account(
        db,
        name=request.name,
        platform=request.platform,
        status=request.status,
        config=request.config,
    )
    return AccountCreateResponse(
        id=account.id,
        name=account.name,
        platform=account.platform,
        status=account.status,
        app_id=account.app_id,
        issued_secret_key=issued_secret_key,
        created_at=account.created_at,
        updated_at=account.updated_at,
    )


@router.get("", response_model=list[AccountSummary])
def list_accounts_api(
    platform: str | None = Query(default=None),
    db: Session | None = Depends(get_db),
) -> list[AccountSummary]:
    if db is None:
        return []
    query = db.query(PlatformAccount)
    accounts = query.order_by(PlatformAccount.id.desc()).all()
    if platform:
        target_platform = normalize_platform(platform)
        accounts = [x for x in accounts if normalize_platform(x.platform) == target_platform]
    result = []
    for item in accounts:
        account = ensure_account_credentials(db, item)
        result.append(
            AccountSummary(
                id=account.id,
                name=account.name,
                platform=normalize_platform(account.platform),
                status=account.status,
                app_id=account.app_id,
                created_at=account.created_at,
                updated_at=account.updated_at,
            )
        )
    return result


@router.get("/credentials/source", response_model=CredentialSourceScanResponse)
def scan_credential_source_api(
    refresh: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    keyword: str | None = Query(default=None),
    platform: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_db: bool = Query(default=False),
    db: Session | None = Depends(get_db),
) -> CredentialSourceScanResponse:
    source_entries = list_credential_entries(refresh=refresh)
    rows: list[dict[str, object]] = []

    for item in source_entries:
        token_block = item.config.get("token") if isinstance(item.config, dict) else {}
        token_status = ""
        token_updated_at = ""
        access_token = str(item.config.get("access_token", "")).strip() if isinstance(item.config, dict) else ""
        refresh_token = str(item.config.get("refresh_token", "")).strip() if isinstance(item.config, dict) else ""
        if isinstance(token_block, dict):
            token_status = str(token_block.get("token_status", "")).strip()
            token_updated_at = str(token_block.get("last_refresh_at", "")).strip() or str(token_block.get("synced_at", "")).strip()
            access_token = str(token_block.get("access_token", "")).strip() or access_token
            refresh_token = str(token_block.get("refresh_token", "")).strip() or refresh_token
        row = {
            "source_path": item.source_path,
            "platform": item.platform,
            "name": item.name,
            "status": "ready" if bool(item.app_id) and bool(item.secret_key) else "partial",
            "app_id": item.app_id,
            "has_secret_key": bool(item.secret_key),
            "has_access_token": bool(item.access_token),
            "access_token": access_token or None,
            "refresh_token": refresh_token or None,
            "token_status": token_status or ("ready" if bool(item.access_token) else "missing"),
            "token_updated_at": token_updated_at or None,
        }
        rows.append(row)

    if include_db and db is not None:
        db_accounts = db.query(PlatformAccount).order_by(PlatformAccount.id.desc()).all()
        for account in db_accounts:
            has_access_token = False
            app_id = account.app_id
            provider_app_id = ""
            has_provider_secret = False
            access_token = ""
            refresh_token = ""
            try:
                cfg = decode_account_config(account)
                provider_app_id = str(cfg.get("app_id", "")).strip()
                if provider_app_id:
                    app_id = provider_app_id
                has_provider_secret = bool(str(cfg.get("secret_key", "")).strip()) or bool(str(cfg.get("secret", "")).strip())
                token_block = cfg.get("token")
                if isinstance(token_block, dict):
                    access_token = str(token_block.get("access_token", "")).strip()
                    refresh_token = str(token_block.get("refresh_token", "")).strip()
                    has_access_token = bool(access_token)
                if not has_access_token:
                    access_token = str(cfg.get("access_token", "")).strip() or str(cfg.get("advertiser_access_token", "")).strip()
                    refresh_token = str(cfg.get("refresh_token", "")).strip() or refresh_token
                    has_access_token = bool(access_token)
            except Exception:
                has_access_token = False

            has_secret = has_provider_secret or bool(account.secret_key_encrypted)
            rows.append(
                {
                    "source_path": "db.platform_accounts.{0}".format(account.id),
                    "platform": normalize_platform(account.platform),
                    "name": account.name,
                    "status": "ready" if bool(app_id) and has_secret else "partial",
                    "app_id": app_id,
                    "has_secret_key": has_secret,
                    "has_access_token": has_access_token,
                    "access_token": access_token or None,
                    "refresh_token": refresh_token or None,
                    "token_status": "ready" if has_access_token else "missing",
                    "token_updated_at": None,
                }
            )

    all_platforms = sorted({str(x.get("platform", "")) for x in rows if x.get("platform")})
    if keyword:
        kw = keyword.strip().lower()
        rows = [
            x
            for x in rows
            if kw in str(x.get("name", "")).lower() or kw in str(x.get("source_path", "")).lower()
        ]
    if platform:
        pf = platform.strip().lower()
        rows = [x for x in rows if str(x.get("platform", "")).lower() == pf]
    if status:
        status_key = status.strip().lower()
        if status_key in {"ready", "partial"}:
            rows = [x for x in rows if x.get("status") == status_key]

    def _row_sort_key(item: dict[str, object]) -> tuple[int, int, str]:
        source_path = str(item.get("source_path", ""))
        if source_path.startswith("db.platform_accounts."):
            # DB-created credentials first, newest first.
            raw_id = source_path.split(".")[-1]
            try:
                account_id = int(raw_id)
            except ValueError:
                account_id = 0
            return (0, -account_id, source_path)
        return (1, 0, source_path)

    rows = sorted(rows, key=_row_sort_key)

    total = len(rows)
    total_pages = max((total + page_size - 1) // page_size, 1)
    safe_page = min(page, total_pages)
    start = (safe_page - 1) * page_size
    page_items = rows[start : start + page_size]

    return CredentialSourceScanResponse(
        page=safe_page,
        page_size=page_size,
        total_pages=total_pages,
        total=total,
        platforms=all_platforms,
        entries=[
            CredentialSourceEntry(
                row_no=start + idx + 1,
                source_path=str(item.get("source_path", "")),
                platform=str(item.get("platform", "")),
                name=str(item.get("name", "")),
                status=str(item.get("status", "partial")),
                app_id=(str(item.get("app_id")) if item.get("app_id") is not None else None),
                has_secret_key=bool(item.get("has_secret_key")),
                has_access_token=bool(item.get("has_access_token")),
                access_token=(str(item.get("access_token")) if item.get("access_token") is not None else None),
                refresh_token=(str(item.get("refresh_token")) if item.get("refresh_token") is not None else None),
                token_status=(str(item.get("token_status")) if item.get("token_status") is not None else None),
                token_updated_at=(str(item.get("token_updated_at")) if item.get("token_updated_at") is not None else None),
            )
            for idx, item in enumerate(page_items)
        ],
    )


@router.post("/credentials/source/upsert", response_model=CredentialSourceUpsertResponse)
def upsert_credential_source_api(request: CredentialSourceUpsertRequest) -> CredentialSourceUpsertResponse:
    normalized_platform = normalize_platform(request.platform)
    registered_platforms = {str(item.get("platform", "")).strip() for item in list_platform_configs()}
    if normalized_platform not in registered_platforms:
        raise HTTPException(status_code=400, detail="platform is not registered: {0}".format(normalized_platform))
    normalized_name = request.name.strip()
    normalized_config = dict(request.config) if isinstance(request.config, dict) else {}
    app_id = str(normalized_config.get("app_id", "")).strip() or None
    previous_app_id = str(request.previous_app_id or "").strip() or None
    if not app_id:
        raise HTTPException(status_code=400, detail="app_id is required")
    normalized_config["app_id"] = app_id

    if normalized_platform in SUPPORTED_TOKEN_PLATFORMS:
        try:
            normalized_config, _ = bootstrap_tokens_for_config(normalized_platform, normalized_config)
        except Exception as exc:
            raise HTTPException(status_code=502, detail="token bootstrap failed: {0}".format(exc)) from exc

    try:
        sync_account_to_credentials_file(
            platform=normalized_platform,
            account_name=normalized_name,
            config=normalized_config,
        )
        if previous_app_id and previous_app_id != app_id:
            delete_accounts_from_credentials_file(app_ids=[previous_app_id])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    has_secret_key = bool(str(normalized_config.get("secret_key", "")).strip()) or bool(str(normalized_config.get("secret", "")).strip())
    token_block = normalized_config.get("token")
    has_access_token = False
    if isinstance(token_block, dict):
        has_access_token = bool(str(token_block.get("access_token", "")).strip())
    if not has_access_token:
        has_access_token = bool(str(normalized_config.get("access_token", "")).strip()) or bool(str(normalized_config.get("advertiser_access_token", "")).strip())

    return CredentialSourceUpsertResponse(
        platform=normalized_platform,
        name=normalized_name,
        source_path="webapp_accounts.{0}.accounts_by_app_id.{1}".format(normalized_platform, app_id),
        app_id=app_id,
        has_secret_key=has_secret_key,
        has_access_token=has_access_token,
    )


@router.get("/credentials/source/item", response_model=CredentialSourceItemDetailResponse)
def get_credential_source_item_api(
    app_id: str = Query(..., min_length=1),
    refresh: bool = Query(default=False),
) -> CredentialSourceItemDetailResponse:
    target = find_credential_entry_by_app_id(app_id=app_id, refresh=refresh)
    if target is None:
        raise HTTPException(status_code=404, detail="Credential source item not found")
    return CredentialSourceItemDetailResponse(
        source_path=target.source_path,
        platform=normalize_platform(target.platform),
        name=target.name,
        app_id=target.app_id,
        config=target.config,
    )


@router.post("/credentials/source/delete", response_model=CredentialSourceBatchDeleteResponse)
def delete_credential_source_items_api(request: CredentialSourceBatchDeleteRequest) -> CredentialSourceBatchDeleteResponse:
    deleted = delete_accounts_from_credentials_file(app_ids=request.app_ids)
    return CredentialSourceBatchDeleteResponse(total=len(request.app_ids), deleted=deleted)


@router.post("/credentials/source/token/refresh", response_model=CredentialSourceTokenRefreshResponse)
def refresh_credential_source_token_api(request: CredentialSourceTokenRefreshRequest) -> CredentialSourceTokenRefreshResponse:
    app_id = str(request.app_id).strip()
    if not app_id:
        raise HTTPException(status_code=400, detail="app_id is required")

    current = find_credential_entry_by_app_id(app_id=app_id, refresh=True)
    if current is None:
        raise HTTPException(status_code=404, detail="Credential source item not found")

    normalized_platform = normalize_platform(current.platform)
    if normalized_platform not in SUPPORTED_TOKEN_PLATFORMS:
        raise HTTPException(
            status_code=400,
            detail="更新失败：当前平台不支持 Token 强制更新",
        )

    current_config = dict(current.config) if isinstance(current.config, dict) else {}
    current_config["app_id"] = app_id
    try:
        refreshed_config, status = bootstrap_tokens_for_config(normalized_platform, current_config)
    except Exception as exc:
        raise HTTPException(status_code=502, detail="更新失败：{0}".format(exc)) from exc

    if status != "refreshed":
        raise HTTPException(status_code=400, detail="更新失败：{0}".format(status))

    sync_account_to_credentials_file(
        platform=normalized_platform,
        account_name=current.name,
        config=refreshed_config,
    )

    token_block = refreshed_config.get("token")
    token_status = None
    token_updated_at = None
    has_access_token = False
    access_token = None
    refresh_token = None
    if isinstance(token_block, dict):
        token_status = str(token_block.get("token_status", "")).strip() or None
        token_updated_at = str(token_block.get("last_refresh_at", "")).strip() or None
        access_token = str(token_block.get("access_token", "")).strip() or None
        refresh_token = str(token_block.get("refresh_token", "")).strip() or None
        has_access_token = bool(access_token)

    if not has_access_token:
        access_token = (
            str(refreshed_config.get("access_token", "")).strip()
            or str(refreshed_config.get("advertiser_access_token", "")).strip()
            or None
        )
        refresh_token = str(refreshed_config.get("refresh_token", "")).strip() or refresh_token
        has_access_token = bool(access_token)

    return CredentialSourceTokenRefreshResponse(
        app_id=app_id,
        platform=normalized_platform,
        name=current.name,
        has_access_token=has_access_token,
        access_token=access_token,
        refresh_token=refresh_token,
        token_status=token_status or ("ready" if has_access_token else "missing"),
        token_updated_at=token_updated_at,
    )


@router.post("/credentials/source/sync", response_model=CredentialSourceSyncResponse)
def sync_credential_source_api(
    refresh: bool = Query(default=True),
    db: Session | None = Depends(get_db),
) -> CredentialSourceSyncResponse:
    if db is None:
        return CredentialSourceSyncResponse(total=0, created=0, updated=0, accounts=[])
    created, updated, accounts = sync_accounts_from_credential_source(db, refresh=refresh)
    return CredentialSourceSyncResponse(
        total=created + updated,
        created=created,
        updated=updated,
        accounts=[
            AccountSummary(
                id=item.id,
                name=item.name,
                platform=item.platform,
                status=item.status,
                app_id=item.app_id,
                created_at=item.created_at,
                updated_at=item.updated_at,
            )
            for item in accounts
        ],
    )


@router.get("/{account_id}", response_model=AccountDetail)
def get_account_api(account_id: int, db: Session | None = Depends(get_db)) -> AccountDetail:
    db = _require_db(db)
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account = ensure_account_credentials(db, account)

    return AccountDetail(
        id=account.id,
        name=account.name,
        platform=account.platform,
        status=account.status,
        app_id=account.app_id,
        created_at=account.created_at,
        updated_at=account.updated_at,
        config=decode_account_config(account),
        secret_key_masked=get_secret_key_masked(account),
        ip_whitelist=decode_ip_whitelist(account),
        credential_updated_at=account.credential_updated_at,
    )


@router.patch("/{account_id}", response_model=AccountDetail)
def update_account_api(
    account_id: int,
    request: AccountUpdateRequest,
    db: Session | None = Depends(get_db),
) -> AccountDetail:
    db = _require_db(db)
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account = ensure_account_credentials(db, account)

    account = update_account(
        db,
        account,
        name=request.name,
        status=request.status,
        config=request.config,
    )
    return AccountDetail(
        id=account.id,
        name=account.name,
        platform=account.platform,
        status=account.status,
        app_id=account.app_id,
        created_at=account.created_at,
        updated_at=account.updated_at,
        config=decode_account_config(account),
        secret_key_masked=get_secret_key_masked(account),
        ip_whitelist=decode_ip_whitelist(account),
        credential_updated_at=account.credential_updated_at,
    )


@router.post("/{account_id}/disable", response_model=AccountSummary)
def disable_account_api(account_id: int, db: Session | None = Depends(get_db)) -> AccountSummary:
    db = _require_db(db)
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account = ensure_account_credentials(db, account)

    account = update_account(db, account, status="disabled")
    return AccountSummary(
        id=account.id,
        name=account.name,
        platform=account.platform,
        status=account.status,
        app_id=account.app_id,
        created_at=account.created_at,
        updated_at=account.updated_at,
    )


@router.get("/{account_id}/credentials", response_model=AccountCredentialsView)
def get_account_credentials_api(account_id: int, db: Session | None = Depends(get_db)) -> AccountCredentialsView:
    db = _require_db(db)
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account = ensure_account_credentials(db, account)
    return AccountCredentialsView(
        app_id=account.app_id or "",
        secret_key_masked=get_secret_key_masked(account),
        ip_whitelist=decode_ip_whitelist(account),
        credential_updated_at=account.credential_updated_at,
    )


@router.post("/{account_id}/credentials/reset", response_model=AccountCredentialsResetResponse)
def reset_account_credentials_api(account_id: int, db: Session | None = Depends(get_db)) -> AccountCredentialsResetResponse:
    db = _require_db(db)
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account = ensure_account_credentials(db, account)
    account, issued_secret_key = reset_account_secret_key(db, account)
    return AccountCredentialsResetResponse(
        app_id=account.app_id or "",
        issued_secret_key=issued_secret_key,
        credential_updated_at=account.credential_updated_at,
    )


@router.put("/{account_id}/ip-whitelist", response_model=AccountCredentialsView)
def update_account_ip_whitelist_api(
    account_id: int,
    request: AccountIPWhitelistUpdateRequest,
    db: Session | None = Depends(get_db),
) -> AccountCredentialsView:
    db = _require_db(db)
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account = ensure_account_credentials(db, account)
    account = update_ip_whitelist(db, account, request.ip_whitelist)
    return AccountCredentialsView(
        app_id=account.app_id or "",
        secret_key_masked=get_secret_key_masked(account),
        ip_whitelist=decode_ip_whitelist(account),
        credential_updated_at=account.credential_updated_at,
    )
