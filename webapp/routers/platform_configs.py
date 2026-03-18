from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from webapp.db import get_db
from webapp.models import PlatformAccount
from webapp.schemas import PlatformConfigCreateRequest, PlatformConfigDeleteResponse, PlatformConfigItem
from webapp.services.platform_alias import normalize_platform
from webapp.services.platform_configs import create_platform_config, delete_platform_config, list_platform_configs


router = APIRouter(prefix="/platform-configs", tags=["platform-configs"])


@router.get("", response_model=list[PlatformConfigItem])
def list_platform_configs_api() -> list[PlatformConfigItem]:
    return [PlatformConfigItem(**item) for item in list_platform_configs()]


@router.post("", response_model=PlatformConfigItem)
def create_platform_config_api(request: PlatformConfigCreateRequest) -> PlatformConfigItem:
    try:
        item = create_platform_config(
            platform=request.platform,
            label=request.label,
            helper=request.helper,
            docs_url=request.docs_url,
            status=request.status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PlatformConfigItem(**item)


@router.delete("/{platform}", response_model=PlatformConfigDeleteResponse)
def delete_platform_config_api(platform: str, db: Session | None = Depends(get_db)) -> PlatformConfigDeleteResponse:
    normalized_platform = normalize_platform(platform)
    if not normalized_platform:
        raise HTTPException(status_code=400, detail="platform is required")

    used_platforms: set[str] = set()
    if db is not None:
        used_platforms = {
            normalize_platform(str(x.platform))
            for x in db.query(PlatformAccount.platform).distinct().all()
            if normalize_platform(str(x.platform))
        }

    try:
        deleted = delete_platform_config(platform=normalized_platform, used_platforms=used_platforms)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PlatformConfigDeleteResponse(platform=normalized_platform, deleted=deleted)
