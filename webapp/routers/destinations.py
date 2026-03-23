from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from webapp.config import get_storage_root
from webapp.deps import require_db
from webapp.db import get_db
from webapp.error_messages import (
    DESTINATION_PROFILE_NOT_FOUND,
    FILE_NOT_FOUND,
    INVALID_FILE_NAME,
    INVALID_FILE_PATH,
    ONLY_MANAGED_LOCAL_DESTINATION_SUPPORTS_FILE_EXPLORER,
)
from webapp.models import DestinationProfile
from webapp.schemas import (
    DestinationDeleteResponse,
    DestinationFileListResponse,
    DestinationProfileCreateRequest,
    DestinationTestRequest,
    DestinationTestResponse,
    DestinationProfileUpdateRequest,
    DestinationProfileView,
    StorageRetentionRunSummary,
    StorageRetentionSettingsUpdateRequest,
    StorageRetentionSettingsView,
)
from webapp.services.destinations import (
    create_destination_profile,
    delete_destination_profile,
    list_managed_destination_files,
    list_destination_profiles,
    test_destination,
    update_destination_profile,
)
from webapp.services.storage_retention import (
    get_storage_retention_settings,
    run_storage_retention_once,
    update_storage_retention_settings,
)


router = APIRouter(prefix="/destinations", tags=["destinations"])

@router.get("", response_model=list[DestinationProfileView])
def list_destinations_api(db: Session | None = Depends(get_db)) -> list[DestinationProfileView]:
    db = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    return list_destination_profiles(db)


@router.post("", response_model=DestinationProfileView)
def create_destination_api(
    request: DestinationProfileCreateRequest,
    db: Session | None = Depends(get_db),
) -> DestinationProfileView:
    db = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    return create_destination_profile(db, request)


@router.post("/test", response_model=DestinationTestResponse)
def test_destination_api(
    request: DestinationTestRequest,
    db: Session | None = Depends(get_db),
) -> DestinationTestResponse:
    _ = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    result = test_destination(request)
    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)
    return result


@router.get("/retention/settings", response_model=StorageRetentionSettingsView)
def get_retention_settings_api() -> StorageRetentionSettingsView:
    return get_storage_retention_settings()


@router.put("/retention/settings", response_model=StorageRetentionSettingsView)
def update_retention_settings_api(
    request: StorageRetentionSettingsUpdateRequest,
) -> StorageRetentionSettingsView:
    return update_storage_retention_settings(request)


@router.post("/retention/run", response_model=StorageRetentionRunSummary)
def run_retention_once_api() -> StorageRetentionRunSummary:
    return run_storage_retention_once(force=True)


@router.put("/{profile_id}", response_model=DestinationProfileView)
def update_destination_api(
    profile_id: int,
    request: DestinationProfileUpdateRequest,
    db: Session | None = Depends(get_db),
) -> DestinationProfileView:
    db = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    profile = db.get(DestinationProfile, profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail=DESTINATION_PROFILE_NOT_FOUND)
    return update_destination_profile(db, profile, request)


@router.delete("/{profile_id}", response_model=DestinationDeleteResponse)
def delete_destination_api(
    profile_id: int,
    purge_files: bool = Query(default=False),
    db: Session | None = Depends(get_db),
) -> DestinationDeleteResponse:
    db = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    profile = db.get(DestinationProfile, profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail=DESTINATION_PROFILE_NOT_FOUND)
    try:
        return delete_destination_profile(db, profile, purge_files=purge_files)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{profile_id}/files", response_model=DestinationFileListResponse)
def list_destination_files_api(
    profile_id: int,
    db: Session | None = Depends(get_db),
) -> DestinationFileListResponse:
    db = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    profile = db.get(DestinationProfile, profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail=DESTINATION_PROFILE_NOT_FOUND)
    if str(profile.destination_type).strip().lower() != "managed_local_file":
        raise HTTPException(status_code=400, detail=ONLY_MANAGED_LOCAL_DESTINATION_SUPPORTS_FILE_EXPLORER)
    try:
        listing = list_managed_destination_files(profile)
        return DestinationFileListResponse(
            profile_id=listing.profile_id,
            profile_name=listing.profile_name,
            relative_path=listing.relative_path,
            files=listing.files,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{profile_id}/files/download")
def download_destination_file_api(
    profile_id: int,
    name: str = Query(..., min_length=1),
    db: Session | None = Depends(get_db),
) -> FileResponse:
    db = require_db(db, detail="Database is disabled. Destination APIs are unavailable.")
    profile = db.get(DestinationProfile, profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail=DESTINATION_PROFILE_NOT_FOUND)
    if str(profile.destination_type).strip().lower() != "managed_local_file":
        raise HTTPException(status_code=400, detail=ONLY_MANAGED_LOCAL_DESTINATION_SUPPORTS_FILE_EXPLORER)

    listing = list_managed_destination_files(profile)
    target_name = Path(name).name
    if target_name != name:
        raise HTTPException(status_code=400, detail=INVALID_FILE_NAME)
    if target_name not in {x.name for x in listing.files}:
        raise HTTPException(status_code=404, detail=FILE_NOT_FOUND)

    root = get_storage_root().resolve()
    file_path = (Path(listing.absolute_path) / target_name).resolve()
    if not file_path.is_relative_to(root):
        raise HTTPException(status_code=400, detail=INVALID_FILE_PATH)
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail=FILE_NOT_FOUND)
    return FileResponse(path=str(file_path), filename=target_name, media_type="application/octet-stream")
