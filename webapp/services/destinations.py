from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import uuid

from sqlalchemy.orm import Session

from webapp.models import DestinationProfile, SyncConnection, SyncProject
from webapp.config import get_storage_root
from webapp.schemas import (
    DestinationProfileCreateRequest,
    DestinationProfileUpdateRequest,
    DestinationProfileView,
    DestinationFileItem,
    DestinationFileListResponse,
    DestinationDeleteResponse,
    DestinationTestRequest,
    DestinationTestResponse,
)


@dataclass
class ManagedDestinationListing:
    profile_id: int
    profile_name: str
    relative_path: str
    absolute_path: str
    files: list[DestinationFileItem]


def _safe_load_config(text: str) -> dict:
    try:
        payload = json.loads(text or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _slugify_name(name: str) -> str:
    raw = str(name or "").strip().lower()
    if not raw:
        return "default_destination"
    slug = re.sub(r"\s+", "_", raw)
    slug = re.sub(r"[^\w\-\u4e00-\u9fff]+", "_", slug)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "default_destination"


def _managed_relative_path(name: str) -> str:
    return "destinations/{0}".format(_slugify_name(name))


def _ensure_storage_root() -> Path:
    root = get_storage_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _to_view(item: DestinationProfile) -> DestinationProfileView:
    return DestinationProfileView(
        id=item.id,
        name=item.name,
        engine_category=item.engine_category,
        destination_type=item.destination_type,
        status=item.status,
        config=_safe_load_config(item.config_json),
        created_at=item.created_at,
        updated_at=item.updated_at,
    )


def list_destination_profiles(db: Session) -> list[DestinationProfileView]:
    rows = db.query(DestinationProfile).order_by(DestinationProfile.id.desc()).all()
    return [_to_view(item) for item in rows]


def create_destination_profile(db: Session, request: DestinationProfileCreateRequest) -> DestinationProfileView:
    existing = db.query(DestinationProfile).filter(DestinationProfile.name == request.name).first()
    if existing is not None:
        existing.engine_category = request.engine_category
        existing.destination_type = request.destination_type
        existing.status = request.status
        existing.config_json = json.dumps(request.config or {}, ensure_ascii=False)
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return _to_view(existing)

    row = DestinationProfile(
        name=request.name,
        engine_category=request.engine_category,
        destination_type=request.destination_type,
        status=request.status,
        config_json=json.dumps(request.config or {}, ensure_ascii=False),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _to_view(row)


def update_destination_profile(db: Session, profile: DestinationProfile, request: DestinationProfileUpdateRequest) -> DestinationProfileView:
    if request.name is not None:
        profile.name = request.name
    if request.engine_category is not None:
        profile.engine_category = request.engine_category
    if request.destination_type is not None:
        profile.destination_type = request.destination_type
    if request.status is not None:
        profile.status = request.status
    if request.config is not None:
        profile.config_json = json.dumps(request.config, ensure_ascii=False)

    db.add(profile)
    db.commit()
    db.refresh(profile)
    return _to_view(profile)


def test_destination(request: DestinationTestRequest) -> DestinationTestResponse:
    destination_type = str(request.destination_type or "").strip().lower()
    config = request.config if isinstance(request.config, dict) else {}

    if destination_type not in {"local_file", "managed_local_file"}:
        return DestinationTestResponse(success=True, message="Connection test passed", normalized_path="")

    if destination_type == "managed_local_file":
        target_name = str(request.name or config.get("profile_name") or "").strip()
        if not target_name:
            return DestinationTestResponse(success=False, message="destination name is required", normalized_path="")
        root = _ensure_storage_root()
        path = root / _managed_relative_path(target_name)
    else:
        raw_path = str(config.get("local_root_path") or config.get("local_path") or "").strip()
        if not raw_path:
            return DestinationTestResponse(success=False, message="local_root_path is required", normalized_path="")
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            return DestinationTestResponse(success=False, message="local_root_path must be an absolute path", normalized_path="")

    try:
        path.mkdir(parents=True, exist_ok=True)
        probe_name = ".wl_probe_{0}.tmp".format(uuid.uuid4().hex)
        probe_file = path / probe_name
        fd = os.open(str(probe_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, b"wonderlab_destination_probe\n")
        finally:
            os.close(fd)
        probe_file.unlink(missing_ok=True)
    except PermissionError:
        return DestinationTestResponse(
            success=False,
            message="Destination path is not writable: permission denied",
            normalized_path=str(path),
        )
    except Exception as exc:
        return DestinationTestResponse(
            success=False,
            message="Destination path validation failed: {0}".format(exc),
            normalized_path=str(path),
        )

    return DestinationTestResponse(
        success=True,
        message="Destination path is writable",
        normalized_path=str(path),
    )


def list_managed_destination_files(profile: DestinationProfile) -> ManagedDestinationListing:
    config = _safe_load_config(profile.config_json)
    relative = str(config.get("managed_relative_path") or "").strip()
    if not relative:
        relative = _managed_relative_path(profile.name)

    root = _ensure_storage_root().resolve()
    base_path = (root / relative).resolve()
    if not base_path.is_relative_to(root):
        raise ValueError("invalid managed path")

    files: list[DestinationFileItem] = []
    if base_path.exists() and base_path.is_dir():
        for item in base_path.iterdir():
            if not item.is_file():
                continue
            suffix = item.suffix.lower()
            if suffix not in {".jsonl", ".json", ".csv"}:
                continue
            stat = item.stat()
            files.append(
                DestinationFileItem(
                    name=item.name,
                    size_bytes=int(stat.st_size),
                    modified_at=datetime.fromtimestamp(stat.st_mtime),
                )
            )
    files.sort(key=lambda x: x.modified_at, reverse=True)

    return ManagedDestinationListing(
        profile_id=profile.id,
        profile_name=profile.name,
        relative_path=relative,
        absolute_path=str(base_path),
        files=files,
    )


def delete_destination_profile(
    db: Session,
    profile: DestinationProfile,
    *,
    purge_files: bool = False,
) -> DestinationDeleteResponse:
    destination_name = str(profile.name or "").strip()
    if not destination_name:
        raise ValueError("invalid destination name")

    active_connection = (
        db.query(SyncConnection)
        .filter(SyncConnection.destination == destination_name, SyncConnection.status == 1)
        .order_by(SyncConnection.id.asc())
        .first()
    )
    if active_connection is not None:
        raise ValueError(
            "该目标库正在被『{0}』使用，请先暂停或修改对应任务后再删除。".format(active_connection.name)
        )

    active_project = (
        db.query(SyncProject)
        .filter(SyncProject.destination == destination_name, SyncProject.status == 1)
        .order_by(SyncProject.id.asc())
        .first()
    )
    if active_project is not None:
        raise ValueError(
            "该目标库正在被『{0}』使用，请先暂停或修改对应任务后再删除。".format(active_project.name)
        )

    files_deleted = False
    if purge_files and str(profile.destination_type or "").strip().lower() == "managed_local_file":
        listing = list_managed_destination_files(profile)
        target = Path(listing.absolute_path).resolve()
        root = _ensure_storage_root().resolve()
        if target.exists() and target.is_dir() and target.is_relative_to(root):
            shutil.rmtree(target)
            files_deleted = True

    profile_id = int(profile.id)
    profile_name = str(profile.name)
    db.delete(profile)
    db.commit()

    return DestinationDeleteResponse(
        deleted=True,
        profile_id=profile_id,
        profile_name=profile_name,
        files_deleted=files_deleted,
        message="Destination deleted",
    )
