from __future__ import annotations

from fastapi import APIRouter

from webapp.schemas import AppSettingsUpdateRequest, AppSettingsView
from webapp.services.settings import get_app_settings, update_app_settings


router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("", response_model=AppSettingsView)
def get_settings_api() -> AppSettingsView:
    return get_app_settings()


@router.put("", response_model=AppSettingsView)
def update_settings_api(request: AppSettingsUpdateRequest) -> AppSettingsView:
    return update_app_settings(request)
