from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class AccountCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    platform: str = Field(..., min_length=1, max_length=64)
    config: Dict[str, Any]
    status: str = Field(default="active")


class AccountUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    status: Optional[str] = Field(default=None)
    config: Optional[Dict[str, Any]] = None


class AccountSummary(BaseModel):
    id: int
    name: str
    platform: str
    status: str
    app_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class AccountDetail(AccountSummary):
    config: Dict[str, Any]
    secret_key_masked: Optional[str] = None
    ip_whitelist: list[str] = Field(default_factory=list)
    credential_updated_at: Optional[datetime] = None


class AccountCreateResponse(AccountSummary):
    issued_secret_key: str


class AccountCredentialsView(BaseModel):
    app_id: str
    secret_key_masked: str
    ip_whitelist: list[str]
    credential_updated_at: Optional[datetime] = None


class AccountCredentialsResetResponse(BaseModel):
    app_id: str
    issued_secret_key: str
    credential_updated_at: datetime


class AccountIPWhitelistUpdateRequest(BaseModel):
    ip_whitelist: list[str] = Field(default_factory=list)


class CredentialSourceEntry(BaseModel):
    row_no: int
    source_path: str
    platform: str
    name: str
    status: str = "partial"
    app_id: Optional[str] = None
    has_secret_key: bool = False
    has_access_token: bool = False
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_status: Optional[str] = None
    token_updated_at: Optional[str] = None


class CredentialSourceScanResponse(BaseModel):
    page: int
    page_size: int
    total_pages: int
    total: int
    platforms: list[str]
    entries: list[CredentialSourceEntry]


class CredentialSourceSyncResponse(BaseModel):
    total: int
    created: int
    updated: int
    accounts: list[AccountSummary]


class CredentialSourceUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    platform: str = Field(..., min_length=1, max_length=64)
    config: Dict[str, Any]
    status: str = Field(default="active")
    previous_app_id: Optional[str] = None


class CredentialSourceUpsertResponse(BaseModel):
    platform: str
    name: str
    source_path: str
    app_id: Optional[str] = None
    has_secret_key: bool = False
    has_access_token: bool = False


class CredentialSourceItemDetailResponse(BaseModel):
    source_path: str
    platform: str
    name: str
    app_id: Optional[str] = None
    config: Dict[str, Any]


class CredentialSourceBatchDeleteRequest(BaseModel):
    app_ids: list[str] = Field(default_factory=list)


class CredentialSourceBatchDeleteResponse(BaseModel):
    total: int
    deleted: int


class CredentialSourceTokenRefreshRequest(BaseModel):
    app_id: str = Field(..., min_length=1)


class CredentialSourceTokenRefreshResponse(BaseModel):
    app_id: str
    platform: str
    name: str
    has_access_token: bool = False
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_status: Optional[str] = None
    token_updated_at: Optional[str] = None


class PlatformConfigItem(BaseModel):
    platform: str
    label: str
    helper: str = ""
    docs_url: str = ""
    status: str = "active"
    mutable: bool = True


class PlatformConfigCreateRequest(BaseModel):
    platform: str = Field(..., min_length=1, max_length=64)
    label: str = Field(..., min_length=1, max_length=128)
    helper: str = ""
    docs_url: str = ""
    status: str = "active"


class PlatformConfigDeleteResponse(BaseModel):
    platform: str
    deleted: bool


class TaskCreateWechatOrdersRequest(BaseModel):
    account_id: int
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    time_type: str = Field(default="create_time")
    page_size: int = Field(default=50, ge=1, le=100)


class TaskCreateMetaReportRequest(BaseModel):
    account_id: int
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    level: str = Field(default="ad")
    dry_run: bool = Field(default=True)


class TaskSummary(BaseModel):
    id: int
    account_id: int
    task_type: str
    status: str
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]


class TaskDetail(TaskSummary):
    request_payload: Dict[str, Any]
    result_payload: Dict[str, Any]
    error_message: str
