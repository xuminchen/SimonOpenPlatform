from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


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


class CredentialSourceTokenRefreshBatchRequest(BaseModel):
    app_ids: list[str] = Field(default_factory=list)


class CredentialSourceTokenRefreshBatchItem(BaseModel):
    app_id: str
    ok: bool
    message: str = ""
    result: Optional[CredentialSourceTokenRefreshResponse] = None


class CredentialSourceTokenRefreshBatchResponse(BaseModel):
    total: int
    refreshed: int
    failed: int
    items: list[CredentialSourceTokenRefreshBatchItem]


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


class PlatformConfigUpdateRequest(BaseModel):
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


class ConnectionTestRequest(BaseModel):
    platform_code: str = Field(..., min_length=1, max_length=64)
    credential_id: Optional[int] = None
    app_id: Optional[str] = None


class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    latency_ms: int


class ConnectionSchemaField(BaseModel):
    name: str
    type: str
    primary_key: bool = False
    cursor_candidate: bool = False


class ConnectionSchemaItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    stream_name: str
    description: str = ""
    supported_sync_modes: list[str] = Field(default_factory=list)
    default_cursor_field: list[str] = Field(default_factory=list)
    source_defined_cursor: str = ""
    schema_payload: Dict[str, Any] = Field(
        validation_alias="schema",
        serialization_alias="schema",
    )


class ConnectionSchemaResponse(BaseModel):
    platform_code: str
    streams: list[ConnectionSchemaItem]


class DynamicSchemaField(BaseModel):
    name: str
    path: list[str] = Field(default_factory=list)
    type: str = "STRING"
    source_type: str = "Unknown"
    is_primary_key: bool = False
    is_cursor_field: bool = False
    selected: bool = True
    is_new: bool = False


class DynamicSchemaContract(BaseModel):
    stream_name: str
    description: str = ""
    discovered_at: str
    fields: list[DynamicSchemaField] = Field(default_factory=list)


class DynamicSchemaDiscoverRequest(BaseModel):
    platform_code: str = Field(..., min_length=1, max_length=64)
    stream_name: str = Field(..., min_length=1, max_length=128)
    credential_id: Optional[int] = None
    app_id: Optional[str] = None


class ProjectDynamicSchemaDiscoverRequest(BaseModel):
    stream_name: str = Field(..., min_length=1, max_length=128)


class SyncConnectionStreamCreate(BaseModel):
    stream_name: str = Field(..., min_length=1, max_length=128)
    sync_mode: str = Field(default="INCREMENTAL")
    cursor_field: str = ""
    primary_key: str = ""


class SyncConnectionCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    platform_code: str = Field(..., min_length=1, max_length=64)
    credential_id: Optional[int] = None
    app_id: Optional[str] = None
    app_ids: list[str] = Field(default_factory=list)
    schedule_cron: str = Field(..., min_length=1, max_length=64)
    destination: str = Field(default="ClickHouse_DW", min_length=1, max_length=128)
    status: int = Field(default=1, ge=0, le=2)
    streams: list[SyncConnectionStreamCreate] = Field(default_factory=list)


class SyncConnectionStreamView(BaseModel):
    id: int
    stream_name: str
    sync_mode: str
    cursor_field: str
    primary_key: str


class SyncConnectionView(BaseModel):
    id: int
    name: str
    platform_code: str
    credential_id: Optional[int] = None
    app_id: Optional[str] = None
    app_ids: list[str] = Field(default_factory=list)
    schedule_cron: str
    destination: str
    status: int
    last_sync_time: Optional[datetime] = None
    last_sync_status: str
    created_at: datetime
    updated_at: datetime
    streams: list[SyncConnectionStreamView] = Field(default_factory=list)


class SyncConnectionBatchStatusUpdateRequest(BaseModel):
    connection_ids: list[int] = Field(default_factory=list)
    status: int = Field(..., ge=0, le=2)


class SyncConnectionBatchStatusUpdateResponse(BaseModel):
    total: int
    updated: int


class SyncConnectionBatchDeleteRequest(BaseModel):
    connection_ids: list[int] = Field(default_factory=list)


class SyncConnectionBatchDeleteResponse(BaseModel):
    total: int
    deleted: int


class DestinationProfileCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    engine_category: str = Field(..., min_length=1, max_length=32)
    destination_type: str = Field(..., min_length=1, max_length=64)
    status: str = Field(default="active")
    config: Dict[str, Any] = Field(default_factory=dict)


class DestinationProfileUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    engine_category: Optional[str] = Field(default=None, min_length=1, max_length=32)
    destination_type: Optional[str] = Field(default=None, min_length=1, max_length=64)
    status: Optional[str] = Field(default=None)
    config: Optional[Dict[str, Any]] = None


class DestinationProfileView(BaseModel):
    id: int
    name: str
    engine_category: str
    destination_type: str
    status: str
    config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class DestinationTestRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    destination_type: str = Field(..., min_length=1, max_length=64)
    config: Dict[str, Any] = Field(default_factory=dict)


class DestinationTestResponse(BaseModel):
    success: bool
    message: str
    normalized_path: str = ""


class DestinationFileItem(BaseModel):
    name: str
    size_bytes: int
    modified_at: datetime


class DestinationFileListResponse(BaseModel):
    profile_id: int
    profile_name: str
    relative_path: str
    files: list[DestinationFileItem] = Field(default_factory=list)


class DestinationDeleteResponse(BaseModel):
    deleted: bool
    profile_id: int
    profile_name: str
    files_deleted: bool = False
    message: str = ""


class StorageRetentionSettingsView(BaseModel):
    enabled: bool = False
    retention_days: int = Field(default=30, ge=1, le=3650)


class StorageRetentionSettingsUpdateRequest(BaseModel):
    enabled: bool
    retention_days: int = Field(default=30, ge=1, le=3650)


class StorageRetentionRunSummary(BaseModel):
    ok: bool = True
    enabled: bool = False
    retention_days: int = 30
    scanned_files: int = 0
    deleted_files: int = 0
    deleted_dirs: int = 0
    message: str = ""


class SyncProjectCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    platform_code: str = Field(..., min_length=1, max_length=64)
    credential_id: Optional[int] = None
    app_id: Optional[str] = None
    app_ids: list[str] = Field(default_factory=list)
    schedule_cron: str = Field(default="0 * * * *", min_length=1, max_length=64)
    destination: str = Field(default="ClickHouse_DW", min_length=1, max_length=128)
    status: int = Field(default=1, ge=0, le=2)


class SyncProjectView(BaseModel):
    id: int
    name: str
    platform_code: str
    credential_id: Optional[int] = None
    app_id: Optional[str] = None
    app_ids: list[str] = Field(default_factory=list)
    schedule_cron: str
    destination: str
    status: int
    created_at: datetime
    updated_at: datetime


class SyncStreamTaskCreate(BaseModel):
    stream_name: str = Field(..., min_length=1, max_length=128)
    sync_mode: str = Field(default="INCREMENTAL")
    cursor_field: str = ""
    primary_key: str = ""
    schema_contract: Optional[Dict[str, Any]] = None
    routine_cron: str = Field(default="0 * * * *", min_length=1, max_length=64)


class SyncStreamTaskBatchCreateRequest(BaseModel):
    streams: list[SyncStreamTaskCreate] = Field(default_factory=list)


class SyncStreamTaskView(BaseModel):
    id: int
    project_id: int
    stream_name: str
    sync_mode: str
    cursor_field: str
    primary_key: str
    schema_contract: Dict[str, Any] = Field(default_factory=dict)
    routine_cron: str
    last_cursor_value: str = ""
    last_routine_status: str
    last_routine_started_at: Optional[datetime] = None
    last_routine_finished_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class SyncExecutionSubmitRoutineRequest(BaseModel):
    stream_task_ids: list[int] = Field(default_factory=list)
    triggered_by: str = Field(default="system")


class SyncExecutionSubmitBackfillRequest(BaseModel):
    stream_task_ids: list[int] = Field(default_factory=list)
    start_time: str = Field(..., min_length=1, max_length=32)
    end_time: str = Field(..., min_length=1, max_length=32)
    triggered_by: str = Field(default="user")


class SyncExecutionInstanceView(BaseModel):
    id: int
    project_id: int
    stream_task_id: Optional[int] = None
    execution_type: str
    status: str
    start_time: str
    end_time: str
    triggered_by: str
    request_payload: Dict[str, Any]
    result_payload: Dict[str, Any]
    error_message: str
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class StreamPreviewResponse(BaseModel):
    project_id: int
    stream_name: str
    destination: str
    execution_id: Optional[int] = None
    execution_status: str = ""
    execution_error_message: str = ""
    execution_finished_at: Optional[datetime] = None
    total_records: int = 0
    returned_records: int = 0
    columns: list[str] = Field(default_factory=list)
    rows: list[Dict[str, Any]] = Field(default_factory=list)
    raw_response: Dict[str, Any] = Field(default_factory=dict)


class BuilderAuthStrategy(BaseModel):
    type: str = Field(default="None")
    inject_into: str = Field(default="header")
    key_name: str = Field(default="")
    test_variable: str = Field(default="token")


class BuilderPaginationStrategy(BaseModel):
    type: str = Field(default="none")
    cursor_path: str = Field(default="")
    inject_param: str = Field(default="")


class BuilderExtractionStrategy(BaseModel):
    record_selector: str = Field(default="$.data.list")


class BuilderRequestConfig(BaseModel):
    url_base: str = Field(default="")
    url_path: str = Field(default="")
    method: str = Field(default="GET")
    headers: Dict[str, Any] = Field(default_factory=dict)
    query_params: Dict[str, Any] = Field(default_factory=dict)
    body: Dict[str, Any] = Field(default_factory=dict)


class BuilderTestRequest(BaseModel):
    platform_code: str = Field(default="", max_length=64)
    stream_name: str = Field(..., min_length=1, max_length=128)
    request_config: BuilderRequestConfig
    auth_strategy: BuilderAuthStrategy = Field(default_factory=BuilderAuthStrategy)
    pagination_strategy: BuilderPaginationStrategy = Field(default_factory=BuilderPaginationStrategy)
    extraction_strategy: BuilderExtractionStrategy = Field(default_factory=BuilderExtractionStrategy)
    test_variables: Dict[str, Any] = Field(default_factory=dict)


class BuilderSchemaField(BaseModel):
    name: str
    path: list[str] = Field(default_factory=list)
    type: str
    source_type: str


class BuilderTestResponse(BaseModel):
    request_preview: Dict[str, Any] = Field(default_factory=dict)
    raw_response: Dict[str, Any] = Field(default_factory=dict)
    extracted_records: list[Dict[str, Any]] = Field(default_factory=list)
    inferred_schema: list[BuilderSchemaField] = Field(default_factory=list)


class BuilderStreamPublishRequest(BaseModel):
    platform_code: str = Field(..., min_length=1, max_length=64)
    stream_name: str = Field(..., min_length=1, max_length=128)
    display_name: str = Field(default="", max_length=128)
    doc_url: str = Field(default="", max_length=1024)
    request_config: BuilderRequestConfig
    auth_strategy: BuilderAuthStrategy = Field(default_factory=BuilderAuthStrategy)
    pagination_strategy: BuilderPaginationStrategy = Field(default_factory=BuilderPaginationStrategy)
    extraction_strategy: BuilderExtractionStrategy = Field(default_factory=BuilderExtractionStrategy)
    supported_sync_modes: list[str] = Field(default_factory=lambda: ["full_refresh", "incremental"])


class BuilderStreamView(BaseModel):
    id: int
    platform_code: str
    stream_name: str
    display_name: str
    doc_url: str
    request_config: Dict[str, Any]
    auth_strategy: Dict[str, Any]
    pagination_strategy: Dict[str, Any]
    extraction_strategy: Dict[str, Any]
    supported_sync_modes: list[str] = Field(default_factory=list)
    status: str
    created_at: datetime
    updated_at: datetime


class AccountStreamsUpdateRequest(BaseModel):
    streams: list[str] = Field(default_factory=list)


class AccountStreamsResponse(BaseModel):
    account_id: int
    streams: list[str] = Field(default_factory=list)


class AppSettingsView(BaseModel):
    db_enabled_runtime: bool = False
    db_enabled_next: bool = False
    database_url_runtime: str = ""
    database_url_next: str = ""
    db_enabled_source: str = "default"
    database_url_source: str = "default"
    restart_required: bool = True


class AppSettingsUpdateRequest(BaseModel):
    db_enabled: bool
    database_url: str = ""
