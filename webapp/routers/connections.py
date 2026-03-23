from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from webapp.deps import require_db
from webapp.db import get_db
from webapp.error_messages import PLATFORM_CODE_REQUIRED, PROJECT_NOT_FOUND, platform_not_registered
from webapp.models import PlatformAccount
from webapp.schemas import (
    SyncConnectionBatchDeleteRequest,
    SyncConnectionBatchDeleteResponse,
    SyncConnectionBatchStatusUpdateRequest,
    SyncConnectionBatchStatusUpdateResponse,
    ConnectionSchemaItem,
    ConnectionSchemaResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    DynamicSchemaContract,
    DynamicSchemaDiscoverRequest,
    ProjectDynamicSchemaDiscoverRequest,
    SyncConnectionCreateRequest,
    SyncConnectionView,
    SyncExecutionSubmitBackfillRequest,
    SyncExecutionSubmitRoutineRequest,
    SyncExecutionInstanceView,
    StreamPreviewResponse,
    SyncProjectCreateRequest,
    SyncProjectView,
    SyncStreamTaskBatchCreateRequest,
    SyncStreamTaskView,
)
from webapp.services.accounts import decode_account_config
from webapp.services.connection_connectors import (
    discover_dynamic_schema_contract,
    get_connector,
    test_connection_with_latency_ms,
)
from webapp.services.connections import (
    add_stream_tasks,
    create_connection,
    create_project,
    delete_connections_batch,
    get_project,
    list_connections,
    list_executions,
    list_projects,
    get_stream_preview,
    project_app_ids,
    project_to_view,
    list_stream_tasks,
    submit_backfill,
    submit_routine,
    update_connections_status_batch,
)
from webapp.services.credential_source import find_credential_entry_by_app_id
from webapp.services.platform_alias import normalize_platform
from webapp.services.platform_configs import list_platform_configs


router = APIRouter(prefix="/connections", tags=["connections"])

def _ensure_registered_platform(platform_code: str) -> str:
    normalized = normalize_platform(platform_code)
    if not normalized:
        raise HTTPException(status_code=400, detail=PLATFORM_CODE_REQUIRED)
    allowed = {normalize_platform(str(item.get("platform", ""))) for item in list_platform_configs()}
    if normalized not in allowed:
        raise HTTPException(status_code=400, detail=platform_not_registered(normalized))
    return normalized


def _resolve_credential_config(db: Session, request: ConnectionTestRequest) -> dict:
    if request.credential_id is not None:
        account = db.get(PlatformAccount, request.credential_id)
        if account is not None:
            return decode_account_config(account)

    if request.app_id:
        item = find_credential_entry_by_app_id(request.app_id, refresh=True)
        if item is not None and isinstance(item.config, dict):
            return item.config

    return {}


def _must_get_project(db: Session, project_id: int):
    project = get_project(db, project_id)
    if project is None:
        raise HTTPException(status_code=404, detail=PROJECT_NOT_FOUND)
    return project


# Legacy compatibility endpoints.
@router.get("", response_model=list[SyncConnectionView])
def list_connections_api(db: Session | None = Depends(get_db)) -> list[SyncConnectionView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    return list_connections(db)


# Legacy compatibility endpoints.
@router.post("", response_model=SyncConnectionView)
def create_connection_api(request: SyncConnectionCreateRequest, db: Session | None = Depends(get_db)) -> SyncConnectionView:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    _ensure_registered_platform(request.platform_code)
    return create_connection(db, request)


@router.post("/batch/status", response_model=SyncConnectionBatchStatusUpdateResponse)
def update_connections_status_batch_api(
    request: SyncConnectionBatchStatusUpdateRequest,
    db: Session | None = Depends(get_db),
) -> SyncConnectionBatchStatusUpdateResponse:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    return update_connections_status_batch(db, connection_ids=request.connection_ids, status=request.status)


@router.post("/batch/delete", response_model=SyncConnectionBatchDeleteResponse)
def delete_connections_batch_api(
    request: SyncConnectionBatchDeleteRequest,
    db: Session | None = Depends(get_db),
) -> SyncConnectionBatchDeleteResponse:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    return delete_connections_batch(db, connection_ids=request.connection_ids)


@router.get("/projects", response_model=list[SyncProjectView])
def list_projects_api(db: Session | None = Depends(get_db)) -> list[SyncProjectView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    return list_projects(db)


@router.post("/projects", response_model=SyncProjectView)
def create_project_api(request: SyncProjectCreateRequest, db: Session | None = Depends(get_db)) -> SyncProjectView:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    request.platform_code = _ensure_registered_platform(request.platform_code)
    return create_project(db, request)


@router.get("/projects/{project_id}", response_model=SyncProjectView)
def get_project_api(project_id: int, db: Session | None = Depends(get_db)) -> SyncProjectView:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    project = _must_get_project(db, project_id)
    return project_to_view(project)


@router.get("/projects/{project_id}/streams", response_model=list[SyncStreamTaskView])
def list_project_streams_api(project_id: int, db: Session | None = Depends(get_db)) -> list[SyncStreamTaskView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    _must_get_project(db, project_id)
    return list_stream_tasks(db, project_id)


@router.post("/projects/{project_id}/streams", response_model=list[SyncStreamTaskView])
def add_project_streams_api(
    project_id: int,
    request: SyncStreamTaskBatchCreateRequest,
    db: Session | None = Depends(get_db),
) -> list[SyncStreamTaskView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    project = _must_get_project(db, project_id)
    return add_stream_tasks(db, project=project, request=request)


@router.get("/projects/{project_id}/streams/{stream_name}/preview", response_model=StreamPreviewResponse)
def get_project_stream_preview_api(
    project_id: int,
    stream_name: str,
    limit: int = Query(default=50, ge=1, le=500),
    db: Session | None = Depends(get_db),
) -> StreamPreviewResponse:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    project = _must_get_project(db, project_id)
    return get_stream_preview(db, project=project, stream_name=stream_name, limit=limit)


@router.get("/projects/{project_id}/executions", response_model=list[SyncExecutionInstanceView])
def list_project_executions_api(
    project_id: int,
    limit: int = Query(default=200, ge=1, le=500),
    db: Session | None = Depends(get_db),
) -> list[SyncExecutionInstanceView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    _must_get_project(db, project_id)
    return list_executions(db, project_id=project_id, limit=limit)


@router.post("/projects/{project_id}/executions/routine", response_model=list[SyncExecutionInstanceView])
def submit_routine_api(
    project_id: int,
    request: SyncExecutionSubmitRoutineRequest,
    db: Session | None = Depends(get_db),
) -> list[SyncExecutionInstanceView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    project = _must_get_project(db, project_id)
    return submit_routine(db, project=project, request=request)


@router.post("/projects/{project_id}/executions/backfill", response_model=list[SyncExecutionInstanceView])
def submit_backfill_api(
    project_id: int,
    request: SyncExecutionSubmitBackfillRequest,
    db: Session | None = Depends(get_db),
) -> list[SyncExecutionInstanceView]:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    project = _must_get_project(db, project_id)
    return submit_backfill(db, project=project, request=request)


@router.post("/test", response_model=ConnectionTestResponse)
def test_connection_api(request: ConnectionTestRequest, db: Session | None = Depends(get_db)) -> ConnectionTestResponse:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    normalized_platform = _ensure_registered_platform(request.platform_code)
    connector = get_connector(normalized_platform)

    credential = _resolve_credential_config(db, request)
    ok, latency_ms = test_connection_with_latency_ms(connector, credential)
    if ok:
        return ConnectionTestResponse(success=True, message="连接测试成功", latency_ms=latency_ms)
    return ConnectionTestResponse(success=False, message="连接失败：缺少可用 access_token", latency_ms=latency_ms)


@router.get("/schema", response_model=ConnectionSchemaResponse)
def get_schema_api(
    platform_code: str = Query(..., min_length=1),
    db: Session | None = Depends(get_db),
) -> ConnectionSchemaResponse:
    _ = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    normalized_platform = _ensure_registered_platform(platform_code)
    connector = get_connector(normalized_platform)

    streams = []
    for item in connector.discover_schema():
        stream_name = str(item.get("stream_name", "")).strip()
        if not stream_name:
            continue
        default_cursor_fields = item.get("default_cursor_field") or []
        source_defined_cursor = ""
        if isinstance(default_cursor_fields, list) and default_cursor_fields:
            source_defined_cursor = str(default_cursor_fields[0])

        streams.append(
            ConnectionSchemaItem(
                stream_name=stream_name,
                description=str(item.get("description", "")),
                supported_sync_modes=[str(x) for x in (item.get("supported_sync_modes") or [])],
                default_cursor_field=[str(x) for x in default_cursor_fields],
                source_defined_cursor=source_defined_cursor,
                schema_payload=item.get("schema") if isinstance(item.get("schema"), dict) else {"fields": []},
            )
        )

    return ConnectionSchemaResponse(platform_code=normalized_platform, streams=streams)


@router.post("/schema/discover", response_model=DynamicSchemaContract)
def discover_dynamic_schema_api(
    request: DynamicSchemaDiscoverRequest,
    db: Session | None = Depends(get_db),
) -> DynamicSchemaContract:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    normalized_platform = _ensure_registered_platform(request.platform_code)
    connector = get_connector(normalized_platform)
    credential = _resolve_credential_config(
        db,
        ConnectionTestRequest(
            platform_code=normalized_platform,
            credential_id=request.credential_id,
            app_id=request.app_id,
        ),
    )

    stream_meta = None
    for item in connector.discover_schema():
        if str(item.get("stream_name", "")).strip() == request.stream_name:
            stream_meta = item
            break

    contract = discover_dynamic_schema_contract(
        connector=connector,
        stream_name=request.stream_name,
        credential=credential,
        stream_meta=stream_meta if isinstance(stream_meta, dict) else {},
    )
    return DynamicSchemaContract(**contract)


@router.post("/projects/{project_id}/schema/discover", response_model=DynamicSchemaContract)
def discover_project_dynamic_schema_api(
    project_id: int,
    request: ProjectDynamicSchemaDiscoverRequest,
    db: Session | None = Depends(get_db),
) -> DynamicSchemaContract:
    db = require_db(db, detail="Database is disabled. Connection APIs are unavailable.")
    project = _must_get_project(db, project_id)
    normalized_platform = _ensure_registered_platform(project.platform_code)
    connector = get_connector(normalized_platform)

    candidate_app_ids: list[str] = []
    candidate_app_ids.extend(project_app_ids(project))
    if project.app_id:
        candidate_app_ids.append(str(project.app_id).strip())
    if project.credential_id is not None:
        account = db.get(PlatformAccount, project.credential_id)
        if account is not None and account.app_id:
            candidate_app_ids.append(str(account.app_id).strip())

    dedup_app_ids: list[str] = []
    seen: set[str] = set()
    for app_id in candidate_app_ids:
        if not app_id or app_id in seen:
            continue
        seen.add(app_id)
        dedup_app_ids.append(app_id)

    sampled_credential: dict = {}
    for app_id in dedup_app_ids:
        item = find_credential_entry_by_app_id(app_id, refresh=False)
        if item is None or not isinstance(item.config, dict):
            continue
        ok, _ = test_connection_with_latency_ms(connector, item.config)
        if ok:
            sampled_credential = item.config
            break
        if not sampled_credential:
            sampled_credential = item.config

    if not sampled_credential and project.credential_id is not None:
        account = db.get(PlatformAccount, project.credential_id)
        if account is not None:
            sampled_credential = decode_account_config(account)

    stream_meta = None
    for item in connector.discover_schema():
        if str(item.get("stream_name", "")).strip() == request.stream_name:
            stream_meta = item
            break

    contract = discover_dynamic_schema_contract(
        connector=connector,
        stream_name=request.stream_name,
        credential=sampled_credential if isinstance(sampled_credential, dict) else {},
        stream_meta=stream_meta if isinstance(stream_meta, dict) else {},
    )
    return DynamicSchemaContract(**contract)
