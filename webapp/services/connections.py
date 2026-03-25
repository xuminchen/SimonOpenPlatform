from __future__ import annotations

from collections import defaultdict
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
import re
import threading
from typing import Any

from sqlalchemy.orm import Session

from webapp.config import get_storage_root
from webapp.db import SessionLocal
from webapp.json_helpers import safe_json_dict
from webapp.models import (
    AlertChannel,
    DestinationProfile,
    PlatformAccount,
    SyncExecutionInstance,
    SyncProject,
    SyncStreamTask,
)
from webapp.schemas import (
    StreamPreviewResponse,
    SyncConnectionBatchDeleteResponse,
    SyncConnectionBatchStatusUpdateResponse,
    SyncConnectionCreateRequest,
    SyncConnectionStreamView,
    SyncConnectionView,
    SyncExecutionSubmitBackfillRequest,
    SyncExecutionSubmitRoutineRequest,
    SyncExecutionInstanceView,
    SyncProjectReadinessResponse,
    ProjectReadinessCheck,
    SyncProjectCreateRequest,
    SyncProjectView,
    SyncStreamTaskBatchCreateRequest,
    SyncStreamTaskView,
)
from webapp.services.alerts import emit_execution_failure_alert
from webapp.services.accounts import decode_account_config
from webapp.services.connection_connectors import get_connector, test_connection_with_latency_ms
from webapp.services.credential_source import find_credential_entry_by_app_id
from webapp.services.platform_configs import list_platform_configs


EXECUTION_TYPE_ROUTINE = "ROUTINE"
EXECUTION_TYPE_BACKFILL = "BACKFILL"
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"

_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="wl-conn")
_LOGGER = logging.getLogger(__name__)
_DESTINATION_FILE_LOCK = threading.Lock()


def _now_utc() -> datetime:
    return datetime.utcnow()


def _iso_now() -> str:
    return _now_utc().replace(microsecond=0).isoformat()


def _default_routine_start() -> str:
    return (_now_utc() - timedelta(days=1)).replace(microsecond=0).isoformat()


def _connector_failure_message(result: dict[str, Any]) -> str:
    summaries = result.get("raw_responses_by_advertiser")
    if not isinstance(summaries, list):
        return ""

    normalized = [item for item in summaries if isinstance(item, dict)]
    if not normalized:
        return ""
    if any(bool(item.get("ok")) for item in normalized):
        return ""

    for item in normalized:
        message = str(item.get("message", "")).strip()
        if message:
            return message
    return "上游接口请求全部失败"


def _normalize_sync_mode(mode: Any) -> str:
    raw = str(mode or "").strip().upper()
    if raw == "FULL_REFRESH":
        return "FULL_REFRESH"
    return "INCREMENTAL"


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


def _resolve_destination_base_path(profile: DestinationProfile) -> Path:
    root = get_storage_root().resolve()
    config = safe_json_dict(profile.config_json)
    relative = str(config.get("managed_relative_path") or "").strip()
    if not relative:
        relative = _managed_relative_path(profile.name)
    base = (root / relative).resolve()
    if not base.is_relative_to(root):
        raise ValueError("invalid destination path")
    base.mkdir(parents=True, exist_ok=True)
    return base


def _extract_record_day(record: dict[str, Any], fallback_day: str) -> str:
    for key in ("time", "date", "stat_time", "create_time", "update_time", "day"):
        value = record.get(key)
        text = str(value or "").strip()
        if len(text) >= 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", text[:10]):
            return text[:10]
    return fallback_day


def _persist_records_to_destination(
    db: Session,
    *,
    project: SyncProject,
    stream: SyncStreamTask,
    execution: SyncExecutionInstance,
    result: dict[str, Any],
) -> dict[str, Any]:
    destination_name = str(project.destination or "").strip()
    if not destination_name:
        return {"stored": False, "reason": "destination is empty"}

    profile = (
        db.query(DestinationProfile)
        .filter(DestinationProfile.name == destination_name)
        .order_by(DestinationProfile.id.desc())
        .first()
    )
    if profile is None:
        return {"stored": False, "reason": "destination profile not found"}

    destination_type = str(profile.destination_type or "").strip().lower()
    if destination_type != "managed_local_file":
        return {"stored": False, "reason": "destination type is not managed_local_file"}

    records = result.get("records")
    if not isinstance(records, list):
        records = []
    normalized_records = [item for item in records if isinstance(item, dict)]
    fallback_day = str(execution.end_time or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", fallback_day):
        fallback_day = _now_utc().strftime("%Y-%m-%d")

    file_name = "{0}.json".format(str(stream.stream_name or "stream").strip() or "stream")
    base_path = _resolve_destination_base_path(profile)
    target_file = base_path / file_name

    with _DESTINATION_FILE_LOCK:
        if target_file.exists():
            try:
                with target_file.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
            except Exception:
                payload = {}
        else:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        # Even with no records, keep the day key to make execution windows explicit.
        if fallback_day not in payload or not isinstance(payload.get(fallback_day), list):
            payload[fallback_day] = payload.get(fallback_day) if isinstance(payload.get(fallback_day), list) else []

        for item in normalized_records:
            day = _extract_record_day(item, fallback_day)
            day_rows = payload.get(day)
            if not isinstance(day_rows, list):
                day_rows = []
                payload[day] = day_rows
            day_rows.append(item)

        with target_file.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
            fp.write("\n")

    return {
        "stored": True,
        "file_path": str(target_file),
        "record_count": len(normalized_records),
        "date_keys": sorted([key for key in payload.keys() if isinstance(key, str)]),
    }


def _discover_schema(
    *,
    platform_code: str,
    schema_cache: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    if schema_cache is not None and platform_code in schema_cache:
        return schema_cache[platform_code]
    connector = get_connector(platform_code)
    payload = connector.discover_schema()
    schema = [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
    if schema_cache is not None:
        schema_cache[platform_code] = schema
    return schema


def _default_cursor_field_for_stream(
    *,
    platform_code: str,
    stream_name: str,
    schema_cache: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    try:
        schema = _discover_schema(platform_code=platform_code, schema_cache=schema_cache)
    except Exception as exc:
        _LOGGER.warning(
            "discover schema failed when resolving default cursor platform_code=%s stream_name=%s error=%s",
            platform_code,
            stream_name,
            exc,
        )
        return ""

    for item in schema:
        name = str(item.get("stream_name", "")).strip()
        if name != stream_name:
            continue
        defaults = item.get("default_cursor_field") or []
        if isinstance(defaults, list):
            for value in defaults:
                cursor = str(value or "").strip()
                if cursor:
                    return cursor
        break
    return ""


def _normalize_app_ids(raw_app_ids: list[str] | None) -> list[str]:
    if not raw_app_ids:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in raw_app_ids:
        app_id = str(item or "").strip()
        if not app_id or app_id in seen:
            continue
        seen.add(app_id)
        result.append(app_id)
    return result


def _project_app_ids(project: SyncProject) -> list[str]:
    app_ids: list[str] = []
    try:
        payload = json.loads(project.app_ids_json or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = []
    if isinstance(payload, list):
        app_ids = _normalize_app_ids([str(x) for x in payload])
    if app_ids:
        return app_ids
    fallback = str(project.app_id or "").strip()
    return [fallback] if fallback else []


def project_app_ids(project: SyncProject) -> list[str]:
    return _project_app_ids(project)


def _to_project_view(item: SyncProject) -> SyncProjectView:
    app_ids = _project_app_ids(item)
    return SyncProjectView(
        id=item.id,
        name=item.name,
        platform_code=item.platform_code,
        credential_id=item.credential_id,
        app_id=item.app_id,
        app_ids=app_ids,
        schedule_cron=item.schedule_cron,
        destination=item.destination,
        status=item.status,
        created_at=item.created_at,
        updated_at=item.updated_at,
    )


def project_to_view(item: SyncProject) -> SyncProjectView:
    return _to_project_view(item)


def _basic_cron_valid(expr: str) -> bool:
    value = str(expr or "").strip()
    if not value:
        return False
    parts = value.split()
    return len(parts) in {5, 6}


def evaluate_project_readiness(db: Session, *, project: SyncProject) -> SyncProjectReadinessResponse:
    checks: list[ProjectReadinessCheck] = []

    def _push_check(*, key: str, label: str, status: str, message: str, detail: dict[str, Any] | None = None) -> None:
        checks.append(
            ProjectReadinessCheck(
                key=key,
                label=label,
                status=status,
                message=message,
                detail=detail if isinstance(detail, dict) else {},
            )
        )

    platform_code = str(project.platform_code or "").strip().lower()
    supported_platforms = {
        str(item.get("platform", "")).strip().lower()
        for item in list_platform_configs()
        if isinstance(item, dict)
    }
    platform_ok = bool(platform_code) and platform_code in supported_platforms
    _push_check(
        key="platform_registered",
        label="平台注册有效性",
        status="PASS" if platform_ok else "FAIL",
        message="平台已注册" if platform_ok else "平台未注册或编码为空",
        detail={"platform_code": platform_code, "registered_platform_count": len(supported_platforms)},
    )

    streams = (
        db.query(SyncStreamTask)
        .filter(SyncStreamTask.project_id == project.id)
        .order_by(SyncStreamTask.id.asc())
        .all()
    )
    stream_count = len(streams)
    _push_check(
        key="stream_configured",
        label="Stream 配置完整性",
        status="PASS" if stream_count > 0 else "FAIL",
        message="已配置 {0} 个 stream".format(stream_count) if stream_count > 0 else "未配置任何 stream",
        detail={"stream_count": stream_count},
    )

    incremental_missing_cursor = [
        item.stream_name
        for item in streams
        if str(item.sync_mode or "").strip().upper() == "INCREMENTAL" and not str(item.cursor_field or "").strip()
    ]
    _push_check(
        key="incremental_cursor",
        label="增量游标校验",
        status="PASS" if not incremental_missing_cursor else "FAIL",
        message="增量 stream 游标配置完整" if not incremental_missing_cursor else "存在未配置游标的增量 stream",
        detail={"missing_cursor_streams": incremental_missing_cursor},
    )

    schedule_ok = _basic_cron_valid(project.schedule_cron)
    _push_check(
        key="project_schedule",
        label="项目调度表达式",
        status="PASS" if schedule_ok else "FAIL",
        message="调度表达式有效" if schedule_ok else "调度表达式无效",
        detail={"schedule_cron": str(project.schedule_cron or "")},
    )

    destination_name = str(project.destination or "").strip()
    destination_profile = (
        db.query(DestinationProfile)
        .filter(DestinationProfile.name == destination_name)
        .order_by(DestinationProfile.id.desc())
        .first()
    )
    destination_exists = destination_profile is not None
    destination_active = destination_exists and str(destination_profile.status or "").strip().lower() == "active"
    destination_status = "PASS" if destination_active else ("WARN" if destination_exists else "FAIL")
    destination_message = (
        "目标配置存在且为 active"
        if destination_active
        else ("目标配置存在但状态非 active" if destination_exists else "目标配置不存在")
    )
    _push_check(
        key="destination_profile",
        label="目标配置可用性",
        status=destination_status,
        message=destination_message,
        detail={
            "destination": destination_name,
            "destination_type": str(destination_profile.destination_type or "") if destination_profile else "",
            "destination_status": str(destination_profile.status or "") if destination_profile else "",
        },
    )

    credential = _resolve_project_credential(db, project)
    connector = get_connector(platform_code)
    credential_ok = False
    latency_ms = 0
    if isinstance(credential, dict) and credential:
        credential_ok, latency_ms = test_connection_with_latency_ms(connector, credential)
    _push_check(
        key="credential_connectivity",
        label="凭证连通性",
        status="PASS" if credential_ok else "FAIL",
        message="凭证可用" if credential_ok else "凭证缺失或连通性校验失败",
        detail={
            "credential_id": project.credential_id,
            "app_ids": _project_app_ids(project),
            "latency_ms": latency_ms,
        },
    )

    active_alert_channels = (
        db.query(AlertChannel)
        .filter(AlertChannel.status == "active")
        .order_by(AlertChannel.id.asc())
        .all()
    )
    _push_check(
        key="alert_channel",
        label="告警通道可用性",
        status="PASS" if active_alert_channels else "WARN",
        message="存在可用告警通道" if active_alert_channels else "未配置 active 告警通道",
        detail={"active_channel_count": len(active_alert_channels)},
    )

    ready = all(item.status != "FAIL" for item in checks)
    return SyncProjectReadinessResponse(
        project_id=project.id,
        project_name=project.name,
        ready=ready,
        generated_at=_now_utc(),
        checks=checks,
    )


def _to_stream_task_view(item: SyncStreamTask) -> SyncStreamTaskView:
    return SyncStreamTaskView(
        id=item.id,
        project_id=item.project_id,
        stream_name=item.stream_name,
        sync_mode=item.sync_mode,
        cursor_field=item.cursor_field,
        primary_key=item.primary_key,
        schema_contract=safe_json_dict(item.schema_contract_json),
        routine_cron=item.routine_cron,
        last_cursor_value=item.last_cursor_value or "",
        last_routine_status=item.last_routine_status,
        last_routine_started_at=item.last_routine_started_at,
        last_routine_finished_at=item.last_routine_finished_at,
        created_at=item.created_at,
        updated_at=item.updated_at,
    )


def _to_execution_view(item: SyncExecutionInstance) -> SyncExecutionInstanceView:
    return SyncExecutionInstanceView(
        id=item.id,
        project_id=item.project_id,
        stream_task_id=item.stream_task_id,
        execution_type=item.execution_type,
        status=item.status,
        start_time=item.start_time,
        end_time=item.end_time,
        triggered_by=item.triggered_by,
        request_payload=safe_json_dict(item.request_payload),
        result_payload=safe_json_dict(item.result_payload),
        error_message=item.error_message,
        created_at=item.created_at,
        started_at=item.started_at,
        finished_at=item.finished_at,
    )


def _resolve_project_app_id(db: Session, *, credential_id: int | None, app_id: str | None) -> str | None:
    resolved = app_id
    if credential_id is None:
        return resolved
    account = db.get(PlatformAccount, credential_id)
    if account is not None and account.app_id:
        resolved = account.app_id
    return resolved


def _resolve_project_credential(db: Session, project: SyncProject) -> dict[str, Any]:
    if project.credential_id is not None:
        account = db.get(PlatformAccount, project.credential_id)
        if account is not None:
            return decode_account_config(account)

    for app_id in _project_app_ids(project):
        item = find_credential_entry_by_app_id(app_id, refresh=False)
        if item is not None and isinstance(item.config, dict):
            return dict(item.config)

    return {}


def list_projects(db: Session) -> list[SyncProjectView]:
    rows = db.query(SyncProject).order_by(SyncProject.id.desc()).all()
    return [_to_project_view(item) for item in rows]


def get_project(db: Session, project_id: int) -> SyncProject | None:
    return db.get(SyncProject, project_id)


def create_project(db: Session, request: SyncProjectCreateRequest) -> SyncProjectView:
    normalized_app_ids = _normalize_app_ids(request.app_ids)
    if not normalized_app_ids and request.app_id:
        normalized_app_ids = _normalize_app_ids([request.app_id])

    resolved_app_id = _resolve_project_app_id(
        db,
        credential_id=request.credential_id,
        app_id=(normalized_app_ids[0] if normalized_app_ids else request.app_id),
    )
    if resolved_app_id and not normalized_app_ids:
        normalized_app_ids = [resolved_app_id]
    item = SyncProject(
        name=request.name,
        platform_code=request.platform_code,
        credential_id=request.credential_id,
        app_id=(normalized_app_ids[0] if normalized_app_ids else resolved_app_id),
        app_ids_json=json.dumps(normalized_app_ids, ensure_ascii=False),
        schedule_cron=request.schedule_cron,
        destination=request.destination,
        status=request.status,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return _to_project_view(item)


def update_project_app_ids(
    db: Session,
    *,
    project: SyncProject,
    app_ids: list[str],
) -> SyncProjectView:
    normalized_app_ids = _normalize_app_ids(app_ids)
    if not normalized_app_ids:
        raise ValueError("app_ids is empty")

    resolved_app_id = _resolve_project_app_id(
        db,
        credential_id=project.credential_id,
        app_id=normalized_app_ids[0] if normalized_app_ids else project.app_id,
    )
    if resolved_app_id and not normalized_app_ids:
        normalized_app_ids = [resolved_app_id]

    project.app_id = normalized_app_ids[0] if normalized_app_ids else resolved_app_id
    project.app_ids_json = json.dumps(normalized_app_ids, ensure_ascii=False)
    db.add(project)
    db.commit()
    db.refresh(project)
    return _to_project_view(project)


def list_stream_tasks(db: Session, project_id: int) -> list[SyncStreamTaskView]:
    rows = (
        db.query(SyncStreamTask)
        .filter(SyncStreamTask.project_id == project_id)
        .order_by(SyncStreamTask.id.asc())
        .all()
    )
    return [_to_stream_task_view(item) for item in rows]


def get_stream_preview(
    db: Session,
    *,
    project: SyncProject,
    stream_name: str,
    limit: int = 50,
) -> StreamPreviewResponse:
    normalized_stream = str(stream_name or "").strip()
    if not normalized_stream:
        return StreamPreviewResponse(
            project_id=project.id,
            stream_name="",
            destination=str(project.destination or ""),
            execution_id=None,
            execution_status="",
            execution_error_message="",
            execution_finished_at=None,
            total_records=0,
            returned_records=0,
            columns=[],
            rows=[],
            raw_response={},
        )

    stream_task = (
        db.query(SyncStreamTask)
        .filter(
            SyncStreamTask.project_id == project.id,
            SyncStreamTask.stream_name == normalized_stream,
        )
        .order_by(SyncStreamTask.id.asc())
        .first()
    )
    if stream_task is None:
        return StreamPreviewResponse(
            project_id=project.id,
            stream_name=normalized_stream,
            destination=str(project.destination or ""),
            execution_id=None,
            execution_status="",
            execution_error_message="",
            execution_finished_at=None,
            total_records=0,
            returned_records=0,
            columns=[],
            rows=[],
            raw_response={},
        )

    execution_rows = (
        db.query(SyncExecutionInstance)
        .filter(
            SyncExecutionInstance.project_id == project.id,
            SyncExecutionInstance.stream_task_id == stream_task.id,
            SyncExecutionInstance.finished_at.isnot(None),
        )
        .order_by(SyncExecutionInstance.id.desc())
        .limit(50)
        .all()
    )
    if not execution_rows:
        return StreamPreviewResponse(
            project_id=project.id,
            stream_name=normalized_stream,
            destination=str(project.destination or ""),
            execution_id=None,
            execution_status="",
            execution_error_message="",
            execution_finished_at=None,
            total_records=0,
            returned_records=0,
            columns=[],
            rows=[],
            raw_response={},
        )

    def _extract_response(execution: SyncExecutionInstance) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
        payload = safe_json_dict(execution.result_payload)
        response_payload = payload.get("result") if isinstance(payload.get("result"), dict) else payload
        if not isinstance(response_payload, dict):
            return {}, [], False

        records: list[dict[str, Any]] = []
        nested_records = response_payload.get("records")
        if isinstance(nested_records, list):
            records = [item for item in nested_records if isinstance(item, dict)]
        elif response_payload:
            records = [response_payload]
        has_content = bool(response_payload) or len(records) > 0
        return response_payload, records, has_content

    execution = None
    response_payload: dict[str, Any] = {}
    records: list[dict[str, Any]] = []

    for row in execution_rows:
        current_payload, current_records, has_content = _extract_response(row)
        if str(row.status or "").upper() == STATUS_SUCCESS and has_content:
            execution = row
            response_payload = current_payload
            records = current_records
            break

    if execution is None:
        for row in execution_rows:
            current_payload, current_records, has_content = _extract_response(row)
            if has_content:
                execution = row
                response_payload = current_payload
                records = current_records
                break

    if execution is None:
        execution = execution_rows[0]
        response_payload, records, _ = _extract_response(execution)

    safe_limit = max(1, min(int(limit or 50), 500))
    total_records = len(records)
    rows = records[:safe_limit]

    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            name = str(key or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            columns.append(name)

    return StreamPreviewResponse(
        project_id=project.id,
        stream_name=normalized_stream,
        destination=str(project.destination or ""),
        execution_id=execution.id,
        execution_status=str(execution.status or ""),
        execution_error_message=str(execution.error_message or ""),
        execution_finished_at=execution.finished_at,
        total_records=total_records,
        returned_records=len(rows),
        columns=columns,
        rows=rows,
        raw_response=response_payload if isinstance(response_payload, dict) else {},
    )


def add_stream_tasks(
    db: Session,
    *,
    project: SyncProject,
    request: SyncStreamTaskBatchCreateRequest,
) -> list[SyncStreamTaskView]:
    stream_rows: list[SyncStreamTask] = []
    schema_cache: dict[str, list[dict[str, Any]]] = {}
    for item in request.streams:
        stream_name = str(item.stream_name or "").strip()
        if not stream_name:
            continue

        sync_mode = _normalize_sync_mode(item.sync_mode)
        cursor_field = str(item.cursor_field or "").strip()
        if sync_mode == "INCREMENTAL" and not cursor_field:
            cursor_field = _default_cursor_field_for_stream(
                platform_code=project.platform_code,
                stream_name=stream_name,
                schema_cache=schema_cache,
            )

        row = (
            db.query(SyncStreamTask)
            .filter(
                SyncStreamTask.project_id == project.id,
                SyncStreamTask.stream_name == stream_name,
            )
            .order_by(SyncStreamTask.id.asc())
            .first()
        )
        if row is None:
            row = SyncStreamTask(
                project_id=project.id,
                stream_name=stream_name,
            )

        row.sync_mode = sync_mode
        row.cursor_field = cursor_field
        row.primary_key = str(item.primary_key or "").strip()
        row.schema_contract_json = json.dumps(
            item.schema_contract if isinstance(item.schema_contract, dict) else {},
            ensure_ascii=False,
        )
        row.routine_cron = str(item.routine_cron or "").strip() or str(project.schedule_cron or "0 * * * *")
        db.add(row)
        stream_rows.append(row)

    db.commit()
    for row in stream_rows:
        db.refresh(row)
    return [_to_stream_task_view(item) for item in stream_rows]


def list_executions(db: Session, *, project_id: int, limit: int = 200) -> list[SyncExecutionInstanceView]:
    rows = (
        db.query(SyncExecutionInstance)
        .filter(SyncExecutionInstance.project_id == project_id)
        .order_by(SyncExecutionInstance.id.desc())
        .limit(limit)
        .all()
    )
    return [_to_execution_view(item) for item in rows]


def _select_stream_tasks(
    db: Session,
    *,
    project_id: int,
    stream_task_ids: list[int],
) -> list[SyncStreamTask]:
    query = db.query(SyncStreamTask).filter(SyncStreamTask.project_id == project_id)
    if stream_task_ids:
        query = query.filter(SyncStreamTask.id.in_(stream_task_ids))
    rows = query.order_by(SyncStreamTask.id.asc()).all()
    return rows


def _create_execution_instance(
    db: Session,
    *,
    project: SyncProject,
    stream: SyncStreamTask,
    execution_type: str,
    start_time: str,
    end_time: str,
    triggered_by: str,
) -> SyncExecutionInstance:
    payload = {
        "project_id": project.id,
        "stream_task_id": stream.id,
        "platform_code": project.platform_code,
        "stream_name": stream.stream_name,
        "sync_mode": stream.sync_mode,
        "cursor_field": stream.cursor_field,
        "start_time": start_time,
        "end_time": end_time,
        "execution_type": execution_type,
    }
    instance = SyncExecutionInstance(
        project_id=project.id,
        stream_task_id=stream.id,
        execution_type=execution_type,
        status=STATUS_PENDING,
        start_time=start_time,
        end_time=end_time,
        triggered_by=triggered_by,
        request_payload=json.dumps(payload, ensure_ascii=False),
    )
    db.add(instance)
    return instance


def submit_routine(
    db: Session,
    *,
    project: SyncProject,
    request: SyncExecutionSubmitRoutineRequest,
) -> list[SyncExecutionInstanceView]:
    streams = _select_stream_tasks(db, project_id=project.id, stream_task_ids=request.stream_task_ids)
    if not streams:
        return []

    created: list[SyncExecutionInstance] = []
    now_iso = _iso_now()
    for stream in streams:
        if stream.sync_mode.upper() == "INCREMENTAL":
            start_time = stream.last_cursor_value.strip() or _default_routine_start()
        else:
            start_time = ""

        instance = _create_execution_instance(
            db,
            project=project,
            stream=stream,
            execution_type=EXECUTION_TYPE_ROUTINE,
            start_time=start_time,
            end_time=now_iso,
            triggered_by=request.triggered_by,
        )
        created.append(instance)

    db.commit()
    for item in created:
        db.refresh(item)
        _EXECUTOR.submit(_run_execution_instance, item.id)

    return [_to_execution_view(item) for item in created]


def submit_backfill(
    db: Session,
    *,
    project: SyncProject,
    request: SyncExecutionSubmitBackfillRequest,
) -> list[SyncExecutionInstanceView]:
    streams = _select_stream_tasks(db, project_id=project.id, stream_task_ids=request.stream_task_ids)
    if not streams:
        return []

    created: list[SyncExecutionInstance] = []
    for stream in streams:
        instance = _create_execution_instance(
            db,
            project=project,
            stream=stream,
            execution_type=EXECUTION_TYPE_BACKFILL,
            start_time=request.start_time,
            end_time=request.end_time,
            triggered_by=request.triggered_by,
        )
        created.append(instance)

    db.commit()
    for item in created:
        db.refresh(item)
        _EXECUTOR.submit(_run_execution_instance, item.id)

    return [_to_execution_view(item) for item in created]


def _run_execution_instance(execution_id: int) -> None:
    if SessionLocal is None:
        return

    db = SessionLocal()
    try:
        execution = db.get(SyncExecutionInstance, execution_id)
        if execution is None:
            return
        stream = db.get(SyncStreamTask, execution.stream_task_id) if execution.stream_task_id else None
        project = db.get(SyncProject, execution.project_id)
        if stream is None or project is None:
            execution.status = STATUS_FAILED
            execution.error_message = "execution stream/project not found"
            execution.finished_at = _now_utc()
            db.add(execution)
            db.commit()
            return

        execution.status = STATUS_RUNNING
        execution.started_at = _now_utc()
        if execution.execution_type == EXECUTION_TYPE_ROUTINE:
            stream.last_routine_started_at = execution.started_at
            stream.last_routine_status = STATUS_RUNNING
            db.add(stream)
        db.add(execution)
        db.commit()

        request_payload = safe_json_dict(execution.request_payload)
        connector = get_connector(project.platform_code)
        credential = _resolve_project_credential(db, project)

        state = {
            "execution_type": execution.execution_type,
            "sync_mode": stream.sync_mode,
            "cursor_field": stream.cursor_field,
            "cursor_value": stream.last_cursor_value,
            "start_time": execution.start_time,
            "end_time": execution.end_time,
        }
        result = connector.pull_data(stream.stream_name, credential, state=state)
        if not isinstance(result, dict):
            result = {"records": [], "next_state": {}}

        connector_failure = _connector_failure_message(result)
        if connector_failure:
            execution.status = STATUS_FAILED
            execution.error_message = connector_failure
            execution.result_payload = json.dumps(
                {
                    "request": request_payload,
                    "result": result,
                },
                ensure_ascii=False,
            )
            if execution.execution_type == EXECUTION_TYPE_ROUTINE:
                stream.last_routine_status = STATUS_FAILED
                stream.last_routine_finished_at = _now_utc()
                db.add(stream)
            try:
                emit_execution_failure_alert(
                    db,
                    project=project,
                    stream=stream,
                    execution=execution,
                    error_message=connector_failure,
                    payload={"result": result, "reason": "upstream_all_failed"},
                )
            except Exception as alert_exc:  # pragma: no cover - alert side effects
                _LOGGER.warning("emit execution alert failed execution_id=%s error=%s", execution.id, alert_exc)
            db.add(execution)
            return

        next_state = result.get("next_state") if isinstance(result.get("next_state"), dict) else {}
        effective_request_window = {
            "start_time": execution.start_time,
            "end_time": execution.end_time,
            "start_date": str(next_state.get("start_date", "")).strip() or "",
            "end_date": str(next_state.get("end_date", "")).strip() or "",
            "advertiser_ids": next_state.get("advertiser_ids") if isinstance(next_state.get("advertiser_ids"), list) else [],
            "cursor": str(next_state.get("cursor", "")).strip() or "",
        }

        execution.status = STATUS_SUCCESS
        destination_write_summary = _persist_records_to_destination(
            db,
            project=project,
            stream=stream,
            execution=execution,
            result=result,
        )
        execution.result_payload = json.dumps(
            {
                "request": request_payload,
                "result": result,
                "effective_request_window": effective_request_window,
                "idempotent_write_strategy": "UPSERT_OR_DELETE_INSERT_BY_PRIMARY_KEY_AND_WINDOW",
                "destination_write": destination_write_summary,
            },
            ensure_ascii=False,
        )
        execution.error_message = ""

        # 红线规约：补数任务永远不修改日常游标。
        if execution.execution_type == EXECUTION_TYPE_ROUTINE:
            next_cursor = str(next_state.get("cursor", "")).strip() or execution.end_time
            if stream.sync_mode.upper() == "INCREMENTAL":
                stream.last_cursor_value = next_cursor
            stream.last_routine_status = STATUS_SUCCESS
            stream.last_routine_finished_at = _now_utc()
            db.add(stream)

    except Exception as exc:
        execution = db.get(SyncExecutionInstance, execution_id)
        if execution is not None:
            execution.status = STATUS_FAILED
            execution.error_message = str(exc)
            execution.result_payload = "{}"
            project = db.get(SyncProject, execution.project_id)
            if execution.execution_type == EXECUTION_TYPE_ROUTINE and execution.stream_task_id:
                stream = db.get(SyncStreamTask, execution.stream_task_id)
                if stream is not None:
                    stream.last_routine_status = STATUS_FAILED
                    stream.last_routine_finished_at = _now_utc()
                    db.add(stream)
                    if project is not None:
                        try:
                            emit_execution_failure_alert(
                                db,
                                project=project,
                                stream=stream,
                                execution=execution,
                                error_message=str(exc),
                                payload={"reason": "runtime_exception"},
                            )
                        except Exception as alert_exc:  # pragma: no cover - alert side effects
                            _LOGGER.warning("emit execution alert failed execution_id=%s error=%s", execution.id, alert_exc)
            db.add(execution)
    finally:
        execution = db.get(SyncExecutionInstance, execution_id)
        if execution is not None and execution.finished_at is None:
            execution.finished_at = _now_utc()
            db.add(execution)
        db.commit()
        db.close()


def _to_legacy_connection_view(project: SyncProject, streams: list[SyncStreamTask]) -> SyncConnectionView:
    app_ids = _project_app_ids(project)
    stream_views = [
        SyncConnectionStreamView(
            id=item.id,
            stream_name=item.stream_name,
            sync_mode=item.sync_mode,
            cursor_field=item.cursor_field,
            primary_key=item.primary_key,
        )
        for item in streams
    ]

    latest_status = "PENDING"
    latest_time = None
    if streams:
        sorted_streams = sorted(
            streams,
            key=lambda x: (x.last_routine_finished_at is None, x.last_routine_finished_at),
            reverse=True,
        )
        top = sorted_streams[0]
        latest_status = top.last_routine_status
        latest_time = top.last_routine_finished_at

    schedule_cron = streams[0].routine_cron if streams else str(project.schedule_cron or "0 * * * *")
    return SyncConnectionView(
        id=project.id,
        name=project.name,
        platform_code=project.platform_code,
        credential_id=project.credential_id,
        app_id=project.app_id,
        app_ids=app_ids,
        schedule_cron=schedule_cron,
        destination=project.destination,
        status=project.status,
        last_sync_time=latest_time,
        last_sync_status=latest_status,
        created_at=project.created_at,
        updated_at=project.updated_at,
        streams=stream_views,
    )


# Legacy compatibility layer for current frontend implementation.
def list_connections(db: Session) -> list[SyncConnectionView]:
    projects = db.query(SyncProject).order_by(SyncProject.id.desc()).all()
    if not projects:
        return []

    project_ids = [item.id for item in projects]
    stream_rows = (
        db.query(SyncStreamTask)
        .filter(SyncStreamTask.project_id.in_(project_ids))
        .order_by(SyncStreamTask.project_id.asc(), SyncStreamTask.id.asc())
        .all()
    )
    streams_by_project: dict[int, list[SyncStreamTask]] = defaultdict(list)
    for stream in stream_rows:
        streams_by_project[int(stream.project_id)].append(stream)

    result: list[SyncConnectionView] = []
    for project in projects:
        result.append(_to_legacy_connection_view(project, streams_by_project.get(int(project.id), [])))
    return result


# Legacy compatibility layer for current frontend implementation.
def create_connection(db: Session, request: SyncConnectionCreateRequest) -> SyncConnectionView:
    project_request = SyncProjectCreateRequest(
        name=request.name,
        platform_code=request.platform_code,
        credential_id=request.credential_id,
        app_id=request.app_id,
        app_ids=request.app_ids,
        schedule_cron=request.schedule_cron,
        destination=request.destination,
        status=request.status,
    )
    project_view = create_project(db, project_request)
    project = db.get(SyncProject, project_view.id)
    if project is None:
        raise ValueError("Failed to create project")

    stream_request = SyncStreamTaskBatchCreateRequest(
        streams=[
            {
                "stream_name": stream.stream_name,
                "sync_mode": stream.sync_mode,
                "cursor_field": stream.cursor_field,
                "primary_key": stream.primary_key,
                "routine_cron": request.schedule_cron,
            }
            for stream in request.streams
        ]
    )
    add_stream_tasks(db, project=project, request=stream_request)

    streams = (
        db.query(SyncStreamTask)
        .filter(SyncStreamTask.project_id == project.id)
        .order_by(SyncStreamTask.id.asc())
        .all()
    )
    return _to_legacy_connection_view(project, streams)


def _normalize_connection_ids(connection_ids: list[int]) -> list[int]:
    unique_ids: list[int] = []
    seen: set[int] = set()
    for raw_id in connection_ids:
        try:
            value = int(raw_id)
        except (TypeError, ValueError):
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        unique_ids.append(value)
    return unique_ids


def update_connections_status_batch(
    db: Session,
    *,
    connection_ids: list[int],
    status: int,
) -> SyncConnectionBatchStatusUpdateResponse:
    normalized_ids = _normalize_connection_ids(connection_ids)
    if not normalized_ids:
        return SyncConnectionBatchStatusUpdateResponse(total=0, updated=0)

    rows = (
        db.query(SyncProject)
        .filter(SyncProject.id.in_(normalized_ids))
        .all()
    )

    updated = 0
    for item in rows:
        if item.status == status:
            continue
        item.status = status
        db.add(item)
        updated += 1

    db.commit()
    return SyncConnectionBatchStatusUpdateResponse(total=len(normalized_ids), updated=updated)


def delete_connections_batch(
    db: Session,
    *,
    connection_ids: list[int],
) -> SyncConnectionBatchDeleteResponse:
    normalized_ids = _normalize_connection_ids(connection_ids)
    if not normalized_ids:
        return SyncConnectionBatchDeleteResponse(total=0, deleted=0)

    rows = (
        db.query(SyncProject)
        .filter(SyncProject.id.in_(normalized_ids))
        .all()
    )
    for item in rows:
        db.delete(item)
    db.commit()
    return SyncConnectionBatchDeleteResponse(total=len(normalized_ids), deleted=len(rows))


def shutdown_connections_executor(*, wait: bool = False) -> None:
    _EXECUTOR.shutdown(wait=wait, cancel_futures=True)
