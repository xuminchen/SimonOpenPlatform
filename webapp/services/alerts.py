from __future__ import annotations

from datetime import datetime
import json
from typing import Any

import requests
from sqlalchemy.orm import Session

from webapp.json_helpers import safe_json_dict
from webapp.models import AlertChannel, AlertEvent, SyncExecutionInstance, SyncProject, SyncStreamTask
from webapp.schemas import (
    AlertChannelCreateRequest,
    AlertChannelUpdateRequest,
    AlertChannelView,
    AlertChannelTestRequest,
    AlertChannelTestResponse,
    AlertEventListResponse,
    AlertEventView,
)


def _normalize_channel_type(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"webhook", "http", "https"}:
        return "webhook"
    return "webhook"


def _normalize_status(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"active", "inactive"}:
        return raw
    return "active"


def _channel_url(config: dict[str, Any]) -> str:
    return str(config.get("webhook_url") or config.get("url") or "").strip()


def _to_channel_view(item: AlertChannel) -> AlertChannelView:
    return AlertChannelView(
        id=item.id,
        name=item.name,
        channel_type=item.channel_type,
        status=item.status,
        config=safe_json_dict(item.config_json),
        created_at=item.created_at,
        updated_at=item.updated_at,
    )


def _to_event_view(item: AlertEvent) -> AlertEventView:
    return AlertEventView(
        id=item.id,
        project_id=item.project_id,
        stream_task_id=item.stream_task_id,
        execution_id=item.execution_id,
        severity=item.severity,
        title=item.title,
        message=item.message,
        status=item.status,
        delivery_error=item.delivery_error,
        payload=safe_json_dict(item.payload_json),
        created_at=item.created_at,
        notified_at=item.notified_at,
    )


def list_alert_channels(db: Session) -> list[AlertChannelView]:
    rows = db.query(AlertChannel).order_by(AlertChannel.id.desc()).all()
    return [_to_channel_view(item) for item in rows]


def get_alert_channel(db: Session, channel_id: int) -> AlertChannel | None:
    return db.get(AlertChannel, channel_id)


def create_alert_channel(db: Session, request: AlertChannelCreateRequest) -> AlertChannelView:
    row = AlertChannel(
        name=str(request.name or "").strip(),
        channel_type=_normalize_channel_type(request.channel_type),
        status=_normalize_status(request.status),
        config_json=json.dumps(request.config if isinstance(request.config, dict) else {}, ensure_ascii=False),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _to_channel_view(row)


def update_alert_channel(db: Session, channel: AlertChannel, request: AlertChannelUpdateRequest) -> AlertChannelView:
    if request.name is not None:
        channel.name = str(request.name or "").strip()
    if request.channel_type is not None:
        channel.channel_type = _normalize_channel_type(request.channel_type)
    if request.status is not None:
        channel.status = _normalize_status(request.status)
    if request.config is not None:
        channel.config_json = json.dumps(request.config if isinstance(request.config, dict) else {}, ensure_ascii=False)

    db.add(channel)
    db.commit()
    db.refresh(channel)
    return _to_channel_view(channel)


def delete_alert_channel(db: Session, channel: AlertChannel) -> None:
    db.delete(channel)
    db.commit()


def test_alert_channel(request: AlertChannelTestRequest) -> AlertChannelTestResponse:
    config = request.config if isinstance(request.config, dict) else {}
    if _normalize_channel_type(request.channel_type) != "webhook":
        return AlertChannelTestResponse(success=False, message="only webhook channel is supported")

    url = _channel_url(config)
    if not url:
        return AlertChannelTestResponse(success=False, message="webhook_url is required")

    headers = config.get("headers")
    if not isinstance(headers, dict):
        headers = {}
    headers = {str(k): str(v) for k, v in headers.items()}
    timeout_seconds = int(config.get("timeout_seconds") or 5)
    timeout_seconds = max(1, min(timeout_seconds, 30))

    payload = {
        "event": "alert_channel_test",
        "message": "WonderLab alert channel connectivity test",
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=(3, timeout_seconds))
        if response.status_code >= 400:
            return AlertChannelTestResponse(success=False, message="webhook returned status {0}".format(response.status_code))
        return AlertChannelTestResponse(success=True, message="webhook test succeeded")
    except Exception as exc:
        return AlertChannelTestResponse(success=False, message="webhook test failed: {0}".format(exc))


def list_alert_events(
    db: Session,
    *,
    limit: int = 200,
    project_id: int | None = None,
    status: str = "",
) -> AlertEventListResponse:
    query = db.query(AlertEvent)
    if project_id is not None:
        query = query.filter(AlertEvent.project_id == project_id)
    normalized_status = str(status or "").strip().lower()
    if normalized_status:
        query = query.filter(AlertEvent.status == normalized_status)
    rows = query.order_by(AlertEvent.id.desc()).limit(max(1, min(int(limit), 500))).all()
    return AlertEventListResponse(total=len(rows), events=[_to_event_view(item) for item in rows])


def emit_execution_failure_alert(
    db: Session,
    *,
    project: SyncProject,
    stream: SyncStreamTask,
    execution: SyncExecutionInstance,
    error_message: str,
    payload: dict[str, Any] | None = None,
) -> None:
    context_payload = payload if isinstance(payload, dict) else {}
    title = "Execution failed: {0}/{1}".format(project.name, stream.stream_name)
    message = str(error_message or "execution failed").strip() or "execution failed"

    event = AlertEvent(
        project_id=project.id,
        stream_task_id=stream.id,
        execution_id=execution.id,
        severity="ERROR",
        title=title,
        message=message,
        status="pending",
        payload_json=json.dumps(
            {
                "project_id": project.id,
                "project_name": project.name,
                "stream_task_id": stream.id,
                "stream_name": stream.stream_name,
                "execution_id": execution.id,
                "execution_type": execution.execution_type,
                "execution_status": execution.status,
                "start_time": execution.start_time,
                "end_time": execution.end_time,
                "context": context_payload,
            },
            ensure_ascii=False,
        ),
    )
    db.add(event)
    db.flush()

    channels = (
        db.query(AlertChannel)
        .filter(AlertChannel.status == "active")
        .order_by(AlertChannel.id.asc())
        .all()
    )
    if not channels:
        event.status = "skipped"
        event.delivery_error = "no active alert channel"
        event.notified_at = datetime.utcnow()
        db.add(event)
        return

    failures: list[str] = []
    sent_count = 0
    for channel in channels:
        if str(channel.channel_type or "").strip().lower() != "webhook":
            continue
        config = safe_json_dict(channel.config_json)
        url = _channel_url(config)
        if not url:
            failures.append("channel#{0} missing webhook_url".format(channel.id))
            continue
        headers = config.get("headers")
        if not isinstance(headers, dict):
            headers = {}
        headers = {str(k): str(v) for k, v in headers.items()}
        timeout_seconds = int(config.get("timeout_seconds") or 5)
        timeout_seconds = max(1, min(timeout_seconds, 30))
        request_payload = {
            "event": "sync_execution_failed",
            "severity": event.severity,
            "title": event.title,
            "message": event.message,
            "payload": safe_json_dict(event.payload_json),
            "created_at": datetime.utcnow().isoformat(),
        }
        try:
            response = requests.post(url, json=request_payload, headers=headers, timeout=(3, timeout_seconds))
            if response.status_code >= 400:
                failures.append("channel#{0} status={1}".format(channel.id, response.status_code))
                continue
            sent_count += 1
        except Exception as exc:
            failures.append("channel#{0} error={1}".format(channel.id, exc))

    if sent_count > 0 and not failures:
        event.status = "sent"
    elif sent_count > 0 and failures:
        event.status = "partial"
    else:
        event.status = "failed"
    event.delivery_error = "; ".join(failures)
    event.notified_at = datetime.utcnow()
    db.add(event)
