from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from webapp.deps import require_db
from webapp.db import get_db
from webapp.error_messages import ALERT_CHANNEL_NOT_FOUND
from webapp.schemas import (
    AlertChannelCreateRequest,
    AlertChannelUpdateRequest,
    AlertChannelView,
    AlertChannelTestRequest,
    AlertChannelTestResponse,
    AlertEventListResponse,
)
from webapp.services.alerts import (
    create_alert_channel,
    delete_alert_channel,
    get_alert_channel,
    list_alert_channels,
    list_alert_events,
    test_alert_channel,
    update_alert_channel,
)


router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/channels", response_model=list[AlertChannelView])
def list_alert_channels_api(db: Session | None = Depends(get_db)) -> list[AlertChannelView]:
    db = require_db(db, detail="Database is disabled. Alert APIs are unavailable.")
    return list_alert_channels(db)


@router.post("/channels", response_model=AlertChannelView)
def create_alert_channel_api(
    request: AlertChannelCreateRequest,
    db: Session | None = Depends(get_db),
) -> AlertChannelView:
    db = require_db(db, detail="Database is disabled. Alert APIs are unavailable.")
    return create_alert_channel(db, request)


@router.put("/channels/{channel_id}", response_model=AlertChannelView)
def update_alert_channel_api(
    channel_id: int,
    request: AlertChannelUpdateRequest,
    db: Session | None = Depends(get_db),
) -> AlertChannelView:
    db = require_db(db, detail="Database is disabled. Alert APIs are unavailable.")
    channel = get_alert_channel(db, channel_id)
    if channel is None:
        raise HTTPException(status_code=404, detail=ALERT_CHANNEL_NOT_FOUND)
    return update_alert_channel(db, channel, request)


@router.delete("/channels/{channel_id}")
def delete_alert_channel_api(
    channel_id: int,
    db: Session | None = Depends(get_db),
) -> dict:
    db = require_db(db, detail="Database is disabled. Alert APIs are unavailable.")
    channel = get_alert_channel(db, channel_id)
    if channel is None:
        raise HTTPException(status_code=404, detail=ALERT_CHANNEL_NOT_FOUND)
    delete_alert_channel(db, channel)
    return {"deleted": True, "channel_id": channel_id}


@router.post("/channels/test", response_model=AlertChannelTestResponse)
def test_alert_channel_api(request: AlertChannelTestRequest) -> AlertChannelTestResponse:
    return test_alert_channel(request)


@router.get("/events", response_model=AlertEventListResponse)
def list_alert_events_api(
    limit: int = Query(default=200, ge=1, le=500),
    project_id: int | None = Query(default=None),
    status: str = Query(default=""),
    db: Session | None = Depends(get_db),
) -> AlertEventListResponse:
    db = require_db(db, detail="Database is disabled. Alert APIs are unavailable.")
    return list_alert_events(db, limit=limit, project_id=project_id, status=status)
