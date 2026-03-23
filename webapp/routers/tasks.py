from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from webapp.deps import require_db
from webapp.db import get_db
from webapp.error_messages import ACCOUNT_NOT_FOUND, TASK_NOT_FOUND, account_platform_mismatch
from webapp.json_helpers import safe_json_dict
from webapp.models import PlatformAccount, SyncTask
from webapp.schemas import TaskCreateMetaReportRequest, TaskCreateWechatOrdersRequest, TaskDetail, TaskSummary
from webapp.services.task_runner import submit_task
from webapp.services.tasks import TASK_TYPE_META_REPORT, TASK_TYPE_WECHAT_ORDERS, create_task, execute_task


router = APIRouter(prefix="/tasks", tags=["tasks"])


def _to_task_summary(task: SyncTask) -> TaskSummary:
    return TaskSummary(
        id=task.id,
        account_id=task.account_id,
        task_type=task.task_type,
        status=task.status,
        created_at=task.created_at,
        started_at=task.started_at,
        finished_at=task.finished_at,
    )


def _require_account_platform(db: Session, *, account_id: int, platform: str) -> PlatformAccount:
    account = db.get(PlatformAccount, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail=ACCOUNT_NOT_FOUND)
    if account.platform != platform:
        raise HTTPException(status_code=400, detail=account_platform_mismatch(platform))
    return account


@router.get("", response_model=list[TaskSummary])
def list_tasks(
    status: str | None = Query(default=None),
    account_id: int | None = Query(default=None),
    db: Session | None = Depends(get_db),
) -> list[TaskSummary]:
    if db is None:
        return []
    query = db.query(SyncTask)
    if status:
        query = query.filter(SyncTask.status == status)
    if account_id is not None:
        query = query.filter(SyncTask.account_id == account_id)

    tasks = query.order_by(SyncTask.id.desc()).limit(200).all()
    return [_to_task_summary(item) for item in tasks]


@router.post("/wechat-orders", response_model=TaskDetail)
def create_wechat_orders_task(request: TaskCreateWechatOrdersRequest, db: Session | None = Depends(get_db)) -> TaskDetail:
    db = require_db(db, detail="Database is disabled. Task APIs are unavailable.")
    _require_account_platform(db, account_id=request.account_id, platform="wechat_shop")

    task = create_task(
        db,
        account_id=request.account_id,
        task_type=TASK_TYPE_WECHAT_ORDERS,
        payload={
            "start_date": request.start_date,
            "end_date": request.end_date,
            "time_type": request.time_type,
            "page_size": request.page_size,
        },
    )
    task = execute_task(db, task)
    return _to_task_detail(task)


@router.post("/wechat-orders/submit", response_model=TaskSummary)
def submit_wechat_orders_task(request: TaskCreateWechatOrdersRequest, db: Session | None = Depends(get_db)) -> TaskSummary:
    db = require_db(db, detail="Database is disabled. Task APIs are unavailable.")
    _require_account_platform(db, account_id=request.account_id, platform="wechat_shop")

    task = create_task(
        db,
        account_id=request.account_id,
        task_type=TASK_TYPE_WECHAT_ORDERS,
        payload={
            "start_date": request.start_date,
            "end_date": request.end_date,
            "time_type": request.time_type,
            "page_size": request.page_size,
        },
    )
    submit_task(task.id)
    return _to_task_summary(task)


@router.post("/meta-report/submit", response_model=TaskSummary)
def submit_meta_report_task(request: TaskCreateMetaReportRequest, db: Session | None = Depends(get_db)) -> TaskSummary:
    db = require_db(db, detail="Database is disabled. Task APIs are unavailable.")
    _require_account_platform(db, account_id=request.account_id, platform="meta_ads")

    task = create_task(
        db,
        account_id=request.account_id,
        task_type=TASK_TYPE_META_REPORT,
        payload={
            "start_date": request.start_date,
            "end_date": request.end_date,
            "level": request.level,
            "dry_run": request.dry_run,
        },
    )
    submit_task(task.id)
    return _to_task_summary(task)


@router.get("/{task_id}", response_model=TaskDetail)
def get_task(task_id: int, db: Session | None = Depends(get_db)) -> TaskDetail:
    db = require_db(db, detail="Database is disabled. Task APIs are unavailable.")
    task = db.get(SyncTask, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=TASK_NOT_FOUND)
    return _to_task_detail(task)


def _to_task_detail(task: SyncTask) -> TaskDetail:
    return TaskDetail(
        id=task.id,
        account_id=task.account_id,
        task_type=task.task_type,
        status=task.status,
        created_at=task.created_at,
        started_at=task.started_at,
        finished_at=task.finished_at,
        request_payload=safe_json_dict(task.request_payload),
        result_payload=safe_json_dict(task.result_payload),
        error_message=task.error_message,
    )
