from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from webapp.db import get_db
from webapp.models import PlatformAccount, SyncTask
from webapp.schemas import TaskCreateMetaReportRequest, TaskCreateWechatOrdersRequest, TaskDetail, TaskSummary
from webapp.services.task_runner import submit_task
from webapp.services.tasks import TASK_TYPE_META_REPORT, TASK_TYPE_WECHAT_ORDERS, create_task, execute_task


router = APIRouter(prefix="/tasks", tags=["tasks"])


def _require_db(db: Session | None) -> Session:
    if db is None:
        raise HTTPException(status_code=503, detail="Database is disabled. Task APIs are unavailable.")
    return db


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
    return [
        TaskSummary(
            id=item.id,
            account_id=item.account_id,
            task_type=item.task_type,
            status=item.status,
            created_at=item.created_at,
            started_at=item.started_at,
            finished_at=item.finished_at,
        )
        for item in tasks
    ]


@router.post("/wechat-orders", response_model=TaskDetail)
def create_wechat_orders_task(request: TaskCreateWechatOrdersRequest, db: Session | None = Depends(get_db)) -> TaskDetail:
    db = _require_db(db)
    account = db.get(PlatformAccount, request.account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    if account.platform != "wechat_shop":
        raise HTTPException(status_code=400, detail="Selected account platform is not wechat_shop")

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
    db = _require_db(db)
    account = db.get(PlatformAccount, request.account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    if account.platform != "wechat_shop":
        raise HTTPException(status_code=400, detail="Selected account platform is not wechat_shop")

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
    return TaskSummary(
        id=task.id,
        account_id=task.account_id,
        task_type=task.task_type,
        status=task.status,
        created_at=task.created_at,
        started_at=task.started_at,
        finished_at=task.finished_at,
    )


@router.post("/meta-report/submit", response_model=TaskSummary)
def submit_meta_report_task(request: TaskCreateMetaReportRequest, db: Session | None = Depends(get_db)) -> TaskSummary:
    db = _require_db(db)
    account = db.get(PlatformAccount, request.account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    if account.platform != "meta_ads":
        raise HTTPException(status_code=400, detail="Selected account platform is not meta_ads")

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
    return TaskSummary(
        id=task.id,
        account_id=task.account_id,
        task_type=task.task_type,
        status=task.status,
        created_at=task.created_at,
        started_at=task.started_at,
        finished_at=task.finished_at,
    )


@router.get("/{task_id}", response_model=TaskDetail)
def get_task(task_id: int, db: Session | None = Depends(get_db)) -> TaskDetail:
    db = _require_db(db)
    task = db.get(SyncTask, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
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
        request_payload=json.loads(task.request_payload or "{}"),
        result_payload=json.loads(task.result_payload or "{}"),
        error_message=task.error_message,
    )
