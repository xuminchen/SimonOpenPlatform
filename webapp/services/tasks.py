from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

from sqlalchemy.orm import Session

from webapp.adapters.registry import get_adapter
from webapp.models import PlatformAccount, SyncTask
from webapp.services.accounts import decode_account_config


TASK_TYPE_WECHAT_ORDERS = "sync_wechat_orders"
TASK_TYPE_META_REPORT = "sync_meta_report"


def create_task(db: Session, *, account_id: int, task_type: str, payload: Dict[str, Any]) -> SyncTask:
    task = SyncTask(
        account_id=account_id,
        task_type=task_type,
        status="pending",
        request_payload=json.dumps(payload, ensure_ascii=False),
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


def execute_task(db: Session, task: SyncTask) -> SyncTask:
    account = db.get(PlatformAccount, task.account_id)
    if account is None:
        raise ValueError("Account not found: {0}".format(task.account_id))

    payload = json.loads(task.request_payload or "{}")
    task.status = "running"
    task.started_at = datetime.utcnow()
    db.add(task)
    db.commit()
    db.refresh(task)

    try:
        adapter = get_adapter(account.platform)
        account_config = decode_account_config(account)

        if task.task_type == TASK_TYPE_WECHAT_ORDERS:
            result = adapter.sync_orders(account_name=account.name, account_config=account_config, payload=payload)
        elif task.task_type == TASK_TYPE_META_REPORT:
            result = adapter.sync_ads_report(account_name=account.name, account_config=account_config, payload=payload)
        else:
            raise ValueError("Unsupported task type: {0}".format(task.task_type))

        task.status = "success"
        task.result_payload = json.dumps(result, ensure_ascii=False)
        task.error_message = ""
    except Exception as exc:
        task.status = "failed"
        task.result_payload = "{}"
        task.error_message = str(exc)

    task.finished_at = datetime.utcnow()
    db.add(task)
    db.commit()
    db.refresh(task)
    return task
