from __future__ import annotations

import json
import random
import time
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from webapp.deps import require_db
from webapp.db import get_db
from webapp.json_helpers import safe_json_dict, safe_json_list
from webapp.models import PlatformApiStream
from webapp.schemas import (
    BuilderStreamPublishRequest,
    BuilderStreamView,
    BuilderTestRequest,
    BuilderTestResponse,
)


router = APIRouter(prefix="/builder", tags=["builder"])

def _standard_type(value: Any) -> tuple[str, str]:
    if isinstance(value, bool):
        return "BOOLEAN", "Boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER", "Integer"
    if isinstance(value, float):
        return "FLOAT", "Float"
    if isinstance(value, dict):
        return "JSON", "Object"
    if isinstance(value, list):
        return "JSON", "Array"
    return "STRING", type(value).__name__


def _flatten_payload(data: Any, path: list[str] | None = None) -> list[tuple[list[str], Any]]:
    current_path = path or []
    rows: list[tuple[list[str], Any]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            key_name = str(key)
            next_path = [*current_path, key_name]
            if isinstance(value, dict):
                rows.extend(_flatten_payload(value, next_path))
            else:
                rows.append((next_path, value))
        return rows
    rows.append((current_path or ["data"], data))
    return rows


def _jsonpath_select(payload: Any, json_path: str) -> Any:
    expr = str(json_path or "").strip()
    if not expr or expr == "$":
        return payload
    if expr.startswith("$."):
        expr = expr[2:]
    segments = [x for x in expr.split(".") if x]

    current: Any = payload
    for seg in segments:
        if isinstance(current, list):
            if seg == "*":
                current = current
                continue
            if seg.isdigit():
                idx = int(seg)
                if idx < 0 or idx >= len(current):
                    return []
                current = current[idx]
                continue
            return []
        if not isinstance(current, dict):
            return []
        if seg not in current:
            return []
        current = current.get(seg)
    return current


def _inject_auth(
    *,
    headers: dict[str, Any],
    query_params: dict[str, Any],
    body: dict[str, Any],
    auth_strategy: dict[str, Any],
    test_vars: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    auth_type = str(auth_strategy.get("type") or "None").strip().lower()
    if auth_type in {"none", ""}:
        return headers, query_params, body

    inject_into = str(auth_strategy.get("inject_into") or "header").strip().lower()
    key_name = str(auth_strategy.get("key_name") or "").strip() or "Authorization"
    test_key = str(auth_strategy.get("test_variable") or "token").strip() or "token"
    token = str(test_vars.get(test_key) or test_vars.get("token") or test_vars.get("access_token") or "").strip()
    if not token:
        return headers, query_params, body

    value = token
    if auth_type in {"bearer token", "bearertoken", "oauth2.0", "oauth2"} and key_name.lower() == "authorization":
        value = token if token.lower().startswith("bearer ") else "Bearer {0}".format(token)

    if inject_into == "query":
        query_params[key_name] = value
    elif inject_into == "body":
        body[key_name] = value
    else:
        headers[key_name] = value
    return headers, query_params, body


def _request_with_retry(
    *,
    method: str,
    url: str,
    headers: dict[str, Any],
    query_params: dict[str, Any],
    body: dict[str, Any],
    max_retry: int = 4,
) -> dict[str, Any]:
    retryable_status = {429, 500, 502, 504}
    fatal_status = {401, 403, 404}
    method_value = str(method or "GET").strip().upper()

    last_error = "request failed"
    for attempt in range(1, max_retry + 1):
        try:
            response = requests.request(
                method=method_value,
                url=url,
                headers=headers,
                params=query_params,
                json=body if method_value in {"POST", "PUT", "PATCH"} else None,
                timeout=(10, 60),
            )
            if response.status_code in fatal_status:
                raise HTTPException(status_code=400, detail="fatal upstream status: {0}".format(response.status_code))
            if response.status_code in retryable_status:
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    sleep_seconds = float(retry_after) if (retry_after and str(retry_after).isdigit()) else 1.5
                else:
                    sleep_seconds = min(10.0, (2 ** (attempt - 1)) * 0.8 + random.uniform(0, 0.4))
                if attempt >= max_retry:
                    response.raise_for_status()
                time.sleep(sleep_seconds)
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"data": payload}
            return payload
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover - network/runtime errors
            last_error = str(exc)
            if attempt >= max_retry:
                break
            sleep_seconds = min(10.0, (2 ** (attempt - 1)) * 0.8 + random.uniform(0, 0.4))
            time.sleep(sleep_seconds)
    raise HTTPException(status_code=502, detail="upstream request failed: {0}".format(last_error))


def _to_stream_view(item: PlatformApiStream) -> BuilderStreamView:
    return BuilderStreamView(
        id=item.id,
        platform_code=item.platform_code,
        stream_name=item.stream_name,
        display_name=item.display_name,
        doc_url=item.doc_url,
        request_config=safe_json_dict(item.request_config_json),
        auth_strategy=safe_json_dict(item.auth_strategy_json),
        pagination_strategy=safe_json_dict(item.pagination_strategy_json),
        extraction_strategy=safe_json_dict(item.extraction_strategy_json),
        supported_sync_modes=[str(x) for x in safe_json_list(item.supported_sync_modes_json)],
        status=item.status,
        created_at=item.created_at,
        updated_at=item.updated_at,
    )


@router.post("/test", response_model=BuilderTestResponse)
def test_builder_api(request: BuilderTestRequest) -> BuilderTestResponse:
    cfg = request.request_config
    method = str(cfg.method or "GET").strip().upper()
    base = str(cfg.url_base or "").strip()
    path = str(cfg.url_path or "").strip()
    if not base:
        raise HTTPException(status_code=400, detail="request_config.url_base is required")
    url = urljoin(base.rstrip("/") + "/", path.lstrip("/")) if path else base

    headers = dict(cfg.headers or {})
    query_params = dict(cfg.query_params or {})
    body = dict(cfg.body or {})
    headers, query_params, body = _inject_auth(
        headers=headers,
        query_params=query_params,
        body=body,
        auth_strategy=request.auth_strategy.model_dump(),
        test_vars=request.test_variables,
    )

    # 防止测试拉全量，强制压缩采样参数。
    query_params.setdefault("limit", 1)
    query_params.setdefault("page_size", 1)
    body.setdefault("limit", 1)
    body.setdefault("page_size", 1)

    raw = _request_with_retry(
        method=method,
        url=url,
        headers=headers,
        query_params=query_params,
        body=body,
    )

    extracted = _jsonpath_select(raw, request.extraction_strategy.record_selector)
    if isinstance(extracted, dict):
        extracted_records: list[dict[str, Any]] = [extracted]
    elif isinstance(extracted, list):
        extracted_records = [x for x in extracted if isinstance(x, dict)]
    else:
        extracted_records = []
    extracted_records = extracted_records[:50]

    field_map: dict[str, dict[str, Any]] = {}
    if extracted_records:
        flattened = _flatten_payload(extracted_records[0], [])
        for path_parts, value in flattened:
            if not path_parts:
                continue
            std_type, source_type = _standard_type(value)
            key = ".".join(path_parts)
            if key not in field_map:
                field_map[key] = {
                    "name": path_parts[-1],
                    "path": path_parts,
                    "type": std_type,
                    "source_type": source_type,
                }

    return BuilderTestResponse(
        request_preview={
            "url": url,
            "method": method,
            "headers": headers,
            "query_params": query_params,
            "body": body if method in {"POST", "PUT", "PATCH"} else {},
        },
        raw_response=raw,
        extracted_records=extracted_records,
        inferred_schema=list(field_map.values()),
    )


@router.post("/streams", response_model=BuilderStreamView)
def save_builder_stream_api(
    request: BuilderStreamPublishRequest,
    db: Session | None = Depends(get_db),
) -> BuilderStreamView:
    db = require_db(db, detail="Database is disabled. Builder APIs are unavailable.")
    existed = (
        db.query(PlatformApiStream)
        .filter(
            PlatformApiStream.platform_code == request.platform_code,
            PlatformApiStream.stream_name == request.stream_name,
        )
        .order_by(PlatformApiStream.id.desc())
        .first()
    )
    if existed is None:
        existed = PlatformApiStream(
            platform_code=request.platform_code,
            stream_name=request.stream_name,
            created_at=datetime.utcnow(),
        )
        db.add(existed)

    existed.display_name = request.display_name or request.stream_name
    existed.doc_url = request.doc_url or ""
    existed.request_config_json = json.dumps(request.request_config.model_dump(), ensure_ascii=False)
    existed.auth_strategy_json = json.dumps(request.auth_strategy.model_dump(), ensure_ascii=False)
    existed.pagination_strategy_json = json.dumps(request.pagination_strategy.model_dump(), ensure_ascii=False)
    existed.extraction_strategy_json = json.dumps(request.extraction_strategy.model_dump(), ensure_ascii=False)
    existed.supported_sync_modes_json = json.dumps(request.supported_sync_modes, ensure_ascii=False)
    existed.status = "published"

    db.commit()
    db.refresh(existed)
    return _to_stream_view(existed)


@router.get("/streams", response_model=list[BuilderStreamView])
def list_builder_streams_api(
    platform_code: str = Query(default=""),
    db: Session | None = Depends(get_db),
) -> list[BuilderStreamView]:
    db = require_db(db, detail="Database is disabled. Builder APIs are unavailable.")
    query = db.query(PlatformApiStream)
    if platform_code.strip():
        query = query.filter(PlatformApiStream.platform_code == platform_code.strip())
    rows = query.order_by(PlatformApiStream.updated_at.desc(), PlatformApiStream.id.desc()).all()
    return [_to_stream_view(item) for item in rows]

