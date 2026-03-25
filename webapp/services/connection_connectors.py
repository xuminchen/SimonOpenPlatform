from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urljoin

import requests

from webapp.db import SessionLocal
from webapp.json_helpers import safe_json_dict, safe_json_list
from webapp.models import PlatformApiStream
from webapp.services.red_juguang_api import RedJuGuangApiClient


_RED_REPORT_TIMEZONE = timezone(timedelta(hours=8))


class ISourceConnector(Protocol):
    platform_code: str

    def test_connection(self, credential: dict[str, Any]) -> bool:
        ...

    def discover_schema(self) -> list[dict[str, Any]]:
        ...

    def pull_data(self, stream_name: str, credential: dict[str, Any], state: dict[str, Any] | None = None) -> dict[str, Any]:
        ...


@dataclass
class StaticSourceConnector:
    platform_code: str
    streams: list[dict[str, Any]]

    def test_connection(self, credential: dict[str, Any]) -> bool:
        token = ""
        if isinstance(credential.get("token"), dict):
            token = str(credential["token"].get("access_token", "")).strip()
        if not token:
            token = str(credential.get("access_token", "")).strip() or str(credential.get("advertiser_access_token", "")).strip()
        return bool(token)

    def discover_schema(self) -> list[dict[str, Any]]:
        return list(self.streams)

    def pull_data(self, stream_name: str, credential: dict[str, Any], state: dict[str, Any] | None = None) -> dict[str, Any]:
        _ = (credential, state, stream_name)
        return {"records": [], "next_state": state or {}}


@dataclass
class GenericSourceConnector(StaticSourceConnector):
    def _list_platform_stream_rows(self) -> list[PlatformApiStream]:
        if SessionLocal is None:
            return []
        db = SessionLocal()
        try:
            rows = (
                db.query(PlatformApiStream)
                .filter(
                    PlatformApiStream.platform_code == self.platform_code,
                    PlatformApiStream.status == "published",
                )
                .order_by(PlatformApiStream.updated_at.desc(), PlatformApiStream.id.desc())
                .all()
            )
            return [item for item in rows if isinstance(item, PlatformApiStream)]
        finally:
            db.close()

    def _find_stream_row(self, stream_name: str) -> PlatformApiStream | None:
        target = str(stream_name or "").strip()
        if not target or SessionLocal is None:
            return None
        db = SessionLocal()
        try:
            row = (
                db.query(PlatformApiStream)
                .filter(
                    PlatformApiStream.platform_code == self.platform_code,
                    PlatformApiStream.stream_name == target,
                    PlatformApiStream.status == "published",
                )
                .order_by(PlatformApiStream.updated_at.desc(), PlatformApiStream.id.desc())
                .first()
            )
            return row if isinstance(row, PlatformApiStream) else None
        finally:
            db.close()

    @staticmethod
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

    @staticmethod
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
        token = str(
            test_vars.get(test_key)
            or test_vars.get("token")
            or test_vars.get("access_token")
            or test_vars.get("advertiser_access_token")
            or ""
        ).strip()
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

    @staticmethod
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
                    raise RuntimeError("fatal upstream status: {0}".format(response.status_code))
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
            except Exception as exc:
                last_error = str(exc)
                if attempt >= max_retry:
                    break
                sleep_seconds = min(10.0, (2 ** (attempt - 1)) * 0.8 + random.uniform(0, 0.4))
                time.sleep(sleep_seconds)
        raise RuntimeError("upstream request failed: {0}".format(last_error))

    @staticmethod
    def _credential_test_variables(credential: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        vars_payload: dict[str, Any] = {}
        if isinstance(credential, dict):
            vars_payload.update(credential)
            token = credential.get("token")
            if isinstance(token, dict):
                vars_payload.update(token)
        state_vars = state.get("test_variables")
        if isinstance(state_vars, dict):
            vars_payload.update(state_vars)
        return vars_payload

    @staticmethod
    def _extract_records(raw: dict[str, Any], selector: str) -> list[dict[str, Any]]:
        extracted = GenericSourceConnector._jsonpath_select(raw, selector)
        if isinstance(extracted, dict):
            return [extracted]
        if isinstance(extracted, list):
            return [x for x in extracted if isinstance(x, dict)]
        return []

    def _pull_published_stream(self, row: PlatformApiStream, credential: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        request_cfg = safe_json_dict(row.request_config_json)
        auth_cfg = safe_json_dict(row.auth_strategy_json)
        extraction_cfg = safe_json_dict(row.extraction_strategy_json)
        pagination_cfg = safe_json_dict(row.pagination_strategy_json)

        method = str(request_cfg.get("method") or "GET").strip().upper()
        base = str(request_cfg.get("url_base") or "").strip()
        path = str(request_cfg.get("url_path") or "").strip()
        if not base:
            raise ValueError("published stream request_config.url_base is empty")
        url = urljoin(base.rstrip("/") + "/", path.lstrip("/")) if path else base

        headers = dict(request_cfg.get("headers") or {})
        query_params = dict(request_cfg.get("query_params") or {})
        body = dict(request_cfg.get("body") or {})
        headers, query_params, body = self._inject_auth(
            headers=headers,
            query_params=query_params,
            body=body,
            auth_strategy=auth_cfg,
            test_vars=self._credential_test_variables(credential, state),
        )

        # Runtime supports lightweight cursor/page injection.
        cursor = str(state.get("cursor_value") or "").strip()
        pagination_type = str(pagination_cfg.get("type") or "").strip().lower()
        inject_param = str(pagination_cfg.get("inject_param") or "").strip()
        if cursor and pagination_type in {"cursor", "offset"} and inject_param:
            query_params[inject_param] = cursor

        limit_value = int(state.get("limit") or state.get("page_size") or 100)
        limit_value = max(1, min(limit_value, 500))
        if "limit" not in query_params and "limit" not in body:
            query_params["limit"] = limit_value
        if "page_size" not in query_params and "page_size" not in body:
            query_params["page_size"] = limit_value

        raw_response = self._request_with_retry(
            method=method,
            url=url,
            headers=headers,
            query_params=query_params,
            body=body,
        )
        selector = str(extraction_cfg.get("record_selector") or "$.data.list").strip() or "$.data.list"
        records = self._extract_records(raw_response, selector)

        cursor_field = str(state.get("cursor_field") or "").strip()
        next_cursor = str(state.get("end_time") or state.get("cursor_value") or "").strip()
        if cursor_field and records:
            candidates = [str(item.get(cursor_field, "")).strip() for item in records if item.get(cursor_field) not in (None, "")]
            if candidates:
                next_cursor = max(candidates)

        return {
            "records": records,
            "next_state": {"cursor": next_cursor},
            "raw_response": raw_response,
            "request_preview": {
                "url": url,
                "method": method,
                "headers": headers,
                "query_params": query_params,
                "body": body if method in {"POST", "PUT", "PATCH"} else {},
            },
            "stream_runtime": {
                "platform_code": row.platform_code,
                "stream_name": row.stream_name,
                "supported_sync_modes": [str(x) for x in safe_json_list(row.supported_sync_modes_json)],
            },
        }

    def discover_schema(self) -> list[dict[str, Any]]:
        rows = self._list_platform_stream_rows()
        if not rows:
            return super().discover_schema()

        discovered: list[dict[str, Any]] = []
        for row in rows:
            modes = [str(x).upper() for x in safe_json_list(row.supported_sync_modes_json)]
            if not modes:
                modes = ["FULL_REFRESH", "INCREMENTAL"]
            discovered.append(
                {
                    "stream_name": str(row.stream_name),
                    "description": str(row.display_name or row.stream_name),
                    "supported_sync_modes": modes,
                    "default_cursor_field": ["updated_at"],
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "STRING", "primary_key": True},
                            {"name": "updated_at", "type": "TIMESTAMP", "cursor_candidate": True},
                        ]
                    },
                }
            )

        # keep fallback static rows for newly registered platform with no builder stream selected.
        fallback_rows = super().discover_schema()
        fallback_names = {str(item.get("stream_name", "")).strip() for item in discovered}
        for item in fallback_rows:
            name = str(item.get("stream_name", "")).strip()
            if name and name not in fallback_names:
                discovered.append(item)
        return discovered

    def pull_data(self, stream_name: str, credential: dict[str, Any], state: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime_state = state if isinstance(state, dict) else {}
        row = self._find_stream_row(stream_name)
        if row is not None:
            return self._pull_published_stream(row, credential, runtime_state)
        return super().pull_data(stream_name, credential, state=runtime_state)


@dataclass
class RedReportConnector(StaticSourceConnector):
    def _extract_access_token(self, credential: dict[str, Any]) -> str:
        token = credential.get("token")
        if isinstance(token, dict):
            value = str(token.get("access_token", "")).strip() or str(token.get("advertiser_access_token", "")).strip()
            if value:
                return value
        return (
            str(credential.get("access_token", "")).strip()
            or str(credential.get("advertiser_access_token", "")).strip()
        )

    def _extract_advertiser_ids(self, credential: dict[str, Any], state: dict[str, Any] | None) -> list[int]:
        # Allow manual override for debugging/replay on one advertiser.
        if state and state.get("advertiser_id") not in (None, ""):
            try:
                return [int(state.get("advertiser_id"))]
            except (TypeError, ValueError):
                pass

        result: list[int] = []
        seen: set[int] = set()

        def _push(value: Any) -> None:
            if value in (None, ""):
                return
            try:
                key = int(value)
            except (TypeError, ValueError):
                return
            if key in seen:
                return
            seen.add(key)
            result.append(key)

        for key in ("advertiser_id", "account_id"):
            _push(credential.get(key))

        token = credential.get("token")
        if isinstance(token, dict):
            _push(token.get("advertiser_id"))
            advertisers = token.get("approval_advertisers")
            if isinstance(advertisers, list):
                for item in advertisers:
                    if not isinstance(item, dict):
                        continue
                    _push(item.get("advertiser_id"))

        return result

    def _normalize_to_date(self, value: Any, fallback: datetime) -> str:
        max_day = (datetime.now(_RED_REPORT_TIMEZONE) - timedelta(days=1)).date()
        text = str(value or "").strip()
        if not text:
            target = fallback.date()
            if target > max_day:
                target = max_day
            return target.strftime("%Y-%m-%d")

        raw = text.replace("Z", "+00:00")
        if " " in raw and "T" not in raw:
            raw = raw.replace(" ", "T")
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is not None:
                dt = dt.astimezone(_RED_REPORT_TIMEZONE).replace(tzinfo=None)
            target = dt.date()
            if target > max_day:
                target = max_day
            return target.strftime("%Y-%m-%d")
        except ValueError:
            if len(text) >= 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", text[:10]):
                try:
                    target = datetime.strptime(text[:10], "%Y-%m-%d").date()
                    if target > max_day:
                        target = max_day
                    return target.strftime("%Y-%m-%d")
                except ValueError:
                    pass
        target = fallback.date()
        if target > max_day:
            target = max_day
        return target.strftime("%Y-%m-%d")

    def _invoke_client_method(self, method: Any, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return method(payload)

        params = list(signature.parameters.values())
        if len(params) == 1 and str(params[0].name or "") == "payload":
            return method(payload)
        return method(**payload)

    def _extract_records(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        data = response.get("data")
        if not isinstance(data, dict):
            return []
        rows = data.get("data_list")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return []

    def _response_summary(self, *, advertiser_id: int, response: dict[str, Any]) -> dict[str, Any]:
        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        rows = data.get("data_list") if isinstance(data.get("data_list"), list) else []
        code = response.get("code")
        success = response.get("success")
        message = response.get("msg") or response.get("message") or ""
        total_count = data.get("total_count")
        if total_count in (None, ""):
            page = data.get("page") if isinstance(data.get("page"), dict) else {}
            total_count = page.get("total_count")
        return {
            "advertiser_id": advertiser_id,
            "ok": (code in (None, 0, "0")) and (success is not False),
            "code": code,
            "success": success,
            "message": str(message or ""),
            "record_count": len(rows),
            "total_count": total_count if isinstance(total_count, int) else None,
            "page_num": data.get("page_num"),
            "page_size": data.get("page_size"),
        }

    def _next_cursor_from_records(self, records: list[dict[str, Any]], cursor_field: str, fallback: str) -> str:
        key = str(cursor_field or "").strip()
        if not key:
            return fallback
        candidates = [str(row.get(key, "")).strip() for row in records if isinstance(row, dict) and row.get(key) not in (None, "")]
        if not candidates:
            return fallback
        return max(candidates)

    def test_connection(self, credential: dict[str, Any]) -> bool:
        return bool(self._extract_access_token(credential))

    def pull_data(self, stream_name: str, credential: dict[str, Any], state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = state or {}
        stream = str(stream_name or "").strip().lower()

        method_by_stream = {
            "offline_campaign": "offline_campaign_report",
            "offline_creative": "offline_creative_report",
            "offline_account": "offline_account_report",
            "offline_unit": "offline_unit_report",
            "offline_keyword": "offline_keyword_report",
            "offline_search_word": "offline_search_word_report",
            "offline_note": "offline_note_report",
            "offline_spu": "offline_spu_report",
            "offline_easy_promotion_group": "offline_easy_promotion_group_report",
            "offline_easy_promotion_note": "offline_easy_promotion_note_report",
            "offline_easy_promotion_base": "offline_easy_promotion_base_report",
            "campaign_group_base_list": "campaign_group_base_list",
            "ube_extra_query": "query_ube_extra",
        }
        method_name = method_by_stream.get(stream)
        if not method_name and stream.startswith("offline_"):
            method_name = "{0}_report".format(stream)
        if not method_name:
            # Keep backward-compatible empty result for unsupported streams.
            return {"records": [], "next_state": state}

        access_token = self._extract_access_token(credential)
        if not access_token:
            raise ValueError("missing access_token for red connector")
        advertiser_ids = self._extract_advertiser_ids(credential, state)
        if not advertiser_ids:
            raise ValueError("missing advertiser_id for red connector")
        sample_mode = bool(state.get("sample_mode"))
        # Preview/测试链路也需要覆盖所有 advertiser_id，保证结果是全量合并视图。

        now_utc = datetime.now(timezone.utc)
        start_raw = state.get("start_time") or state.get("cursor_value") or ""
        end_raw = state.get("end_time") or now_utc.isoformat()

        default_start = now_utc - timedelta(days=7 if sample_mode else 1)
        start_date = self._normalize_to_date(start_raw, default_start)
        end_date = self._normalize_to_date(end_raw, now_utc)

        client = RedJuGuangApiClient(access_token=access_token)
        method = getattr(client, method_name)
        all_records: list[dict[str, Any]] = []
        raw_responses_by_advertiser: list[dict[str, Any]] = []
        for advertiser_id in advertiser_ids:
            payload: dict[str, Any] = {
                "advertiser_id": advertiser_id,
                "start_date": start_date,
                "end_date": end_date,
                "page_num": 1,
                "page_size": 1 if sample_mode else int(state.get("page_size") or state.get("limit") or 100),
                "auto_paginate": not sample_mode,
            }
            payload["page_size"] = max(1, min(int(payload["page_size"]), 500))
            response = self._invoke_client_method(method, payload)
            raw_responses_by_advertiser.append(self._response_summary(advertiser_id=advertiser_id, response=response))
            records = self._extract_records(response)
            for item in records:
                if "advertiser_id" not in item:
                    item["advertiser_id"] = advertiser_id
                all_records.append(item)

        cursor_field = str(state.get("cursor_field") or "time").strip()
        next_cursor = self._next_cursor_from_records(all_records, cursor_field=cursor_field, fallback=end_date)

        return {
            "records": all_records,
            "next_state": {
                "cursor": next_cursor,
                "start_date": start_date,
                "end_date": end_date,
                "advertiser_ids": advertiser_ids,
            },
            "raw_responses_by_advertiser": raw_responses_by_advertiser,
        }


_RED_STREAM_DEFINITIONS: list[dict[str, Any]] = [
    {"stream_name": "offline_campaign", "description": "计划层级离线报表数据", "primary_key": "campaign_id", "cursor": "time"},
    {"stream_name": "offline_creative", "description": "创意层级离线报表数据", "primary_key": "creativity_id", "cursor": "time"},
    {"stream_name": "offline_account", "description": "账户层级离线报表数据", "primary_key": "account_id", "cursor": "time"},
    {"stream_name": "offline_unit", "description": "单元层级离线报表数据", "primary_key": "unit_id", "cursor": "time"},
    {"stream_name": "offline_keyword", "description": "关键词层级离线报表数据", "primary_key": "keyword_id", "cursor": "time"},
    {"stream_name": "offline_search_word", "description": "搜索词层级离线报表数据", "primary_key": "search_word", "cursor": "time"},
    {"stream_name": "offline_note", "description": "笔记层级离线报表数据", "primary_key": "note_id", "cursor": "time"},
    {"stream_name": "offline_spu", "description": "SPU 商品层级离线报表数据", "primary_key": "spu_id", "cursor": "time"},
    {"stream_name": "offline_easy_promotion_group", "description": "轻投放广告组离线报表数据", "primary_key": "group_id", "cursor": "time"},
    {"stream_name": "offline_easy_promotion_note", "description": "轻投放笔记离线报表数据", "primary_key": "note_id", "cursor": "time"},
    {"stream_name": "offline_easy_promotion_base", "description": "轻投放基础离线报表数据", "primary_key": "promotion_id", "cursor": "time"},
    {"stream_name": "campaign_group_base_list", "description": "广告组基础列表数据", "primary_key": "campaign_group_id", "cursor": "create_time"},
    {"stream_name": "ube_extra_query", "description": "UBE 扩展查询数据", "primary_key": "campaign_group_id", "cursor": "update_time"},
]


def _build_red_stream_schemas(*, prefix: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _RED_STREAM_DEFINITIONS:
        stream_name = str(item["stream_name"])
        description = str(item["description"])
        primary_key = str(item["primary_key"])
        cursor_field = str(item["cursor"])
        rows.append(
            {
                "stream_name": stream_name,
                "description": "{0}{1}".format(prefix, description),
                "supported_sync_modes": ["FULL_REFRESH", "INCREMENTAL"],
                "default_cursor_field": [cursor_field],
                "schema": {
                    "fields": [
                        {"name": primary_key, "type": "STRING", "primary_key": True},
                        {"name": cursor_field, "type": "TIMESTAMP", "cursor_candidate": True},
                        {"name": "impression", "type": "INTEGER"},
                        {"name": "click", "type": "INTEGER"},
                        {"name": "cost", "type": "DECIMAL"},
                    ]
                },
            }
        )
    return rows


_STATIC_SCHEMA_BY_PLATFORM: dict[str, list[dict[str, Any]]] = {
    "red_juguang": _build_red_stream_schemas(),
    "wechat_shop": [
        {
            "stream_name": "orders",
            "description": "订单列表",
            "supported_sync_modes": ["FULL_REFRESH", "INCREMENTAL"],
            "default_cursor_field": ["update_time", "create_time"],
            "schema": {
                "fields": [
                    {"name": "order_id", "type": "STRING", "primary_key": True},
                    {"name": "create_time", "type": "TIMESTAMP", "cursor_candidate": True},
                    {"name": "update_time", "type": "TIMESTAMP", "cursor_candidate": True},
                    {"name": "status", "type": "STRING"},
                    {"name": "total_fee", "type": "INTEGER"},
                ]
            },
        }
    ],
    "oceanengine": [
        {
            "stream_name": "ad_report",
            "description": "广告报表明细",
            "supported_sync_modes": ["FULL_REFRESH", "INCREMENTAL"],
            "default_cursor_field": ["stat_time"],
            "schema": {
                "fields": [
                    {"name": "ad_id", "type": "STRING", "primary_key": True},
                    {"name": "stat_time", "type": "TIMESTAMP", "cursor_candidate": True},
                    {"name": "impressions", "type": "INTEGER"},
                    {"name": "cost", "type": "DECIMAL"},
                ]
            },
        }
    ],
    "jlgg": [
        {
            "stream_name": "ad_report",
            "description": "巨量广告报表明细",
            "supported_sync_modes": ["FULL_REFRESH", "INCREMENTAL"],
            "default_cursor_field": ["stat_time"],
            "schema": {
                "fields": [
                    {"name": "ad_id", "type": "STRING", "primary_key": True},
                    {"name": "stat_time", "type": "TIMESTAMP", "cursor_candidate": True},
                    {"name": "impressions", "type": "INTEGER"},
                    {"name": "cost", "type": "DECIMAL"},
                ]
            },
        }
    ],
    "jlqc": [
        {
            "stream_name": "ad_report",
            "description": "巨量千川报表明细",
            "supported_sync_modes": ["FULL_REFRESH", "INCREMENTAL"],
            "default_cursor_field": ["stat_time"],
            "schema": {
                "fields": [
                    {"name": "ad_id", "type": "STRING", "primary_key": True},
                    {"name": "stat_time", "type": "TIMESTAMP", "cursor_candidate": True},
                    {"name": "impressions", "type": "INTEGER"},
                    {"name": "cost", "type": "DECIMAL"},
                ]
            },
        }
    ],
    "red_chengfeng": _build_red_stream_schemas(prefix="乘风"),
}


_CONNECTOR_REGISTRY: dict[str, ISourceConnector] = {}
for platform_code, streams in _STATIC_SCHEMA_BY_PLATFORM.items():
    if platform_code in {"red_juguang", "red_chengfeng"}:
        _CONNECTOR_REGISTRY[platform_code] = RedReportConnector(platform_code=platform_code, streams=streams)
    else:
        _CONNECTOR_REGISTRY[platform_code] = StaticSourceConnector(platform_code=platform_code, streams=streams)


def get_connector(platform_code: str) -> ISourceConnector:
    key = str(platform_code or "").strip().lower()
    if key in _CONNECTOR_REGISTRY:
        return _CONNECTOR_REGISTRY[key]

    # Fallback schema for newly registered platforms in platform_configs.json.
    return GenericSourceConnector(
        platform_code=key or "unknown",
        streams=[
            {
                "stream_name": "default_stream",
                "description": "默认数据流（请按平台实际能力后续完善）",
                "supported_sync_modes": ["FULL_REFRESH", "INCREMENTAL"],
                "default_cursor_field": ["updated_at"],
                "schema": {
                    "fields": [
                        {"name": "id", "type": "STRING", "primary_key": True},
                        {"name": "updated_at", "type": "TIMESTAMP", "cursor_candidate": True},
                    ]
                },
            }
        ],
    )


def test_connection_with_latency_ms(connector: ISourceConnector, credential: dict[str, Any]) -> tuple[bool, int]:
    start = time.perf_counter()
    ok = connector.test_connection(credential)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return ok, max(elapsed_ms, 1)


def _snake_case(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return "field"
    text = re.sub(r"[^0-9A-Za-z_]+", "_", raw)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    if not text:
        return "field"
    if text[0].isdigit():
        text = "f_{0}".format(text)
    return text


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
    if value is None:
        return "STRING", "Null"

    text = str(value)
    raw = text.strip()
    lowered = raw.lower()
    if lowered in {"true", "false"}:
        return "BOOLEAN", "Boolean String"
    if re.fullmatch(r"-?\d+\.\d+", raw):
        return "FLOAT", "Decimal String"
    if re.fullmatch(r"\d{10}(\d{3})?", raw):
        return "TIMESTAMP", "Unix Timestamp"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2})?", raw):
        return "TIMESTAMP", "DateTime String"
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
            elif isinstance(value, list):
                rows.append((next_path, value))
            else:
                rows.append((next_path, value))
        return rows
    rows.append((current_path or ["data"], data))
    return rows


def _infer_flags(name: str, path: list[str], stream_meta: dict[str, Any]) -> tuple[bool, bool]:
    key = name.lower()
    source_fields = stream_meta.get("schema", {}).get("fields") if isinstance(stream_meta.get("schema"), dict) else []
    is_primary_key = key == "id" or key.endswith("_id")
    is_cursor_field = any(x in key for x in ("time", "date", "updated_at", "created_at"))

    if isinstance(source_fields, list):
        for item in source_fields:
            field_name = str(item.get("name", "")).strip().lower()
            if field_name != key:
                continue
            if bool(item.get("primary_key")):
                is_primary_key = True
            if bool(item.get("cursor_candidate")):
                is_cursor_field = True

    defaults = stream_meta.get("default_cursor_field") or []
    if isinstance(defaults, list) and defaults:
        if str(path[-1]).strip().lower() == str(defaults[0]).strip().lower():
            is_cursor_field = True

    return is_primary_key, is_cursor_field


def discover_dynamic_schema_contract(
    *,
    connector: ISourceConnector,
    stream_name: str,
    credential: dict[str, Any],
    stream_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stream_meta = stream_meta or {}
    state = {
        "sample_mode": True,
        # 强制只抽样 1 条，避免在配置期触发大批量拉取
        "page_size": 1,
        "limit": 1,
        "sample_size": 1,
    }
    pull_result = connector.pull_data(stream_name, credential, state=state)
    records = []
    if isinstance(pull_result, dict) and isinstance(pull_result.get("records"), list):
        records = pull_result.get("records") or []

    flattened: list[tuple[list[str], Any]] = []
    for row in records[:1]:
        flattened.extend(_flatten_payload(row, []))

    if not flattened:
        schema_fields = stream_meta.get("schema", {}).get("fields") if isinstance(stream_meta.get("schema"), dict) else []
        if isinstance(schema_fields, list):
            for item in schema_fields:
                fname = str(item.get("name", "")).strip()
                if not fname:
                    continue
                ftype = str(item.get("type", "STRING")).strip().upper() or "STRING"
                flattened.append(([fname], ftype))

    dedup: dict[str, dict[str, Any]] = {}
    for path, value in flattened:
        key = ".".join(path)
        normalized_name = _snake_case(path[-1] if path else "field")
        if isinstance(value, str) and value in {"STRING", "INTEGER", "FLOAT", "BOOLEAN", "TIMESTAMP", "JSON"}:
            std_type = value
            source_type = value
        else:
            std_type, source_type = _standard_type(value)
        is_pk, is_cursor = _infer_flags(normalized_name, path, stream_meta)
        selected = std_type != "JSON"
        field = {
            "name": normalized_name,
            "path": path,
            "type": std_type,
            "source_type": source_type,
            "is_primary_key": is_pk,
            "is_cursor_field": is_cursor,
            "selected": selected,
            "is_new": False,
        }
        if key not in dedup:
            dedup[key] = field

    return {
        "stream_name": stream_name,
        "description": "实时动态推断的 {0} 结构".format(stream_name),
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "fields": list(dedup.values()),
    }
