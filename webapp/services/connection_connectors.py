from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
import re
import time
from dataclasses import dataclass
from typing import Any, Protocol

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
    pass


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
