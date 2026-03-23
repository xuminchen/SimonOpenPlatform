from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


_RED_REPORT_TIMEZONE = timezone(timedelta(hours=8))


RED_JUGUANG_DOC_URLS: dict[str, str] = {
    "4417": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4417",
    "4112": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4112",
    "4301": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4301",
    "4302": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4302",
    "3216": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3216",
    "3215": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3215",
    "3211": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3211",
    "2729": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=2729",
    "3150": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3150",
    "3044": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3044",
    "3158": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3158",
    "4594": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4594",
    "4684": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4684",
    "4647": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4647",
    "4644": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=4644",
    "3835": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3835",
    "3803": "http://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3803",
    "3714": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3714",
    "2738": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=2738",
    "2735": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=2735",
    "2736": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=2736",
    "2737": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=2737",
    "3073": "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=3073",
}

# 23 篇文档仅提供 articleId，非请求地址。
# 仅在已确认真实地址时进行映射，其余保持 None，避免把文档 ID 错当 endpoint。
DEFAULT_ARTICLE_ENDPOINTS: dict[str, str | None] = {
    "4417": "/api/open/jg/data/report/offline/easy/promotion/group",
    "4112": None,
    "4301": None,
    "4302": None,
    "3216": None,
    "3215": None,
    "3211": None,
    "2729": None,
    "3150": None,
    "3044": None,
    "3158": None,
    "4594": None,
    "4684": None,
    "4647": None,
    "4644": None,
    "3835": None,
    "3803": None,
    "3714": None,
    "2738": None,
    "2735": None,
    "2736": None,
    "2737": None,
    "3073": None,
}

# 这些是当前仓库里“已真实使用过”的小红书请求地址，和 articleId 无直接绑定关系。
KNOWN_RED_JUGUANG_ENDPOINTS: dict[str, str] = {
    "oauth_access_token": "/api/open/oauth2/access_token",
    "oauth_refresh_token": "/api/open/oauth2/refresh_token",
    "jg_account_balance_info": "/api/open/jg/account/balance/info",
    "jg_account_order_info": "/api/open/jg/account/order/info",
    "jg_campaign_group_base_list": "/api/open/jg/campaign/group/base/list",
    "jg_ube_extra_query": "/api/open/jg/ube/extra/query",
    "jg_note_list": "/api/open/jg/note/list",
    "jg_report_realtime_target": "/api/open/jg/data/report/realtime/target",
    "jg_report_offline_account": "/api/open/jg/data/report/offline/account",
    "jg_report_offline_campaign": "/api/open/jg/data/report/offline/campaign",
    "jg_report_offline_unit": "/api/open/jg/data/report/offline/unit",
    "jg_report_offline_creative": "/api/open/jg/data/report/offline/creative",
    "jg_report_offline_keyword": "/api/open/jg/data/report/offline/keyword",
    "jg_report_offline_search_word": "/api/open/jg/data/report/offline/search/word",
    "jg_report_offline_note": "/api/open/jg/data/report/offline/note",
    "jg_report_offline_spu": "/api/open/jg/data/report/offline/spu",
    "jg_report_offline_easy_promotion_base": "/api/open/jg/data/report/offline/easy/promotion/base",
    "jg_report_offline_easy_promotion_note": "/api/open/jg/data/report/offline/easy/promotion/note",
    "jg_report_offline_easy_promotion_group": "/api/open/jg/data/report/offline/easy/promotion/group",
}


class RedJuGuangApiClient:
    base_url = "https://adapi.xiaohongshu.com"

    def __init__(
        self,
        *,
        access_token: str = "",
        timeout_seconds: int = 30,
        article_endpoints: dict[str, str | None] | None = None,
    ) -> None:
        self.access_token = str(access_token or "").strip()
        self.article_endpoints = dict(DEFAULT_ARTICLE_ENDPOINTS)
        if article_endpoints:
            self.article_endpoints.update(article_endpoints)
        self.http_client = HttpClient(
            HttpRequestConfig(
                timeout_seconds=timeout_seconds,
                max_retries=3,
                retry_interval_seconds=1.2,
            )
        )

    def _request(
        self,
        *,
        endpoint: str,
        payload: dict[str, Any] | None = None,
        method: str = "post",
        with_access_token: bool = True,
        event_name: str = "webapp_red_juguang_api",
    ) -> dict[str, Any]:
        url = endpoint if endpoint.startswith("http") else "{0}{1}".format(self.base_url, endpoint)
        headers = {"content-type": "application/json"}
        if with_access_token:
            if not self.access_token:
                raise ValueError("access_token is required")
            headers["Access-Token"] = self.access_token

        body = json.dumps(payload or {}, ensure_ascii=False)
        result = self.http_client.request_json(
            method=method.lower(),
            url=url,
            headers=headers,
            data=body if method.lower() in {"post", "put", "patch"} else None,
            success_checker=lambda p: isinstance(p, dict),
            event_name=event_name,
        )
        if not result.ok:
            raise RuntimeError(result.error or result.message or "request failed")
        if not isinstance(result.data, dict):
            raise RuntimeError("invalid response payload")
        return result.data

    def call_article(
        self,
        article_id: str,
        *,
        payload: dict[str, Any] | None = None,
        method: str = "post",
        with_access_token: bool = True,
    ) -> dict[str, Any]:
        key = str(article_id or "").strip()
        endpoint = self.article_endpoints.get(key)
        if not endpoint:
            doc_url = RED_JUGUANG_DOC_URLS.get(key, "")
            raise ValueError("article {0} endpoint is not mapped yet. doc={1}".format(key, doc_url))
        return self._request(
            endpoint=endpoint,
            payload=payload,
            method=method,
            with_access_token=with_access_token,
            event_name="webapp_red_juguang_article_{0}".format(key),
        )

    # --- 23 个文档接口方法入口 ---
    def api_4417(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_payload = dict(payload or {})
        if "page_num" not in normalized_payload and "page" in normalized_payload:
            normalized_payload["page_num"] = normalized_payload.pop("page")
        page_num = int(normalized_payload.get("page_num") or 1)
        page_size = int(normalized_payload.get("page_size") or 20)
        normalized_payload["page_num"] = max(1, page_num)
        normalized_payload["page_size"] = max(1, min(page_size, 500))
        return self.call_article("4417", payload=normalized_payload, with_access_token=True)

    def api_4112(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4112", payload=payload, with_access_token=False)

    def api_4301(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4301", payload=payload)

    def api_4302(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4302", payload=payload)

    def api_3216(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3216", payload=payload)

    def api_3215(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3215", payload=payload)

    def api_3211(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3211", payload=payload)

    def api_2729(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("2729", payload=payload)

    def api_3150(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3150", payload=payload)

    def api_3044(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3044", payload=payload)

    def api_3158(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3158", payload=payload)

    def api_4594(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4594", payload=payload)

    def api_4684(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4684", payload=payload)

    def api_4647(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4647", payload=payload)

    def api_4644(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("4644", payload=payload)

    def api_3835(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3835", payload=payload)

    def api_3803(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3803", payload=payload)

    def api_3714(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3714", payload=payload)

    def api_2738(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("2738", payload=payload)

    def api_2735(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("2735", payload=payload)

    def api_2736(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("2736", payload=payload)

    def api_2737(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("2737", payload=payload)

    def api_3073(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.call_article("3073", payload=payload)

    # --- 已确认的真实接口能力（非 articleId 占位） ---
    def offline_spu_report(
        self,
        *,
        advertiser_id: int,
        start_date: str,
        end_date: str,
        time_unit: str = "DAY",
        sort_column: str = "",
        sort: str = "",
        page_num: int = 1,
        page_size: int = 20,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not advertiser_id:
            raise ValueError("advertiser_id is required")
        if not str(start_date or "").strip():
            raise ValueError("start_date is required")
        if not str(end_date or "").strip():
            raise ValueError("end_date is required")
        size = max(1, min(int(page_size or 20), 500))
        page = max(1, int(page_num or 1))

        payload: dict[str, Any] = {
            "advertiser_id": int(advertiser_id),
            "start_date": str(start_date).strip(),
            "end_date": str(end_date).strip(),
            "time_unit": str(time_unit or "DAY").strip() or "DAY",
            "page_num": page,
            "page_size": size,
        }
        if sort_column:
            payload["sort_column"] = str(sort_column).strip()
        if sort:
            payload["sort"] = str(sort).strip()
        if isinstance(extra, dict):
            payload.update(extra)

        return self._call_known_endpoint(
            "jg_report_offline_spu",
            payload=payload,
            event_name="webapp_red_juguang_offline_spu_report",
        )

    def offline_note_report(
        self,
        *,
        advertiser_id: int,
        start_date: str,
        end_date: str,
        time_unit: str = "DAY",
        page_num: int = 1,
        page_size: int = 20,
        sort_column: str = "",
        sort: str = "",
        marketing_target: list[int] | None = None,
        bidding_strategy: list[int] | None = None,
        optimize_target: list[int] | None = None,
        placement: list[int] | None = None,
        promotion_target: list[int] | None = None,
        programmatic: list[int] | None = None,
        delivery_mode: list[int] | None = None,
        split_columns: list[str] | None = None,
        data_caliber: int | None = None,
        filters: list[dict[str, Any]] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not advertiser_id:
            raise ValueError("advertiser_id is required")
        if not str(start_date or "").strip():
            raise ValueError("start_date is required")
        if not str(end_date or "").strip():
            raise ValueError("end_date is required")
        size = max(1, min(int(page_size or 20), 500))
        page = max(1, int(page_num or 1))

        payload: dict[str, Any] = {
            "advertiser_id": int(advertiser_id),
            "start_date": str(start_date).strip(),
            "end_date": str(end_date).strip(),
            "time_unit": str(time_unit or "DAY").strip() or "DAY",
            "page_num": page,
            "page_size": size,
        }
        if sort_column:
            payload["sort_column"] = str(sort_column).strip()
        if sort:
            payload["sort"] = str(sort).strip()
        if marketing_target:
            payload["marketing_target"] = marketing_target
        if bidding_strategy:
            payload["bidding_strategy"] = bidding_strategy
        if optimize_target:
            payload["optimize_target"] = optimize_target
        if placement:
            payload["placement"] = placement
        if promotion_target:
            payload["promotion_target"] = promotion_target
        if programmatic:
            payload["programmatic"] = programmatic
        if delivery_mode:
            payload["delivery_mode"] = delivery_mode
        if split_columns:
            payload["split_columns"] = split_columns
        if data_caliber is not None:
            payload["data_caliber"] = int(data_caliber)
        if filters:
            payload["filters"] = filters
        if isinstance(extra, dict):
            payload.update(extra)

        return self._call_known_endpoint(
            "jg_report_offline_note",
            payload=payload,
            event_name="webapp_red_juguang_offline_note_report",
        )

    def _normalize_report_payload(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        normalized = dict(payload or {})
        clamped_start = self._clamp_date_string_to_yesterday(normalized.get("start_date"))
        if clamped_start:
            normalized["start_date"] = clamped_start
        clamped_end = self._clamp_date_string_to_yesterday(normalized.get("end_date"))
        if clamped_end:
            normalized["end_date"] = clamped_end
        if "page_num" not in normalized and "page" in normalized:
            normalized["page_num"] = normalized.pop("page")
        if "page_num" in normalized:
            normalized["page_num"] = max(1, int(normalized["page_num"]))
        if "page_size" in normalized:
            normalized["page_size"] = max(1, min(int(normalized["page_size"]), 500))
        return normalized

    def _extract_page_context(
        self, response: dict[str, Any], request_payload: dict[str, Any]
    ) -> tuple[int | None, int | None, int]:
        data = response.get("data")
        if not isinstance(data, dict):
            return None, None, 1

        total_count: int | None = None
        page_size: int | None = None
        current_page: int = 1

        if isinstance(data.get("total_count"), int):
            total_count = int(data["total_count"])
        page = data.get("page")
        if isinstance(page, dict):
            if isinstance(page.get("total_count"), int):
                total_count = int(page["total_count"])
            if isinstance(page.get("page_index"), int):
                current_page = max(1, int(page["page_index"]))
            if isinstance(page.get("page_size"), int):
                page_size = int(page["page_size"])
            if isinstance(page.get("page_num"), int):
                current_page = max(1, int(page["page_num"]))

        if isinstance(data.get("page_num"), int):
            current_page = max(1, int(data["page_num"]))
        if isinstance(data.get("page_size"), int):
            page_size = int(data["page_size"])

        if page_size is None:
            raw_page_size = request_payload.get("page_size")
            if raw_page_size is not None:
                page_size = max(1, int(raw_page_size))

        if page_size is None:
            data_list = data.get("data_list")
            if isinstance(data_list, list) and data_list:
                page_size = len(data_list)

        return total_count, page_size, current_page

    def _call_known_endpoint(
        self,
        endpoint_key: str,
        *,
        payload: dict[str, Any] | None = None,
        event_name: str,
    ) -> dict[str, Any]:
        normalized = self._normalize_report_payload(payload)
        auto_paginate = bool(normalized.pop("auto_paginate", True))
        max_pages = max(1, int(normalized.pop("max_pages", 200)))
        if not normalized.get("advertiser_id"):
            raise ValueError("advertiser_id is required")

        first_resp = self._request(
            endpoint=KNOWN_RED_JUGUANG_ENDPOINTS[endpoint_key],
            payload=normalized,
            method="post",
            with_access_token=True,
            event_name=event_name,
        )
        if not auto_paginate:
            return first_resp

        data = first_resp.get("data")
        if not isinstance(data, dict):
            return first_resp
        first_page_list = data.get("data_list")
        if not isinstance(first_page_list, list):
            return first_resp

        total_count, page_size, current_page = self._extract_page_context(first_resp, normalized)
        if total_count is None or page_size is None or page_size <= 0:
            return first_resp
        if len(first_page_list) >= total_count:
            return first_resp

        total_pages = (total_count + page_size - 1) // page_size
        if total_pages <= 1:
            return first_resp

        all_rows = list(first_page_list)
        for next_page in range(current_page + 1, min(total_pages, max_pages) + 1):
            next_payload = dict(normalized)
            next_payload["page_num"] = next_page
            page_resp = self._request(
                endpoint=KNOWN_RED_JUGUANG_ENDPOINTS[endpoint_key],
                payload=next_payload,
                method="post",
                with_access_token=True,
                event_name="{0}_page_{1}".format(event_name, next_page),
            )
            page_data = page_resp.get("data")
            if not isinstance(page_data, dict):
                break
            page_rows = page_data.get("data_list")
            if not isinstance(page_rows, list) or not page_rows:
                break
            all_rows.extend(page_rows)
            if len(all_rows) >= total_count:
                break

        data["data_list"] = all_rows
        data["total_count"] = total_count
        return first_resp

    def offline_easy_promotion_group_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_easy_promotion_group",
            payload=payload,
            event_name="webapp_red_juguang_offline_easy_promotion_group_report",
        )

    def offline_easy_promotion_note_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_easy_promotion_note",
            payload=payload,
            event_name="webapp_red_juguang_offline_easy_promotion_note_report",
        )

    def offline_easy_promotion_base_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_easy_promotion_base",
            payload=payload,
            event_name="webapp_red_juguang_offline_easy_promotion_base_report",
        )

    def offline_account_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_account",
            payload=payload,
            event_name="webapp_red_juguang_offline_account_report",
        )

    def offline_campaign_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_campaign",
            payload=payload,
            event_name="webapp_red_juguang_offline_campaign_report",
        )

    def offline_unit_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_unit",
            payload=payload,
            event_name="webapp_red_juguang_offline_unit_report",
        )

    def offline_creative_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_creative",
            payload=payload,
            event_name="webapp_red_juguang_offline_creative_report",
        )

    def offline_keyword_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_keyword",
            payload=payload,
            event_name="webapp_red_juguang_offline_keyword_report",
        )

    def offline_search_word_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_report_offline_search_word",
            payload=payload,
            event_name="webapp_red_juguang_offline_search_word_report",
        )

    def campaign_group_base_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_campaign_group_base_list",
            payload=payload,
            event_name="webapp_red_juguang_campaign_group_base_list",
        )

    def query_ube_extra(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call_known_endpoint(
            "jg_ube_extra_query",
            payload=payload,
            event_name="webapp_red_juguang_ube_extra_query",
        )

    @staticmethod
    def _clamp_date_string_to_yesterday(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        if len(text) < 10:
            return text
        candidate = text[:10]
        try:
            day = datetime.strptime(candidate, "%Y-%m-%d").date()
        except ValueError:
            return text
        max_day = (datetime.now(_RED_REPORT_TIMEZONE) - timedelta(days=1)).date()
        if day > max_day:
            return max_day.strftime("%Y-%m-%d")
        return candidate
