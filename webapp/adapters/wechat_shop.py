from __future__ import annotations

import datetime
from typing import Any, Dict, List

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


class WechatShopAdapter:
    platform = "wechat_shop"

    def __init__(self) -> None:
        self.client = HttpClient(
            HttpRequestConfig(
                timeout_seconds=30,
                max_retries=3,
                retry_interval_seconds=1,
            )
        )

    @staticmethod
    def _resolve_window(start_date: str | None, end_date: str | None) -> tuple[int, int]:
        max_day = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
        if start_date and end_date:
            start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
            end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        else:
            end_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=59)
            start_dt = end_dt.replace(hour=0, minute=0, second=0)
        if start_dt.date() > max_day:
            start_dt = datetime.datetime.combine(max_day, datetime.time(0, 0, 0))
        if end_dt.date() > max_day:
            end_dt = datetime.datetime.combine(max_day, datetime.time(23, 59, 59))
        if start_dt > end_dt:
            start_dt = end_dt.replace(hour=0, minute=0, second=0)
        return int(start_dt.timestamp()), int(end_dt.timestamp())

    def _get_access_token(self, app_id: str, secret: str) -> str:
        result = self.client.request_json(
            method="get",
            url="https://api.weixin.qq.com/cgi-bin/token",
            params={
                "grant_type": "client_credential",
                "appid": app_id,
                "secret": secret,
            },
            success_checker=lambda payload: isinstance(payload, dict) and "access_token" in payload,
            event_name="phase1_wechat_access_token",
        )
        if not result.ok or not isinstance(result.data, dict):
            raise RuntimeError("Failed to get wechat access token: {0}".format(result.error or result.message))
        return str(result.data["access_token"])

    def sync_orders(self, *, account_name: str, account_config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        app_id = str(account_config.get("app_id", "")).strip()
        secret = str(account_config.get("secret", "")).strip()
        if not app_id or not secret:
            raise ValueError("wechat_shop account config must include app_id and secret")

        start_ts, end_ts = self._resolve_window(payload.get("start_date"), payload.get("end_date"))
        time_type = str(payload.get("time_type", "create_time"))
        page_size = int(payload.get("page_size", 50))

        access_token = self._get_access_token(app_id=app_id, secret=secret)
        url = "https://api.weixin.qq.com/channels/ec/order/list/get?access_token={0}".format(access_token)
        req_payload: Dict[str, Any] = {
            "{0}_range".format(time_type): {"start_time": start_ts, "end_time": end_ts},
            "page_size": page_size,
        }

        all_order_ids: List[str] = []
        page_count = 0

        while True:
            response = self.client.request_json(
                method="post",
                url=url,
                json_data=req_payload,
                success_checker=lambda body: isinstance(body, dict) and (body.get("errcode", 0) == 0),
                event_name="phase1_wechat_orders",
            )
            if not response.ok or not isinstance(response.data, dict):
                raise RuntimeError("Wechat order list request failed: {0}".format(response.error or response.message))

            body = response.data
            page_count += 1
            ids = body.get("order_id_list", [])
            if isinstance(ids, list):
                all_order_ids.extend([str(item) for item in ids])

            if not body.get("has_more"):
                break

            next_key = body.get("next_key")
            if not next_key:
                break
            req_payload["next_key"] = next_key

            if page_count >= 20:
                break

        return {
            "platform": self.platform,
            "account_name": account_name,
            "time_type": time_type,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "page_count": page_count,
            "order_count": len(all_order_ids),
            "sample_order_ids": all_order_ids[:10],
        }
