from __future__ import annotations

from typing import Any, Dict

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


class MetaAdsAdapter:
    platform = "meta_ads"

    def __init__(self) -> None:
        self.client = HttpClient(
            HttpRequestConfig(
                timeout_seconds=45,
                max_retries=3,
                retry_interval_seconds=1.5,
            )
        )

    def sync_ads_report(self, *, account_name: str, account_config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        access_token = str(account_config.get("access_token", "")).strip()
        ad_account_id = str(account_config.get("ad_account_id", "")).strip()
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        level = str(payload.get("level", "ad"))
        dry_run = bool(payload.get("dry_run", True))

        if dry_run:
            return {
                "platform": self.platform,
                "account_name": account_name,
                "mode": "dry_run",
                "level": level,
                "start_date": start_date,
                "end_date": end_date,
                "message": "Meta adapter skeleton is ready. Set dry_run=false to call Graph API.",
            }

        if not access_token or not ad_account_id:
            raise ValueError("meta_ads config must include access_token and ad_account_id when dry_run=false")

        url = "https://graph.facebook.com/v22.0/act_{0}/insights".format(ad_account_id)
        params = {
            "access_token": access_token,
            "level": level,
            "fields": "account_name,campaign_name,adset_name,ad_name,impressions,clicks,spend",
            "time_range": "{\"since\":\"%s\",\"until\":\"%s\"}" % (start_date, end_date),
            "limit": 100,
        }

        result = self.client.request_json(
            method="get",
            url=url,
            params=params,
            success_checker=lambda body: isinstance(body, dict) and "data" in body,
            event_name="phase1_meta_ads_report",
        )
        if not result.ok or not isinstance(result.data, dict):
            raise RuntimeError("Meta report request failed: {0}".format(result.error or result.message))

        rows = result.data.get("data", [])
        return {
            "platform": self.platform,
            "account_name": account_name,
            "mode": "live",
            "level": level,
            "start_date": start_date,
            "end_date": end_date,
            "row_count": len(rows) if isinstance(rows, list) else 0,
            "sample_rows": rows[:5] if isinstance(rows, list) else [],
        }
