"""Ads channels API aggregation facade."""

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


def create_ads_http_client(timeout_seconds: int = 45) -> HttpClient:
    return HttpClient(HttpRequestConfig(timeout_seconds=timeout_seconds, max_retries=4, retry_interval_seconds=1.5))


__all__ = ["create_ads_http_client"]
