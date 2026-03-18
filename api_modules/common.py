from __future__ import annotations

from typing import Any, Dict, Optional

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


def request_json(
    *,
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Any = None,
    json_data: Any = None,
    timeout_seconds: int = 30,
    event_name: str = "api_modules_request",
) -> Any:
    client = HttpClient(HttpRequestConfig(timeout_seconds=timeout_seconds, max_retries=3, retry_interval_seconds=1.0))
    result = client.request_json(
        method=method,
        url=url,
        headers=headers,
        params=params,
        data=data,
        json_data=json_data,
        timeout_seconds=timeout_seconds,
        event_name=event_name,
    )
    if not result.ok:
        raise RuntimeError("HTTP request failed: {0} {1}: {2}".format(method.upper(), url, result.error or result.message))
    return result.data


__all__ = ["HttpClient", "HttpRequestConfig", "request_json"]
