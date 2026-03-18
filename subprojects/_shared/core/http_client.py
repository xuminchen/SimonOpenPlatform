from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence

import requests

from subprojects._shared.core.models import ApiResult
from subprojects._shared.core.observability import log_event, new_request_id, now_ms


@dataclass
class HttpRequestConfig:
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_interval_seconds: float = 1.0
    retry_on_statuses: Sequence[int] = (429, 500, 502, 503, 504)


class HttpClient:
    def __init__(self, config: Optional[HttpRequestConfig] = None):
        self.config = config or HttpRequestConfig()
        self._session = requests.Session()

    def close(self) -> None:
        self._session.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Any = None,
        json_data: Any = None,
        auth: Any = None,
        timeout_seconds: Optional[int] = None,
        success_checker: Optional[Callable[[Any], bool]] = None,
        event_name: str = "http_request",
    ) -> ApiResult:
        req_id = new_request_id()
        timeout = timeout_seconds or self.config.timeout_seconds
        last_error = ""
        last_status_code = 0
        total_started_ms = now_ms()

        for attempt in range(1, self.config.max_retries + 1):
            started_ms = now_ms()
            try:
                resp = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    data=data,
                    json=json_data,
                    auth=auth,
                    timeout=timeout,
                )
                elapsed_ms = now_ms() - started_ms
                last_status_code = resp.status_code

                payload = None
                parse_error = ""
                try:
                    payload = resp.json()
                except ValueError as exc:
                    parse_error = str(exc)

                ok = resp.ok
                if success_checker is not None and payload is not None:
                    ok = bool(success_checker(payload))

                log_event(
                    event_name,
                    request_id=req_id,
                    method=method.upper(),
                    url=url,
                    attempt=attempt,
                    status_code=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    ok=ok,
                )

                if ok:
                    return ApiResult.from_success(
                        status_code=resp.status_code,
                        data=payload if payload is not None else resp.text,
                        request_id=req_id,
                        elapsed_ms=elapsed_ms,
                    )

                last_error = "status={0}, parse_error={1}".format(resp.status_code, parse_error)
                retryable_status = resp.status_code in self.config.retry_on_statuses
                if (not retryable_status) and (attempt < self.config.max_retries):
                    break
            except requests.RequestException as exc:
                elapsed_ms = now_ms() - started_ms
                last_error = str(exc)
                log_event(
                    event_name,
                    request_id=req_id,
                    method=method.upper(),
                    url=url,
                    attempt=attempt,
                    status_code=0,
                    elapsed_ms=elapsed_ms,
                    ok=False,
                    error=last_error,
                )

            if attempt < self.config.max_retries:
                time.sleep(self.config.retry_interval_seconds)

        return ApiResult.from_failure(
            status_code=last_status_code,
            code="HTTP_REQUEST_FAILED",
            message="Request failed after retries",
            error=last_error,
            request_id=req_id,
            elapsed_ms=now_ms() - total_started_ms,
            data=None,
        )

    def download_stream(
        self,
        *,
        url: str,
        save_path: str,
        headers: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> ApiResult:
        req_id = new_request_id()
        timeout = timeout_seconds or self.config.timeout_seconds
        started = now_ms()
        try:
            parent_dir = os.path.dirname(save_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            resp = self._session.get(url, headers=headers, stream=True, timeout=timeout, allow_redirects=True)
            if not resp.ok:
                return ApiResult.from_failure(
                    status_code=resp.status_code,
                    code="DOWNLOAD_FAILED",
                    message="Download request failed",
                    error=resp.text,
                    request_id=req_id,
                    elapsed_ms=now_ms() - started,
                )
            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return ApiResult.from_success(
                status_code=resp.status_code,
                data={"save_path": save_path},
                request_id=req_id,
                elapsed_ms=now_ms() - started,
            )
        except requests.RequestException as exc:
            return ApiResult.from_failure(
                status_code=0,
                code="DOWNLOAD_EXCEPTION",
                message="Download request exception",
                error=str(exc),
                request_id=req_id,
                elapsed_ms=now_ms() - started,
            )
