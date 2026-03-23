from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Any, Dict, Optional

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=30,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)


@dataclass
class TiktokShopTokenProvider:
    app_key: str
    app_secret: str
    auth_code: str
    base_url: str = "https://auth.tiktok-shops.com"

    def get_access_token(self) -> Dict[str, Any]:
        result = HTTP_CLIENT.request_json(
            method="get",
            url=f"{self.base_url}/api/v2/token/get",
            params={
                "app_key": self.app_key,
                "app_secret": self.app_secret,
                "auth_code": self.auth_code,
                "grant_type": "authorized_code",
            },
            success_checker=lambda payload: isinstance(payload, dict) and "code" in payload,
            event_name="tiktok_shop_get_token",
        )
        if not result.ok:
            raise RuntimeError(result.error or result.message)
        return result.data

    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        result = HTTP_CLIENT.request_json(
            method="get",
            url=f"{self.base_url}/api/v2/token/refresh",
            params={
                "app_key": self.app_key,
                "app_secret": self.app_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
            success_checker=lambda payload: isinstance(payload, dict) and "code" in payload,
            event_name="tiktok_shop_refresh_token",
        )
        if not result.ok:
            raise RuntimeError(result.error or result.message)
        return result.data


@dataclass
class OceanEngineTokenProvider:
    app_id: str
    secret: str

    def get_by_auth_code(self, auth_code: str) -> Dict[str, Any]:
        result = HTTP_CLIENT.request_json(
            method="post",
            url="https://ad.oceanengine.com/open_api/oauth2/access_token/",
            data={
                "app_id": self.app_id,
                "secret": self.secret,
                "grant_type": "auth_code",
                "auth_code": auth_code,
            },
            success_checker=lambda payload: isinstance(payload, dict),
            event_name="oceanengine_get_token",
        )
        if not result.ok:
            raise RuntimeError(result.error or result.message)
        return result.data

    def refresh(self, refresh_token: str) -> Dict[str, Any]:
        result = HTTP_CLIENT.request_json(
            method="post",
            url="https://ad.oceanengine.com/open_api/oauth2/refresh_token/",
            data={
                "app_id": self.app_id,
                "secret": self.secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            success_checker=lambda payload: isinstance(payload, dict),
            event_name="oceanengine_refresh_token",
        )
        if not result.ok:
            raise RuntimeError(result.error or result.message)
        return result.data


@dataclass
class WechatShopTokenProvider:
    app_id: str
    secret: str

    def get_access_token(self) -> Dict[str, Any]:
        result = HTTP_CLIENT.request_json(
            method="get",
            url="https://api.weixin.qq.com/cgi-bin/token",
            params={
                "grant_type": "client_credential",
                "appid": self.app_id,
                "secret": self.secret,
                "force_refresh": True,
            },
            success_checker=lambda payload: isinstance(payload, dict) and "access_token" in payload,
            event_name="wechat_shop_get_token",
        )
        if not result.ok:
            raise RuntimeError(result.error or result.message)
        return result.data


def tiktok_shop_sign(full_url: str, query_params: Dict[str, Any], headers: Dict[str, str], body_str: str, app_secret: str) -> str:
    parsed_url = urlparse(full_url)
    path = parsed_url.path
    filtered_params = {k: v for k, v in query_params.items() if k not in ["sign", "access_token"]}
    sorted_keys = sorted(filtered_params.keys())
    kv_string = "".join(f"{key}{filtered_params[key]}" for key in sorted_keys)
    content_type = headers.get("content-type", "")
    body = "" if content_type.startswith("multipart/form-data") else body_str
    sign_str = f"{app_secret}{path}{kv_string}{body}{app_secret}"
    return hmac.new(app_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256).hexdigest()


def request_tiktok_signed_json(
    *,
    method: str,
    url: str,
    app_secret: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    body: Any = None,
    event_name: str = "tiktok_shop_api",
) -> Dict[str, Any]:
    request_params = dict(params or {})
    request_headers = headers or {}
    if body is None:
        body_str = ""
    elif isinstance(body, str):
        body_str = body
    else:
        body_str = json.dumps(body, ensure_ascii=False)
    request_params["timestamp"] = int(time.time())
    request_params["sign"] = tiktok_shop_sign(url, request_params, request_headers, body_str, app_secret)
    result = HTTP_CLIENT.request_json(
        method=method,
        url=url,
        headers=request_headers,
        params=request_params,
        data=body_str or None,
        success_checker=lambda payload: isinstance(payload, dict),
        event_name=event_name,
    )
    if not result.ok:
        raise RuntimeError(result.error or result.message)
    return result.data
