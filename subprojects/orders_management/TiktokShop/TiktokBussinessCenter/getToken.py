import sys
import os
from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig
from subprojects._shared.core.settings import get_env
from subprojects._shared.core.api_credentials import get_credentials

TOKEN_CONFIG = get_credentials("tiktok_business_center", default={})
app_id = get_env("TIKTOK_BC_APP_ID", TOKEN_CONFIG.get("app_id"))
secret = get_env("TIKTOK_BC_SECRET", TOKEN_CONFIG.get("secret"))
auth_code = get_env("TIKTOK_BC_AUTH_CODE", TOKEN_CONFIG.get("auth_code"))
advertiser_access_token = get_env("TIKTOK_BC_ACCESS_TOKEN", TOKEN_CONFIG.get("advertiser_access_token"))

if not app_id or not secret:
    raise ValueError("Missing tiktok_business_center.app_id/secret in config/api_credentials.json")

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

# 方法2：设置环境变量（确保定时任务能读取）
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ['NO_PROXY'] = 'localhost,127.0.0.1'

HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=30,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)


def get_access_token():
    url = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
    header = {
        "Content-Type": "application/json",
    }
    data = {
        "app_id": app_id,
        "secret": secret,
        "auth_code": auth_code,
    }
    response = HTTP_CLIENT.request_json(
        method="post",
        url=url,
        headers=header,
        json_data=data,
        success_checker=lambda payload: isinstance(payload, dict),
        event_name="tiktok_bc_access_token",
    )
    if not response.ok:
        raise RuntimeError("get_access_token failed: {0}".format(response.error or response.message))
    response_json = response.data
    print(response_json)


def get_advertiser_list():
    if not advertiser_access_token:
        raise ValueError("Missing tiktok_business_center.advertiser_access_token in config/api_credentials.json")
    url = "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/"
    headers = {
        "Access-Token": advertiser_access_token,
    }
    params = {
        "app_id": app_id,
        "secret": secret,
    }
    response = HTTP_CLIENT.request_json(
        method="get",
        url=url,
        headers=headers,
        params=params,
        success_checker=lambda payload: isinstance(payload, dict),
        event_name="tiktok_bc_advertiser_list",
    )
    if not response.ok:
        raise RuntimeError("get_advertiser_list failed: {0}".format(response.error or response.message))
    response_json = response.data
    if response_json.get("code") != 0:
        print(response_json)
        return

    advertiser_list = []
    for advertiser in response_json["data"]["list"]:
        advertiser_id = advertiser["advertiser_id"]
        advertiser_name = advertiser["advertiser_name"]
        advertiser_data = {
            "advertiser_id": advertiser_id,
            "advertiser_name": advertiser_name,
        }
        advertiser_list.append(advertiser_data)

    return advertiser_list


if __name__ == '__main__':
    advertiser_list = get_advertiser_list()
    print(advertiser_list)
