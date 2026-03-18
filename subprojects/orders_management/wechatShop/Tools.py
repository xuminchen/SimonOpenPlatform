import json
import time

from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig
from subprojects._shared.core.api_credentials import get_credentials


HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=30,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)


def get_access_token(app_id, secret):
    url = "https://api.weixin.qq.com/cgi-bin/token"
    params = {
        "grant_type": "client_credential",
        "appid": app_id,
        "secret": secret,
        "force_refresh": True
    }
    result = HTTP_CLIENT.request_json(
        method="get",
        url=url,
        params=params,
        success_checker=lambda payload: isinstance(payload, dict) and ("access_token" in payload),
        event_name="wechat_access_token",
    )
    if not result.ok:
        raise RuntimeError("get_access_token failed: {0}".format(result.error or result.message))
    response_json = result.data
    print(response_json)
    return response_json["access_token"]


def send_request(url, params=None, data=None, times=5, method="post"):
    for i in range(1, times + 1):
        result = HTTP_CLIENT.request_json(
            method=method,
            url=url,
            params=params,
            json_data=data if method.lower() == "post" else None,
            data=None if method.lower() == "post" else json.dumps(data),
            success_checker=lambda payload: isinstance(payload, dict),
            event_name="wechat_send_request",
        )
        if not result.ok:
            print("第{0}次请求失败".format(i))
            print("params:{0},data:{1}".format(params, data))
            print(result.error or result.message)
            time.sleep(1 * (2 ** i))
            continue

        response_json = result.data
        if response_json.get("errmsg") == "ok" or response_json.get("errcode") == 0:
            return response_json

        print("第{0}次请求失败".format(i))
        print("params:{0},data:{1}".format(params, data))
        print(response_json)
        time.sleep(1 * (2 ** i))

    return None


def refresh_data_source(url):
    result = HTTP_CLIENT.request_json(
        method="get",
        url=url,
        success_checker=lambda payload: isinstance(payload, dict),
        event_name="wechat_bi_refresh",
    )
    if not result.ok:
        print("refresh_data_source failed: {0}".format(result.error or result.message))
        return None
    return result.data


if __name__ == '__main__':
    shop_map = get_credentials("wechat_shop", "shops", default={})
    if not shop_map:
        raise ValueError("Missing wechat_shop.shops in config/api_credentials.json")
    first_shop = next(iter(shop_map.values()))
    app_id = first_shop.get("app_id")
    secret = first_shop.get("secret")
    print(get_access_token(app_id, secret))
