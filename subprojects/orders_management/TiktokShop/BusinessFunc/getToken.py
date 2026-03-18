import time
from subprojects._shared.core.settings import get_env
from subprojects._shared.core.api_credentials import get_credentials
from subprojects._shared.core.auth.providers import (
    TiktokShopTokenProvider,
    request_tiktok_signed_json,
)
from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig

import gosql_v3

TOKEN_CONFIG = get_credentials("tiktok_shop", default={})

app_key = get_env("TIKTOK_SHOP_APP_KEY", TOKEN_CONFIG.get("app_key"))
app_secret = get_env("TIKTOK_SHOP_APP_SECRET", TOKEN_CONFIG.get("app_secret"))
auth_code = get_env(
    "TIKTOK_SHOP_AUTH_CODE",
    TOKEN_CONFIG.get("auth_code"),
)
access_token = get_env(
    "TIKTOK_SHOP_ACCESS_TOKEN",
    TOKEN_CONFIG.get("access_token"),
)

if not app_key or not app_secret:
    raise ValueError("Missing tiktok_shop.app_key/app_secret in config/api_credentials.json")

TOKEN_PROVIDER = TiktokShopTokenProvider(app_key=app_key, app_secret=app_secret, auth_code=auth_code)
HTTP_CLIENT = HttpClient(HttpRequestConfig(timeout_seconds=30, max_retries=4, retry_interval_seconds=1.5))

shop_info = [{
    'shop_code': 'USLCN4E7AQ', 'shop_id': '7495896608152062300', 'shop_name': 'WonderBlue',
    'shop_cipher': 'TTP_UHd08wAAAABKNQAuH4gfTjRY0btkD_bR',
}
]


def request_signed_json(method, url, params=None, headers=None, body=None, event_name="tiktok_shop_api"):
    return request_tiktok_signed_json(
        method=method,
        url=url,
        app_secret=app_secret,
        headers=headers,
        params=params,
        body=body,
        event_name=event_name,
    )


def refresh_data_source(url):
    result = HTTP_CLIENT.request_json(
        method="get",
        url=url,
        success_checker=lambda payload: isinstance(payload, dict),
        event_name="tiktok_shop_bi_refresh",
    )
    if not result.ok:
        print("refresh_data_source failed: {0}".format(result.error or result.message))
        return None
    return result.data


def get_token():
    response_json = TOKEN_PROVIDER.get_access_token()
    if response_json.get("code") == 0:
        token_data = {
            "access_token": response_json["data"]["access_token"],
            "access_token_expire_in": response_json["data"]["access_token_expire_in"],
            "refresh_token": response_json["data"]["refresh_token"],
            "refresh_token_expire_in": response_json["data"]["refresh_token_expire_in"],
            "seller_name": response_json["data"]["seller_name"],
            "app_key": app_key,
            "app_secret": app_secret
        }
        gosql_v3.api_to_sql([token_data], "api_tiktok_shop_token")


def refresh_access_token(refresh_token):
    response_json = TOKEN_PROVIDER.refresh_access_token(refresh_token)
    if response_json.get("code") == 0:
        token_data = {
            "access_token": response_json["data"]["access_token"],
            "access_token_expire_in": response_json["data"]["access_token_expire_in"],
            "refresh_token": response_json["data"]["refresh_token"],
            "refresh_token_expire_in": response_json["data"]["refresh_token_expire_in"],
            "seller_name": response_json["data"]["seller_name"],
            "app_key": app_key,
            "app_secret": app_secret
        }
        gosql_v3.api_to_sql([token_data], "api_tiktok_shop_token")

        return token_data["access_token"]


def should_refresh_token(buffer_days=3):
    token_data = gosql_v3.execute_query(
        "select `access_token`,`access_token_expire_in`,`refresh_token`,`refresh_token_expire_in`,`seller_name`,`app_key` from api_tiktok_shop_token")
    for token_info in token_data:
        access_token, access_token_expire_in, refresh_token, refresh_token_expire_in, seller_name, app_key = token_info
        current_timestamp = int(time.time())
        refresh_threshold = int(access_token_expire_in) - buffer_days * 86400  # 提前三天的时间戳
        if current_timestamp >= refresh_threshold:
            print("Token 即将过期，建议刷新")
            refresh_access_token(refresh_token)
        else:
            print("Token 仍有效，无需刷新")



if __name__ == "__main__":
    should_refresh_token()
    # url = "https://open-api.tiktokglobalshop.com/authorization/202309/shops"
    # p = {
    #     "app_key": "29a39d",
    #     "sign": "bc721f0e0182914e3487b81df204de37a352fc3aa96947efda6dc1e5dd0d5290",
    #     "timestamp": "1623812664"
    # }
    # h = {
    #     "x-tts-access-token": "TTP_pwSm2AAAAABmmtFz1xlyKMnwg74T2GJ5s0uQbS8jPjb_GkdFVCxPqzQXSyuyfXdQa0AqyDsea2tYFNVf4XeqgZHFfPyv0Vs659QqyLYfsGzanZ5XZAin3_ZkcIxxS0_In6u6XDeU96k",
    #     "content-type": "application/json"
    # }
    # print(cal_sign(url, p, h))
