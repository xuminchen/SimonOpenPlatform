import json
import random
import time
import datetime
import os
from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig
from subprojects._shared.core.settings import get_env
from subprojects._shared.core.api_credentials import get_credentials

script_dir = os.path.dirname(os.path.abspath(__file__))
token_file_path = os.path.join(script_dir, 'token.json')

TOKEN_CONFIG = get_credentials("tencent_ads", default={})
client_id_raw = get_env("TENCENT_ADS_CLIENT_ID", TOKEN_CONFIG.get("client_id"))
client_secret = get_env("TENCENT_ADS_CLIENT_SECRET", TOKEN_CONFIG.get("client_secret"))
if not client_id_raw or not client_secret:
    raise ValueError("Missing tencent_ads.client_id/client_secret in config/api_credentials.json")
client_id = int(client_id_raw)

HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=30,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)


def refresh_access_token(access_token, refresh_token):
    interface = 'oauth/refresh_token'
    url = 'https://api.e.qq.com/v3.0/' + interface
    common_parameters = {
        'access_token': access_token,
        'timestamp': int(time.time()),
        'nonce': str(time.time()) + str(random.randint(0, 999999)),
    }
    parameters = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }
    parameters.update(common_parameters)
    for k in parameters:
        if type(parameters[k]) is not str:
            parameters[k] = json.dumps(parameters[k])

    result = HTTP_CLIENT.request_json(
        method="get",
        url=url,
        params=parameters,
        success_checker=lambda payload: isinstance(payload, dict) and "code" in payload,
        event_name="tencent_refresh_token",
    )
    if not result.ok:
        raise RuntimeError("refresh_access_token failed: {0}".format(result.error or result.message))
    token_data = result.data
    print(token_data)
    if token_data["code"] == 0:
        save_data = {"token_json": json.dumps(token_data),
                     "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        with open(token_file_path, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2)
        print(f"Token已保存到 token.json，获取时间: {save_data['time']}")
    return token_data["data"]["access_token"]


def get_token():
    with open(token_file_path, 'r', encoding='utf-8') as f:
        file_data = json.load(f)
    token_data = json.loads(file_data["token_json"])
    access_token = token_data["data"]["access_token"]
    refresh_token = token_data["data"]["refresh_token"]
    time_delta = datetime.datetime.now() - datetime.datetime.strptime(file_data["time"], "%Y-%m-%d %H:%M:%S")
    if time_delta.days > 25:
        print("Token接近过期，正在刷新...")
        return refresh_access_token(access_token, refresh_token)
    else:
        print(f"Token有效，无需刷新，token信息为：{access_token}")
        return access_token


