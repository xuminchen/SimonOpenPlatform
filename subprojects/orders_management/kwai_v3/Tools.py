# -*- coding: utf-8 -*-
import time
import requests
import json
import hashlib
import hmac
import base64
from subprojects._shared.core.api_credentials import get_credentials


app_info = get_credentials("kwai_v3", "accounts", default={})

def sign_param(app_key, access_token, sign_secret, method, param_json, timestamp):
    """ 加密  """
    s = "access_token=" + access_token + "&appkey=" + app_key + "&method=" + method + "&param=" + param_json + \
        "&signMethod=HMAC_SHA256&timestamp=" + str(timestamp) + "&version=1&signSecret=" + sign_secret

    h = hmac.new(bytes(sign_secret, 'utf-8'), bytes(s, 'utf-8'), digestmod=hashlib.sha256).digest()
    return base64.b64encode(h).decode()


def fetch_resp(app_key, access_token, sign_secret,api_method, params, data=None, headers=None, method="GET"):
    """ get数据 """
    timestamp = str(int(time.time()))
    url = f"https://open.kwaixiaodian.com/{api_method.replace('.', '/')}"

    param_json = json.dumps(params, ensure_ascii=False, separators=(',', ':'))
    sign = sign_param(app_key, access_token, sign_secret, api_method, param_json, timestamp)
    # 对请求的公共参数进行排序和序列化
    final_param = {
        "method": api_method,
        "appkey": app_key,
        "access_token": access_token,
        "timestamp": timestamp,
        "version": 1,
        "sign": sign,
        "signMethod": "HMAC_SHA256",
        "param": param_json
    }

    final_param = dict(((k, final_param[k]) for k in sorted(final_param.keys())))

    for i in range(3):
        try:
            response = requests.request(method=method, url=url, data=data, headers=headers, params=final_param)
            response_json = response.json()
            # print(response_json)
            if response_json['code'] == 0:
                time.sleep(3)
                continue

            elif response_json['code'] in [10001]:
                time.sleep(3)
                continue

            return response_json
        except requests.RequestException as e:
            print(e)
            continue








