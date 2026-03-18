import traceback
import os

import requests
import json
import logging
import time

from AccessToken import AccessToken
from Utils import Utils
from subprojects._shared.core.api_credentials import get_credentials


class StarRequest:

    request_num = 0

    def __init__(self):
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
        today_time = Utils().get_now_time('%Y-%m-%d')
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(log_dir, 'qc_log_' + today_time + '.log'),
            level=logging.INFO,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
        )

    def request_get(self, url, header, params):
        r_utf8_json = {}
        # 发送GET请求
        while(True):
            try:
                response = requests.get(url, params=params, headers=header)
                r_utf8 = response.text
                r_utf8_json = json.loads(r_utf8)
                if r_utf8_json['code'] != 0:
                    print(response.text)
                    logging.info(response.text)
                    self.request_num += 1
                else:
                    self.request_num = 0
                    break

                if r_utf8_json.get('code') == 40100:
                    print("请求次数过多")
                    logging.info("请求次数过多")
                    time.sleep(60)
                    continue
            except Exception:
                print("请求失败！！！！！")
                logging.info("请求失败！！！！！")
                logging.info(Exception)
                logging.error("\n" + traceback.format_exc())

                print("开始等待")
                time.sleep(10)
                self.request_num += 1

            if self.request_num >= 20:
                print("请求多次失败，请求终止")
                logging.info("请求多次失败，请求终止")
                self.request_num = 0
                break

        # r_utf8 = Utils().Unicode_to_zh(r.text).replace('\\', '')

        return r_utf8_json

if __name__ == '__main__':
    apps = get_credentials("xhs_juguang", "apps", default={})
    app_id = ""
    secret = ""
    if isinstance(apps, dict):
        for item in apps.values():
            if not isinstance(item, dict):
                continue
            app_id = str(item.get("app_id", "")).strip()
            secret = str(item.get("secret", "")).strip()
            if app_id and secret:
                break
    if not app_id or not secret:
        raise ValueError("Missing xhs_juguang.apps.*.app_id/secret in config/api_credentials.json")

    url = 'https://ad.oceanengine.com/open_api/2/star/demand/list/'
    # 请求Header（字典形式储存）
    header = {
        "Access-Token": AccessToken().refresh_token_new(app_id, secret)
    }
    # 请求Body（字典形式储存）
    params = {
        "star_id": '1693176613609485'
    }

    print(StarRequest().request_get(url, header, params))
