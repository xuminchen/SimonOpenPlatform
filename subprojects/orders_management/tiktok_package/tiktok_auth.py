from utils import *
import json
import hmac
import time
from subprojects._shared.core.api_credentials import get_credentials


class Tiktok(object):
    def __init__(self):
        config = get_credentials("orders_management", "tiktok_package", default={})
        self.app_key = config.get("app_key")
        self.app_secret = config.get("app_secret")
        shop_map = config.get("shops", {})
        self.shop_id_mapping = {str(shop_id): shop_name for shop_name, shop_id in shop_map.items()}
        if not self.app_key or not self.app_secret:
            raise ValueError("Missing orders_management.tiktok_package.app_key/app_secret in config/api_credentials.json")

    def get_signature(self, method, timestamp, param):
        # @xuhuaxin: param必须按照字典的键进行排序，再序列化，并禁用Html Escape（separators=(',', ':')）
        param = dict(((k, param[k]) for k in sorted(param.keys())))
        param_json = json.dumps(param, ensure_ascii=False, separators=(',', ':'))

        # 使用加号拼接参数
        param_pattern = 'app_key' + self.app_key + 'method' + method + 'param_json' + param_json + 'timestamp' + timestamp + 'v2'
        sign_pattern = self.app_secret + param_pattern + self.app_secret

        # 使用 hmac.new() 计算 HMAC-SHA256 签名
        signature = hmac.new(self.app_secret.encode(), sign_pattern.encode(), hashlib.sha256).hexdigest()
        return signature

    def fetch_resp(self, method, param, access_token, times=5):
        """
        :param times: 重试次数
        :param param: 参数
        :param access_token:
        :param method: 请求的接口名称
        :return: resp.json
        """

        timestamp = str(int(time.time()))
        url = f"https://openapi-fxg.jinritemai.com/{method.replace('.', '/')}"

        # 对请求的查询参数进行排序和序列化
        param = dict(((k, param[k]) for k in sorted(param.keys())))
        param_json = json.dumps(param, ensure_ascii=False, separators=(',', ':'))

        # sign获取
        sign = self.get_signature(method, timestamp, param)

        # 对请求的公共参数进行排序和序列化
        final_param = {
            "method": method,
            "app_key": self.app_key,
            "access_token": access_token,
            "timestamp": timestamp,
            "v": "2",
            "sign": sign,
            "sign_method": "hmac-sha256",
            "param_json": param_json
        }

        final_param = dict(((k, final_param[k]) for k in sorted(final_param.keys())))

        # 最后的表单数据
        final_data = {
            "param_json": param_json
        }

        i = 0
        resp = None
        print(f"start {method}..")
        while i < times:
            try:
                resp = requests.post(url, json=final_data, params=final_param, timeout=60).json()
                # print(resp)
                if resp['code'] == 10000:
                    return resp
                else:
                    break
            except Exception:
                print(f"第{i + 1}次请求错误，等5秒重新尝试")
                time.sleep(5)
                i += 1
        print("???")
        if resp:
            print(f"返回response有问题，请检查，以下是返回的response:{resp}")
            raise Exception("Response Error!")

    def get_token(self, shop_id):
        """
        :param shop_id: 店铺ID
        :return: access_token, refresh_token
        """

        method = "token.create"
        timestamp = str(int(time.time()))
        param = {"shop_id": shop_id, "code": "", "grant_type": "authorization_self"}
        signature = self.get_signature(method, timestamp, param)
        url = 'https://openapi-fxg.jinritemai.com/token/create?app_key=%s&method=token.create&param_json={' \
              '"code":"","grant_type":"authorization_self",' \
              '"shop_id":"%s"}&timestamp=%s&v=2&sign=%s&sign_method=hmac-sha256' % (
                  self.app_key, shop_id, timestamp, signature)
        # print(url)
        resp = requests.post(url=url).json()
        # if resp['code']!=10000:
        #     print(resp)

        if resp['data']['expires_in'] < 1800:
            p = {"refresh_token": resp['data']['refresh_token'], "grant_type": "refresh_token"}
            resp = self.fetch_resp("token.refresh", p, resp['data']['access_token'])

        access_token = resp['data']['access_token']
        refresh_token = resp['data']['refresh_token']
        return access_token, refresh_token
