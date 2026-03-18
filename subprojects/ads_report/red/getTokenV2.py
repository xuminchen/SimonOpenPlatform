import datetime
import requests
import json
from subprojects._shared.core import db_client as gosql_v3


def get_access_token(info: dict):
    url = "https://adapi.xiaohongshu.com/api/open/oauth2/access_token"
    header = {
        "content-type": "application/json",
    }

    shop_name = info.get("shop_name")
    app_id = info.get("app_id")
    secret = info.get("secret")
    auth_code = info.get("auth_code")
    platform = info.get("platform")
    data = {
        "app_id": app_id,
        "secret": secret,
        "auth_code": auth_code
    }
    account_data = []
    resp = requests.post(url=url, data=json.dumps(data), headers=header).json()
    print(resp)

    if info["platform"] == "蒲公英":
        account_data.append({
            "shop_name": shop_name,
            "app_id": app_id,
            "secret": secret,
            "auth_code": auth_code,
            "platform": platform,
            "access_token": resp['data']['access_token'],
            "refresh_token": resp['data']['refresh_token'],
            "advertiser_id": resp['data']['user_id'],
            "advertiser_name": resp['data']['corporation_name'],
        })
    else:
        if resp["data"]["approval_advertisers"]:
            for advertiser_info in resp["data"]["approval_advertisers"]:
                advertiser_data = {
                    "shop_name": shop_name,
                    "app_id": app_id,
                    "secret": secret,
                    "auth_code": auth_code,
                    "platform": platform,
                    "access_token": resp['data']['access_token'],
                    "refresh_token": resp['data']['refresh_token'],
                    "advertiser_id": advertiser_info["advertiser_id"],
                    "advertiser_name": advertiser_info["advertiser_name"],
                }
                print(advertiser_data)
                account_data.append(advertiser_data)
    delete_sql = f"DELETE FROM `xhs_token_info` WHERE app_id = '{app_id}'"
    gosql_v3.api_to_sql(json_data=account_data, sql_name="xhs_token_info", first_execute_sql=delete_sql)


def refresh_access_token():
    url = "https://adapi.xiaohongshu.com/api/open/oauth2/refresh_token"
    header = {
        "content-type": "application/json",
    }

    today = datetime.datetime.today().date()

    for advertiser_info in gosql_v3.execute_query(
            "SELECT DISTINCT shop_name,platform,app_id,secret,access_token,refresh_token,platform,DATE(update_time_etl) FROM `xhs_token_info`"):
        shop_name, platform, app_id, secret, access_token, refresh_token, platform, create_at = advertiser_info
        if create_at == today:
            print(f"{app_id}的token有效，创建时间为：{create_at}，token值为：{access_token}")
        else:
            print(f"{app_id}的token无效，正在刷新...")
            account_data = []
            body = {
                "app_id": app_id,
                "secret": secret,
                "refresh_token": refresh_token,
            }
            resp = requests.post(url=url, data=json.dumps(body), headers=header).json()

            access_token = resp["data"]["access_token"]
            refresh_token = resp["data"]["refresh_token"]

            if platform == "蒲公英":
                account_data.append({
                    "shop_name": shop_name,
                    "app_id": app_id,
                    "secret": secret,
                    "platform": platform,
                    "access_token": resp['data']['access_token'],
                    "refresh_token": resp['data']['refresh_token'],
                    "advertiser_id": resp['data']['user_id'],
                    "advertiser_name": resp['data']['corporation_name'],
                })
            else:
                if resp["data"]["approval_advertisers"]:
                    for advertisers in resp["data"]["approval_advertisers"]:
                        advertiser_data = {
                            "shop_name": shop_name,
                            "app_id": app_id,
                            "secret": secret,
                            "platform": platform,
                            "access_token": access_token,
                            "refresh_token": refresh_token,
                            "advertiser_id": advertisers["advertiser_id"],
                            "advertiser_name": advertisers["advertiser_name"],
                        }
                        account_data.append(advertiser_data)
            print(f"{app_id}的token刷新成功，新的token为【{access_token}】")
            delete_sql = f"DELETE FROM `xhs_token_info` WHERE app_id = '{app_id}'"
            gosql_v3.api_to_sql(json_data=account_data, sql_name="xhs_token_info", first_execute_sql=delete_sql)


if __name__ == '__main__':
    # information = {
    #     "shop_name": "WONDERLAB国内",
    #     "app_id": "344",
    #     "secret": "blYYfSq2LmaQTLw8",
    #     "auth_code": "3e791d6cb2b9f459804d56dfac6f006a",
    #     "platform": "聚光",
    # }
    # get_access_token(information)
    refresh_access_token()
