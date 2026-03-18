import datetime
import requests
import json
import JuGuang


def get_account_finance_data(access_token, shop_name, advertiser_name, advertiser_id, start_date, end_date):
    url = "https://adapi.xiaohongshu.com/api/open/jg/account/order/info"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    data = {
        "advertiser_id": int(advertiser_id),
        "start_time": start_date,
        "end_time": end_date,
        "page_size": 50,
        "page": 1,
    }
    more_data = []
    while True:
        resp = requests.post(url=url, data=json.dumps(data), headers=header).json()
        if not resp['data']['account_trade_detail']:
            break
        for i in resp['data']['account_trade_detail']:
            i = dict(i)
            one_data = {
                'launch_date': i['launch_date'],
                'operate_type': i.get('operate_type', ''),
                'trade_time': i['trade_time'],
                'account_name': i.get('account_name', '-'),
                'order_amount': i.get('order_amount', 0),
                'balance': i.get('balance', 0),
                'transfer_object': i.get('transfer_object', '-'),
                'remark': i.get('remark', '-'),
                'account_type': i.get('account_type', ''),
                'account_type_name': i.get('account_type_name', ''),
                'business_type_name': i.get('business_type_name', ''),
                'shop_name': shop_name,
                'advertiser_id': advertiser_id,
                'advertiser_name': advertiser_name
            }
            more_data.append(one_data)
        if len(resp['data']['account_trade_detail']) < 50:
            break
        else:
            data['page'] += 1
    return more_data


query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
shop_data = []
for adv_info in JuGuang.gosql_v3.execute_query(query_sql):
    print(adv_info)
    access_token, shop_name, advertiser_id, advertiser_name = adv_info
    start_time = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    end_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    shop_data.extend(
        get_account_finance_data(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                                 advertiser_id=advertiser_id, start_date=start_time, end_date=end_time))

JuGuang.gosql_v3.api_to_sql(json_data=shop_data, sql_name="api_xhs_jg_account_finance_record")
