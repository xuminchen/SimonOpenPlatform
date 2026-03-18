import requests
import Tools
import gosql_v3
import datetime


def get_wechat_channels_accounts(account_id, access_token):
    interface = "wechat_channels_accounts/get"
    parameters = {
        "account_id": account_id,
        "page": 1,
        "page_size": 20
    }
    fields = ['wechat_channels_account_id', 'wechat_channels_account_name', 'created_time', 'last_modified_time',
              'wechat_channels_spam_block', 'wechat_channels_spam_slient', 'wechat_channels_account_icon',
              'authorization_type', 'authorization_scope', 'is_blocked', 'is_private', 'is_ad_acct',
              'authorization_begin_time', 'authorization_ttl', 'is_disable', 'disable_message', 'created_source_list',
              'authorization_status']

    d = Tools.get_api_data(interface, parameters, fields=fields, access_token=access_token)
    for item in d:
        print(item)
        item["account_id"] = account_id
        item["created_time"] = Tools.ts_to_dt(item["created_time"])
        item["last_modified_time"] = Tools.ts_to_dt(item["last_modified_time"])
        item["authorization_begin_time"] = Tools.ts_to_dt(item["authorization_begin_time"])

    return d


if __name__ == '__main__':
    # 主体： 深圳精准健康食物科技有限公司第一分公司
    token = Tools.ACCESS_TOKEN
    Tools.get_business_unit_account(access_token=token)
    wechat_channels_accounts = []
    for i in Tools.organization_account_relation_get(access_token=token):
        # print(i)
        account_id = i["account_id"]
        wechat_channels_accounts.extend(get_wechat_channels_accounts(account_id,token))
    #
    gosql_v3.api_to_sql(json_data=wechat_channels_accounts, sql_name="api_tencent_ad_wechat_channels_account")
