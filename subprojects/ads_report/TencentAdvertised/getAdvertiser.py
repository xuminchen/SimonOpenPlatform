import Tools
import json
import getToken

def get_advertiser(account_id, access_token):
    interface = "advertiser/get"
    parameters = {
        "account_id": account_id,
        "page": 1,
        "page_size": 20,
        "pagination_mode": "PAGINATION_MODE_NORMAL",
        "fields": json.dumps(['account_id', 'mdm_name', 'memo'])
    }
    fields = ['account_id', 'mdm_name', 'memo']
    d = Tools.get_api_data(interface, parameters, fields=fields, access_token=access_token)
    for item in d:
        print(item)


token = getToken.get_token()
Tools.get_business_unit_account(access_token=token)
wechat_channels_accounts = []
# print(Tools.organization_account_relation_get(access_token=token))
for i in Tools.organization_account_relation_get(access_token=token):
    print(i)
    account_id = i["account_id"]
    get_advertiser(account_id, token)
