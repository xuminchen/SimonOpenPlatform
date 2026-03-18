import json
import random
import requests
import time
import gosql_v3
import datetime
import getToken

ACCESS_TOKEN = getToken.get_token()


def send_requests(interface, parameters, method='get', access_token=ACCESS_TOKEN):
    url = 'https://api.e.qq.com/v3.0/' + interface

    common_parameters = {
        'access_token': access_token,
        'timestamp': int(time.time()),
        'nonce': str(time.time()) + str(random.randint(0, 999999)),
    }

    if method == 'get':
        parameters.update(common_parameters)
        for k in parameters:
            if type(parameters[k]) is not str:
                parameters[k] = json.dumps(parameters[k])
        response = requests.get(url=url, params=parameters)
    else:
        response = requests.post(url=url, params=common_parameters, json=parameters)

    if response.json()["code"] == 0:
        return response.json()
    else:
        print(response.json())


def get_api_data(interface, parameters, fields, access_token=ACCESS_TOKEN):
    data_list = []
    while True:
        response_json = send_requests(interface, parameters, method="get", access_token=access_token)

        if response_json is None or response_json["data"].get("list") is None:
            break

        for item in response_json["data"]["list"]:
            api_item = {
                **{field: item.get(field, "-") for field in fields}
            }

            data_list.append(api_item)

        total_page = response_json["data"]["page_info"]["total_page"]
        current_page = response_json["data"]["page_info"]["page"]
        if current_page >= total_page:
            break
        else:
            parameters["page"] = int(current_page) + 1

    return data_list


def organization_account_relation_get(access_token=ACCESS_TOKEN):
    interface = 'organization_account_relation/get'
    url = 'https://api.e.qq.com/v3.0/' + interface

    common_parameters = {
        'access_token': access_token,
        'timestamp': int(time.time()),
        'nonce': str(time.time()) + str(random.randint(0, 999999)),
    }

    parameters = {
        "page": 1,
        "page_size": 100,
        "pagination_mode": "PAGINATION_MODE_NORMAL"
    }
    item_list = []
    while True:
        parameters.update(common_parameters)
        for k in parameters:
            if type(parameters[k]) is not str:
                parameters[k] = json.dumps(parameters[k])

        r = requests.get(url, params=parameters)
        print(r.json())
        for i in r.json()["data"]["list"]:
            item_list.append(i)
        if len(r.json()["data"]["list"]) < 100:
            break
        else:
            parameters["page"] = int(parameters["page"]) + 1
            common_parameters = {
                'access_token': access_token,
                'timestamp': int(time.time()),
                'nonce': str(time.time()) + str(random.randint(0, 999999)),
            }

    return item_list


def get_business_unit_account(access_token=ACCESS_TOKEN):
    account_list = [i["account_id"] for i in
                    organization_account_relation_get(access_token=access_token)]

    sub_lists = [account_list[i:i + 30] for i in range(0, len(account_list), 30)]
    business_unit_data = []
    for sub_list in sub_lists:
        interface = 'business_unit_account/get'
        parameters = {
            "account_id_list": sub_list,
        }
        response_data = send_requests(interface=interface, method="post", parameters=parameters,
                                      access_token=access_token)

        for item in response_data["data"]["list"]:
            business_unit_data.append({
                "organization_id": item["organization_id"],
                "organization_name": item["organization_name"],
                "account_id": item["account_id"],
            })

    gosql_v3.api_to_sql(business_unit_data, "api_tencent_ad_business_unit")


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(int(ts)).strftime(
        "%Y-%m-%d %H:%M:%S") if ts and ts != 0 else datetime.datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    get_business_unit_account()
