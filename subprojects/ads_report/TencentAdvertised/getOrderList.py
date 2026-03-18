import json
import random
import requests
import time
from subprojects._shared.core.api_credentials import get_credentials


def ecommerce_order_get(access_token=None):
    if not access_token:
        access_token = get_credentials("tencent_ads", "access_token", default="")
    if not access_token:
        raise ValueError("Missing access token. Set tencent_ads.access_token in config/api_credentials.json")
    interface = 'ecommerce_order/get'
    url = 'https://api.e.qq.com/v3.0/' + interface
    print(url)
    common_parameters = {
        'access_token': access_token,
        'timestamp': int(time.time()),
        'nonce': str(time.time()) + str(random.randint(0, 999999)),
    }

    parameters = {
        "account_id": 65716644,
        "filtering":
            [

                {
                    "field": "ecommerce_order_status",
                    "operator": "IN",
                    "values":
                        [
                            "SHIPPED",
                            "DELIVERED"
                        ]
                }
            ],
        "date_range":
            {
                "start_date": "2025-07-10",
                "end_date": "2025-07-20"
            },
        "page": 1,
        "page_size": 20
    }

    parameters.update(common_parameters)
    for k in parameters:
        if type(parameters[k]) is not str:
            parameters[k] = json.dumps(parameters[k])

    r = requests.get(url, params=parameters)
    print(r.text)


print(ecommerce_order_get())

print(int(time.time()))
print(str(time.time()) + str(random.randint(0, 999999)))
