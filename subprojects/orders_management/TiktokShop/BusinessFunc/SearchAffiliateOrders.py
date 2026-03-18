from BusinessFunc import getToken
import datetime


def get_affiliate_orders(start_time, end_time):

    url = 'https://open-api.tiktokglobalshop.com/affiliate_seller/202410/orders/search'
    start_time_timestamp = int(datetime.datetime.strptime(start_time, '%Y-%m-%d').timestamp())
    end_time_timestamp = int(datetime.datetime.strptime(end_time, '%Y-%m-%d').timestamp())
    data = {
        "create_time_lt": end_time_timestamp,
        "create_time_ge": start_time_timestamp,
    }

    params = {
        "shop_cipher": "TTP_UHd08wAAAABKNQAuH4gfTjRY0btkD_bR",
        "app_key": getToken.app_key,
        "page_size": 20
    }
    headers = {
        'content-type': 'application/json',
        'x-tts-access-token': getToken.access_token
    }

    response_json = getToken.request_signed_json(
        method="post",
        url=url,
        params=params,
        headers=headers,
        body=data,
        event_name="tiktok_shop_affiliate_orders",
    )
    print(response_json)


get_affiliate_orders('2025-04-01', '2025-04-17')
