import datetime
from BusinessFunc import getToken


def get_shop_performance(start_date: str, end_date: str):
    start_ts = int(datetime.datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    url = "https://open-api.tiktokglobalshop.com/analytics/202405/shop/performance"
    params = {
        "app_key": getToken.app_key,
        "shop_cipher": "TTP_UHd08wAAAABKNQAuH4gfTjRY0btkD_bR",
        "shop_id": "7495896608152062300",
        "page_size": 100,
        "sort_field": "create_time",
        "start_date_ge": start_date,
        "end_date_lt": end_date
    }
    headers = {
        'content-type': 'application/json',
        'x-tts-access-token': getToken.access_token
    }
    response_json = getToken.request_signed_json(
        method="get",
        url=url,
        params=params,
        headers=headers,
        body=None,
        event_name="tiktok_shop_performance",
    )
    print(response_json)


get_shop_performance("2025-04-01", "2025-04-22")
