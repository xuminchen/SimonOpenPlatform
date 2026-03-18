import json
import requests
import datetime
import getToken
import os
from subprojects._shared.core.api_credentials import get_credentials


def create_report(advertiser_id, start_date, end_date, metrics_fields, dimension_fields, filtering=None):
    """
    通用报表请求函数，支持自动翻页并返回统一结构数据
    """
    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
    if not getToken.advertiser_access_token:
        raise ValueError("Missing tiktok_business_center.advertiser_access_token in config/api_credentials.json")
    headers = {
        "Access-Token": getToken.advertiser_access_token,
        "Content-Type": "application/json"
    }

    params = {
        "advertiser_id": advertiser_id,
        "service_type": "AUCTION",
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "dimensions": json.dumps(dimension_fields),
        "metrics": json.dumps(metrics_fields),
        "start_date": start_date,
        "end_date": end_date,
        "filtering": json.dumps(filtering) if filtering else None,
        "query_lifetime": 0,
        "page": 1,
        "page_size": 1000
    }
    proxies = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890',
    }

    # 方法2：设置环境变量（确保定时任务能读取）
    os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
    os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'


    all_data = []

    while True:
        response = requests.get(url, params=params, headers=headers, proxies=proxies, timeout=60)
        result = response.json()

        if result["code"] != 0:
            print("API 请求失败:", result)
            break

        data_list = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})

        for page_data in data_list:
            metrics = page_data.get("metrics", {})
            dimensions = page_data.get("dimensions", {})

            merged_data = {
                "advertiser": advertiser_id,
                **{field: metrics.get(field) for field in metrics_fields},
                **{field: dimensions.get(field) for field in dimension_fields}
            }

            # 打印非零 spend 的数据（调试用途）
            # if str(merged_data["spend"]) != '0':
            #     print(page_data)

            all_data.append(merged_data)

        current_page = page_info.get("page", 1)
        total_page = page_info.get("total_page", 1)

        if current_page >= total_page:
            break
        else:
            params["page"] += 1

    return all_data


# @getToken.task_record.record_task("API-获取TikTok基础报表")
def get_base_report():
    """
    获取基础报表数据
    """
    METRICS_FIELDS = [
        "ad_name",
        "spend", "cpc", "cpm", "impressions", "clicks", "ctr",
        "conversion", "cost_per_conversion", "conversion_rate", "conversion_rate_v2",
        "video_play_actions", "video_watched_2s", "video_watched_6s", "engaged_view",
        "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
        "average_video_play",
        "onsite_shopping_roas", "onsite_shopping", "cost_per_onsite_shopping",
        "onsite_shopping_rate", "value_per_onsite_shopping", "total_onsite_shopping_value",
        "onsite_initiate_checkout_count", "cost_per_onsite_initiate_checkout_count",
        "onsite_initiate_checkout_count_rate", "value_per_onsite_initiate_checkout_count",
        "total_onsite_initiate_checkout_count_value", "onsite_on_web_detail",
        "cost_per_onsite_on_web_detail", "onsite_on_web_detail_rate",
        "value_per_onsite_on_web_detail", "total_onsite_on_web_detail_value",
        "onsite_on_web_cart", "cost_per_onsite_on_web_cart", "onsite_on_web_cart_rate",
        "value_per_onsite_on_web_cart", "total_onsite_on_web_cart_value"
    ]
    filtering = [{
        "field_name": "ad_status",
        "filter_type": "IN",
        "filter_value": json.dumps(["STATUS_ALL"])
    }
    ]
    DIMENSION_FIELDS = ["stat_time_day", "post_id", "ad_id"]

    start_date = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # start_date = "2025-05-01"
    # end_date = "2025-05-31"

    for advertiser_data in getToken.get_advertiser_list():
        advertiser_id = advertiser_data["advertiser_id"]
        print(f"开始执行订单数据获取,获取广告主ID:{advertiser_id}...")
        report_data = create_report(
            advertiser_id=advertiser_id,
            start_date=start_date,
            end_date=end_date,
            metrics_fields=METRICS_FIELDS,
            dimension_fields=DIMENSION_FIELDS,
        )
        # 示例：插入数据库
        getToken.gosql_v3.api_to_sql(report_data, "api_tiktok_oversea_ad_post_basic_report")

    # 刷新 BI 数据源
    requests.get(
        get_credentials("bi_refresh_urls", "ds_ue88e74043d7d4bac9af7971", required=True))


if __name__ == '__main__':
    get_base_report()
