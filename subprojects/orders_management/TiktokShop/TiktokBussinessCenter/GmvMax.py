import time
import datetime
import requests
import json
import getToken
import os
from subprojects._shared.core.api_credentials import get_credentials


def get_date_range(days=5, start_time=None, end_time=None, need_split=True):
    date_range = []
    if start_time and end_time:
        start_date = datetime.datetime.strptime(start_time, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_time, "%Y-%m-%d")
    else:
        start_date = datetime.datetime.now() - datetime.timedelta(days=days)
        end_date = datetime.datetime.now() - datetime.timedelta(days=1)

    if need_split:
        while start_date <= end_date:
            date_range.append(start_date.strftime("%Y-%m-%d"))
            start_date += datetime.timedelta(days=1)
        date_range.reverse()
        return date_range
    else:
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_gmv_max_report(start_date, end_date, dimension_fields, metrics_fields, filtering,
                       advertiser_id="7467779801718456336", store_id="7495896608152062300", page_size=1000,
                       add_data=None):
    if not getToken.advertiser_access_token:
        raise ValueError("Missing tiktok_business_center.advertiser_access_token in config/api_credentials.json")
    proxies = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890',
    }

    # 方法2：设置环境变量（确保定时任务能读取）
    os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
    os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
    os.environ['NO_PROXY'] = 'localhost,127.0.0.1'

    url = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"
    headers = {
        "Access-Token": getToken.advertiser_access_token,
        "Content-Type": "application/json"
    }
    params = {
        "advertiser_id": advertiser_id,
        "store_ids": json.dumps([store_id]),
        "dimensions": json.dumps(dimension_fields),
        "metrics": json.dumps(metrics_fields),
        "filtering": json.dumps(filtering) if filtering else None,
        "start_date": start_date,
        "end_date": end_date,
        "page": 1,
        "page_size": page_size
    }

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
                "advertiser_id": advertiser_id,
                **{field: metrics.get(field) for field in metrics_fields},
                **{field: dimensions.get(field) for field in dimension_fields}
            }
            # print(merged_data)
            # if str(merged_data["cost"]) != '0.00':
            #     print(merged_data)
            if add_data:
                merged_data.update(add_data)

            all_data.append(merged_data)

        current_page = page_info.get("page", 1)
        total_page = page_info.get("total_page", 1)

        if current_page >= total_page:
            break
        else:
            params["page"] += 1

    return all_data


# @getToken.task_record.record_task("API-获取TikTok-Gmvmax推广系列报表")
def get_gmv_max_report_task_campaign():
    """
    获取 GMV Max 推广系列列表
    """
    start_date, end_date = get_date_range(days=5, need_split=False)
    # start_date, end_date = "2025-06-01", "2025-06-26"
    print(f"开始获取{start_date} ~ {end_date}的GMV Max推广系列列表...")
    dimensions = ["campaign_id", "stat_time_day"]
    metrics = ["campaign_id", "operation_status", "campaign_name", "schedule_type", "schedule_start_time",
               "schedule_end_time", "target_roi_budget", "bid_type", "max_delivery_budget", "roas_bid", "cost",
               "net_cost", "orders", "cost_per_order", "gross_revenue", "roi"]
    filtering = {
        "gmv_max_promotion_types": ["PRODUCT"],
    }
    campaign_data = get_gmv_max_report(dimension_fields=dimensions,
                                       metrics_fields=metrics,
                                       filtering=filtering,
                                       start_date=start_date,
                                       end_date=end_date,
                                       page_size=100)
    getToken.gosql_v3.api_to_sql(campaign_data, "api_tiktok_oversea_gmv_max_campaign_report")
    time.sleep(1)
    requests.get(
        get_credentials("bi_refresh_urls", "ds_kc223c7142b8243d5a9d88d3", required=True))


# @getToken.task_record.record_task("API-获取TikTok-Gmvmax产品系列报表")
def get_gmv_max_report_task_product():
    """
    获取 GMV Max 产品报表
    """
    start_date, end_date = get_date_range(days=5, need_split=False)
    print(f"开始获取{start_date} ~ {end_date}的GMV Max产品列表...")
    dimensions = ["item_group_id", "stat_time_day"]
    metrics = ["product_name", "item_group_id", "product_image_url", "product_status", "bid_type", "orders",
               "gross_revenue"]
    for query in getToken.gosql_v3.execute_sql(
            "SELECT DISTINCT campaign_id FROM `api_tiktok_oversea_gmv_max_campaign_report` WHERE stat_time_day = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)"):
        campaign_id = query[0]

        filtering = {
            "campaign_ids": [campaign_id],
        }
        product_data = get_gmv_max_report(dimension_fields=dimensions, metrics_fields=metrics, filtering=filtering,
                                          start_date=start_date, end_date=end_date, page_size=1000,
                                          add_data={"campaign_id": campaign_id})

        getToken.gosql_v3.api_to_sql(product_data, "api_tiktok_oversea_gmv_max_product_report")
        time.sleep(1)

    requests.get(
        get_credentials("bi_refresh_urls", "ds_kc223c7142b8243d5a9d88d3", required=True))


# @getToken.task_record.record_task("API-获取TikTok-Gmvmax素材系列报表")
def get_gmv_max_report_task_metrics():
    """
    获取 GMV Max 素材报表
    """
    for date_str in get_date_range(days=5, need_split=True):
        query_sql = f"""
                SELECT DISTINCT
                  p.advertiser_id,p.campaign_id,p.item_group_id
                FROM
                    api_tiktok_oversea_gmv_max_product_report AS p
                    WHERE  p.gross_revenue > 0
                    AND p.stat_time_day = '{date_str}'
                """
        for advertiser_id, campaign_id, item_group_id in getToken.gosql_v3.execute_query(query_sql):
            start_date, end_date = date_str, date_str
            time.sleep(1)
            print(advertiser_id, campaign_id, item_group_id, start_date, end_date)
            dimensions = ["item_id"]
            metrics = ["title", "item_id", "tt_account_name", "tt_account_profile_image_url",
                       "tt_account_authorization_type", "shop_content_type", "orders", "gross_revenue",
                       "product_impressions", "product_clicks", "product_click_rate", "ad_click_rate",
                       "ad_conversion_rate", "ad_video_view_rate_2s", "ad_video_view_rate_6s", "ad_video_view_rate_p25",
                       "ad_video_view_rate_p50", "ad_video_view_rate_p75", "ad_video_view_rate_p100"]
            for creative_types in ["ADS_AND_ORGANIC", "ORGANIC", "REMOVED"]:
                # time.sleep(1)
                filtering = {
                    "creative_types": [creative_types],
                    "item_group_ids": [item_group_id],
                    "campaign_ids": [campaign_id]
                }
                add_data = {"campaign_id": campaign_id, "item_group_id": item_group_id,
                            "start_date": start_date, "end_date": end_date}
                metrics_data = get_gmv_max_report(dimension_fields=dimensions, metrics_fields=metrics,
                                                  filtering=filtering,
                                                  start_date=start_date, end_date=end_date, page_size=1000,
                                                  add_data=add_data)
                time.sleep(1)

                getToken.gosql_v3.api_to_sql(metrics_data, "api_tiktok_oversea_gmv_max_material_report")

    requests.get(
        get_credentials("bi_refresh_urls", "ds_pc92cb813207944c5be30123", required=True))


def check_data():
    dimensions = ["campaign_id", "item_id", "item_group_id"]
    metrics = ["orders", "gross_revenue",
               "product_impressions", "product_clicks", "product_click_rate", "ad_click_rate",
               "creative_delivery_status",
               "ad_conversion_rate", "ad_video_view_rate_2s", "ad_video_view_rate_6s", "ad_video_view_rate_p25",
               "ad_video_view_rate_p50", "ad_video_view_rate_p75", "ad_video_view_rate_p100", "cost"]

    campaign_id_list = getToken.gosql_v3.execute_query(
        "select distinct campaign_id  from api_tiktok_oversea_gmv_max_product_report")
    campaign_id_list = [str(campaign_id[0]) for campaign_id in campaign_id_list]

    item_group_list = getToken.gosql_v3.execute_query(
        "select distinct item_group_id  from api_tiktok_oversea_gmv_max_product_report")
    item_group_list = [str(item_group[0]) for item_group in item_group_list]

    filtering = {
        # "creative_types": ["ADS_AND_ORGANIC"],
        "item_group_ids": item_group_list,
        "campaign_ids": campaign_id_list,
        "creative_delivery_statuses": ["IN_QUEUE", "LEARNING", "DELIVERING", "AUTHORIZATION_NEEDED",
                                       "EXCLUDED", "UNAVAILABLE", "REJECTED", "NOT_DELIVERYING"]
    }
    for date_str in get_date_range(days=3, need_split=True):
        add_data = {"start_date": date_str, "end_date": date_str}

        metrics_data = get_gmv_max_report(dimension_fields=dimensions, metrics_fields=metrics, filtering=filtering,
                                          start_date=date_str, end_date=date_str, page_size=1000,
                                          add_data=add_data)
        getToken.gosql_v3.api_to_sql(metrics_data, "api_tiktok_oversea_gmv_max_material_report_copy1")
        time.sleep(1)


if __name__ == '__main__':
    get_gmv_max_report_task_campaign()
    time.sleep(1)
    get_gmv_max_report_task_product()
    time.sleep(1)
    get_gmv_max_report_task_metrics()
    time.sleep(1)
    # check_data()
    # for date_str in get_date_range(days=10, need_split=True):
    #     print(date_str)
