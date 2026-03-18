# -*- coding: utf-8 -*-
# 启动执行文件
from kwai_run import KwaiAPIClient
import requests
from subprojects._shared.core.api_credentials import get_credentials

def fetch_kwai_data():
    """ 快手数据获取
        domestic_client.fetch_orders(days=2)
        domestic_client.fetch_orders(start_date="2025-06-15", end_date="2025-06-16")
    """

    # 创建客户端实例
    domestic_client = KwaiAPIClient("快手国内")
    intl_client = KwaiAPIClient("快手国际")

    # 获取订单数据
    print("正在获取国内订单最近1天数据...")
    domestic_client.fetch_orders(days=4)

    print("\n正在获取国际订单最近1天数据...")
    intl_client.fetch_orders(days=4)

    # 获取退款数据
    print("\n正在获取国内店铺退款数据...")
    domestic_client.fetch_refunds(days=4, shop_name='WONDERLAB官方旗舰店')

    print("\n正在获取国际店铺退款数据...")
    intl_client.fetch_refunds(days=4, shop_name='WONDERLAB海外旗舰店')

    # 获取账单数据
    print("\n正在获取国内店铺账单数据...")
    domestic_client.fetch_bills(days=4, shop_name='WONDERLAB官方旗舰店')

    print("\n正在获取国际店铺账单数据...")
    intl_client.fetch_bills(days=4, shop_name='WONDERLAB海外旗舰店')

    requests.get(
        get_credentials("bi_refresh_urls", "ds_lebaf4c4de0d94a64ad1a1b2", required=True))  # 订单
    requests.get(
        get_credentials("bi_refresh_urls", "ds_p2593f6a9d8924912a54c047", required=True))  # 账单
    requests.get(
        get_credentials("bi_refresh_urls", "ds_d95030aa994bc484497e93a6", required=True))  # 售后单






if __name__ == '__main__':
    fetch_kwai_data()