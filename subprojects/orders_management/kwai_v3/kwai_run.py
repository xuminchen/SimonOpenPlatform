# -*- coding: utf-8 -*-
from timestr_util import get_many_days
from kwai_api_template import KwaiAPITemplate


class KwaiAPIClient:
    def __init__(self, platform):
        """
        :param platform: 平台类型，可选 "快手国内" 或 "快手国际"
        """
        self.platform = platform
        self.api = KwaiAPITemplate(platform)

    def fetch_orders(self, days=None, start_date=None, end_date=None, shop_name=None):
        """获取订单数据"""
        return self._fetch_data(self.api.get_orders_v2, days, start_date, end_date, shop_name)

    def fetch_refunds(self, days=None, start_date=None, end_date=None, shop_name=None):
        """获取退款数据"""
        return self._fetch_data(self.api.get_order_refund, days, start_date, end_date, shop_name)

    def fetch_bills(self, days=None, start_date=None, end_date=None, shop_name=None):
        """获取账单数据"""
        return self._fetch_data(self.api.get_bill_list, days, start_date, end_date, shop_name)

    def _fetch_data(self, api_method, days, start_date, end_date, shop_name):
        """统一数据获取方法"""
        if days:
            get_many_days(api_method, days=days, shop_name=shop_name)
        else:
            if not start_date or not end_date:
                raise ValueError("必须提供开始日期和结束日期")
            get_many_days(api_method, start_date_str=start_date, end_date_str=end_date, shop_name=shop_name)


# 使用示例
if __name__ == '__main__':
    # 获取国内店铺最近7天订单
    client = KwaiAPIClient("快手国内")
    client.fetch_orders(days=7)

    # 获取国际店铺指定日期范围的退款数据
    # client = KwaiAPIClient("快手国际")
    # client.fetch_refunds(start_date="2025-06-01", end_date="2025-06-15")

