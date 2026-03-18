from tiktok_package.tiktok_auth import Tiktok
from utils import *
from decimal import Decimal
import datetime
import math
import json
import time
import argparse


class TiktokOrderOperator(Tiktok):
    def __init__(self):
        super().__init__()

    def parse_detail(self, data, shop_id=None):
        seller_remark_dict = {
            0: "灰",
            1: "紫",
            2: "青",
            3: "绿",
            4: "橙",
            5: "红",
        }
        after_sale_status_dict = {
            6: "售后申请",
            27: "拒绝售后申请",
            12: "售后成功",
            7: "售后退货中",
            11: "售后已发货",
            29: "售后退货拒绝",
            13: "【换货返回：换货售后换货商家发货】，【补寄返回：补寄待用户收货】",
            14: "【换货返回：（换货）售后换货用户收货】，【补寄返回：（补寄）用户已收货】",
            28: "售后失败",
            51: "订单取消成功",
            53: "逆向交易已完成",
        }
        refund_status_dict = {
            1: "待退款",
            3: "退款成功",
            4: "退款失败",
        }
        ad_env_type_map = {
            "video": "短视频",
            "live": "直播"
        }

        order_detail = data
        all_d = []
        for sub_order in order_detail['sku_order_list']:
            sub_order = dict(sub_order)
            sub_order_data = {
                "main_order_id": sub_order['parent_order_id'], "sub_order_id": sub_order['order_id'],
                "product_name": sub_order['product_name'], "spec": sub_order['spec'][0]['value'],
                "item_num": sub_order['item_num'], "product_id": sub_order['product_id'],
                "code": sub_order['code'],
                "goods_price": str(Decimal(sub_order['goods_price']) / Decimal('100')),
                "pay_amount": str(Decimal(sub_order['pay_amount']) / Decimal('100')),
                "post_amount": str(Decimal(sub_order['post_amount']) / Decimal('100')),
                "promotion_amount": str(Decimal(sub_order['promotion_amount']) / Decimal('100')),
                "modify_amount": str(Decimal(sub_order['modify_amount']) / Decimal('100')),
                "promotion_pay_amount": str(Decimal(sub_order['promotion_pay_amount']) / Decimal('100')),
                "promotion_redpack_amount": str(
                    Decimal(sub_order['promotion_redpack_amount']) / Decimal('100')),
                "mask_post_receiver": sub_order['mask_post_receiver'],
                "mask_post_tel": sub_order['mask_post_tel'],
                "province": sub_order['mask_post_addr']['province']['name'],
                "city": sub_order['mask_post_addr']['city']['name'],
                "town": sub_order['mask_post_addr']['town']['name'],
                "street": sub_order['mask_post_addr']['street']['name'],
                "address": sub_order['mask_post_addr']['detail'],
                "buyer_words": order_detail['buyer_words'],
                "create_time": str(datetime.datetime.fromtimestamp(int(sub_order["create_time"])) if
                                   sub_order[
                                       "create_time"] != 0 else None),
                "flag_color": seller_remark_dict.get(order_detail['seller_remark_stars']),
                "seller_words": order_detail['seller_words'],
                "finish_time": str(datetime.datetime.fromtimestamp(int(sub_order["finish_time"])) if
                                   sub_order[
                                       "finish_time"] != 0 else None),
                "pay_time": str(datetime.datetime.fromtimestamp(int(sub_order["pay_time"])) if sub_order[
                                                                                                   "pay_time"] != 0 else None),
                "app_channel": sub_order['b_type_desc'],
                "flow_source": sub_order['c_biz_desc'],
                "order_status": sub_order['order_status_desc'],
                "promise_send_time": str(datetime.datetime.fromtimestamp(int(sub_order["exp_ship_time"])) if
                                         sub_order[
                                             "exp_ship_time"] != 0 else None),
                "order_type": sub_order['order_type_desc'],
                "luban_page_id": sub_order['page_id'], "author_id": sub_order['author_id'],
                "author_name": sub_order['author_name'], "after_sale_status": after_sale_status_dict.get(
                    sub_order['after_sale_info']['after_sale_status']),
                "refund_status": refund_status_dict.get(sub_order['after_sale_info']['refund_status']),
                "cancel_reason": sub_order['cancel_reason'],
                "plan_send_time": str(datetime.datetime.fromtimestamp(
                    int(sub_order["appointment_ship_time"])) if sub_order[
                                                                    "appointment_ship_time"] != 0 else None),
                "warehouse_id": sub_order['inventory_list'][0]['warehouse_id'],
                "warehouse_name": sub_order['inventory_list'][0]['warehouse_name'] if 'warehouse_name' in
                                                                                      sub_order[
                                                                                          'inventory_list'][
                                                                                          0] else '',
                "ad_channel": ad_env_type_map.get(sub_order['ad_env_type'], "无"),
                "ship_time": str(datetime.datetime.fromtimestamp(int(sub_order["ship_time"])) if sub_order[
                                                                                                     "ship_time"] != 0 else None),
                "promotion_platform_amount": str(
                    Decimal(sub_order['promotion_platform_amount']) / Decimal('100')),
                "promotion_shop_amount": str(
                    Decimal(sub_order['promotion_shop_amount']) / Decimal('100')),
                "promotion_talent_amount": str(
                    Decimal(sub_order['promotion_talent_amount']) / Decimal('100')),
                "earliest_receipt_time": str(datetime.datetime.fromtimestamp(
                    int(order_detail["appointment_ship_time"])) if
                                             order_detail[
                                                 "earliest_receipt_time"] != 0 else None),
                "tax_amount": str(Decimal(sub_order["tax_amount"]) / Decimal('100')), 'flow_type': ''
            }

            # 判断流量类型

            for item in sub_order['sku_order_tag_ui']:
                if item['key'] == "compass_source_ad_mark":
                    sub_order_data['flow_type'] = '广告'
                    break
                if item['key'] == "compass_source_not_ad_mark":
                    sub_order_data['flow_type'] = '非广告'
                    break

            # 判断流量渠道、流量体裁
            sub_order_data['flow_genre'] = ''
            sub_order_data['flow_channel'] = ''
            for item in sub_order['sku_order_tag_ui']:
                if item['extra'] and 'compass_first_level_entrance_text' in json.loads(item['extra']):
                    sub_order_data['flow_genre'] = item['text']
                    sub_order_data['flow_channel'] = json.loads(item['extra'])['compass_first_level_entrance_text']
                    break

            if shop_id:
                sub_order_data['shop_id'] = shop_id
                sub_order_data['shop_name'] = self.shop_id_mapping.get(shop_id)

            all_d.append(sub_order_data)

        return all_d

    def get_order_search_list(self, start_date_str, end_time_str, shop_id, order_status=None, sku_id=None,
                              just_cnt=False):

        # 构造请求参数，发送请求获取数据
        order_list = []
        # app = Tiktok.TiktokApp()
        method = "order.searchList"
        start_time_timestamp = int(datetime.datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S').timestamp())
        end_time_timestamp = int(datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S').timestamp())
        param = {
            "create_time_start": start_time_timestamp,
            "create_time_end": end_time_timestamp,
            "size": 100,
            "page": 0
        }

        if order_status == "未发货":
            param['combine_status'] = [{"order_status": "2"}]
        elif order_status == "部分发货":
            param['combine_status'] = [{"order_status": "101"}]
        elif order_status == "待支付":
            param['combine_status'] = [{"order_status": "1"}]
        elif order_status == "已支付":
            param['combine_status'] = [{"order_status": "105"}]
        elif order_status == "已取消":
            param['combine_status'] = [{"order_status": "4"}]
        elif order_status == "已完成":
            param['combine_status'] = [{"order_status": "5"}]
        elif order_status == "待发货":
            param['combine_status'] = [{"order_status": "2"}]

        if sku_id:
            param['product'] = sku_id
        token = self.get_token(shop_id)

        while True:
            print("请求第" + str(param["page"]) + "页。")
            resp = self.fetch_resp(method, param, token)
            total_order_cnt = int(resp["data"]["total"])
            size = 100
            total_pages = math.ceil(total_order_cnt / size)

            if just_cnt:
                return total_order_cnt

            for i in resp['data']['shop_order_list']:
                order_list.extend(self.parse_detail(i, shop_id))

            if int(param["page"]) == total_pages:
                break
            param["page"] = str(int(param["page"]) + 1)

        return order_list
        # gosql_v2.api_to_sql(order_list, "tiktok_orders_api")


if __name__ == '__main__':
    app = TiktokOrderOperator()
    all_orders = []
    start_time = "2024-06-13 00:00:00"
    end_time = "2024-06-13 23:59:59"
    for shop_id in app.shop_id_mapping.keys():
        print(shop_id)
        order_list = app.get_order_search_list(start_time, end_time, shop_id)
        all_orders.extend(order_list)

    print(all_orders)
