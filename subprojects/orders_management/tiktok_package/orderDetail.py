# -*- coding: utf-8 -*-
import time
import requests

import Tiktok
from decimal import Decimal
import datetime
import json
import math
# import gosql_v2
import argparse
from subprojects._shared.db import MySQLDatabase
from subprojects._shared.core.api_credentials import get_credentials

id_map = {
    "1688498": "WONDERLAB官方旗舰店",
    "13746708": "WONDERLAB营养食品旗舰店",
    "20184489": "WONDERLAB营养膳食旗舰店",
    "14651306": "奇美研营养膳食旗舰店",
    "47644908": "WONDERLAB母婴旗舰店",
    "58303283": "WONDERLAB海外旗舰店",
    "84479281": "万益蓝WONDERLAB健康营养膳食专卖店"
}


def connect_to_db():
    return MySQLDatabase().connect()


def get_order_detail(shop_id, order_id):
    with open("test.json", "r") as f:
        resp = json.loads(f.read())
    print(resp)
    method = "order.orderDetail"
    p = {
        "shop_order_id": order_id
    }

    path = "/home/wonderlab/pytask/xhx_test/test.json"
    app = Tiktok.TiktokApp()
    token = app.get_token(shop_id)
    resp = app.fetch_resp(method, p, token)
    # parse_detail(resp['data']['shop_order_detail'])
    with open(path, "w") as f:
        f.write(json.dumps(resp))
    #

    #
    # print(resp)


def process_data(start_date_str, end_date_str, shop_id):
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    total = get_total_data(start_date_str, end_date_str, shop_id, "获取数量")

    if total <= 50000:
        return [(start_date_str, end_date_str)]

    if start_date.date() == end_date.date():
        # 如果是同一天，则按照时间段再次分割
        time_half = (end_date - start_date).total_seconds() // 7200

        morning_end = start_date + datetime.timedelta(hours=time_half, minutes=59, seconds=59)
        afternoon_start = morning_end + datetime.timedelta(seconds=1)

        morning_data = process_data(start_date_str, morning_end.strftime('%Y-%m-%d %H:%M:%S'), shop_id)
        afternoon_data = process_data(afternoon_start.strftime('%Y-%m-%d %H:%M:%S'), end_date_str, shop_id)
        return morning_data + afternoon_data

    # 否则按天分割
    mid_date = start_date + datetime.timedelta(days=(end_date - start_date).days // 2)
    mid_date_str = mid_date.strftime('%Y-%m-%d ') + "23:59:59"
    left_data = process_data(start_date_str, mid_date_str, shop_id)
    right_data = process_data((mid_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'), end_date_str,
                              shop_id)
    return left_data + right_data


def get_total_data(start_date_str, end_time_str, shop_id, do="获取数据"):
    # 构造请求参数，发送请求获取数据
    order_list = []
    app = Tiktok.TiktokApp()
    method = "order.searchList"
    start_time_timestamp = int(datetime.datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S').timestamp())
    end_time_timestamp = int(datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S').timestamp())
    param = {
        "create_time_start": start_time_timestamp,
        "create_time_end": end_time_timestamp,
        "size": 100,
        "page": 0
    }

    token = app.get_token(shop_id)

    while True:
        resp = app.fetch_resp(method, param, token)
        total = int(resp["data"]["total"])
        size = 100
        total_pages = math.ceil(total / size)

        if do == "获取数量":
            return total

        for i in resp['data']['shop_order_list']:
            order_list.extend(parse_detail(i, shop_id))

        if len(order_list) >= 5000:
            print("数据超过5000，先落库休息一下...")
            # gosql_v2.api_to_sql(order_list, "tiktok_orders_api")
            order_list = []
            time.sleep(5)

        if int(param["page"]) == total_pages:
            break
        param["page"] = str(int(param["page"]) + 1)

    # gosql_v2.api_to_sql(order_list, "tiktok_orders_api")


def parse_detail(data, shop_id=None):
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
            "main_order_id": sub_order['parent_order_id'],
            "sub_order_id": sub_order['order_id'],
            "product_name": sub_order['product_name'],
            "spec": sub_order['spec'][0]['value'],
            "item_num": sub_order['item_num'],
            "product_id": sub_order['product_id'],
            "code": sub_order['code'],
            "goods_price": str(Decimal(sub_order['goods_price']) / Decimal('100')),
            "pay_amount": str(Decimal(sub_order['pay_amount']) / Decimal('100')),  # 标记-订单应付金额
            "post_amount": str(Decimal(sub_order['post_amount']) / Decimal('100')),
            "promotion_amount": str(Decimal(sub_order['promotion_amount']) / Decimal('100')),
            # 平台优惠、商家优惠、达人优惠
            "modify_amount": str(Decimal(sub_order['modify_amount']) / Decimal('100')),  # 标记-商家改价/改价金额变化量
            "promotion_pay_amount": str(Decimal(sub_order['promotion_pay_amount']) / Decimal('100')),
            "promotion_redpack_amount": str(Decimal(sub_order['promotion_redpack_amount']) / Decimal('100')),
            # 支付方式未找到.
            # 手续费未找到.
            "mask_post_receiver": sub_order['mask_post_receiver'],
            "mask_post_tel": sub_order['mask_post_tel'],
            "province": sub_order['mask_post_addr']['province']['name'],
            "city": sub_order['mask_post_addr']['city']['name'],
            "town": sub_order['mask_post_addr']['town']['name'],
            "street": sub_order['mask_post_addr']['street']['name'],
            "address": sub_order['mask_post_addr']['detail'],
            # 是否修改过地址未找到.
            "buyer_words": order_detail['buyer_words'],
            "create_time": datetime.datetime.fromtimestamp(int(sub_order["create_time"])) if sub_order[
                                                                                                 "create_time"] != 0 else None,
            "flag_color": seller_remark_dict.get(order_detail['seller_remark_stars']),
            "seller_words": order_detail['seller_words'],
            "finish_time": datetime.datetime.fromtimestamp(int(sub_order["finish_time"])) if sub_order[
                                                                                                 "finish_time"] != 0 else None,
            "pay_time": datetime.datetime.fromtimestamp(int(sub_order["pay_time"])) if sub_order[
                                                                                           "pay_time"] != 0 else None,
            "app_channel": sub_order['b_type_desc'],
            "flow_source": sub_order['c_biz_desc'],
            "order_status": sub_order['order_status_desc'],
            "promise_send_time": datetime.datetime.fromtimestamp(int(sub_order["exp_ship_time"])) if sub_order[
                                                                                                         "exp_ship_time"] != 0 else None,
            "order_type": sub_order['order_type_desc'],
            "luban_page_id": sub_order['page_id'],
            "author_id": sub_order['author_id'],
            "author_name": sub_order['author_name'],
            # 所属门店信息ID未找到，有相似 sub_order['store_info']['store_id']
            "after_sale_status": after_sale_status_dict.get(sub_order['after_sale_info']['after_sale_status']),
            "refund_status": refund_status_dict.get(sub_order['after_sale_info']['refund_status']),

            "cancel_reason": sub_order['cancel_reason'],
            "plan_send_time": datetime.datetime.fromtimestamp(int(sub_order["appointment_ship_time"])) if sub_order[
                                                                                                              "appointment_ship_time"] != 0 else None,
            "warehouse_id": sub_order['inventory_list'][0]['warehouse_id'],
            "warehouse_name": sub_order['inventory_list'][0]['warehouse_name'] if 'warehouse_name' in
                                                                                  sub_order['inventory_list'][
                                                                                      0] else '',
            # 是否安心购未找到，
            "ad_channel": ad_env_type_map.get(sub_order['ad_env_type'], "无"),
            # 发货主体未找到，
            # 发货主体明细购未找到，
            "ship_time": datetime.datetime.fromtimestamp(int(sub_order["ship_time"])) if sub_order[
                                                                                             "ship_time"] != 0 else None,
            # 降价类优惠没有找到
            "promotion_platform_amount": str(
                Decimal(sub_order['promotion_platform_amount']) / Decimal('100')),
            "promotion_shop_amount": str(
                Decimal(sub_order['promotion_shop_amount']) / Decimal('100')),  # 标记-商家优惠金额/店铺优惠金额
            "promotion_talent_amount": str(
                Decimal(sub_order['promotion_talent_amount']) / Decimal('100')),
            "earliest_receipt_time": datetime.datetime.fromtimestamp(int(order_detail["appointment_ship_time"])) if
            order_detail[
                "earliest_receipt_time"] != 0 else None,
            # 是否平台仓自流转没有找到
            #
            "tax_amount": str(Decimal(sub_order["tax_amount"]) / Decimal('100')),
            # 货品ID没找到
            # 是否福袋订单没找到

        }

        # 判断流量类型
        sub_order_data['flow_type'] = ''

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
            sub_order_data['shop_name'] = id_map.get(shop_id)

        all_d.append(sub_order_data)

    return all_d


def select_shop():
    conn = connect_to_db()
    cursor = conn.cursor()
    need_run_shop = []

    # Wonderlab其他店铺检索引流方式
    sql_str = """
    SELECT distinct t0.shop_id
        ,ifnull(t1.lost_order_cnt,0) as lost_order_cnt
    from 
    ( 
        SELECT *
        from tiktok_middleware 
        where shop_id not like '%海外%' 
        and  shop_id not like '%奇美研%'
    )t0
    left join
    ( 
        SELECT r1.shop_id
            ,count(r1.order_id) as lost_order_cnt
        from 
        ( 
            SELECT *
            from tiktok_middleware 
            where shop_id not like '%海外%' 
            and  shop_id not like '%奇美研%'
        )r1
        left join
        ( 
            SELECT DISTINCT main_order_id
            from tiktok_orders_api 
            where  pay_time>=current_date-2
            and pay_time<current_date
        )r2
        on r1.order_id=r2.main_order_id 
        where r2.main_order_id  is null
        group by 1
    )t1    
    on t0.shop_id=t1.shop_id
    """
    shop_map = {
        "1688498": "WONDERLAB官方旗舰店",
        "13746708": "WONDERLAB营养食品旗舰店",
        "20184489": "WONDERLAB营养膳食旗舰店",
        "14651306": "奇美研营养膳食旗舰店",
        "47644908": "WONDERLAB母婴旗舰店",
        "58303283": "WONDERLAB海外旗舰店"
    }
    cursor.execute(sql_str)
    for i in cursor.fetchall():
        if i[1] != 0:
            need_run_shop.append(i[0])

    need_run_shop.append('58303283')
    need_run_shop.append('14651306')
    cursor.close()
    conn.close()

    return need_run_shop


if __name__ == '__main__':
    # option-1: 抖店全量订单抓取
    # option-2: 流量数据补全
    parser = argparse.ArgumentParser()
    parser.add_argument("--option", type=int, default=1)
    args = parser.parse_args()
    option = args.option

    print(option)
    # run_shop_list = ['20184489']
    if option == 1:
        run_shop_list = select_shop()
        # run_shop_list = ['84479281']
        start_time = (datetime.datetime.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d 00:00:00")
    else:
        run_shop_list = ['1688498', '13746708', '20184489', '14651306', '47644908', '58303283', '84479281']
        start_time = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")

    if len(run_shop_list) > 0:
        end_time = datetime.datetime.today().strftime("%Y-%m-%d 23:59:59")
        for shop_id in run_shop_list:
            for i in process_data(start_time, end_time, shop_id):
                get_total_data(i[0], i[1], shop_id)
        requests.get(
            get_credentials("bi_refresh_urls", "ds_c830e2553b6754e2e80379b9", required=True))
    else:
        print("引流方式覆盖完整，无需重跑")

    # a = "58303283"
    # b = "6928985840691123291"
    # get_order_detail(a, b)
# get_total_data(start_timestamp, end_timestamp, "1688498")
# path = "/home/wonderlab/pytask/xhx_test/test.json"
# with open(path,"w") as f:
#     f.write(json.dumps(resp))

# with open("test.json", "r") as f:
#     resp = json.loads(f.read())
#
# print(resp)
#
# for i in resp['data']['shop_order_list']:
#     parse_detail(i)
