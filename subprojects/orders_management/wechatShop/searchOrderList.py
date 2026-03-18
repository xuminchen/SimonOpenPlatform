import datetime
import gosql_v3
import Tools
import argparse
from subprojects._shared.core.api_credentials import get_credentials


def load_wechat_shop_map():
    shop_map = get_credentials("wechat_shop", "shops", default={})
    if not shop_map:
        raise ValueError("Missing wechat_shop.shops in config/api_credentials.json")
    return shop_map


def timestamp_to_datetime(timestamp):
    """将时间戳转换为可读的日期时间格式"""
    if timestamp:
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return None


def cent_to_yuan(amount):
    """将分转换为元"""
    if amount is not None:
        return amount / 100
    return 0


def search_order_list(access_token, time_type="create_time", start_date=None, end_date=None, time_range_days=7):
    # 确定开始和结束时间
    if start_date and end_date:
        start_time = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
        end_time = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_time = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=59)
        start_time = (datetime.datetime.now() - datetime.timedelta(days=time_range_days)).replace(hour=0, minute=0,
                                                                                                  second=0)

    print(f"订单{time_type}:{start_time}-{end_time}")
    # 循环按天获取数据
    date_ranges = [
        (
            # 开始时间 → 转成 int 时间戳
            int((start_time + datetime.timedelta(days=i)).timestamp()),
            # 结束时间 → 转成 int 时间戳
            int(min(start_time + datetime.timedelta(days=i + 6), end_time).timestamp())
        )
        for i in range(0, (end_time - start_time).days + 1, 7)
    ]

    all_orders = []
    for start_time_dt, end_time_dt in date_ranges:
        # 分页获取数据
        url = f"https://api.weixin.qq.com/channels/ec/order/list/get?access_token={access_token}"
        payload = {
            f"{time_type}_range": {
                "start_time": start_time_dt,
                "end_time": end_time_dt
            },
            "page_size": 100,
        }

        has_more = True
        while has_more:
            response_json = Tools.send_request(url=url, data=payload, method="post")
            if not response_json:
                print("微信订单查询失败: empty response")
                break

            print(response_json)
            order_id_list = response_json.get('order_id_list', [])
            all_orders.extend(order_id_list)

            has_more = response_json.get("has_more", False)
            if has_more:
                payload['next_key'] = response_json.get('next_key')
            else:
                break

    return all_orders


def search_order_detail(access_token, order_id, shop_name):
    url = f"https://api.weixin.qq.com/channels/ec/order/get?access_token={access_token}"
    payload = {
        "order_id": order_id
    }

    order_data = Tools.send_request(url, data=payload, method="post")

    order = order_data['order']
    order_detail = order.get('order_detail', {})

    status_mapping = {
        10: '待付款',
        12: '礼物待收下',
        13: '凑单买凑团中',
        20: '待发货',
        21: '部分发货',
        30: '待收货',
        100: '完成',
        200: '全部商品售后之后，订单取消',
        250: '未付款用户主动取消或超时未付款订单自动取消'
    }
    order_scene_mapping = {
        1: '其他',
        2: '直播间',
        3: '短视频',
        4: '商品分享',
        5: '商品橱窗主页',
        6: '公众号文章商品卡片'
    }
    # 销售渠道映射
    sale_channel_mapping = {
        0: '关联账号',
        1: '合作账号',
        2: '授权号',
        100: '达人带货',
        101: '带货机构推广',
        102: '其他'
    }

    # 带货账号类型映射
    account_type_mapping = {
        1: '视频号',
        2: '公众号',
        3: '小程序',
        4: '企业微信',
        5: '带货达人',
        6: '服务号',
        1000: '带货机构'
    }
    commission_handling_progress_mapping = {
        0: '未生成',
        1: '已生成',
    }
    # 分账方类型映射
    commission_type_mapping = {
        0: '达人',
        1: '带货机构'
    }

    # 分账状态映射
    commission_status_mapping = {
        1: '未结算',
        2: '已结算'
    }

    price = order_detail.get('price_info', {})
    payment = order_detail.get('pay_info', {})
    delivery_info = order_detail.get('delivery_info', {})
    address = delivery_info.get('address_info', {})
    settle_info = order_detail.get('settle_info', {})
    ext_info = order_detail.get('ext_info', {})
    aftersale_detail = order_detail.get('aftersale_detail ', {})

    order_info = {
        'order_id': order.get('order_id'),
        'openid': order.get('openid'),
        'unionid': order.get('unionid'),
        'create_time': timestamp_to_datetime(order.get('create_time')),
        'update_time': timestamp_to_datetime(order.get('update_time')),
        'status': status_mapping.get(order.get('status'), '未知'),
        'product_price': cent_to_yuan(price.get('product_price')),
        'order_price': cent_to_yuan(price.get('order_price')),
        'freight': cent_to_yuan(price.get('freight')),
        'discounted_price': cent_to_yuan(price.get('discounted_price')),
        'is_discounted': price.get('is_discounted'),
        'original_order_price': cent_to_yuan(price.get('original_order_price')),
        'estimate_product_price': cent_to_yuan(price.get('estimate_product_price')),
        'merchant_receieve_price': cent_to_yuan(price.get('merchant_receieve_price')),
        'merchant_discounted_price': cent_to_yuan(price.get('merchant_discounted_price')),
        'finder_discounted_price': cent_to_yuan(price.get('finder_discounted_price')),
        'payment_method': payment.get('payment_method'),
        'prepay_time': timestamp_to_datetime(payment.get('prepay_time')),
        'pay_time': timestamp_to_datetime(payment.get('pay_time')),
        # 地址信息
        'user_name': address.get('user_name'),
        'tel_number': address.get('tel_number'),
        'province_name': address.get('province_name'),
        'city_name': address.get('city_name'),
        'county_name': address.get('county_name'),
        'detail_info': address.get('detail_info'),
        'postal_code': address.get('postal_code'),
        'national_code': address.get('national_code'),
        'house_number': address.get('house_number'),
        # 配送信息
        'ship_done_time': timestamp_to_datetime(delivery_info.get('ship_done_time')) if delivery_info.get(
            'ship_done_time') else None,
        'deliver_method': delivery_info.get('deliver_method'),
        'ewaybill_order_code': delivery_info.get('ewaybill_order_code'),

        # 结算信息
        'predict_commission_fee': cent_to_yuan(settle_info.get('predict_commission_fee', 0)),
        'commission_fee': cent_to_yuan(settle_info.get('commission_fee', 0)),
        'predict_wecoin_commission': cent_to_yuan(settle_info.get('predict_wecoin_commission', 0)),
        'wecoin_commission': cent_to_yuan(settle_info.get('wecoin_commission', 0)),
        'settle_time': timestamp_to_datetime(settle_info.get('settle_time')) if delivery_info.get(
            'ship_done_time') else None,

        # 扩展信息
        'customer_notes': ext_info.get('customer_notes'),
        'merchant_notes': ext_info.get('merchant_notes'),
        'confirm_receipt_time': timestamp_to_datetime(ext_info.get('confirm_receipt_time')),
        'finder_id': ext_info.get('finder_id'),
        'live_id': ext_info.get('live_id'),
        'order_scene': order_scene_mapping.get(ext_info.get('order_scene'), '未知'),
        'vip_order_session_id': ext_info.get('vip_order_session_id'),
        'commission_handling_progress': commission_handling_progress_mapping.get(
            ext_info.get('commission_handling_progress'),
            '未知'),

        'shop_name': shop_name

    }

    products = order_detail.get('product_infos', [])
    product_infos = []
    for product in products:
        product_dict = {
            'order_id': order.get('order_id'),
            'product_id': product.get('product_id'),
            'sku_id': product.get('sku_id'),
            'thumb_img': product.get('thumb_img'),
            'sale_price': cent_to_yuan(product.get('sale_price')),
            'sku_cnt': product.get('sku_cnt'),
            'title': product.get('title'),
            'on_aftersale_sku_cnt': product.get('on_aftersale_sku_cnt'),
            'finish_aftersale_sku_cnt': product.get('finish_aftersale_sku_cnt'),
            'sku_code': product.get('sku_code'),
            'market_price': cent_to_yuan(product.get('market_price')),
            'sku_attrs': str(product.get('sku_attrs', [])),
            'real_price': cent_to_yuan(product.get('real_price')),
            'out_product_id': product.get('out_product_id'),
            'out_sku_id': product.get('out_sku_id'),
            'is_discounted': product.get('is_discounted'),
            'estimate_price': cent_to_yuan(product.get('estimate_price')),
            'out_warehouse_id': product.get('out_warehouse_id'),
            'delivery_deadline': timestamp_to_datetime(product.get('delivery_deadline')),
            'merchant_discounted_price': cent_to_yuan(product.get('merchant_discounted_price')),
            'finder_discounted_price': cent_to_yuan(product.get('finder_discounted_price')),
            'product_unique_id': product.get('product_unique_id'),

        }
        # print(product_dict)
        product_infos.append(product_dict)

    source_infos = order_detail.get('source_infos', [])
    source_info_list = []
    for source_info in source_infos:
        source_info_dict = {
            'order_id': order.get('order_id'),
            'sku_id': source_info.get('sku_id'),
            'account_type': account_type_mapping.get(source_info.get('account_type'), '未知'),
            'account_id': source_info.get('account_id'),
            'sale_channel': sale_channel_mapping.get(source_info.get('sale_channel'), '未知'),
            'account_nickname': source_info.get('account_nickname'),
            'content_type': source_info.get('content_type'),
            'content_id': source_info.get('content_id'),
        }

        source_info_list.append(source_info_dict)

    commission_infos = order_detail.get('commission_infos', [])
    commission_info_list = []
    for commission_info in commission_infos:
        commission_info_dict = {
            'order_id': order.get('order_id'),
            'sku_id': commission_info.get('sku_id'),
            'nickname': commission_info.get('nickname'),
            'type': commission_type_mapping.get(commission_info.get('type'), '未知'),
            'status': commission_status_mapping.get(commission_info.get('status'), '未知'),
            'amount': cent_to_yuan(commission_info.get('amount')),
            'finder_id': commission_info.get('finder_id'),
            'openfinderid': commission_info.get('openfinderid'),
            'talent_id': commission_info.get('talent_id'),
            'agency_id': commission_info.get('agency_id'),
        }
        commission_info_list.append(commission_info_dict)

    return order_info, product_infos, source_info_list, commission_info_list


def go(time_type, time_range_days=1):
    order, product, source, commission = [], [], [], []
    shop_map = load_wechat_shop_map()
    for shop_name, app_info in shop_map.items():
        app_id = app_info.get('app_id')
        secret = app_info.get('secret')
        access_token = Tools.get_access_token(app_id=app_id, secret=secret)

        order_list = search_order_list(access_token=access_token, time_type=time_type, time_range_days=time_range_days)
        for order_id in order_list:
            order_info, product_infos, source_infos, commission_infos = search_order_detail(access_token, order_id,
                                                                                            shop_name)
            order.append(order_info)
            product.extend(product_infos)
            source.extend(source_infos)
            commission.extend(commission_infos)

        print(f'时间类型：{time_type}，店铺名称：{shop_name}，订单数量：{len(order_list)}')

    gosql_v3.api_to_sql(order, 'api_wechat_shop_order_info')
    gosql_v3.api_to_sql(product, 'api_wechat_shop_order_product')
    gosql_v3.api_to_sql(source, 'api_wechat_shop_order_source')
    gosql_v3.api_to_sql(commission, 'api_wechat_shop_order_commission')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--option", type=int, default=1, help="1=create_time，2=update_time")
    args = parser.parse_args()

    if args.option == 1:
        t_type = "create_time"
    else:
        t_type = "update_time"

    go(time_type=t_type)
    # token = "96_XyGuEBIjlL9JTSku72tCuc31kAQKUn9FqB5QdqRMhQ3Xobo8YdTlY3Q0_HF1A__Gdb_OsgDXHS_FDgCTfeDU7ZiupEM516qRbmsIJaWoRcVpbW3TLiK3JrqdmbUSKDfAJAASL"
    # search_order_detail(token, "3730049440287696384","shop_name")
    Tools.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_o8bd4da4055d744e9a25ea6f", required=True))
    Tools.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_q5caeec741cb34e148281557", required=True))
    Tools.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_b9386035cc7c743a69773ede", required=True))
    Tools.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_mb85572c8602f4483a7ca2c6", required=True))
