import datetime
import time
import json
import pytz
import getToken
import argparse
from subprojects._shared.core.api_credentials import get_credentials

field_mapping = {
    # 基础订单字段
    "order_id": "订单ID",
    "buyer_email": "买家邮箱",
    "buyer_message": "买家留言",
    "cancel_order_sla_time": "取消订单SLA时间",
    "create_time": "订单创建时间",
    "delivery_option_id": "配送选项ID",
    "delivery_option_name": "配送选项名称",
    "delivery_sla_time": "交付SLA时间",
    "delivery_time": "实际交付时间",
    "delivery_type": "配送类型",
    "fulfillment_type": "履行类型",
    "has_updated_recipient_address": "收件人地址是否更新过",
    "is_cod": "是否为货到付款",
    "is_exchange_order": "是否为换货订单",
    "is_on_hold_order": "是否为挂起订单",
    "is_replacement_order": "是否为补发订单",
    "is_sample_order": "是否为样品订单",
    "paid_time": "订单支付时间",
    "payment_method_name": "支付方式名称",
    "recipient_full_address": "完整收件地址",
    "recipient_phone": "收件人电话",
    "rts_sla_time": "准备发货SLA时间",
    "shipping_due_time": "物流截止时间",
    "shipping_provider": "物流服务提供商",
    "shipping_type": "物流类型",
    "status": "订单状态",
    "tracking_number": "物流跟踪号",
    "tts_sla_time": "TikTok Shop SLA时间",
    "update_time": "订单最后更新时间",
    "user_id": "用户ID",
    "warehouse_id": "仓库ID",
    "cancellation_initiator": "取消发起者",
    "handling_duration_days": "处理时长天数",
    "handling_duration_type": "处理时长类型",
    "need_upload_invoice": "是否需要上传发票",
    "request_cancel_time": "请求取消时间",
    "delivery_option_required_delivery_time": "配送选项要求送达时间",
    "delivery_due_time": "物流截止时间",
    "collection_due_time": "收集截止时间",
    "pick_up_cut_off_time": "取件截止时间",
    "fast_dispatch_sla_time": "快速发货SLA时间",
    "commerce_platform": "电商平台",
    "order_type": "订单类型",
    "release_date": "发布时间",
    "auto_combine_group_id": "自动组合组ID",
    "cpf_name": "CPF名称",
    "is_buyer_request_cancel": "是否买家申请取消",
    "replaced_order_id": "替换订单ID",
    "exchange_source_order_id": "换货来源订单ID",
    "collection_time": "收集时间",
    "cancel_time": "取消时间",

    # 支付信息
    "payment_currency": "支付货币类型",
    "original_shipping_fee": "原始运费",
    "original_total_product_price": "原始商品总价",
    "platform_discount": "平台折扣",
    "shipping_fee": "实际运费",
    "shipping_fee_cofunded_discount": "运费共同折扣",
    "shipping_fee_platform_discount": "运费平台折扣",
    "shipping_fee_seller_discount": "运费卖家折扣",
    "shipping_fee_tax": "运费税",
    "sub_total": "小计金额",
    "tax": "总税额",
    "total_amount": "总金额",
    "small_order_fee": "小额订单费",
    "retail_delivery_fee": "零售配送费",
    "buyer_service_fee": "买家服务费",
    "handling_fee": "处理费",
    "shipping_insurance_fee": "运费保险费",
    "item_insurance_fee": "商品保险费",
    "product_tax": "商品税",

    # 商品项字段
    "currency": "商品货币类型",
    "display_status": "商品状态",
    "line_item_id": "商品项ID",
    "is_dangerous_good": "是否为危险品",
    "is_gift": "是否为赠品",
    "tax_amount": "商品税额",
    "tax_rate": "商品税率",
    "tax_type": "商品税类型",
    "original_price": "商品原价",
    "package_id": "包裹ID",
    "package_status": "包裹状态",
    "product_id": "商品ID",
    "product_name": "商品名称",
    "rts_time": "商品准备发货时间",
    "seller_discount": "卖家折扣",
    "seller_sku": "卖家SKU",
    "shipping_provider_id": "物流服务提供商ID",
    "shipping_provider_name": "物流服务提供商名称",
    "sku_id": "SKU ID",
    "sku_image": "SKU图片URL",
    "sku_type": "SKU类型",
    "sku_name": "SKU名称",
    "sale_price": "商品售价",
    "combined_listing_skus": "组合SKU列表",
    "handling_duration": "处理时长",
    "cancel_user": "取消用户",
    "cancel_reason": "取消原因",

    # 收件人地址
    "recipient_first_name_local_script": "收件人名本地脚本",
    "recipient_last_name_local_script": "收件人姓本地脚本",
    "delivery_preferences": "配送偏好",
    "address_detail": "详细地址",
    "address_line1": "地址行1",
    "address_line2": "地址行2",
    "address_line3": "地址行3",
    "address_line4": "地址行4",
    "district_info": "地区信息",
    "first_name": "收件人名",
    "last_name": "收件人姓",
    "name": "收件人全名",
    "phone_number": "收件人电话号码",
    "postal_code": "邮政编码",
    "region_code": "地区代码",

    # 包裹信息
    "package_weight": "包裹重量",
    "package_dimensions": "包裹尺寸",
    # 新增字段
    "seller_note": "卖家备注",
    "split_or_combine_tag": "拆分或合并标签",
    "cpf": "CPF编号",
}
token = \
    getToken.gosql_v3.execute_query(
        "select `access_token` from api_tiktok_shop_token where `seller_name` = 'WonderBlue'")[0][0]


def convert_timestamp_to_us_pacific(timestamp, timezone):
    if timestamp:
        dt = datetime.datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        dt = dt.astimezone(timezone)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return ''


def get_order_list(start_ts, end_ts, time_type="create_time"):
    url = "https://open-api.tiktokglobalshop.com/order/202309/orders/search"
    params = {
        "app_key": getToken.app_key,
        "shop_cipher": "TTP_UHd08wAAAABKNQAuH4gfTjRY0btkD_bR",
        "shop_id": "7495896608152062300",
        "page_size": 100,
        "sort_field": "create_time",
    }

    body = {
        f"{time_type}_ge": start_ts,
        f"{time_type}_lt": end_ts
    }
    headers = {
        'content-type': 'application/json',
        'x-tts-access-token': token
    }

    order_basic_data = []
    order_item_data = []
    order_package_data = []
    order_line_data = []

    # 定义美区时间（美国太平洋时间）
    us_pacific = pytz.timezone('America/Los_Angeles')

    while True:
        time.sleep(1)
        response_json = getToken.request_signed_json(
            method="post",
            url=url,
            params=params,
            headers=headers,
            body=body,
            event_name="tiktok_shop_search_orders",
        )
        # print(response_json)
        data_len = len(response_json.get('data', {}).get('orders', []))

        for order_data in response_json.get('data', {}).get('orders', []):
            # 解析基础数据
            basic = {
                'order_id': order_data.get('id'),
                'buyer_email': order_data.get('buyer_email'),
                'buyer_message': order_data.get('buyer_message'),
                'cancel_order_sla_time': convert_timestamp_to_us_pacific(order_data.get('cancel_order_sla_time', 0),
                                                                         us_pacific),
                'create_time': convert_timestamp_to_us_pacific(order_data.get('create_time', 0), us_pacific),
                'delivery_option_id': order_data.get('delivery_option_id'),
                'delivery_option_name': order_data.get('delivery_option_name'),
                'delivery_sla_time': convert_timestamp_to_us_pacific(order_data.get('delivery_sla_time', 0),
                                                                     us_pacific),
                'delivery_time': convert_timestamp_to_us_pacific(order_data.get('delivery_time', 0), us_pacific),
                'delivery_type': order_data.get('delivery_type'),
                'fulfillment_type': order_data.get('fulfillment_type'),
                'has_updated_recipient_address': order_data.get('has_updated_recipient_address'),
                'is_cod': order_data.get('is_cod'),
                'is_exchange_order': order_data.get('is_exchange_order'),
                'is_on_hold_order': order_data.get('is_on_hold_order'),
                'is_replacement_order': order_data.get('is_replacement_order'),
                'is_sample_order': order_data.get('is_sample_order'),
                'paid_time': convert_timestamp_to_us_pacific(order_data.get('paid_time', 0), us_pacific),
                'payment_method_name': order_data.get('payment_method_name'),
                'recipient_full_address': order_data.get('recipient_address', {}).get('full_address'),
                'recipient_phone': order_data.get('recipient_address', {}).get('phone_number'),
                'rts_sla_time': convert_timestamp_to_us_pacific(order_data.get('rts_sla_time', 0), us_pacific),
                'rts_time': convert_timestamp_to_us_pacific(order_data.get('rts_time', 0), us_pacific),
                'shipping_due_time': convert_timestamp_to_us_pacific(order_data.get('shipping_due_time', 0),
                                                                     us_pacific),
                'shipping_provider': order_data.get('shipping_provider'),
                'shipping_provider_id': order_data.get('shipping_provider_id'),
                'status': order_data.get('status'),
                'tracking_number': order_data.get('tracking_number'),
                'tts_sla_time': convert_timestamp_to_us_pacific(order_data.get('tts_sla_time', 0), us_pacific),
                'update_time': convert_timestamp_to_us_pacific(order_data.get('update_time', 0), us_pacific),
                'user_id': order_data.get('user_id'),
                'warehouse_id': order_data.get('warehouse_id'),
                'cancellation_initiator': order_data.get('cancellation_initiator'),
                'handling_duration_days': order_data.get('handling_duration', {}).get('days'),
                'handling_duration_type': order_data.get('handling_duration', {}).get('type'),
                'need_upload_invoice': order_data.get('need_upload_invoice'),
                'request_cancel_time': convert_timestamp_to_us_pacific(order_data.get('request_cancel_time', 0),
                                                                       us_pacific),
                'delivery_option_required_delivery_time': convert_timestamp_to_us_pacific(
                    order_data.get('delivery_option_required_delivery_time', 0), us_pacific),
                'delivery_due_time': convert_timestamp_to_us_pacific(order_data.get('delivery_due_time', 0),
                                                                     us_pacific),
                'collection_due_time': convert_timestamp_to_us_pacific(order_data.get('collection_due_time', 0),
                                                                       us_pacific),
                'pick_up_cut_off_time': convert_timestamp_to_us_pacific(order_data.get('pick_up_cut_off_time', 0),
                                                                        us_pacific),
                'fast_dispatch_sla_time': convert_timestamp_to_us_pacific(order_data.get('fast_dispatch_sla_time', 0),
                                                                          us_pacific),
                'commerce_platform': order_data.get('commerce_platform'),
                'order_type': order_data.get('order_type'),
                'release_date': convert_timestamp_to_us_pacific(order_data.get('release_date', 0), us_pacific),
                'auto_combine_group_id': order_data.get('auto_combine_group_id'),
                'cpf_name': order_data.get('cpf_name'),
                'is_buyer_request_cancel': order_data.get('is_buyer_request_cancel'),
                'replaced_order_id': order_data.get('replaced_order_id'),
                'exchange_source_order_id': order_data.get('exchange_source_order_id'),
                'collection_time': convert_timestamp_to_us_pacific(order_data.get('collection_time', 0), us_pacific),
                'cancel_time': convert_timestamp_to_us_pacific(order_data.get('cancel_time', 0), us_pacific),
                # 收件人地址字段
                'recipient_first_name_local_script': order_data.get('recipient_address', {}).get(
                    'first_name_local_script'),
                'recipient_last_name_local_script': order_data.get('recipient_address', {}).get(
                    'last_name_local_script'),
                'delivery_preferences': json.dumps(
                    order_data.get('recipient_address', {}).get('delivery_preferences', {})),
                'address_detail': order_data.get('recipient_address', {}).get('address_detail'),
                'address_line1': order_data.get('recipient_address', {}).get('address_line1'),
                'address_line2': order_data.get('recipient_address', {}).get('address_line2'),
                'address_line3': order_data.get('recipient_address', {}).get('address_line3'),
                'address_line4': order_data.get('recipient_address', {}).get('address_line4'),
                'district_info': json.dumps(order_data.get('recipient_address', {}).get('district_info', [])),
                'first_name': order_data.get('recipient_address', {}).get('first_name'),
                'last_name': order_data.get('recipient_address', {}).get('last_name'),
                'name': order_data.get('recipient_address', {}).get('name'),
                'phone_number': order_data.get('recipient_address', {}).get('phone_number'),
                'postal_code': order_data.get('recipient_address', {}).get('postal_code'),
                'region_code': order_data.get('recipient_address', {}).get('region_code'),
                # 支付信息
                'payment_currency': order_data.get('payment', {}).get('currency'),
                'original_shipping_fee': order_data.get('payment', {}).get('original_shipping_fee'),
                'original_total_product_price': order_data.get('payment', {}).get('original_total_product_price'),
                'platform_discount': order_data.get('payment', {}).get('platform_discount'),
                'seller_discount': order_data.get('payment', {}).get('seller_discount'),
                'shipping_fee': order_data.get('payment', {}).get('shipping_fee'),
                'shipping_fee_cofunded_discount': order_data.get('payment', {}).get('shipping_fee_cofunded_discount'),
                'shipping_fee_platform_discount': order_data.get('payment', {}).get('shipping_fee_platform_discount'),
                'shipping_fee_seller_discount': order_data.get('payment', {}).get('shipping_fee_seller_discount'),
                'sub_total': order_data.get('payment', {}).get('sub_total'),
                'tax': order_data.get('payment', {}).get('tax'),
                'total_amount': order_data.get('payment', {}).get('total_amount'),
                'small_order_fee': order_data.get('payment', {}).get('small_order_fee'),
                'retail_delivery_fee': order_data.get('payment', {}).get('retail_delivery_fee'),
                'buyer_service_fee': order_data.get('payment', {}).get('buyer_service_fee'),
                'handling_fee': order_data.get('payment', {}).get('handling_fee'),
                'shipping_insurance_fee': order_data.get('payment', {}).get('shipping_insurance_fee'),
                'item_insurance_fee': order_data.get('payment', {}).get('item_insurance_fee'),
                'shipping_fee_tax': order_data.get('payment', {}).get('shipping_fee_tax', 0),
                'product_tax': order_data.get('payment', {}).get('product_tax', 0),
                # 新增字段
                'seller_note': order_data.get('seller_note'),
                'split_or_combine_tag': order_data.get('split_or_combine_tag'),
                'cpf': order_data.get('cpf'),
            }
            order_basic_data.append(basic)

            # 解析商品数据（line_items）
            for item in order_data.get('line_items', []):
                item_data = {
                    'order_id': order_data.get('id'),
                    'currency': item.get('currency'),
                    'display_status': item.get('display_status'),
                    'line_item_id': item.get('id'),
                    'is_dangerous_good': item.get('is_dangerous_good'),
                    'is_gift': item.get('is_gift'),
                    'tax_amount': item.get('item_tax', [{}])[0].get('tax_amount', 0),
                    'tax_rate': item.get('item_tax', [{}])[0].get('tax_rate', 0),
                    'tax_type': item.get('item_tax', [{}])[0].get('tax_type'),
                    'original_price': item.get('original_price'),
                    'package_id': item.get('package_id'),
                    'package_status': item.get('package_status'),
                    'product_id': item.get('product_id'),
                    'product_name': item.get('product_name'),
                    'rts_time': convert_timestamp_to_us_pacific(item.get('rts_time', 0), us_pacific),
                    'sale_price': item.get('sale_price'),
                    'seller_discount': item.get('seller_discount'),
                    'seller_sku': item.get('seller_sku'),
                    'shipping_provider_id': item.get('shipping_provider_id'),
                    'shipping_provider_name': item.get('shipping_provider_name'),
                    'sku_id': item.get('sku_id'),
                    'sku_image': item.get('sku_image'),
                    'sku_name': item.get('sku_name'),
                    'sku_type': item.get('sku_type'),
                    'tracking_number': item.get('tracking_number'),
                    'combined_listing_skus': json.dumps(item.get('combined_listing_skus', [])),
                    'handling_duration': json.dumps(item.get('handling_duration', {})),
                    'cancel_user': item.get('cancel_user'),
                    'cancel_reason': item.get('cancel_reason'),
                    'small_order_fee': item.get('small_order_fee'),
                    'retail_delivery_fee': item.get('retail_delivery_fee'),
                    'buyer_service_fee': item.get('buyer_service_fee'),
                    'handling_duration_days': item.get('handling_duration_days'),
                }
                order_item_data.append(item_data)

            # 解析包裹数据（packages）
            for package in order_data.get('packages', []):
                package_data = {
                    'order_id': order_data.get('id'),
                    'package_id': package.get('id'),
                    'package_status': package.get('status'),
                    'package_weight': package.get('weight'),
                    'package_dimensions': json.dumps(package.get('dimensions', {})),
                }
                order_package_data.append(package_data)

            # 解析订单行数据（line_items）
            for line in order_data.get('line_items', []):
                line_data = {
                    'order_id': order_data.get('id'),
                    'currency': line.get('currency'),
                    'display_status': line.get('display_status'),
                    'line_item_id': line.get('id'),
                    'product_name': line.get('product_name'),
                    'sku_name': line.get('sku_name'),
                    'sale_price': line.get('sale_price'),
                    'small_order_fee': line.get('small_order_fee'),
                    'retail_delivery_fee': line.get('retail_delivery_fee'),
                    'buyer_service_fee': line.get('buyer_service_fee'),
                }
                order_line_data.append(line_data)

        if data_len < 100:
            break
        else:
            next_page_token = response_json['data'].get('next_page_token')
            params['page_token'] = next_page_token

    return order_basic_data, order_item_data, order_package_data, order_line_data


# @getToken.task_record.record_task("API-获取TikTok订单数据")
def go(time_type="create_time"):
    us_pacific = pytz.timezone('America/Los_Angeles')
    now_us_pacific = datetime.datetime.now(us_pacific)

    thirty_days_ago_start_us = now_us_pacific - datetime.timedelta(days=2)

    start_ts = int(thirty_days_ago_start_us.timestamp())
    end_ts = int(now_us_pacific.timestamp())

    print(
        f"开始执行订单数据获取,获取订单{time_type}：{thirty_days_ago_start_us.strftime('%Y-%m-%d')} ~ {now_us_pacific.strftime('%Y-%m-%d')}...")

    order_basic_data, order_item_data, order_package_data, order_line_data = get_order_list(start_ts, end_ts, time_type)

    if order_basic_data:
        getToken.gosql_v3.api_to_sql(order_basic_data, "api_tiktok_oversea_order_basic")
    if order_item_data:
        getToken.gosql_v3.api_to_sql(order_item_data, "api_tiktok_oversea_order_item")
    if order_package_data:
        getToken.gosql_v3.api_to_sql(order_package_data, "api_tiktok_oversea_order_package")
    if order_line_data:
        getToken.gosql_v3.api_to_sql(order_line_data, "api_tiktok_oversea_order_line")
    getToken.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_sa02a6fe421714a7bbbc8567", required=True))
    getToken.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_m2ee416c6001e4067aa59f57", required=True))
    getToken.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_la6eb94fbf6874cf0a45b404", required=True))
    getToken.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_ed38716f4a4b245e187a98da", required=True))

    print(f"订单数据获取完成.")
    if order_basic_data:
        print(f"开始删除分区表数据,分区表名：api_tiktok_oversea_order_basic_d3...")
        delete_sql = 'DELETE FROM api_tiktok_oversea_order_basic_d3 WHERE busi_date < CURDATE() - INTERVAL 2 DAY;'
        getToken.gosql_v3.execute_sql(delete_sql)
        today = datetime.date.today().strftime("%Y-%m-%d")
        order_basic_data = [{**item, "busi_date": today} for item in order_basic_data]
        getToken.gosql_v3.api_to_sql(order_basic_data, "api_tiktok_oversea_order_basic_d3")
        print(f"删除完成.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--option", type=int, default=1, help="1=create_time，2=update_time")
    args = parser.parse_args()

    if args.option == 1:
        t_type = "create_time"
    else:
        t_type = "update_time"

    go(time_type=t_type)
