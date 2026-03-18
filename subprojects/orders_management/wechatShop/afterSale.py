import datetime
import json
import gosql_v3
import Tools
from subprojects._shared.core.api_credentials import get_credentials


def load_wechat_shop_map():
    shop_map = get_credentials("wechat_shop", "shops", default={})
    if not shop_map:
        raise ValueError("Missing wechat_shop.shops in config/api_credentials.json")
    return shop_map


def get_after_sale_detail(access_token, after_sale_order_id, shop_name):
    url = "https://api.weixin.qq.com/channels/ec/aftersale/getaftersaleorder"
    params = {
        "access_token": access_token
    }
    data = {
        "after_sale_order_id": after_sale_order_id,
    }
    result = Tools.send_request(url=url, params=params, data=data, method="post")
    if not result:
        print("请求失败: empty response")
        return None

    # 检查是否有错误
    if result.get("errcode") != 0:
        print(f"请求失败: {result.get('errmsg')}")
        return None

    # 提取售后订单信息
    after_sale_order = result.get("after_sale_order", {})
    product_info = after_sale_order.get("product_info", {})
    details = after_sale_order.get("details", {})
    refund_info = after_sale_order.get("refund_info", {})
    return_info = after_sale_order.get("return_info", {})
    merchant_upload_info = after_sale_order.get("merchant_upload_info", {})
    refund_resp = after_sale_order.get("refund_resp", {})

    # 状态映射
    status_mapping = {
        "USER_CANCELD": "用户取消申请",
        "MERCHANT_PROCESSING": "商家受理中",
        "MERCHANT_REJECT_REFUND": "商家拒绝退款",
        "MERCHANT_REJECT_RETURN": "商家拒绝退货退款",
        "USER_WAIT_RETURN": "待买家退货",
        "RETURN_CLOSED": "退货退款关闭",
        "MERCHANT_WAIT_RECEIPT": "待商家收货",
        "MERCHANT_OVERDUE_REFUND": "商家逾期未退款",
        "MERCHANT_REFUND_SUCCESS": "退款完成",
        "MERCHANT_RETURN_SUCCESS": "退货退款完成",
        "PLATFORM_REFUNDING": "平台退款中",
        "PLATFORM_REFUND_FAIL": "平台退款失败",
        "USER_WAIT_CONFIRM": "待用户确认",
        "MERCHANT_REFUND_RETRY_FAIL": "商家打款失败，客服关闭售后",
        "MERCHANT_FAIL": "售后关闭",
        "USER_WAIT_CONFIRM_UPDATE": "待用户处理商家协商",
        "USER_WAIT_HANDLE_MERCHANT_AFTER_SALE": "待用户处理商家代发起的售后申请",
        "WAIT_PACKAGE_INTERCEPT": "物流线上拦截中",
        "MERCHANT_REJECT_EXCHANGE": "商家拒绝换货",
        "MERCHANT_REJECT_RESHIP": "商家拒绝发货",
        "USER_WAIT_RECEIPT": "待用户收货",
        "MERCHANT_EXCHANGE_SUCCESS": "换货完成"
    }

    # 退款原因映射
    refund_reason_mapping = {
        1: "商家通过店铺管理页或者小助手发起退款",
        2: "退货退款场景，商家同意买家未上传物流单号情况下确认收货并退款，该场景限于订单无运费险",
        3: "商家通过后台api发起退款",
        4: "未发货售后平台自动同意",
        5: "平台介入纠纷退款",
        6: "特殊场景下平台强制退款",
        7: "退货退款场景，买家同意没有上传物流单号情况下，商家确认收货并退款，该场景限于订单包含运费险，并无法理赔",
        8: "商家发货超时，平台退款",
        9: "商家处理买家售后申请超时，平台自动同意退款",
        10: "用户确认收货超时，平台退款",
        11: "商家确认收货超时，平台退款"
    }

    # 售后原因映射
    reason_mapping = {
        "INCORRECT_SELECTION": "拍错/多拍",
        "NO_LONGER_WANT": "不想要了",
        "NO_EXPRESS_INFO": "无快递信息",
        "EMPTY_PACKAGE": "包裹为空",
        "REJECT_RECEIVE_PACKAGE": "已拒签包裹",
        "NOT_DELIVERED_TOO_LONG": "快递长时间未送达",
        "NOT_MATCH_PRODUCT_DESC": "与商品描述不符",
        "QUALITY_ISSUE": "质量问题",
        "SEND_WRONG_GOODS": "卖家发错货",
        "THREE_NO_PRODUCT": "三无产品",
        "FAKE_PRODUCT": "假冒产品",
        "NO_REASON_7_DAYS": "七天无理由",
        "INITIATE_BY_PLATFORM": "平台代发起",
        "OTHERS": "其它"
    }

    # 时间戳转换函数
    def timestamp_to_datetime(timestamp):
        if timestamp:
            return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return None

    # 构建包含所有字段的扁平化字典
    parsed_data = {
        # 售后订单基本信息
        "after_sale_order_id": after_sale_order.get("after_sale_order_id"),
        "status": status_mapping.get(after_sale_order.get("status"), after_sale_order.get("status")),
        "openid": after_sale_order.get("openid"),
        "order_id": after_sale_order.get("order_id"),
        "create_time": timestamp_to_datetime(after_sale_order.get("create_time")),
        "update_time": timestamp_to_datetime(after_sale_order.get("update_time")),
        "reason": reason_mapping.get(after_sale_order.get("reason"), after_sale_order.get("reason_text")),
        "type": after_sale_order.get("type"),
        "unionid": after_sale_order.get("unionid"),
        "complaint_id": after_sale_order.get("complaint_id"),

        # 商品信息
        "product_id": product_info.get("product_id"),
        "sku_id": product_info.get("sku_id"),
        "count": product_info.get("count"),
        "fast_refund": product_info.get("fast_refund"),
        "voucher_list": product_info.get("voucher_list", []),
        "gift_product_list": product_info.get("gift_product_list", []),

        # 详情信息
        "desc": details.get("desc"),
        "receive_product": details.get("receive_product"),
        "prove_imgs": details.get("prove_imgs", []),
        "tel_number": details.get("tel_number"),
        "media_id_list": details.get("media_id_list", []),

        # 退款信息
        "amount": refund_info.get("amount", 0) / 100,  # 从分转换为元
        "refund_reason": refund_reason_mapping.get(refund_info.get("refund_reason"), refund_info.get("refund_reason")),

        # 退货信息
        "waybill_id": return_info.get("waybill_id"),
        "delivery_id": return_info.get("delivery_id"),
        "delivery_name": return_info.get("delivery_name"),

        # 商家上传信息
        "reject_reason": merchant_upload_info.get("reject_reason"),
        "refund_certificates": merchant_upload_info.get("refund_certificates", []),

        # 退款响应信息
        "code": refund_resp.get("code"),
        "ret": refund_resp.get("ret"),
        "message": refund_resp.get("message"),
        "shop_name": shop_name,
    }
    print(parsed_data)
    return parsed_data


def get_after_sale_order_list(access_token, time_type="create_time", start_date=None, end_date=None, time_range_days=7):
    # 确定开始和结束时间
    if start_date and end_date:
        start_time = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0,
                                                                                microsecond=0)
        end_time = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59,
                                                                            microsecond=0)
    else:
        end_time = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=59)
        start_time = (datetime.datetime.now() - datetime.timedelta(days=time_range_days)).replace(hour=0, minute=0,
                                                                                                  second=0)

    all_orders = []

    while start_time <= end_time:
        # 设置每天的开始和结束时间（00:00:00到23:59:59）
        start_time_dt = int(start_time.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        end_time_dt = int(start_time.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())

        # 分页获取数据
        has_more = True
        next_key = None

        while has_more:
            url = "https://api.weixin.qq.com/channels/ec/aftersale/getaftersalelist"
            params = {
                "access_token": access_token
            }
            data = {
                f"begin_{time_type}": start_time_dt,
                f"end_{time_type}": end_time_dt,
            }

            # 如果有next_key，添加到请求参数中
            if next_key:
                data["next_key"] = next_key

            result = Tools.send_request(url=url, params=params, data=data, method="post")
            if not result:
                print(f"获取售后单({time_type}) {start_time.strftime('%Y-%m-%d')} 的数据失败: empty response")
                break

            # 检查是否有错误
            if result.get("errcode") == 0:
                orders = result.get("after_sale_order_id_list", [])
                all_orders.extend(orders)
                print(f"获取售后单({time_type}){start_time.strftime('%Y-%m-%d')} 的数据: {len(orders)} 条")

                # 检查是否还有更多数据
                has_more = result.get("has_more", False)
                next_key = result.get("next_key")
            else:
                print(f"获取售后单({time_type}) {start_time.strftime('%Y-%m-%d')} 的数据失败: {result.get('errmsg')}")
                break

        # 移动到下一天
        start_time = start_time + datetime.timedelta(days=1)

    print(f"总共获取到 {len(all_orders)} 条售后订单")
    return all_orders


def run(time_range_days=1):
    for shop_name, app_info in load_wechat_shop_map().items():
        app_id = app_info.get('app_id')
        secret = app_info.get('secret')
        access_token = Tools.get_access_token(app_id=app_id, secret=secret)
        for time_type in ['create_time', 'update_time']:
            orders = get_after_sale_order_list(access_token=access_token, time_type=time_type, time_range_days=time_range_days)
            print(f" {shop_name} 的售后单({time_type})数据量为: {len(orders)} 条")
            after_sale_data = []
            for after_sale_order in orders:
                after_sale_data.append(
                    get_after_sale_detail(
                        access_token=access_token,
                        after_sale_order_id=after_sale_order,
                        shop_name=shop_name,
                    )
                )
            gosql_v3.api_to_sql(after_sale_data, 'api_wechat_shop_after_sale')

    Tools.refresh_data_source(
        get_credentials("bi_refresh_urls", "ds_gdd9536498304454ea52870f", required=True)
    )


if __name__ == "__main__":
    run(time_range_days=1)
