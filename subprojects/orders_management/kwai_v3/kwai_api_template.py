# -*- coding: utf-8 -*-
from Tools import app_info, fetch_resp
from kwai_token_manager import KwaiTokenManager
import datetime
import pandas as pd
import os
from openpyxl import load_workbook
import re
import time

class KwaiAPITemplate:
    def __init__(self, interp_type):
        self.interp_type = interp_type
        self.token_manager = KwaiTokenManager()
        self.init_token()

    def init_token(self):
        """初始化token"""
        self.access_token = self.token_manager.get_valid_token(self.interp_type)
        config = app_info[self.interp_type]
        self.app_key = config["app_key"]
        self.sign_secret = config["sign_secret"]



    def get_orders_v2(self, begin_time, end_time, shop_name=None):
        """获取订单列表"""
        method = "open.order.cursor.list"
        params = {
            "orderViewStatus": 1,
            "pageSize": 50,
            "sort": 1,
            "queryType": 1,
            "beginTime": int(datetime.datetime.strptime(begin_time, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000,
            "endTime": int(datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000,
            "cpsType": 0,
            "cursor": ""
        }

        processed_orders = []  # 存储当前批次获取的订单数据

        while True:
            resp = fetch_resp(self.app_key, self.access_token, self.sign_secret, method, params)

            for order in resp["data"]["orderList"]:
                result = {}

                base_info = order.get('orderBaseInfo', {})
                role_info = base_info.get('orderSellerRoleInfo', {})
                result.update({
                    'orderBaseInfo_discountFee': base_info.get('discountFee'),
                    'orderBaseInfo_buyerNick': base_info.get('buyerNick'),
                    'orderBaseInfo_payTime': datetime.datetime.fromtimestamp(base_info.get('payTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('payTime') else None,
                    'orderBaseInfo_channel': base_info.get('channel'),
                    'orderBaseInfo_remark': base_info.get('remark'),
                    'orderBaseInfo_remindShipmentSign': base_info.get('remindShipmentSign'),
                    'orderBaseInfo_oid': base_info.get('oid'),
                    'orderBaseInfo_sellerOpenId': base_info.get('sellerOpenId'),
                    'orderBaseInfo_expressFee': base_info.get('expressFee'),
                    'orderBaseInfo_orderSellerRoleInfo_roleId': role_info.get('roleId'),
                    'orderBaseInfo_orderSellerRoleInfo_roleName': role_info.get('roleName'),
                    'orderBaseInfo_orderSellerRoleInfo_roleType': role_info.get('roleType'),
                    'orderBaseInfo_buyerImage': base_info.get('buyerImage'),
                    'orderBaseInfo_payType': base_info.get('payType'),
                    'orderBaseInfo_multiplePiecesNo': base_info.get('multiplePiecesNo'),
                    'orderBaseInfo_enableSplitDeliveryOrder': base_info.get('enableSplitDeliveryOrder'),
                    'orderBaseInfo_validPromiseShipmentTimeStamp': datetime.datetime.fromtimestamp(
                        base_info.get('validPromiseShipmentTimeStamp') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('validPromiseShipmentTimeStamp') else None,
                    'orderBaseInfo_governmentDiscount': base_info.get('governmentDiscount'),
                    'orderBaseInfo_sellerNick': base_info.get('sellerNick'),
                    'orderBaseInfo_recvTime': datetime.datetime.fromtimestamp(
                        base_info.get('recvTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('recvTime') else None,
                    'orderBaseInfo_buyerOpenId': base_info.get('buyerOpenId'),
                    'orderBaseInfo_cpsType': base_info.get('cpsType'),
                    'orderBaseInfo_promiseTimeStampOfDelivery': base_info.get('promiseTimeStampOfDelivery'),
                    'orderBaseInfo_refundTime': datetime.datetime.fromtimestamp(
                        base_info.get('refundTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('refundTime') else None,
                    'orderBaseInfo_riskCode': base_info.get('riskCode'),
                    'orderBaseInfo_updateTime': datetime.datetime.fromtimestamp(
                        base_info.get('updateTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('updateTime') else None,
                    'orderBaseInfo_theDayOfDeliverGoodsTime': base_info.get('theDayOfDeliverGoodsTime'),
                    'orderBaseInfo_commentStatus': base_info.get('commentStatus'),
                    'orderBaseInfo_sendTime': datetime.datetime.fromtimestamp(
                        base_info.get('sendTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('sendTime') else None,
                    'orderBaseInfo_tradeInPayAfterPromoAmount': base_info.get('tradeInPayAfterPromoAmount'),
                    'orderBaseInfo_preSale': base_info.get('preSale'),
                    'orderBaseInfo_coType': base_info.get('coType'),
                    'orderBaseInfo_createTime': datetime.datetime.fromtimestamp(
                        base_info.get('createTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('createTime') else None,
                    'orderBaseInfo_totalFee': base_info.get('totalFee'),
                    'orderBaseInfo_sellerDelayPromiseTimeStamp': datetime.datetime.fromtimestamp(
                        base_info.get('sellerDelayPromiseTimeStamp') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('sellerDelayPromiseTimeStamp') else None,
                    'orderBaseInfo_payChannel': base_info.get('payChannel'),
                    'orderBaseInfo_remindShipmentTime': datetime.datetime.fromtimestamp(
                        base_info.get('remindShipmentTime') // 1000).strftime(
                        '%Y-%m-%d %H:%M:%S') if base_info.get('remindShipmentTime') else None,
                    'orderBaseInfo_activityType': base_info.get('activityType'),
                    'orderBaseInfo_allowanceExpressFee': base_info.get('allowanceExpressFee'),
                    'orderBaseInfo_priorityDelivery': base_info.get('priorityDelivery'),
                    'orderBaseInfo_payChannelDiscount': base_info.get('payChannelDiscount'),
                    'orderBaseInfo_status': base_info.get('status'),
                })

                address = order.get('orderAddress', {})
                result.update({
                    'orderAddress_districtCode': address.get('districtCode'),
                    'orderAddress_town': address.get('town'),
                    'orderAddress_city': address.get('city'),
                    'orderAddress_townCode': address.get('townCode'),
                    'orderAddress_cityCode': address.get('cityCode'),
                    'orderAddress_provinceCode': address.get('provinceCode'),
                    'orderAddress_encryptedMobile': address.get('encryptedMobile'),
                    'orderAddress_encryptedConsignee': address.get('encryptedConsignee'),
                    'orderAddress_desensitiseConsignee': address.get('desensitiseConsignee'),
                    'orderAddress_encryptedAddress': address.get('encryptedAddress'),
                    'orderAddress_province': address.get('province'),
                    'orderAddress_district': address.get('district'),
                    'orderAddress_desensitiseMobile': address.get('desensitiseMobile'),
                    'orderAddress_desensitiseAddress': address.get('desensitiseAddress')
                })

                item_info = order.get('orderItemInfo', {})
                category_info = item_info.get('itemExtra', {}).get('categoryInfo', {})
                result.update({
                    'orderItemInfo_itemPicUrl': item_info.get('itemPicUrl'),
                    'orderItemInfo_itemType': item_info.get('itemType'),
                    'orderItemInfo_discountFee': item_info.get('discountFee'),
                    'orderItemInfo_originalPrice': item_info.get('originalPrice'),
                    'orderItemInfo_itemTitle': item_info.get('itemTitle'),
                    'orderItemInfo_orderItemId': item_info.get('orderItemId'),
                    'orderItemInfo_num': item_info.get('num'),
                    'orderItemInfo_itemExtra_brandName': item_info.get('itemExtra', {}).get('brandName'),
                    'orderItemInfo_itemExtra_energyLevel': item_info.get('itemExtra', {}).get('energyLevel'),
                    'orderItemInfo_itemExtra_categoryInfo_govCategory': category_info.get('govCategory'),
                    'orderItemInfo_itemExtra_categoryInfo_itemCid': category_info.get('itemCid'),
                    'orderItemInfo_itemExtra_categoryInfo_govCategoryCode': category_info.get('govCategoryCode'),
                    'orderItemInfo_itemExtra_categoryInfo_categoryName': category_info.get('categoryName'),
                    'orderItemInfo_itemExtra_productNo': item_info.get('itemExtra', {}).get('productNo'),
                    'orderItemInfo_warehouseCode': item_info.get('warehouseCode'),
                    'orderItemInfo_itemId': item_info.get('itemId'),
                    'orderItemInfo_relItemId': item_info.get('relItemId'),
                    'orderItemInfo_relSkuId': item_info.get('relSkuId'),
                    'orderItemInfo_price': item_info.get('price'),
                    'orderItemInfo_itemLinkUrl': item_info.get('itemLinkUrl'),
                    'orderItemInfo_skuNick': item_info.get('skuNick'),
                    'orderItemInfo_skuDesc': item_info.get('skuDesc'),
                    'orderItemInfo_goodsCode': item_info.get('goodsCode'),
                    'orderItemInfo_skuId': item_info.get('skuId')
                })

                cps_info = order.get('orderCpsInfo', {})
                result.update({
                    'orderCpsInfo_kwaiMoneyUserId': cps_info.get('kwaiMoneyUserId'),
                    'orderCpsInfo_distributorName': cps_info.get('distributorName'),
                    'orderCpsInfo_activityUserId': cps_info.get('activityUserId'),
                    'orderCpsInfo_distributorId': cps_info.get('distributorId'),
                    'orderCpsInfo_activityUserNickName': cps_info.get('activityUserNickName'),
                    'orderCpsInfo_kwaiMoneyUserNickName': cps_info.get('kwaiMoneyUserNickName')
                })

                processed_orders.append(result)

            if resp["data"]["cursor"] == "nomore":
                break
            else:
                params["cursor"] = resp["data"]["cursor"]

        return processed_orders

    def get_bill_list(self, begin_time, end_time, shop_name=None):
        """查询账单信息"""
        method = "open.funds.financial.settled.bill.detail"
        params = {
            "settlementStartTime": self._convert_time(begin_time),
            "settlementEndTime": self._convert_time(end_time),
            "size": 100,
            "cursor": ""
        }

        bill_data = []

        while True:
            resp = fetch_resp(self.app_key, self.access_token, self.sign_secret, method, params)

            for item in resp["data"]["orders"]:
                bill_item = {
                    "orderNo": item.get("orderNo"),
                    "productId": item.get("productId"),
                    "productName": item.get("productName"),
                    "productNum": item.get("productNum"),
                    "orderCreateTime": self._format_timestamp(item.get("orderCreateTime")),
                    "actualPayAmount": item.get("actualPayAmount"),
                    "distributorId": item.get("distributorId"),
                    "distributorCommissionAmount": item.get("distributorCommissionAmount"),
                    "activityUserId": item.get("activityUserId"),
                    "activityUserCommissionAmount": item.get("activityUserCommissionAmount"),
                    "kzkId": item.get("kzkId"),
                    "serviceUserId": item.get("serviceUserId"),
                    "serviceAmount": item.get("serviceAmount"),
                    "totalOutgoingAmount": item.get("totalOutgoingAmount"),
                    "settlementStatus": item.get("settlementStatus"),
                    "settlementAmount": item.get("settlementAmount"),
                    "settlementTime": self._format_timestamp(item.get("settlementTime")),
                    "accountChannel": item.get("accountChannel"),
                    "accountName": item.get("accountName"),
                    "anchorHongBaoAmount": item.get("anchorHongBaoAmount"),
                    "collectMode": item.get("collectMode"),
                    "czjAmount": item.get("czjAmount"),
                    "governmentSubsidyAmount": item.get("governmentSubsidyAmount"),
                    "hongbaoDetail": item.get("hongbaoDetail"),
                    "huabeiAmount": item.get("huabeiAmount"),
                    "kzkCommissionAmount": item.get("kzkCommissionAmount"),
                    "mcnId": item.get("mcnId"),
                    "merchantId": item.get("merchantId"),
                    "orderRemark": item.get("orderRemark"),
                    "otherAmount": item.get("otherAmount"),
                    "otherAmountDesc": item.get("otherAmountDesc"),
                    "otherAmountDetail": item.get("otherAmountDetail"),
                    "platformAllowanceAmount": item.get("platformAllowanceAmount"),
                    "platformCommissionAmount": item.get("platformCommissionAmount"),
                    "platformPayMarketAllowanceAmount": item.get("platformPayMarketAllowanceAmount"),
                    "presellSettleAmount": item.get("presellSettleAmount"),
                    "serviceCommissionRole": item.get("serviceCommissionRole"),
                    "settlementRule": item.get("settlementRule"),
                    "totalIncome": item.get("totalIncome"),
                    "totalRefundAmount": item.get("totalRefundAmount"),
                    "shop_name": shop_name,
                }
                bill_data.append(bill_item)

            if resp["data"]["cursor"] == "no_more":
                break
            else:
                time.sleep(2)
                params["cursor"] = resp["data"]["cursor"]

        return bill_data


    def get_order_refund(self, begin_time, end_time, shop_name=None):
        """获取售后订单列表"""
        method = "open.seller.order.refund.pcursor.list"

        # 状态映射表
        handling_way_map = {
            "1": "退货退款",
            "10": "仅退款",
            "3": "换货"
        }
        negotiate_status_map = {
            "0": "未知状态",
            "1": "待商家处理",
            "2": "商家同意",
            "3": "商家驳回，等待买家修改"
        }
        status_map = {
            "10": "买家仅退款申请",
            "11": "买家退货退款申请",
            "20": "平台介入-买家仅退款申请",
            "21": "平台介入-买家退货退款申请",
            "22": "平台介入-已确认退货退款",
            "30": "商品回寄信息待买家更新",
            "40": "商品回寄信息待卖家确认",
            "45": "待买家确认收货",
            "50": "退款执行中",
            "60": "退款成功",
            "70": "退款失败"
        }
        refund_type_map = {
            "0": "未知",
            "1": "买家申请退款",
            "2": "卖家主动退款"
        }
        receipt_status_map = {
            "0": "未知",
            "1": "未收到货",
            "2": "已收到货"
        }

        params = {
            "beginTime": self._convert_time(begin_time),
            "endTime": self._convert_time(end_time),
            "type": 9,
            "cursor": 50,
            "currentPage": 1,
            "pcursor": "",
        }

        refund_data = []

        while True:
            resp = fetch_resp(self.app_key, self.access_token, self.sign_secret, method, params)

            for i in resp["data"]["refundOrderInfoList"]:
                i["handlingWay"] = handling_way_map.get(str(i["handlingWay"]), "")
                i["negotiateStatus"] = negotiate_status_map.get(str(i["negotiateStatus"]), "")
                i["status"] = status_map.get(str(i["status"]), "")
                i["refundType"] = refund_type_map.get(str(i["refundType"]), "")
                i["receiptStatus"] = receipt_status_map.get(str(i["receiptStatus"]), "")
                i["refundFee"] = i["refundFee"]
                i["submitTime"] = datetime.datetime.fromtimestamp(i["submitTime"] // 1000).strftime('%Y-%m-%d %H:%M:%S')
                i["updateTime"] = datetime.datetime.fromtimestamp(i["updateTime"] // 1000).strftime('%Y-%m-%d %H:%M:%S')
                i["createTime"] = datetime.datetime.fromtimestamp(i["createTime"] // 1000).strftime('%Y-%m-%d %H:%M:%S')
                i["endTime"] = datetime.datetime.fromtimestamp(i["endTime"] // 1000).strftime('%Y-%m-%d %H:%M:%S')
                i["expireTime"] = datetime.datetime.fromtimestamp(i["expireTime"] // 1000).strftime('%Y-%m-%d %H:%M:%S')
                i["negotiateUpdateTime"] = datetime.datetime.fromtimestamp(i["negotiateUpdateTime"] // 1000).strftime(
                    '%Y-%m-%d %H:%M:%S')
                i["shop_name"] = shop_name
                refund_data.append(i)
                # print(i)

            if resp["data"]["pcursor"] == "nomore":
                break
            else:
                params["pcursor"] = resp["data"]["pcursor"]

        return refund_data


    def _convert_time(self, time_str):
        """转换时间格式"""
        return int(datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000

    def _format_timestamp(self, timestamp):
        """格式化时间戳"""
        if timestamp:
            return datetime.datetime.fromtimestamp(int(timestamp) // 1000).strftime('%Y-%m-%d %H:%M:%S')
        return None


# 使用示例
if __name__ == '__main__':
    # 初始化国内快手API
    kwai_api = KwaiAPITemplate("快手国内")

    # 调用订单接口
    kwai_api.get_orders_v2("2025-03-01 00:00:00", "2025-03-02 00:00:00")