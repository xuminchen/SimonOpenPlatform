import requests
import json
import gosql_v3
from datetime import datetime
import re
import os
from subprojects._shared.core.api_credentials import get_credentials

SHOPIFY_CONFIG = get_credentials("orders_management", "shopify", default={})
SHOP_DOMAIN = os.environ.get("SHOPIFY_SHOP_DOMAIN", SHOPIFY_CONFIG.get("shop_domain", ""))
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", SHOPIFY_CONFIG.get("access_token", ""))
if not SHOP_DOMAIN or not ACCESS_TOKEN:
    raise ValueError("Missing orders_management.shopify.shop_domain/access_token in config/api_credentials.json")

url = "https://{0}/admin/api/2025-07/graphql.json".format(SHOP_DOMAIN)
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}


def convert_iso_to_mysql_datetime(iso_string):
    """将ISO 8601格式转换为MySQL datetime格式（更健壮的版本）"""
    if not iso_string:
        return None

    try:
        # 处理Z后缀
        if iso_string.endswith('Z'):
            iso_string = iso_string[:-1]  # 移除Z

        # 处理带时区的情况
        if '+' in iso_string:
            # 移除时区部分，只保留日期时间
            iso_string = iso_string.split('+')[0]
        elif '-' in iso_string[10:]:  # 检查是否有负时区
            # 格式：2025-11-02T12:53:00-05:00
            iso_string = iso_string.split('-')[0]  # 取第一个部分

        # 移除毫秒部分（如果有）
        if '.' in iso_string:
            iso_string = iso_string.split('.')[0]

        # 解析日期时间
        # 格式应该是：YYYY-MM-DDTHH:MM:SS
        parts = re.split(r'[-T:]', iso_string)

        if len(parts) >= 6:
            year, month, day, hour, minute, second = parts[:6]

            # 转换为整数
            year = int(year)
            month = int(month)
            day = int(day)
            hour = int(hour)
            minute = int(minute)
            second = int(second)

            # 创建datetime对象
            dt = datetime(year, month, day, hour, minute, second)

            # 转换为MySQL格式
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            print(f"时间格式不符合预期: {iso_string}")
            return iso_string  # 返回原始字符串

    except Exception as e:
        print(f"时间转换失败: {e}, 原始字符串: {iso_string}")
        # 尝试使用简单的字符串替换
        try:
            result = iso_string.replace('T', ' ').replace('Z', '')
            if len(result) >= 19:
                return result[:19]
            return result
        except:
            return iso_string


def safe_get(data, keys, default=None):
    """安全地获取嵌套字典的值"""
    try:
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, list) and isinstance(key, int) and key < len(data):
                data = data[key]
            else:
                return default
            if data is None:
                return default
        return data
    except (KeyError, IndexError, TypeError):
        return default


def get_shopify_orders_with_pagination():
    """获取Shopify订单数据（带分页功能），按SKU拆分行"""
    order_datas = []
    has_next_page = True
    end_cursor = None
    page_count = 0

    while has_next_page:
        page_count += 1
        print(f"正在获取第 {page_count} 页数据...")

        # 构建查询 - 添加了lineItems相关字段
        query = """
        query ($cursor: String) {
          orders(first: 100, after: $cursor, query: "created_at:>2025-03-24") {
            edges {
              cursor
              node {
                id
                name
                currentSubtotalLineItemsQuantity
                updatedAt
                customer { 
                  id 
                  createdAt 
                }
                createdAt
                lineItems(first: 50) {
                  edges {
                    node {
                      id
                      duties {
                        id
                      }
                      lineItemGroup {
                        id
                        productId
                        title
                        variantId
                        variantSku
                      }
                      name
                      product {
                        variants(first: 10) {
                          nodes {
                            barcode
                            displayName
                            id
                            sku
                            title
                          }
                        }
                      }
                      sku
                      variant {
                        barcode
                        displayName
                        id
                        sku
                        title
                      }
                      variantTitle
                    }
                  }
                }
                tags
                netPayment
                totalPrice
                closed
                closedAt
                cancelReason
                cancelledAt
                totalPriceSet{
                  presentmentMoney {amount}
                  shopMoney {amount}
                }
                shippingAddress {
                  city
                  province
                  zip
                  country
                }
              }
            }
            pageInfo {
              hasNextPage
              hasPreviousPage
              startCursor
              endCursor
            }
          }
        }
        """

        # 构建变量
        variables = {}
        if end_cursor:
            variables["cursor"] = end_cursor

        payload = {
            "query": query,
            "variables": variables
        }

        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            response.raise_for_status()
            data = response.json()

            # 检查响应中是否有数据
            if data.get('data') and data['data'].get('orders'):
                orders_data = data['data']['orders']
                orders = orders_data['edges']
                page_info = orders_data['pageInfo']

                print(f"第 {page_count} 页获取到 {len(orders)} 条订单数据")

                for order_edge in orders:
                    order_info = safe_get(order_edge, ['node'])
                    if order_info.get('id') == 'gid://shopify/Order/6938663027058':
                        continue
                    if not order_info:
                        continue


                    # 获取订单基本信息
                    order_id = safe_get(order_info, ['id'])
                    order_name = safe_get(order_info, ['name'])

                    # 处理lineItems - 每个lineItem生成一行数据
                    line_items_edges = safe_get(order_info, ['lineItems', 'edges'], [])

                    if line_items_edges:
                        for line_item_edge in line_items_edges:
                            line_item = safe_get(line_item_edge, ['node'])
                            if not line_item:
                                continue

                            # 处理duties
                            duties = safe_get(line_item, ['duties'], [])
                            duty_ids = []
                            for duty in duties:
                                duty_id = safe_get(duty, ['id'])
                                if duty_id:
                                    duty_ids.append(duty_id)

                            # 处理lineItemGroup
                            line_item_group = safe_get(line_item, ['lineItemGroup'], {})

                            # 处理product variants
                            product_variants = []
                            variants_nodes = safe_get(line_item, ['product', 'variants', 'nodes'], [])
                            for variant in variants_nodes:
                                if variant:
                                    product_variants.append({
                                        'barcode': safe_get(variant, ['barcode']),
                                        'displayName': safe_get(variant, ['displayName']),
                                        'id': safe_get(variant, ['id']),
                                        'sku': safe_get(variant, ['sku']),
                                        'title': safe_get(variant, ['title'])
                                    })

                            # 处理variant信息
                            variant_data = safe_get(line_item, ['variant'], {})

                            # 构建每行数据（按SKU拆分）
                            order_line_data = {
                                # 订单基本信息
                                "orderId": order_id,
                                "orderName": order_name,
                                "currentSubtotalLineItemsQuantity": safe_get(order_info,
                                                                             ["currentSubtotalLineItemsQuantity"]),
                                "updatedAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["updatedAt"])),
                                "customerId": safe_get(order_info, ["customer", "id"]),
                                "customerCreatedAt": convert_iso_to_mysql_datetime(
                                    safe_get(order_info, ["customer", "createdAt"])),
                                "createdAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["createdAt"])),
                                "tags": ", ".join(safe_get(order_info, ["tags"], [])),
                                "netPayment": safe_get(order_info, ["netPayment"]),
                                "totalPrice": safe_get(order_info, ["totalPrice"]),
                                "presentmentMoneyAmount": safe_get(order_info,
                                                                   ["totalPriceSet", "presentmentMoney", "amount"]),
                                "shopMoneyAmount": safe_get(order_info, ["totalPriceSet", "shopMoney", "amount"]),
                                "cancelReason": safe_get(order_info, ["cancelReason"]),
                                "cancelledAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["cancelledAt"])),
                                "closed": safe_get(order_info, ["closed"]),
                                "closedAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["closedAt"])),
                                "shippingCity": safe_get(order_info, ["shippingAddress", "city"]),
                                "shippingCountry": safe_get(order_info, ["shippingAddress", "country"]),
                                "shippingProvince": safe_get(order_info, ["shippingAddress", "province"]),
                                "shippingZip": safe_get(order_info, ["shippingAddress", "zip"]),

                                # LineItem特定信息
                                "lineItemId": safe_get(line_item, ["id"]),
                                "lineItemGroupId": safe_get(line_item_group, ["id"]),
                                "lineItemGroupProductId": safe_get(line_item_group, ["productId"]),
                                "lineItemGroupTitle": safe_get(line_item_group, ["title"]),
                                "lineItemGroupVariantId": safe_get(line_item_group, ["variantId"]),
                                "lineItemGroupVariantSku": safe_get(line_item_group, ["variantSku"]),
                                "lineItemName": safe_get(line_item, ["name"]),
                                "lineItemSku": safe_get(line_item, ["sku"]),
                                "lineItemVariantTitle": safe_get(line_item, ["variantTitle"]),

                                # Variant信息
                                "variantBarcode": safe_get(variant_data, ["barcode"]),
                                "variantDisplayName": safe_get(variant_data, ["displayName"]),
                                "variantId": safe_get(variant_data, ["id"]),
                                "variantSku": safe_get(variant_data, ["sku"]),
                                "variantTitle": safe_get(variant_data, ["title"])
                            }

                            order_datas.append(order_line_data)
                    else:
                        # 如果没有lineItems，仍然创建一行订单数据
                        order_line_data = {
                            "orderId": order_id,
                            "orderName": order_name,
                            "currentSubtotalLineItemsQuantity": safe_get(order_info,
                                                                         ["currentSubtotalLineItemsQuantity"]),
                            "updatedAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["updatedAt"])),
                            "customerId": safe_get(order_info, ["customer", "id"]),
                            "customerCreatedAt": convert_iso_to_mysql_datetime(
                                safe_get(order_info, ["customer", "createdAt"])),
                            "createdAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["createdAt"])),
                            "tags": ", ".join(safe_get(order_info, ["tags"], [])),
                            "netPayment": safe_get(order_info, ["netPayment"]),
                            "totalPrice": safe_get(order_info, ["totalPrice"]),
                            "presentmentMoneyAmount": safe_get(order_info,
                                                               ["totalPriceSet", "presentmentMoney", "amount"]),
                            "shopMoneyAmount": safe_get(order_info, ["totalPriceSet", "shopMoney", "amount"]),
                            "cancelReason": safe_get(order_info, ["cancelReason"]),
                            "cancelledAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["cancelledAt"])),
                            "closed": safe_get(order_info, ["closed"]),
                            "closedAt": convert_iso_to_mysql_datetime(safe_get(order_info, ["closedAt"])),
                            "shippingCity": safe_get(order_info, ["shippingAddress", "city"]),
                            "shippingCountry": safe_get(order_info, ["shippingAddress", "country"]),
                            "shippingProvince": safe_get(order_info, ["shippingAddress", "province"]),
                            "shippingZip": safe_get(order_info, ["shippingAddress", "zip"]),
                            "lineItemId": None,
                            "lineItemGroupId": None,
                            "lineItemGroupProductId": None,
                            "lineItemGroupTitle": None,
                            "lineItemGroupVariantId": None,
                            "lineItemGroupVariantSku": None,
                            "lineItemName": None,
                            "lineItemSku": None,
                            "lineItemVariantTitle": None,
                            "variantBarcode": None,
                            "variantDisplayName": None,
                            "variantId": None,
                            "variantSku": None,
                            "variantTitle": None
                        }
                        order_datas.append(order_line_data)

                # 更新分页信息
                has_next_page = safe_get(page_info, ['hasNextPage'], False)
                end_cursor = safe_get(page_info, ['endCursor'])

                # 如果没有下一页，退出循环
                if not has_next_page:
                    print("已获取所有数据-------分页结束")
                    break

            else:
                print("没有获取到订单数据")
                break

        except requests.exceptionsceptions.RequestException as e:
            print(f"请求失败: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            print(f"响应内容: {response.text[:500]}")  # 打印前500个字符用于调试
            break
        except Exception as e:
            print(f"发生未知错误: {e}")
            import traceback
            print(f"详细错误信息: {traceback.format_exc()}")
            break

    return order_datas


# 主程序
def main():
    # 获取所有订单数据（带分页）
    print("开始获取Shopify订单数据...")
    order_datas = get_shopify_orders_with_pagination()

    print(f"总共获取到 {len(order_datas)} 行订单数据（按SKU拆分）")

    # 保存到数据库
    if order_datas:
        gosql_v3.api_to_sql(json_data=order_datas, sql_name="shopify_order_skus")
        print("订单数据已保存到数据库")

        # 刷新数据源
        try:
            requests.get(
                get_credentials("bi_refresh_urls", "ds_u402c65e0ef01494383c6cee", required=True))
            print("数据源已刷新")
        except Exception as e:
            print(f"刷新数据源失败: {e}")
    else:
        print("没有数据需要保存")


# 运行主程序
if __name__ == "__main__":
    main()
