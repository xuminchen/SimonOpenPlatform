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


def get_shopify_orders_with_pagination():
    """获取Shopify订单数据（带分页功能）"""
    order_datas = []
    has_next_page = True
    end_cursor = None
    page_count = 0

    while has_next_page:
        page_count += 1
        print(f"正在获取第 {page_count} 页数据...")

        # 构建查询（添加了fulfillmentOrders和商品信息）
        query = """
        query ($cursor: String) {
          orders(first: 100, after: $cursor, query: "created_at:>2025-11-21") {
            edges {
              cursor
              node {
                id
                name
                currentSubtotalLineItemsQuantity
                # currentTotalAdditionalFeesSet
                updatedAt
                customer { id createdAt }
                createdAt
                fulfillmentOrders(first: 10) {
                  edges {
                    node {
                      lineItems(first: 20) {
                        edges {
                          node {
                            productTitle
                            variant {
                              title
                            }
                          }
                        }
                      }
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
                    order_info = order_edge['node']

                    # 处理商品信息 - 平铺到订单数据中
                    product_titles = []
                    variant_titles = []

                    # 安全地处理 fulfillmentOrders 数据
                    fulfillment_orders = order_info.get('fulfillmentOrders', {})
                    if fulfillment_orders and fulfillment_orders.get('edges'):
                        for fo_edge in fulfillment_orders['edges']:
                            fo_node = fo_edge['node']
                            line_items = fo_node.get('lineItems', {})

                            if line_items and line_items.get('edges'):
                                for li_edge in line_items['edges']:
                                    # 关键修复：检查节点是否存在
                                    li_node = li_edge['node']
                                    if li_node is None:
                                        continue

                                    variant_info = li_node.get('variant', {})
                                    product_title = li_node.get("productTitle")
                                    variant_title = variant_info.get("title") if variant_info else None

                                    if product_title:
                                        product_titles.append(product_title)
                                    if variant_title:
                                        variant_titles.append(variant_title)

                    # 拼接成字典格式 - 商品信息平铺在订单数据中
                    order_info_basic = {
                        "productTitles": ", ".join(product_titles) if product_titles else None,
                        "variantTitles": ", ".join(variant_titles) if variant_titles else None,
                        "cancelReason": order_info.get("cancelReason"),
                        "cancelledAt": convert_iso_to_mysql_datetime(order_info.get("cancelledAt")),
                        "closed": order_info.get("closed"),
                        "closedAt": convert_iso_to_mysql_datetime(order_info.get("closedAt")),
                        "createdAt": convert_iso_to_mysql_datetime(order_info.get("createdAt")),
                        "customerId": order_info.get("customer", {}).get("id") if order_info.get("customer") else None,
                        "customerCreatedAt": convert_iso_to_mysql_datetime(
                            order_info.get("customer", {}).get("createdAt")) if order_info.get("customer") else None,
                        "id": order_info.get("id"),
                        "currentSubtotalLineItemsQuantity": order_info.get("currentSubtotalLineItemsQuantity"),
                        "name": order_info.get("name"),
                        "netPayment": order_info.get("netPayment"),
                        "shippingCity": order_info.get("shippingAddress", {}).get("city") if order_info.get(
                            "shippingAddress") else None,
                        "shippingCountry": order_info.get("shippingAddress", {}).get("country") if order_info.get(
                            "shippingAddress") else None,
                        "shippingProvince": order_info.get("shippingAddress", {}).get("province") if order_info.get(
                            "shippingAddress") else None,
                        "shippingZip": order_info.get("shippingAddress", {}).get("zip") if order_info.get(
                            "shippingAddress") else None,
                        "tags": order_info.get("tags", []) if order_info.get("tags", []) else None,
                        "totalPrice": order_info.get("totalPrice"),
                        "presentmentMoneyAmount": order_info.get("totalPriceSet", {}).get("presentmentMoney", {}).get(
                            "amount") if order_info.get("totalPriceSet") else None,
                        "shopMoneyAmount": order_info.get("totalPriceSet", {}).get("shopMoney", {}).get(
                            "amount") if order_info.get("totalPriceSet") else None,
                        "updatedAt": convert_iso_to_mysql_datetime(order_info.get("updatedAt"))
                    }

                    order_datas.append(order_info_basic)

                # 更新分页信息
                has_next_page = page_info.get('hasNextPage', False)
                end_cursor = page_info.get('endCursor')

                # 如果没有下一页，退出循环
                if not has_next_page:
                    print("已获取所有数据-------分页结束")
                    break

            else:
                print("没有获取到订单数据")
                break

        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            break
        except Exception as e:
            print(f"发生未知错误: {e}")
            break

    return order_datas


# 主程序
def main():
    # 获取所有订单数据（带分页）
    print("开始获取Shopify订单数据...")
    order_datas = get_shopify_orders_with_pagination()

    print(f"总共获取到 {len(order_datas)} 条订单数据")

    # 保存到数据库
    if order_datas:
        gosql_v3.api_to_sql(json_data=order_datas, sql_name="shopify_orders_information")
        print("订单数据已保存到数据库")
    else:
        print("没有数据需要保存")

    requests.get(
        get_credentials("bi_refresh_urls", "ds_b834d3eed79c8400aba47c32", required=True))


# 运行主程序
if __name__ == "__main__":
    main()
