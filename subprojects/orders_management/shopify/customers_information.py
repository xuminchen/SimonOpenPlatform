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


def get_shopify_customers_with_pagination():
    """获取Shopify客户数据（带分页功能）"""
    customer_datas = []
    appstle_subscription_datas = []
    has_next_page = True
    end_cursor = None
    page_count = 0

    while has_next_page:
        page_count += 1
        print(f"正在获取第 {page_count} 页客户数据...")

        # 构建查询（使用同一个查询，通过变量控制分页）
        query = """
        query ($cursor: String) {
          customers(first: 200, after: $cursor, query: "created_at:>2026-01-21T00:00:00Z") {
            edges {
              cursor
              node {
                id
                numberOfOrders
                amountSpent {
                  amount
                  currencyCode
                }
                createdAt
                updatedAt
                verifiedEmail
                validEmailAddress
                tags
                lifetimeDuration
                defaultAddress {
                  formattedArea
                  city
                  province
                  country
                }
                addresses {
                  city
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
            if data.get('data') and data['data'].get('customers'):
                customers_data = data['data']['customers']
                customers = customers_data['edges']
                page_info = customers_data['pageInfo']

                print(f"第 {page_count} 页获取到 {len(customers)} 条客户数据")

                for customer_edge in customers:
                    customer_info = customer_edge['node']

                    # 拼接成字典格式
                    customer_info_basic = {
                        "id": customer_info.get("id"),
                        "numberOfOrders": customer_info.get("numberOfOrders"),
                        "amountSpentAmount": customer_info.get("amountSpent", {}).get("amount") if customer_info.get(
                            "amountSpent") else None,
                        "amountSpentCurrencyCode": customer_info.get("amountSpent", {}).get(
                            "currencyCode") if customer_info.get("amountSpent") else None,
                        "createdAt": convert_iso_to_mysql_datetime(customer_info.get("createdAt")),
                        "updatedAt": convert_iso_to_mysql_datetime(customer_info.get("updatedAt")),
                        "verifiedEmail": customer_info.get("verifiedEmail"),
                        "validEmailAddress": customer_info.get("validEmailAddress"),
                        "tags": customer_info.get("tags", []) if customer_info.get("tags", []) else None,
                        "lifetimeDuration": customer_info.get("lifetimeDuration"),
                        "defaultAddressFormattedArea": customer_info.get("defaultAddress", {}).get(
                            "formattedArea") if customer_info.get("defaultAddress") else None,
                        "defaultAddressCity": customer_info.get("defaultAddress", {}).get("city") if customer_info.get(
                            "defaultAddress") else None,
                        "defaultAddressProvince": customer_info.get("defaultAddress", {}).get(
                            "province") if customer_info.get("defaultAddress") else None,
                        "defaultAddressCountry": customer_info.get("defaultAddress", {}).get(
                            "country") if customer_info.get("defaultAddress") else None,
                    }

                    customer_datas.append(customer_info_basic)

                    if customer_info.get("tags", []):
                        appstle_tags = [tag for tag in customer_info.get("tags", []) if 'appstle_subscription' in tag]

                        if appstle_tags:
                            appstle_date = {
                                "id": customer_info.get("id"),
                                "tag": appstle_tags[0]  # 只保存包含 appstle_subscription 的标签
                            }
                            appstle_subscription_datas.append(appstle_date)


                # 更新分页信息
                has_next_page = page_info.get('hasNextPage', False)
                end_cursor = page_info.get('endCursor')

                # 如果没有下一页，退出循环
                if not has_next_page:
                    print("已获取所有客户数据，分页结束")
                    break

            else:
                print("没有获取到客户数据")
                break

        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            break

        except Exception as e:
            print(f"发生未知错误: {e}")
            break

    # 返回两个数据列表
    return customer_datas, appstle_subscription_datas


# 主程序
def main():
    # 获取所有客户数据（带分页）
    print("开始获取Shopify客户数据...")
    customer_datas, appstle_subscription_datas = get_shopify_customers_with_pagination()

    print(f"总共获取到 {len(customer_datas)} 条客户数据")
    print(f"总共获取到 {len(appstle_subscription_datas)} 条appstle订阅数据")

    # 保存到数据库
    if customer_datas:
        gosql_v3.api_to_sql(json_data=customer_datas, sql_name="shopify_customers_information")
    else:
        print("没有客户数据需要保存")

    if appstle_subscription_datas:
        gosql_v3.api_to_sql(json_data=appstle_subscription_datas, sql_name="shopify_appstle_subscription")
    else:
        print("没有appstle订阅数据需要保存")

    requests.get(
        get_credentials("bi_refresh_urls", "ds_pb0dbad6344c345018e5274c", required=True))
    requests.get(
        get_credentials("bi_refresh_urls", "ds_l9e0051a04aa94a3b8c69f40", required=True))


# 运行主程序
if __name__ == "__main__":
    main()
