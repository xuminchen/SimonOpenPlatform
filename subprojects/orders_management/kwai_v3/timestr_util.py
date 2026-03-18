# -*- coding: utf-8 -*-
import datetime
import time
import gosql_v2

def get_many_days(api_interface_parameter, days=None, start_date_str=None, end_date_str=None, shop_name=None):
    """
    获取多天数据并自动保存到对应的数据库表
    Args:
        api_interface_parameter: API接口方法
        days: 要获取的天数（从今天往前推）
        start_date_str: 开始日期字符串 (格式: 'YYYY-MM-DD')
        end_date_str: 结束日期字符串 (格式: 'YYYY-MM-DD')
        shop_name: 店铺名称（可选）
    """
    if days:
        start_time = datetime.datetime.today() - datetime.timedelta(days=days)
        end_time = datetime.datetime.today() - datetime.timedelta(days=1)
    elif start_date_str and end_date_str:
        start_time = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
        end_time = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
    else:
        print("日期不完整，请重新输入")
        return 0

    # 根据API方法名确定目标数据库表
    api_method_name = api_interface_parameter.__name__
    if api_method_name == "get_orders_v2":
        db_table = "kwai_api_order_lists"
    elif api_method_name == "get_order_refund":
        db_table = "kwai_api_order_refund"
    elif api_method_name == "get_bill_list":
        db_table = "kwai_api_bill_details"
    else:
        print(f"未知的API方法: {api_method_name}")
        return 0

    total_rows = 0  # 初始化总数据量计数器

    while start_time <= end_time:
        start_date = start_time.strftime("%Y-%m-%d 00:00:00")
        end_date = start_time.strftime("%Y-%m-%d 23:59:59")

        try:
            # 调用API获取数据
            data = api_interface_parameter(start_date, end_date, shop_name)
            current_day_count = len(data)
            print(f"获取 {start_time.date()} 数据成功，数据长度: {current_day_count}")
            total_rows += current_day_count

            # 保存数据到数据库
            if data:
                if db_table == "kwai_api_order_lists":
                    gosql_v2.api_to_sql(data, db_table)
                elif db_table == "kwai_api_order_refund":
                    gosql_v2.api_to_sql(data, db_table, need_filed=[
                        'refund_fee', 'logistics_id', 'refund_reason_desc', 'oid',
                        'refund_type', 'refund_desc', 'rel_item_id', 'seller_id',
                        'rel_sku_id', 'negotiate_status', 'negotiate_update_time',
                        'sku_id', 'refund_reason', 'update_time', 'handling_way',
                        'item_id', 'expire_time', 'submit_time', 'create_time',
                        'sku_nick', 'end_time', 'refund_id', 'receipt_status',
                        'shop_name', 'status'
                    ])
                elif db_table == "kwai_api_bill_details":
                    gosql_v2.api_to_sql(data, db_table)

            time.sleep(2)

        except Exception as e:
            print(f"处理 {start_time.date()} 数据时出错: {str(e)}")

        start_time += datetime.timedelta(days=1)


    # print(f"\n所有日期数据获取完成，总数据长度: {total_rows}")
    return 1


if __name__ == '__main__':
    pass