from datetime import datetime, timedelta
import pandas as pd
import base64
import requests
import hashlib
import json


def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_pre_n_mins_time(n_min):
    return (datetime.now() - timedelta(minutes=n_min)).strftime("%Y-%m-%d %H:%M:%S")


def get_pre_n_datetime(n_day):
    return (datetime.now() - timedelta(days=n_day)).strftime("%Y-%m-%d %H:%M:%S")


def get_current_date():
    return datetime.today().strftime("%Y-%m-%d")


def get_yesterday_date():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_pre_n_date(n_days=1, calc_date=None):
    if calc_date:
        return (datetime.strptime(calc_date, "%Y-%m-%d") - timedelta(days=n_days)).strftime("%Y-%m-%d")
    else:
        return (datetime.now() - timedelta(days=n_days)).strftime("%Y-%m-%d")


def column_number_to_excel_column(column_number):
    import string
    # 初始化列名
    column_name = ''
    # 循环减去1，是因为我们要从A开始，而Python中的索引是从0开始的
    while column_number > 0:
        # 取余得到当前字母的索引
        letter_index = (column_number - 1) % 26
        # 将当前字母加入列名
        column_name = string.ascii_uppercase[letter_index] + column_name
        # 除以26，得到下一个字母的索引
        column_number //= 26
    return column_name


def get_legitimate_timestamp():
    return int(datetime.now().timestamp()) - 1325347200


def split_date_to_hour_list(date_str):
    # 将字符串转换为datetime对象
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # 获取一天中的第一个小时和最后一个小时
    first_hour = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    last_hour = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)

    # 生成时间列表
    time_list = []
    current_hour = first_hour
    while current_hour <= last_hour:
        time_list.append((current_hour.strftime("%Y-%m-%d %H:%M:%S"),
                          (current_hour + timedelta(hours=1) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
                          ))
        current_hour += timedelta(hours=1)

    return time_list


def get_byte(path):
    # 将图片转成二进制流
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str


def get_md5(charset):
    return hashlib.md5(charset.encode()).hexdigest()


def convert_df_to_list(df: pd.DataFrame):
    result = [df.columns.to_list()]
    result.extend(df.values.tolist())
    return result
