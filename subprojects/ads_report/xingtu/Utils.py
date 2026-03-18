# coding=utf-8

import datetime

from psycopg2 import extras as ex
from subprojects._shared.db import PostgresConfig, PostgresDatabase


class Utils():

    # PGDB配置
    # DB_HOST = 'localhost'
    # DB_PORT = 5432
    # DB_USERNAME = 'root'
    # DB_PASSWORD = 'root'
    # DB_NAME = 'firstdb'

    _pg_config = PostgresConfig.from_env()
    DB_HOST = _pg_config.host
    DB_PORT = _pg_config.port
    DB_USERNAME = _pg_config.user
    DB_PASSWORD = _pg_config.password
    DB_NAME = _pg_config.database

    def __init__(self):
        self.db = PostgresDatabase(self._pg_config)

    def _get_conn(self):
        self.db.connect()
        return self.db.conn

    # 写入文件到refresh_token_content.txt
    def write_file(self, content):
        with open('refresh_token_content.txt', 'w') as f:  # 设置文件对象
            f.write(content)

    # 从refresh_token_content.txt文件中获取内容
    def read_file(self):
        f = open("refresh_token_content.txt", "r")  # 设置文件对象
        content = f.read()  # 将txt文件的所有内容读入到字符串str中
        f.close()
        return content

    def read_refresh_token(self, APP_ID, Secret):
        refresh_token = ''
        conn = self._get_conn()
        if conn is None:
            return refresh_token
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT refresh_token FROM jlqc_refresh_token_table WHERE app_id = %s AND secret = %s",
                    (APP_ID, Secret),
                )
                row = cursor.fetchone()
                if row:
                    refresh_token = row[0]
            conn.commit()
        finally:
            self.db.close()
        return refresh_token

    def write_refresh_token(self, refresh_token, APP_ID, Secret):
        conn = self._get_conn()
        if conn is None:
            return
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM jlqc_refresh_token_table WHERE app_id = %s AND secret = %s", (APP_ID, Secret))
                cursor.execute(
                    "INSERT INTO jlqc_refresh_token_table VALUES (%s, %s, %s)",
                    (refresh_token, APP_ID, Secret),
                )
            conn.commit()
        finally:
            self.db.close()


    # 获取PGDB链接并插入数据
    def conn_pgdb(self, sql, datalist):
        conn = self._get_conn()
        if conn is None:
            return
        try:
            with conn.cursor() as cursor:
                ex.execute_values(cursor, sql, datalist, page_size=1000)
            conn.commit()
        finally:
            self.db.close()

    def Unicode_to_zh(self, content):
        # return content.encode('utf-8', 'replace').decode('unicode_escape')
        return content.encode('utf-8', 'replace').decode('utf8')

    # 获取当前时间
    def get_now_time(self, format):
        # 先获得时间数组格式的日期
        return datetime.datetime.now().strftime(format)

    # 获取前X天数据
    def get_X_time_ago(self, num_day):
        dayAgo = (datetime.datetime.now() - datetime.timedelta(days=num_day))
        dayAgo = dayAgo.strftime('%Y-%m-%d')
        return dayAgo

    def data_remove_duplicates(self, data_list):
        remove_duplicates_list = []
        for data in data_list:
            if data not in remove_duplicates_list:
                remove_duplicates_list.append(data)

        return remove_duplicates_list

    def data_remove_duplicates1(self, data_list):
        remove_duplicates_list = {}
        key_list = []
        for data in data_list:
            if (data[0],data[1],data[4],data[6],data[9],data[13]) in key_list:
                print(data)
                print('-'*100)
                print(remove_duplicates_list[(data[0],data[1],data[4],data[6],data[9],data[13])])
                print('-'*100)
            else:
                key_list.append((data[0],data[1],data[4],data[6],data[9],data[13]))
                remove_duplicates_list[(data[0],data[1],data[4],data[6],data[9],data[13])] = data

        return remove_duplicates_list.values()

    def get_date_diff(self, date_1, date_2):
        date1 = datetime.datetime.strptime(date_1, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.datetime.strptime(date_2, "%Y-%m-%d %H:%M:%S")
        diff = date2 - date1
        return abs(diff.days)

    def data_check(self, data_list):
        new_data_list = []
        for data in data_list:
            if data in new_data_list:
                continue
            else:
                new_data_list.append(data)
        return new_data_list

if __name__ == '__main__':

    print(abs(Utils().get_date_diff('2024-01-15 00:00:00', '2024-01-01 00:00:00')))
    print(Utils().get_now_time('%Y-%m-%d %H:%M:%S'))

    print(Utils().get_date_diff(Utils().get_now_time('%Y-%m-%d %H:%M:%S'), '2024-01-01 00:00:00'))
