# -*- coding: utf-8 -*-
# 通过获取授权码(code)复制并更新到数据库
# 当refresh_token过期了执行 一键执行即可
# 区分 interp_type = "快手国内"  "快手国际"
import requests
from datetime import datetime, timedelta
from subprojects._shared.db import MySQLDatabase
from subprojects._shared.core.api_credentials import get_credentials


def connect_to_db():
    """数据库连接"""
    return MySQLDatabase().connect()


app_info = get_credentials("kwai_v3", "accounts", default={})


def get_access_token(app_id, code, app_secret):
    """获取access_token和refresh_token"""
    url = "https://openapi.kwaixiaodian.com/oauth2/access_token"
    params = {
        "app_id": app_id,
        "grant_type": "code",
        "code": code,
        "app_secret": app_secret
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get('result') != 1:
            raise ValueError(f"API返回错误: {data}")

        return {
            "access_token": data['access_token'],
            "refresh_token": data['refresh_token'],
            "expires_in": data['expires_in'],
            "refresh_token_expires_in": data['refresh_token_expires_in']
        }
    except Exception as e:
        print(f"获取token失败: {e}")
        return None


def get_code_from_db(interp_type):
    """从数据库获取code"""
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT code FROM kwai_api_get_token WHERE interp_type = %s"
            cursor.execute(sql, (interp_type,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"获取code失败: {e}")
        return None
    finally:
        conn.close()


def update_token_info(interp_type, token_data):
    """更新token信息到数据库(只存储时间戳形式的过期时间)"""
    conn = connect_to_db()
    try:
        current_time = datetime.now()

        # 计算过期时间
        access_token_expire_time = current_time + timedelta(seconds=token_data['expires_in'])
        refresh_token_expire_time = current_time + timedelta(seconds=token_data['refresh_token_expires_in'])

        with conn.cursor() as cursor:
            # 检查记录是否存在
            check_sql = "SELECT 1 FROM kwai_api_get_token WHERE interp_type = %s"
            cursor.execute(check_sql, (interp_type,))
            exists = cursor.fetchone()

            if exists:
                sql = """UPDATE kwai_api_get_token 
                         SET access_token = %s,
                             refresh_token = %s,
                             create_time = NOW(),
                             access_token_expire_time = %s,
                             refresh_token_expire_time = %s
                         WHERE interp_type = %s"""
            else:
                sql = """INSERT INTO kwai_api_get_token 
                         (access_token, refresh_token, create_time, 
                          interp_type, access_token_expire_time, refresh_token_expire_time)
                         VALUES (%s, %s, NOW(), %s, %s, %s)"""

            cursor.execute(sql, (
                token_data['access_token'],
                token_data['refresh_token'],
                access_token_expire_time,
                refresh_token_expire_time,
                interp_type
            ))
            conn.commit()
            return True
    except Exception as e:
        print(f"更新数据库失败: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def main():
    """主函数"""
    interp_type = "快手国内"  # 可更改为"快手国际"

    # 1. 从数据库获取code
    code = get_code_from_db(interp_type)
    if not code:
        print(f"未找到{interp_type}的授权码")
        return

    # 2. 获取app配置
    config = app_info.get(interp_type)
    if not config:
        print(f"未找到{interp_type}的配置")
        return

    # 3. 获取token
    token_data = get_access_token(
        app_id=config["app_key"],
        code=code,
        app_secret=config["app_secret"]
    )

    if not token_data:
        print("获取token失败")
        return

    # 4. 更新到数据库
    if update_token_info(interp_type, token_data):
        print(f"{interp_type} token更新成功")
    else:
        print(f"{interp_type} token更新失败")


if __name__ == "__main__":
    main()
