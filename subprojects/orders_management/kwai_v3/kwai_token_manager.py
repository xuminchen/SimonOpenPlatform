# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import requests
from subprojects._shared.db import MySQLDatabase
from subprojects._shared.core.api_credentials import get_credentials

class KwaiTokenManager:
    def __init__(self):
        self.db = MySQLDatabase()

    def connect_to_db(self):
        """数据库连接"""
        return self.db.connect()

    def get_token_info(self, interp_type):
        """从数据库获取token信息"""
        conn = self.connect_to_db()
        try:
            with conn.cursor() as cursor:
                sql = """SELECT access_token, refresh_token, 
                                access_token_expire_time, refresh_token_expire_time
                         FROM kwai_api_get_token 
                         WHERE interp_type = %s"""
                cursor.execute(sql, (interp_type,))
                result = cursor.fetchone()
                if result:
                    return {
                        "access_token": result[0],
                        "refresh_token": result[1],
                        "access_token_expire_time": result[2],
                        "refresh_token_expire_time": result[3]
                    }
                return None
        except Exception as e:
            print(f"获取token信息失败: {e}")
            return None
        finally:
            conn.close()

    def is_token_expired(self, expire_time, buffer_hours=2):
        """检查token是否过期"""
        if not expire_time:
            return True
        return (datetime.now() + timedelta(hours=buffer_hours)) > expire_time

    def refresh_token(self, interp_type, app_key, refresh_token, app_secret):
        """刷新token"""
        url = "https://openapi.kwaixiaodian.com/oauth2/refresh_token"
        params = {
            "app_id": app_key,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "app_secret": app_secret
        }

        try:
            response = requests.post(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('result') != 1:
                raise ValueError(f"API返回错误: {data}")

            current_time = datetime.now()
            return {
                "access_token": data['access_token'],
                "refresh_token": data['refresh_token'],
                "access_token_expire_time": current_time + timedelta(seconds=data['expires_in']),
                # refresh_token_expire_time 保持原值不更新
            }
        except Exception as e:
            print(f"刷新token失败: {e}")
            return None

    def update_token(self, interp_type, token_data):
        """更新token到数据库"""
        conn = self.connect_to_db()
        try:
            with conn.cursor() as cursor:
                sql = """UPDATE kwai_api_get_token 
                         SET access_token = %s,
                             refresh_token = %s,
                             access_token_expire_time = %s
                         WHERE interp_type = %s"""
                cursor.execute(sql, (
                    token_data['access_token'],
                    token_data['refresh_token'],
                    token_data['access_token_expire_time'],
                    interp_type
                ))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"更新数据库失败: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_valid_token(self, interp_type):
        """获取有效token，自动处理刷新逻辑"""
        token_info = self.get_token_info(interp_type)
        if not token_info:
            raise Exception(f"未找到 {interp_type} 的token信息")

        # 检查access_token是否有效
        if not self.is_token_expired(token_info['access_token_expire_time']):
            return token_info['access_token']

        # 检查refresh_token是否有效
        if self.is_token_expired(token_info['refresh_token_expire_time']):
            raise Exception(f"{interp_type} refresh_token已过期，需要重新授权")

        # 获取app配置
        app_config = get_credentials("kwai_v3", "accounts", interp_type, default={})

        if not app_config:
            raise Exception(f"未找到 {interp_type} 的配置")

        # 刷新token
        new_token = self.refresh_token(
            interp_type,
            app_config["app_key"],
            token_info["refresh_token"],
            app_config["app_secret"]
        )

        if not new_token:
            raise Exception("刷新token失败")

        # 更新数据库
        if not self.update_token(interp_type, new_token):
            raise Exception("更新token到数据库失败")

        return new_token['access_token']
