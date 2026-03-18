# -*- coding: utf-8 -*-

import json
import time
import datetime
import os
from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig


HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=30,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)


class Xhs:
    def __init__(
        self,
        app_id,
        secret,
        auth_code,
        file_path=None,
    ):
        self.app_id = app_id
        self.secret = secret
        self.auth_code = auth_code
        self.file_path = file_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "test.json",
        )

    def get_access_token(self):
        url = "https://adapi.xiaohongshu.com/api/open/oauth2/access_token"
        header = {
            "content-type": "application/json",
        }
        data = {
            "app_id": self.app_id,
            "secret": self.secret,
            "auth_code": self.auth_code
        }
        result = HTTP_CLIENT.request_json(
            method="post",
            url=url,
            headers=header,
            data=json.dumps(data),
            success_checker=lambda payload: isinstance(payload, dict) and "data" in payload,
            event_name="xhs_get_access_token",
        )
        if not result.ok:
            raise RuntimeError("get_access_token failed: {0}".format(result.error or result.message))
        resp = result.data

        token_data = {
            "create_at": int(time.time()),
            "create_at_str": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "access_token": resp['data']['access_token'],
            "access_token_expires_in": resp['data']['access_token_expires_in'],
            "refresh_token": resp['data']['refresh_token'],
            "refresh_token_expires_in": resp['data']['refresh_token_expires_in'],
            "approval_advertisers": resp['data']['approval_advertisers'],
        }

        self.update_token_file(token_data)

        return resp['data']['access_token'], resp['data']['approval_advertisers']

    def refresh_access_token(self, refresh_token):
        url = "https://adapi.xiaohongshu.com/api/open/oauth2/refresh_token"
        header = {
            "content-type": "application/json",
        }
        data = {
            "app_id": self.app_id,
            "secret": self.secret,
            "refresh_token": refresh_token,
        }

        result = HTTP_CLIENT.request_json(
            method="post",
            url=url,
            headers=header,
            data=json.dumps(data),
            success_checker=lambda payload: isinstance(payload, dict) and "data" in payload,
            event_name="xhs_refresh_access_token",
        )
        if not result.ok:
            raise RuntimeError("refresh_access_token failed: {0}".format(result.error or result.message))
        resp = result.data

        token_data = {
            "create_at": int(time.time()),
            "create_at_str": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "access_token": resp['data']['access_token'],
            "access_token_expires_in": resp['data']['access_token_expires_in'],
            "refresh_token": resp['data']['refresh_token'],
            "refresh_token_expires_in": resp['data']['refresh_token_expires_in'],
            "approval_advertisers": resp['data']['approval_advertisers'],
        }

        self.update_token_file(token_data)

        return resp['data']['access_token'], resp['data']['approval_advertisers']

    def update_token_file(self, token_data):
        try:
            with open(self.file_path, 'r') as f:
                file_content = json.load(f)
        except FileNotFoundError:
            file_content = {}
        except json.JSONDecodeError:
            file_content = {}

        file_content[self.app_id] = token_data

        with open(self.file_path, 'w') as f:
            json.dump(file_content, f, indent=4)

    def get_token(self):
        try:
            with open(self.file_path, 'r') as f:
                file_content = json.load(f)

            if self.app_id not in file_content:
                return self.get_access_token()

            local_content = file_content[self.app_id]
            last_create_at = local_content["create_at"]
            now_time = int(time.time())

            if now_time - last_create_at >= int(local_content["refresh_token_expires_in"]):
                token, approval_advertisers = self.get_access_token()
            elif now_time - last_create_at >= int(local_content["access_token_expires_in"]):
                token, approval_advertisers = self.refresh_access_token(local_content["refresh_token"])
            else:
                token = local_content["access_token"]
                approval_advertisers = local_content["approval_advertisers"]
            return token, approval_advertisers

        except Exception as e:
            print(f"Error: {e}")
            return self.get_access_token()
