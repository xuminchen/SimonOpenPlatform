from Utils import *
from subprojects._shared.core.auth.providers import OceanEngineTokenProvider
from subprojects._shared.core.api_credentials import get_credentials


def get_token_new(auth_code, app_id, secret):
    rel_json = OceanEngineTokenProvider(app_id=app_id, secret=secret).get_by_auth_code(auth_code)
    print(rel_json)
    print(rel_json)
    access_token = rel_json['data']['access_token']
    refresh_token = rel_json['data']['refresh_token']
    print(refresh_token)

    utils = Utils()
    utils.write_file(refresh_token)
    utils.write_refresh_token(refresh_token, app_id, secret)

    return access_token


class AccessToken:

    # 自定义帐号token获取
    def refresh_token_new(self, app_id, secret):
        refresh_token = Utils().read_refresh_token(app_id, secret)
        rel_json = OceanEngineTokenProvider(app_id=app_id, secret=secret).refresh(refresh_token.replace('\n', ''))
        print(rel_json)
        access_token = rel_json['data']['access_token']
        refresh_token = rel_json['data']['refresh_token']

        utils = Utils()
        utils.write_file(refresh_token)
        utils.write_refresh_token(refresh_token, app_id, secret)

        return access_token


if __name__ == '__main__':
    apps = get_credentials("xhs_juguang", "apps", default={})
    if not isinstance(apps, dict):
        raise ValueError("xhs_juguang.apps must be an object in config/api_credentials.json")

    selected_app = None
    for item in apps.values():
        if not isinstance(item, dict):
            continue
        app_id = str(item.get("app_id", "")).strip()
        secret = str(item.get("secret", "")).strip()
        if app_id and secret:
            selected_app = (app_id, secret)
            break

    if selected_app is None:
        raise ValueError("Missing xhs_juguang.apps.*.app_id/secret in config/api_credentials.json")

    print(AccessToken().refresh_token_new(selected_app[0], selected_app[1]))
