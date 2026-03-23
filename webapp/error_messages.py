from __future__ import annotations


ACCOUNT_NOT_FOUND = "Account not found"
APP_ID_REQUIRED = "app_id is required"
APP_IDS_EMPTY = "app_ids is empty"
PLATFORM_CODE_REQUIRED = "platform_code is required"
PLATFORM_REQUIRED = "platform is required"
PROJECT_NOT_FOUND = "Project not found"
TASK_NOT_FOUND = "Task not found"

DESTINATION_PROFILE_NOT_FOUND = "Destination profile not found"
ONLY_MANAGED_LOCAL_DESTINATION_SUPPORTS_FILE_EXPLORER = "Only managed local destination supports file explorer"
INVALID_FILE_NAME = "invalid file name"
FILE_NOT_FOUND = "file not found"
INVALID_FILE_PATH = "invalid file path"
CREDENTIAL_SOURCE_ITEM_NOT_FOUND = "Credential source item not found"
TOKEN_REFRESH_NOT_SUPPORTED = "更新失败：当前平台不支持 Token 强制更新"


def platform_not_registered(platform_code: str) -> str:
    return "platform is not registered: {0}".format(platform_code)


def account_platform_mismatch(platform_code: str) -> str:
    return "Selected account platform is not {0}".format(platform_code)


def token_bootstrap_failed(error: str) -> str:
    return "token bootstrap failed: {0}".format(error)


def token_refresh_failed(reason: str) -> str:
    return "更新失败：{0}".format(reason)
