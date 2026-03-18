from __future__ import annotations

import json
import os
from pathlib import Path
import threading
from typing import Any

from webapp.services.platform_alias import normalize_platform

_FILE_LOCK = threading.Lock()

_SYSTEM_PLATFORMS: dict[str, dict[str, Any]] = {
    "oceanengine": {
        "platform": "oceanengine",
        "label": "OceanEngine",
        "helper": "用于千川授权与 token 刷新。",
        "docs_url": "https://open.oceanengine.com/",
        "status": "active",
        "mutable": False,
    },
    "red_juguang": {
        "platform": "red_juguang",
        "label": "Red_JuGuang",
        "helper": "用于小红书聚光授权与 token 刷新。",
        "docs_url": "https://ad-market.xiaohongshu.com/docs-center",
        "status": "active",
        "mutable": False,
    },
    "red_chengfeng": {
        "platform": "red_chengfeng",
        "label": "Red_ChengFeng",
        "helper": "用于小红书乘风授权与 token 刷新。",
        "docs_url": "https://ad-market.xiaohongshu.com/docs-center",
        "status": "active",
        "mutable": False,
    },
    "wechat_shop": {
        "platform": "wechat_shop",
        "label": "Wechat_Shop",
        "helper": "用于微信小店 API 凭证配置。",
        "docs_url": "",
        "status": "active",
        "mutable": False,
    },
    "meta_ads": {
        "platform": "meta_ads",
        "label": "Meta_Ads",
        "helper": "用于 Meta Ads API 凭证配置。",
        "docs_url": "",
        "status": "active",
        "mutable": False,
    },
}


def _config_path() -> Path:
    configured = os.environ.get("WONDERLAB_PLATFORM_CONFIG_FILE", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[2] / "config" / "platform_configs.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError("Platform config file must be a JSON object: {0}".format(path))
    return data


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def _custom_platforms() -> dict[str, dict[str, Any]]:
    path = _config_path()
    root = _read_json(path)
    raw = root.get("platforms")
    if not isinstance(raw, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        platform = normalize_platform(str(key))
        if not platform:
            continue
        result[platform] = {
            "platform": platform,
            "label": str(value.get("label", platform)).strip() or platform,
            "helper": str(value.get("helper", "")).strip(),
            "docs_url": str(value.get("docs_url", "")).strip(),
            "status": str(value.get("status", "active")).strip() or "active",
            "mutable": True,
        }
    return result


def list_platform_configs() -> list[dict[str, Any]]:
    merged = dict(_SYSTEM_PLATFORMS)
    merged.update(_custom_platforms())
    return [merged[k] for k in sorted(merged.keys())]


def create_platform_config(*, platform: str, label: str, helper: str = "", docs_url: str = "", status: str = "active") -> dict[str, Any]:
    normalized_platform = normalize_platform(platform)
    if not normalized_platform:
        raise ValueError("platform is required")
    if normalized_platform in _SYSTEM_PLATFORMS:
        raise ValueError("system platform cannot be overwritten: {0}".format(normalized_platform))

    path = _config_path()
    with _FILE_LOCK:
        root = _read_json(path)
        bucket = root.get("platforms")
        if not isinstance(bucket, dict):
            bucket = {}
            root["platforms"] = bucket
        bucket[normalized_platform] = {
            "label": str(label or normalized_platform).strip() or normalized_platform,
            "helper": str(helper or "").strip(),
            "docs_url": str(docs_url or "").strip(),
            "status": str(status or "active").strip() or "active",
        }
        _write_json(path, root)

    return {
        "platform": normalized_platform,
        "label": str(label or normalized_platform).strip() or normalized_platform,
        "helper": str(helper or "").strip(),
        "docs_url": str(docs_url or "").strip(),
        "status": str(status or "active").strip() or "active",
        "mutable": True,
    }


def delete_platform_config(*, platform: str, used_platforms: set[str] | None = None) -> bool:
    normalized_platform = normalize_platform(platform)
    if not normalized_platform:
        raise ValueError("platform is required")
    if normalized_platform in _SYSTEM_PLATFORMS:
        raise ValueError("system platform cannot be deleted: {0}".format(normalized_platform))
    if used_platforms and normalized_platform in used_platforms:
        raise ValueError("platform is in use and cannot be deleted: {0}".format(normalized_platform))

    path = _config_path()
    with _FILE_LOCK:
        root = _read_json(path)
        bucket = root.get("platforms")
        if not isinstance(bucket, dict):
            return False
        existed = normalized_platform in bucket
        bucket.pop(normalized_platform, None)
        _write_json(path, root)
    return existed
