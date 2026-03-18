from __future__ import annotations

from typing import Dict

from webapp.adapters.base import PlatformAdapter
from webapp.adapters.meta_ads import MetaAdsAdapter
from webapp.adapters.wechat_shop import WechatShopAdapter


_ADAPTERS: Dict[str, PlatformAdapter] = {
    "wechat_shop": WechatShopAdapter(),
    "meta_ads": MetaAdsAdapter(),
}


def get_adapter(platform: str) -> PlatformAdapter:
    key = platform.strip().lower()
    if key not in _ADAPTERS:
        raise ValueError("Unsupported platform: {0}".format(platform))
    return _ADAPTERS[key]
