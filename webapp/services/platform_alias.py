from __future__ import annotations


PLATFORM_ALIASES = {
    "xhs_juguang": "red_juguang",
    "xhs_chengfeng": "red_chengfeng",
}


def normalize_platform(platform: str) -> str:
    key = str(platform or "").strip().lower()
    if not key:
        return ""
    return PLATFORM_ALIASES.get(key, key)
