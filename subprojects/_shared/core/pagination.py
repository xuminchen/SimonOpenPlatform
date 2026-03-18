from __future__ import annotations

from typing import Any, Dict


def should_continue_by_size(*, items_count: int, page_size: int) -> bool:
    return items_count >= page_size


def next_page_params(page_params: Dict[str, Any], *, page_field: str = "page", step: int = 1) -> Dict[str, Any]:
    next_params = dict(page_params)
    current = int(next_params.get(page_field, 1))
    next_params[page_field] = current + step
    return next_params


def should_continue_by_flag(response_data: Dict[str, Any], *, flag_field: str = "has_more") -> bool:
    return bool(response_data.get(flag_field))
