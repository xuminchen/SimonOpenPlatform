from __future__ import annotations

from typing import Any, Dict, Protocol


class PlatformAdapter(Protocol):
    platform: str

    def sync_orders(self, *, account_name: str, account_config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        ...
