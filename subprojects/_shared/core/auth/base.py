from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Protocol


class AuthProvider(Protocol):
    def build_headers(self) -> Dict[str, str]:
        ...


@dataclass
class BearerTokenProvider:
    token: str
    header_name: str = "Authorization"
    prefix: str = "Bearer "

    def build_headers(self) -> Dict[str, str]:
        token = self.token.strip()
        if not token:
            return {}
        return {self.header_name: "{0}{1}".format(self.prefix, token)}
