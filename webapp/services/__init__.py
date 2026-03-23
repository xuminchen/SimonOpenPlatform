"""Application services for webapp."""

from webapp.services.red_juguang_api import (
    RED_JUGUANG_DOC_URLS,
    DEFAULT_ARTICLE_ENDPOINTS,
    KNOWN_RED_JUGUANG_ENDPOINTS,
    RedJuGuangApiClient,
)

__all__ = [
    "RedJuGuangApiClient",
    "RED_JUGUANG_DOC_URLS",
    "DEFAULT_ARTICLE_ENDPOINTS",
    "KNOWN_RED_JUGUANG_ENDPOINTS",
]
