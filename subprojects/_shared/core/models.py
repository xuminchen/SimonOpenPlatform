from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ApiResult:
    ok: bool
    status_code: int
    code: str
    message: str
    data: Any
    request_id: str = ""
    elapsed_ms: int = 0
    error: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_success(
        cls,
        *,
        status_code: int,
        data: Any,
        request_id: str,
        elapsed_ms: int,
        message: str = "ok",
        code: str = "OK",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ApiResult":
        return cls(
            ok=True,
            status_code=status_code,
            code=code,
            message=message,
            data=data,
            request_id=request_id,
            elapsed_ms=elapsed_ms,
            metadata=metadata or {},
        )

    @classmethod
    def from_failure(
        cls,
        *,
        status_code: int,
        code: str,
        message: str,
        error: str,
        request_id: str,
        elapsed_ms: int,
        data: Any = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ApiResult":
        return cls(
            ok=False,
            status_code=status_code,
            code=code,
            message=message,
            error=error,
            data=data,
            request_id=request_id,
            elapsed_ms=elapsed_ms,
            metadata=metadata or {},
        )
