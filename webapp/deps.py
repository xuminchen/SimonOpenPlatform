from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session


def require_db(db: Session | None, *, detail: str) -> Session:
    if db is None:
        raise HTTPException(status_code=503, detail=detail)
    return db
