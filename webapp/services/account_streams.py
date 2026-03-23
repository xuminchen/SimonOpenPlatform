from __future__ import annotations

from sqlalchemy.orm import Session

from webapp.models import AccountStream, PlatformAccount


def _normalize_streams(streams: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in streams:
        key = str(item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def list_account_stream_names(db: Session, *, account_id: int) -> list[str]:
    rows = (
        db.query(AccountStream)
        .filter(AccountStream.account_id == account_id)
        .order_by(AccountStream.stream_name.asc())
        .all()
    )
    return [str(item.stream_name) for item in rows if str(item.stream_name or "").strip()]


def replace_account_streams(
    db: Session,
    *,
    account: PlatformAccount,
    streams: list[str],
) -> list[str]:
    normalized = _normalize_streams(streams)
    (
        db.query(AccountStream)
        .filter(AccountStream.account_id == int(account.id))
        .delete(synchronize_session=False)
    )
    for stream_name in normalized:
        db.add(
            AccountStream(
                account_id=int(account.id),
                stream_name=stream_name,
            )
        )
    db.commit()
    return list_account_stream_names(db, account_id=int(account.id))
