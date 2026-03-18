from __future__ import annotations

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from webapp.config import DEFAULT_DB_PATH, get_database_url, is_db_enabled


class Base(DeclarativeBase):
    pass


DATABASE_URL = get_database_url()
DB_ENABLED = is_db_enabled()

if DB_ENABLED and DATABASE_URL.startswith("sqlite:///"):
    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = None
SessionLocal = None

if DB_ENABLED:
    engine = create_engine(
        DATABASE_URL,
        future=True,
        connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


def ensure_schema_upgrade() -> None:
    if not DB_ENABLED or engine is None:
        return
    inspector = inspect(engine)
    if "platform_accounts" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("platform_accounts")}
    statements = []

    if "app_id" not in existing_columns:
        statements.append("ALTER TABLE platform_accounts ADD COLUMN app_id VARCHAR(64)")
    if "secret_key_encrypted" not in existing_columns:
        statements.append("ALTER TABLE platform_accounts ADD COLUMN secret_key_encrypted TEXT")
    if "ip_whitelist_json" not in existing_columns:
        statements.append("ALTER TABLE platform_accounts ADD COLUMN ip_whitelist_json TEXT NOT NULL DEFAULT '[]'")
    if "credential_updated_at" not in existing_columns:
        statements.append("ALTER TABLE platform_accounts ADD COLUMN credential_updated_at DATETIME")

    if not statements:
        return

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def get_db():
    if not DB_ENABLED or SessionLocal is None:
        yield None
        return
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
