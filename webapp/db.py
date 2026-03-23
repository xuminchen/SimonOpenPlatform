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

    if "sync_connections" in inspector.get_table_names():
        sync_columns = {col["name"] for col in inspector.get_columns("sync_connections")}
        if "destination" not in sync_columns:
            statements.append("ALTER TABLE sync_connections ADD COLUMN destination VARCHAR(128) NOT NULL DEFAULT 'ClickHouse_DW'")

    if "sync_stream_tasks" in inspector.get_table_names():
        stream_columns = {col["name"] for col in inspector.get_columns("sync_stream_tasks")}
        if "schema_contract_json" not in stream_columns:
            statements.append("ALTER TABLE sync_stream_tasks ADD COLUMN schema_contract_json TEXT NOT NULL DEFAULT '{}'")

    if "sync_projects" in inspector.get_table_names():
        project_columns = {col["name"] for col in inspector.get_columns("sync_projects")}
        if "app_ids_json" not in project_columns:
            statements.append("ALTER TABLE sync_projects ADD COLUMN app_ids_json TEXT NOT NULL DEFAULT '[]'")
        if "schedule_cron" not in project_columns:
            statements.append("ALTER TABLE sync_projects ADD COLUMN schedule_cron VARCHAR(64) NOT NULL DEFAULT '0 * * * *'")

    if "destination_profiles" not in inspector.get_table_names():
        statements.append(
            "CREATE TABLE destination_profiles ("
            "id INTEGER PRIMARY KEY, "
            "name VARCHAR(128) NOT NULL UNIQUE, "
            "engine_category VARCHAR(32) NOT NULL DEFAULT 'database', "
            "destination_type VARCHAR(64) NOT NULL DEFAULT 'PostgreSQL', "
            "status VARCHAR(32) NOT NULL DEFAULT 'active', "
            "config_json TEXT NOT NULL DEFAULT '{}', "
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ")"
        )

    if "platform_api_streams" not in inspector.get_table_names():
        statements.append(
            "CREATE TABLE platform_api_streams ("
            "id INTEGER PRIMARY KEY, "
            "platform_code VARCHAR(64) NOT NULL, "
            "stream_name VARCHAR(128) NOT NULL, "
            "display_name VARCHAR(128) NOT NULL DEFAULT '', "
            "doc_url TEXT NOT NULL DEFAULT '', "
            "request_config_json TEXT NOT NULL DEFAULT '{}', "
            "auth_strategy_json TEXT NOT NULL DEFAULT '{}', "
            "pagination_strategy_json TEXT NOT NULL DEFAULT '{}', "
            "extraction_strategy_json TEXT NOT NULL DEFAULT '{}', "
            "supported_sync_modes_json TEXT NOT NULL DEFAULT '[]', "
            "status VARCHAR(32) NOT NULL DEFAULT 'published', "
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ")"
        )

    if "account_streams" not in inspector.get_table_names():
        statements.append(
            "CREATE TABLE account_streams ("
            "id INTEGER PRIMARY KEY, "
            "account_id INTEGER NOT NULL, "
            "stream_name VARCHAR(128) NOT NULL, "
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "UNIQUE(account_id, stream_name)"
            ")"
        )

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
