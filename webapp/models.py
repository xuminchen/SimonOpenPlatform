from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from webapp.db import Base


class PlatformAccount(Base):
    __tablename__ = "platform_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    platform: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    config_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    app_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    secret_key_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_whitelist_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    credential_updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    tasks: Mapped[list["SyncTask"]] = relationship("SyncTask", back_populates="account")
    streams: Mapped[list["AccountStream"]] = relationship(
        "AccountStream",
        back_populates="account",
        cascade="all, delete-orphan",
    )


class AccountStream(Base):
    __tablename__ = "account_streams"
    __table_args__ = (UniqueConstraint("account_id", "stream_name", name="uq_account_stream"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("platform_accounts.id"), nullable=False, index=True)
    stream_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    account: Mapped[PlatformAccount] = relationship("PlatformAccount", back_populates="streams")


class SyncTask(Base):
    __tablename__ = "sync_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("platform_accounts.id"), nullable=False, index=True)
    task_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    request_payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    result_payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    account: Mapped[PlatformAccount] = relationship("PlatformAccount", back_populates="tasks")


class SyncConnection(Base):
    __tablename__ = "sync_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    platform_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    credential_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True, index=True)
    app_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    schedule_cron: Mapped[str] = mapped_column(String(64), nullable=False)
    destination: Mapped[str] = mapped_column(String(128), nullable=False, default="ClickHouse_DW")
    status: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)
    last_sync_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sync_status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    streams: Mapped[list["SyncConnectionStream"]] = relationship(
        "SyncConnectionStream",
        back_populates="connection",
        cascade="all, delete-orphan",
    )


class SyncConnectionStream(Base):
    __tablename__ = "sync_connection_streams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("sync_connections.id"), nullable=False, index=True)
    stream_name: Mapped[str] = mapped_column(String(128), nullable=False)
    sync_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="INCREMENTAL")
    cursor_field: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    primary_key: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    connection: Mapped[SyncConnection] = relationship("SyncConnection", back_populates="streams")


class SyncProject(Base):
    __tablename__ = "sync_projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    platform_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    credential_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True, index=True)
    app_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    app_ids_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    schedule_cron: Mapped[str] = mapped_column(String(64), nullable=False, default="0 * * * *")
    destination: Mapped[str] = mapped_column(String(128), nullable=False, default="ClickHouse_DW")
    status: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    stream_tasks: Mapped[list["SyncStreamTask"]] = relationship(
        "SyncStreamTask",
        back_populates="project",
        cascade="all, delete-orphan",
    )
    executions: Mapped[list["SyncExecutionInstance"]] = relationship(
        "SyncExecutionInstance",
        back_populates="project",
        cascade="all, delete-orphan",
    )


class SyncStreamTask(Base):
    __tablename__ = "sync_stream_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("sync_projects.id"), nullable=False, index=True)
    stream_name: Mapped[str] = mapped_column(String(128), nullable=False)
    sync_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="INCREMENTAL")
    cursor_field: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    primary_key: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    schema_contract_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    routine_cron: Mapped[str] = mapped_column(String(64), nullable=False, default="0 * * * *")
    last_cursor_value: Mapped[str] = mapped_column(Text, nullable=False, default="")
    last_routine_started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_routine_finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_routine_status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    project: Mapped[SyncProject] = relationship("SyncProject", back_populates="stream_tasks")
    executions: Mapped[list["SyncExecutionInstance"]] = relationship(
        "SyncExecutionInstance",
        back_populates="stream_task",
        cascade="all, delete-orphan",
    )


class SyncExecutionInstance(Base):
    __tablename__ = "sync_execution_instances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("sync_projects.id"), nullable=False, index=True)
    stream_task_id: Mapped[int | None] = mapped_column(ForeignKey("sync_stream_tasks.id"), nullable=True, index=True)
    execution_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True, default="ROUTINE")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING", index=True)
    start_time: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    end_time: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    triggered_by: Mapped[str] = mapped_column(String(32), nullable=False, default="system")
    request_payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    result_payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    project: Mapped[SyncProject] = relationship("SyncProject", back_populates="executions")
    stream_task: Mapped[SyncStreamTask | None] = relationship("SyncStreamTask", back_populates="executions")


class DestinationProfile(Base):
    __tablename__ = "destination_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, index=True, unique=True)
    engine_category: Mapped[str] = mapped_column(String(32), nullable=False, default="database", index=True)
    destination_type: Mapped[str] = mapped_column(String(64), nullable=False, default="PostgreSQL", index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active", index=True)
    config_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class PlatformApiStream(Base):
    __tablename__ = "platform_api_streams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    platform_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    stream_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    display_name: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    doc_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    request_config_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    auth_strategy_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    pagination_strategy_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    extraction_strategy_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    supported_sync_modes_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="published", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )
