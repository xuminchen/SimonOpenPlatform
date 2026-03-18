from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2

from subprojects._shared.core.settings import get_env
from subprojects._shared.db.common import validate_identifier


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    database: str
    user: str
    password: str
    port: int

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        port_raw = get_env("POSTGRES_PORT")
        try:
            port = int(port_raw) if port_raw else 5432
        except ValueError as exc:
            raise ValueError("Invalid POSTGRES_PORT: {0}".format(port_raw)) from exc

        return cls(
            host=get_env("POSTGRES_HOST") or "",
            database=get_env("POSTGRES_DB") or "",
            user=get_env("POSTGRES_USER") or "",
            password=get_env("POSTGRES_PASSWORD") or "",
            port=port,
        )


class PostgresDatabase:
    def __init__(self, config: Optional[PostgresConfig] = None) -> None:
        self.config = config or PostgresConfig.from_env()
        self.conn: Optional[Any] = None

    def connect(self) -> None:
        if self.conn is not None:
            return
        try:
            self.conn = psycopg2.connect(
                host=self.config.host,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                port=self.config.port,
            )
        except psycopg2.Error as exc:
            print(f"Error connecting to the database: {exc}")
            self.conn = None

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def execute_query(self, query: str) -> Sequence[Tuple[Any, ...]]:
        self.connect()
        if self.conn is None:
            return []
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except psycopg2.Error as exc:
            print(f"Database error: {exc}")
            self.conn.rollback()
            return []

    def get_primary_keys(self, table_name: str) -> List[str]:
        safe_table = validate_identifier(table_name, "table_name")
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON kcu.constraint_name = tc.constraint_name
            WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
        """
        self.connect()
        if self.conn is None:
            return []
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (safe_table,))
                rows = cursor.fetchall()
                return [pk[0] for pk in rows] if rows else []
        except psycopg2.Error as exc:
            print(f"Database error: {exc}")
            self.conn.rollback()
            return []

    def upsert_rows(
        self,
        table_name: str,
        field_list: Sequence[str],
        data_list: Sequence[Tuple[Any, ...]],
        first_execute_sql: Optional[str] = None,
    ) -> None:
        safe_table = validate_identifier(table_name, "table_name")
        self.connect()
        if self.conn is None:
            return
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("BEGIN;")
                if first_execute_sql:
                    cursor.execute(first_execute_sql)
                    print("execute first sql success")
                placeholders = ",".join(["%s" for _ in field_list])
                sql_columns = ",".join([f'"{field}"' for field in field_list])
                unique_columns = self.get_primary_keys(safe_table)
                if not unique_columns:
                    raise ValueError(f"No primary keys found for table {safe_table}")
                update_columns = ", ".join([f'"{field}" = EXCLUDED."{field}"' for field in field_list])
                unique_column_str = ", ".join(unique_columns)
                insert_sql = f"""
                    INSERT INTO {safe_table} ({sql_columns})
                    VALUES ({placeholders})
                    ON CONFLICT ({unique_column_str})
                    DO UPDATE SET {update_columns}
                """
                cursor.executemany(insert_sql, data_list)
                self.conn.commit()
                print("Data insert/update success.")
        except psycopg2.Error as exc:
            self.conn.rollback()
            print(f"PostgreSQL database error occurred: {exc}. Transaction rolled back.")

    def __del__(self) -> None:
        self.close()
