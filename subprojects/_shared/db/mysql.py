from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple

import pymysql

from subprojects._shared.core.settings import get_env, get_int_env
from subprojects._shared.db.common import validate_identifier


@dataclass(frozen=True)
class MySQLConfig:
    host: str
    user: str
    password: str
    port: int
    database: str
    charset: str = "utf8mb4"
    autocommit: bool = False

    @classmethod
    def from_env(cls) -> "MySQLConfig":
        return cls(
            host=get_env("WONDERLAB_DB_HOST", "gz-cdb-35ohvk8b.sql.tencentcdb.com") or "",
            user=get_env("WONDERLAB_DB_USER", "rpauser") or "",
            password=get_env("WONDERLAB_DB_PASSWORD", "Passw0rd!") or "",
            port=get_int_env("WONDERLAB_DB_PORT", 58384),
            database=get_env("WONDERLAB_DB_NAME", "wonderlab_rpa") or "",
        )

    def sqlalchemy_url(self) -> str:
        return "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            self.user,
            self.password,
            self.host,
            self.port,
            self.database,
        )


class MySQLDatabase:
    def __init__(self, config: Optional[MySQLConfig] = None):
        self.config = config or MySQLConfig.from_env()

    def connect(self) -> pymysql.Connection:
        return pymysql.connect(
            host=self.config.host,
            user=self.config.user,
            passwd=self.config.password,
            port=self.config.port,
            db=self.config.database,
            charset=self.config.charset,
            autocommit=self.config.autocommit,
        )

    def get_sql_field(self, cursor: Any, table_name: str) -> Dict[str, str]:
        safe_table = validate_identifier(table_name, "table_name")
        sql = "SELECT COLUMN_NAME, column_comment FROM information_schema.COLUMNS WHERE table_name = %s"
        cursor.execute(sql, (safe_table,))
        return {row[1]: row[0] for row in cursor.fetchall()}

    def run_sql(
        self,
        cursor: Any,
        sql_name: str,
        field_list: Sequence[str],
        data_list: Sequence[Tuple[Any, ...]],
        mysql_syntax: str,
        first_execute_sql: Optional[str] = None,
    ) -> None:
        safe_sql_name = validate_identifier(sql_name, "sql_name")
        try:
            cursor.execute("START TRANSACTION;")
            if first_execute_sql:
                first_effect_rows = cursor.execute(first_execute_sql)
                print(f"execute first sql success, effect rows: {first_effect_rows}")
            placeholders = ",".join(["%s" for _ in field_list])
            sql_columns = ",".join([f"`{field}`" for field in field_list])
            if mysql_syntax == "replace":
                insert_sql = f"REPLACE INTO {safe_sql_name}({sql_columns}) values ({placeholders})"
            else:
                insert_sql = f"INSERT IGNORE INTO {safe_sql_name}({sql_columns}) VALUES ({placeholders})"
            effect_rows = cursor.executemany(insert_sql, data_list)
            print(f"Data insert success, effect rows: {effect_rows}, table: 【{safe_sql_name}】")
            cursor.connection.commit()
        except pymysql.MySQLError as exc:
            cursor.connection.rollback()
            print(f"MySQL database error occurred: {exc}. Transaction rolled back. table: 【{safe_sql_name}】")

    def execute_query(self, query: str) -> Tuple[Any, ...]:
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except pymysql.Error as exc:
            print(f"Database error: {exc}")
            conn.rollback()
            return tuple()
        finally:
            conn.close()

    def execute_sql(self, sql_query: str) -> Any:
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                if sql_query.strip().upper().startswith("SELECT"):
                    return cursor.fetchall()
                conn.commit()
                return cursor.rowcount
        except pymysql.MySQLError as exc:
            conn.rollback()
            print(f"An error occurred: {exc}")
            return None
        finally:
            conn.close()
