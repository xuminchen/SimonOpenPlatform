from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from subprojects._shared.db import PostgresConfig, PostgresDatabase, camel_to_snake, clean_data


class DatabaseManager:
    def __init__(self) -> None:
        self.config = PostgresConfig.from_env()
        self.db = PostgresDatabase(self.config)
        self.conn = None

    @property
    def host(self) -> str:
        return self.config.host

    @property
    def database(self) -> str:
        return self.config.database

    @property
    def user(self) -> str:
        return self.config.user

    @property
    def password(self) -> str:
        return self.config.password

    @property
    def port(self) -> int:
        return self.config.port

    def get_db_connection(self) -> None:
        self.db.connect()
        self.conn = self.db.conn

    def close_db_connection(self) -> None:
        self.db.close()
        self.conn = None

    def execute_query(self, query: str) -> Sequence[Tuple[Any, ...]]:
        return self.db.execute_query(query)

    def get_primary_keys(self, table_name: str) -> List[str]:
        return self.db.get_primary_keys(table_name)

    def run_sql(
        self,
        sql_name: str,
        filed_list: Sequence[str],
        data_list: Sequence[Tuple[Any, ...]],
        first_execute_sql: Optional[str] = None,
    ) -> None:
        self.db.upsert_rows(
            table_name=sql_name,
            field_list=filed_list,
            data_list=data_list,
            first_execute_sql=first_execute_sql,
        )

    def api_to_sql(
        self,
        json_data: List[Dict[str, Any]],
        sql_name: str,
        first_execute_sql: Optional[str] = None,
        need_clean: bool = True,
    ) -> None:
        if len(json_data) == 0:
            print("No data to process.")
            return
        try:
            if need_clean:
                cleaned_json_data = [{camel_to_snake(k): clean_data(v) for k, v in item.items()} for item in json_data]
            else:
                cleaned_json_data = [{camel_to_snake(k): v for k, v in item.items()} for item in json_data]
            en_filed = list(map(camel_to_snake, cleaned_json_data[0].keys()))
            data_list = [tuple(item.values()) for item in cleaned_json_data]
            self.run_sql(sql_name, en_filed, data_list, first_execute_sql)
        except Exception as exc:
            print(f"An error occurred: {exc}")
        finally:
            self.close_db_connection()

    @staticmethod
    def camel_to_snake(input_string: str) -> str:
        return camel_to_snake(input_string)

    @staticmethod
    def clean_data(x: Any) -> Any:
        return clean_data(x)

    def __del__(self) -> None:
        self.close_db_connection()
