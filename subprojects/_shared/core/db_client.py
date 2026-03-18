from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import pymysql

from subprojects._shared.db import MySQLDatabase, camel_to_snake, clean_data
from subprojects._shared.db import detect_csv_delimiter as _detect_csv_delimiter
from subprojects._shared.db import read_csv as _read_csv
from subprojects._shared.db import read_table_file

_MYSQL_DB = MySQLDatabase()


def detect_csv_delimiter(file_path: str) -> str:
    return _detect_csv_delimiter(file_path)


def connect_to_db() -> pymysql.Connection:
    return _MYSQL_DB.connect()


def get_list_difference(excel_field: Iterable[str], sql_field: Iterable[str]) -> Dict[str, set]:
    excel_set = set(excel_field)
    sql_set = set(sql_field)
    return {"excel_not_in_sql": excel_set - sql_set, "sql_not_in_excel": sql_set - excel_set}


def get_sql_field(cursor: Any, table_name: str) -> Dict[str, str]:
    return _MYSQL_DB.get_sql_field(cursor, table_name)


def run_sql(
    cursor: Any,
    sql_name: str,
    filed_list: Sequence[str],
    data_list: Sequence[Tuple[Any, ...]],
    mysql_syntax: str,
    first_execute_sql: Optional[str] = None,
) -> None:
    _MYSQL_DB.run_sql(cursor, sql_name, filed_list, data_list, mysql_syntax, first_execute_sql)


def read_csv(file_path: str, cols: Optional[List[str]] = None) -> pd.DataFrame:
    return _read_csv(file_path, cols)


def excel_to_sql(
    sql_name: str,
    file_path: str,
    sheet_name: Any = 0,
    header: int = 0,
    mysql_syntax: str = "replace",
    cols: Optional[List[str]] = None,
    add_data: Optional[Dict[str, Any]] = None,
    first_execute_sql: Optional[str] = None,
    need_clean: Optional[bool] = None,
) -> None:
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            df = read_table_file(file_path=file_path, sheet_name=sheet_name, header=header, cols=cols)
            if df is None:
                return
            df.dropna(how="all", axis=1, inplace=True)
            df.fillna("", inplace=True)
            if add_data:
                for key, value in add_data.items():
                    df[key] = value
            data_field_names = [field_name.strip().replace("*", "") for field_name in df.columns]
            sql_field_dict = get_sql_field(cursor, sql_name)
            differences = get_list_difference(data_field_names, sql_field_dict.keys())
            if differences["excel_not_in_sql"]:
                print(f"Field 【{differences['excel_not_in_sql']}】 mismatch. Update the fields and try again.")
                return
            data_to_sql_field = [sql_field_dict[field] for field in data_field_names]
            if need_clean:
                data_list = [tuple(map(clean_data, row)) for row in df.values.tolist()]
            else:
                data_list = [tuple(row) for row in df.values.tolist()]
            run_sql(cursor, sql_name, data_to_sql_field, data_list, mysql_syntax, first_execute_sql)
    except Exception as exc:
        print(f"An error occurred: {exc}")
    finally:
        conn.close()


def api_to_sql(
    json_data: List[Dict[str, Any]],
    sql_name: str,
    mysql_syntax: str = "replace",
    need_filed: Optional[List[str]] = None,
    first_execute_sql: Optional[str] = None,
    need_clean: bool = True,
) -> None:
    if len(json_data) == 0:
        print("No data to process.")
        return
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            if need_filed:
                filtered_json_data = [{k: v for k, v in item.items() if k in need_filed} for item in json_data]
            else:
                filtered_json_data = json_data
            if need_clean:
                cleaned_json_data = [{camel_to_snake(k): clean_data(v) for k, v in item.items()} for item in filtered_json_data]
            else:
                cleaned_json_data = [{camel_to_snake(k): v for k, v in item.items()} for item in filtered_json_data]
            en_filed = list(map(camel_to_snake, cleaned_json_data[0].keys()))
            data_list = [tuple(item.values()) for item in cleaned_json_data]
            run_sql(cursor, sql_name, en_filed, data_list, mysql_syntax, first_execute_sql)
    except pymysql.MySQLError as exc:
        print(f"An error occurred: {exc}")
    finally:
        conn.close()


def execute_query(query: str) -> Tuple[Any, ...]:
    return _MYSQL_DB.execute_query(query)


def execute_sql(sql_query: str) -> Any:
    return _MYSQL_DB.execute_sql(sql_query)
