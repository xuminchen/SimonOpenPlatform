"""Shared database read/write package."""

from subprojects._shared.db.common import camel_to_snake, clean_data, detect_csv_delimiter, detect_encoding, validate_identifier
from subprojects._shared.db.dataframe_io import normalize_dataframe, read_csv, read_table_file
from subprojects._shared.db.mysql import MySQLConfig, MySQLDatabase
from subprojects._shared.db.postgres import PostgresConfig, PostgresDatabase

__all__ = [
    "camel_to_snake",
    "clean_data",
    "detect_csv_delimiter",
    "detect_encoding",
    "validate_identifier",
    "normalize_dataframe",
    "read_csv",
    "read_table_file",
    "MySQLConfig",
    "MySQLDatabase",
    "PostgresConfig",
    "PostgresDatabase",
]
