from __future__ import annotations

import csv
from typing import Any, Dict, List, Optional

import pandas as pd

from subprojects._shared.db.common import detect_csv_delimiter, detect_encoding


def read_csv(file_path: str, cols: Optional[List[str]] = None) -> pd.DataFrame:
    encoding = detect_encoding(file_path)
    csv_sep = detect_csv_delimiter(file_path)
    with open(file_path, mode="r", encoding=encoding, newline="", errors="ignore") as file:
        reader = csv.DictReader(file, delimiter=csv_sep)
        data = [row for row in reader]
        for row in data:
            for key in row:
                if row[key] == "":
                    row[key] = None
        df = pd.DataFrame(data)
        if cols:
            df = df[cols]
        return df


def read_table_file(file_path: str, sheet_name: Any = 0, header: int = 0, cols: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
    if file_path.endswith("xlsx") or file_path.endswith("xls"):
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header, usecols=cols)
    if file_path.endswith("csv"):
        return read_csv(file_path=file_path, cols=cols)
    return None


def normalize_dataframe(df: pd.DataFrame, add_data: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    normalized = df.copy()
    normalized.dropna(how="all", axis=1, inplace=True)
    normalized.fillna("", inplace=True)
    if add_data:
        for key, value in add_data.items():
            normalized[key] = value
    return normalized
