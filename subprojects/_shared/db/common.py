from __future__ import annotations

import csv
import re
from decimal import Decimal
from typing import Any

import chardet

SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(identifier: str, label: str = "identifier") -> str:
    if not SAFE_IDENTIFIER_RE.match(identifier):
        raise ValueError("Invalid {0}: {1}".format(label, identifier))
    return identifier


def detect_encoding(file_path: str, sample_bytes: int = 8192) -> str:
    with open(file_path, "rb") as file:
        result = chardet.detect(file.read(sample_bytes))
    return result.get("encoding") or "utf-8"


def detect_csv_delimiter(file_path: str) -> str:
    encoding = detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding, errors="ignore") as file:
        sample = file.read(8192)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
        return dialect.delimiter
    except csv.Error:
        pass
    for delimiter in [",", ";", "\t"]:
        if delimiter in sample:
            return delimiter
    return ","


def camel_to_snake(input_string: str) -> str:
    return re.sub(r"([A-Z])", r"_\1", input_string).lower().lstrip("_")


def clean_data(x: Any) -> Any:
    if x is None:
        return None
    text = str(x).strip()
    for char in ["小时"]:
        text = text.replace(char, "")
    if text in ("", "-", "--", "<0.01%", "NA", "NaN", "None"):
        return None

    match_wan = re.match(r"^([+-]?\d*\.?\d+)万$", text)
    if match_wan:
        return float(match_wan.group(1)) * 10000

    text = re.sub(u"[\U00010000-\U0010ffff]", "", text)

    match_number = re.search(r"([\d,]+[\d.]+)", text)
    if match_number:
        matched_text = match_number.group(1)
        text = text.replace(matched_text, matched_text.replace(",", ""))

    match_percent = re.compile(r"^(-?\d+\.?\d*)%$").search(text)
    if match_percent:
        value = float(match_percent.group(1)) / 100
        return Decimal(str(value)).quantize(Decimal("0.0000"))

    date_match = re.match(
        r"(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$",
        text,
    )
    if date_match:
        try:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            year = int(date_match.group(3))
            if 1 <= month <= 12:
                days_in_month = 31
                if month in [4, 6, 9, 11]:
                    days_in_month = 30
                elif month == 2:
                    days_in_month = 29 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 28
                if 1 <= day <= days_in_month:
                    if date_match.group(4):
                        hour = int(date_match.group(4))
                        minute = int(date_match.group(5))
                        second = int(date_match.group(6) or 0)
                        if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                            return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
                    return f"{year:04d}-{month:02d}-{day:02d}"
        except (ValueError, TypeError):
            pass

    return text if text else None
