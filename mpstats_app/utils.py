from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: clean_value(value) for key, value in record.items()}


def clean_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [clean_record(record) for record in records]


def quote_duckdb_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
