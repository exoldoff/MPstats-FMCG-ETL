from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
import io
import json
import logging
from pathlib import Path
from threading import RLock
import time
from typing import Any, Literal
from uuid import uuid4

import pandas as pd

from pipeline.repositories.sql_repository import (
    apply_migrations,
    connect,
    duckdb_transaction,
    measure_duckdb_operation,
    quote_identifier,
    resolve_duckdb_temp_directory,
    sql_literal,
    table_exists,
)

from mpstats_app.config import AppSettings
from mpstats_app.utils import clean_record, clean_records, quote_duckdb_name


SEARCH_COLUMNS = ("SKU", "Артикул", "Название", "Бренд", "Категория")
EXPORT_METADATA_COLUMNS = ("__project_name", "__year", "__month", "__marketplace_code", "__category_key", "__row_hash")
TEXT_DB_TYPES = ("CHAR", "STRING", "TEXT", "VARCHAR")
CUBE_SALES_FILTER_COLUMNS = ("Продажи, шт", "Продажи", "sales")
CUBE_VOLUME_FILTER_COLUMNS = ("Объем, кг", "Объём, кг", "Объем, т", "Объём, т", "Объем", "Объём", "volume_kg", "volume_t", "volume")
IMPORT_METADATA_COLUMNS = (
    "__run_id",
    "__source_file",
    "__imported_at",
    "__project_name",
    "__year",
    "__month",
    "__marketplace_code",
    "__category_key",
    "__row_hash",
)
HEAVY_SLICE_ROWS_LIMIT = 250_000
HEAVY_CATEGORY_ROWS_LIMIT = 1_000_000
REPORT_REVENUE_COLUMNS = ("Выручка, руб", "Выручка", "revenue")
REPORT_VOLUME_KG_COLUMNS = ("Объем, кг", "Объём, кг", "volume_kg")
REPORT_VOLUME_T_COLUMNS = ("Объем, т", "Объём, т", "volume_t")
REPORT_CLASSIFICATION_COLUMNS = ("Тип", "Подкатегория", "Вид", "Вид мяса", "Сегмент")
XLSX_MAX_DATA_ROWS_WITH_HEADER = 1_048_575
CSV_DECIMAL_COMMA_PROTECTED_COLUMNS = {
    "дата",
    "sku",
    "продавец",
    "категория",
    "бренд",
    "год",
    "месяц",
    "подкатегория",
    "тип",
}
RAW_EXPORT_DOUBLE_COLUMNS = (
    "Продажи, шт",
    "Продажи",
    "Выручка, руб",
    "Выручка",
    "Вес, кг",
    "Вес, кг (ед.)",
    "Вес, кг (сумм.)",
    "Объем, кг",
    "Объём, кг",
    "Объем, т",
    "Объём, т",
    "Объем",
    "Объём",
    "Средняя цена, руб",
    "Средняя цена",
    "Цена за кг",
    "Цена",
    "price",
    "revenue",
    "volume",
    "volume_kg",
    "volume_t",
    "sales",
)
RAW_EXPORT_INTEGER_COLUMNS = (
    "Количество магазинов",
    "Кол-во магазинов",
    "stores_count",
)
RAW_EXPORT_DATE_COLUMNS = (
    "Дата",
    "date",
    "data_actual_until",
)
RAW_EXPORT_TIMESTAMP_COLUMNS = (
    "Дата и время",
    "datetime",
    "timestamp",
)
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExportResult:
    output_path: Path
    file_size_bytes: int
    duration_seconds: float
    row_count: int | None
    status: str
    format: str = "csv"
    error: str | None = None


def _table_column_types(con: Any, table_name: str) -> dict[str, str]:
    return {
        str(row[0]): str(row[1]).upper()
        for row in con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table_name],
        ).fetchall()
    }


def _db_type_accepts_text(data_type: str) -> bool:
    upper_type = data_type.upper()
    return any(marker in upper_type for marker in TEXT_DB_TYPES)


def _first_existing_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    existing = set(columns)
    for column in candidates:
        if column in existing:
            return column
    return None


def _stage_column_types(con: Any, table_name: str) -> dict[str, str]:
    return {
        str(row[0]): str(row[1]).upper()
        for row in con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()
    }


def _csv_scan_sql(csv_path: Path) -> str:
    return (
        "read_csv("
        f"{sql_literal(str(csv_path))}, "
        "delim=';', header=true, all_varchar=true, null_padding=true, ignore_errors=false"
        ")"
    )


def _number_expr(column: str, *, table_alias: str | None = None) -> str:
    quoted = quote_duckdb_name(column)
    if table_alias:
        quoted = f"{table_alias}.{quoted}"
    nbsp = "\u00a0"
    return (
        "TRY_CAST("
        f"REPLACE(REPLACE(REPLACE(CAST({quoted} AS VARCHAR), '{nbsp}', ''), ' ', ''), ',', '.') "
        "AS DOUBLE)"
    )


def _positive_import_filter(columns: list[str]) -> str:
    filters: list[str] = []
    sales_column = _first_existing_column(columns, CUBE_SALES_FILTER_COLUMNS)
    volume_column = _first_existing_column(columns, CUBE_VOLUME_FILTER_COLUMNS)
    if sales_column:
        filters.append(f"{_number_expr(sales_column)} > 0")
    if volume_column:
        filters.append(f"{_number_expr(volume_column)} > 0")
    return " AND ".join(filters) if filters else "TRUE"


def _hash_expr(columns: list[str], *, project_name: str | None = None, year: int | None = None, month: int | None = None, marketplace_code: str | None = None, category_key: str | None = None) -> str:
    parts: list[str] = []
    for value in (project_name, year, month, marketplace_code, category_key):
        if value is not None:
            parts.append(sql_literal(str(value)))
    parts.extend(f"COALESCE(CAST({quote_duckdb_name(column)} AS VARCHAR), '<NULL>')" for column in columns)
    return "sha1(concat_ws('|', " + ", ".join(parts) + "))"


def _weight_column_aliases(source_columns: list[str]) -> dict[str, str]:
    if "Вес, кг (сумм.)" not in source_columns:
        return {}
    aliases = {"Вес, кг (сумм.)": "Вес, кг"}
    if "Вес, кг" in source_columns and "Вес, кг (ед.)" not in source_columns:
        aliases["Вес, кг"] = "Вес, кг (ед.)"
    return aliases


def _stage_has_text_values(con: Any, *, stage_table: str, column: str) -> bool:
    text_expr = f"NULLIF(TRIM(CAST({quote_duckdb_name(column)} AS VARCHAR)), '')"
    numeric_expr = _number_expr(column)
    row = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {quote_identifier(stage_table)}
        WHERE {text_expr} IS NOT NULL
          AND {numeric_expr} IS NULL
        """
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _cast_for_target_type(column: str, data_type: str, *, table_alias: str | None = None) -> str:
    quoted = quote_duckdb_name(column)
    if table_alias:
        quoted = f"{table_alias}.{quoted}"
    upper = data_type.upper()
    if _db_type_accepts_text(upper):
        return quoted
    if any(marker in upper for marker in ("INT", "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")):
        return f"TRY_CAST({_number_expr(column, table_alias=table_alias)} AS {upper})"
    if "DATE" in upper or "TIME" in upper:
        return f"TRY_CAST({quoted} AS {upper})"
    if "BOOL" in upper:
        return f"TRY_CAST({quoted} AS BOOLEAN)"
    return quoted


def _create_products_stage(
    con: Any,
    *,
    csv_path: Path,
    stage_table: str,
    run_id: str,
    source_file: str,
    project_name: str | None = None,
    year: int | None = None,
    month: int | None = None,
    marketplace_code: str | None = None,
    category_key: str | None = None,
) -> list[str]:
    raw_table = f"{stage_table}_raw"
    con.execute(f"CREATE OR REPLACE TEMP TABLE {quote_identifier(raw_table)} AS SELECT * FROM {_csv_scan_sql(csv_path)}")
    raw_columns = list(_stage_column_types(con, raw_table))
    source_columns = [column for column in raw_columns if column not in IMPORT_METADATA_COLUMNS]
    if not source_columns:
        raise ValueError(f"В файле нет колонок для загрузки в DuckDB: {csv_path}")

    aliases = _weight_column_aliases(source_columns)
    select_parts = [
        f"{quote_duckdb_name(column)} AS {quote_duckdb_name(aliases[column])}" if column in aliases else quote_duckdb_name(column)
        for column in source_columns
    ]
    select_parts.extend(
        [
            f"{sql_literal(run_id)} AS {quote_duckdb_name('__run_id')}",
            f"{sql_literal(source_file)} AS {quote_duckdb_name('__source_file')}",
            f"now() AS {quote_duckdb_name('__imported_at')}",
        ]
    )
    if project_name is not None:
        select_parts.append(f"{sql_literal(project_name)} AS {quote_duckdb_name('__project_name')}")
    if year is not None:
        select_parts.append(f"{int(year)} AS {quote_duckdb_name('__year')}")
    if month is not None:
        select_parts.append(f"{int(month)} AS {quote_duckdb_name('__month')}")
    if marketplace_code is not None:
        select_parts.append(f"{sql_literal(marketplace_code)} AS {quote_duckdb_name('__marketplace_code')}")
    if category_key is not None:
        select_parts.append(f"{sql_literal(category_key)} AS {quote_duckdb_name('__category_key')}")
    select_parts.append(
        f"{_hash_expr(source_columns, project_name=project_name, year=year, month=month, marketplace_code=marketplace_code, category_key=category_key)} "
        f"AS {quote_duckdb_name('__row_hash')}"
    )
    positive_filter = _positive_import_filter(source_columns)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {quote_identifier(stage_table)} AS
        SELECT * EXCLUDE (__row_number)
        FROM (
            SELECT
                {", ".join(select_parts)},
                ROW_NUMBER() OVER (
                    PARTITION BY {_hash_expr(source_columns, project_name=project_name, year=year, month=month, marketplace_code=marketplace_code, category_key=category_key)}
                    ORDER BY {_hash_expr(source_columns, project_name=project_name, year=year, month=month, marketplace_code=marketplace_code, category_key=category_key)}
                ) AS __row_number
            FROM {quote_identifier(raw_table)}
            WHERE {positive_filter}
        )
        WHERE __row_number = 1
        """
    )
    return list(_stage_column_types(con, stage_table))


def _ensure_table_accepts_stage_columns(con: Any, *, table_name: str, quoted_table: str, stage_table: str, stage_columns: list[str]) -> dict[str, str]:
    column_types = _table_column_types(con, table_name)
    stage_types = _stage_column_types(con, stage_table)
    for column in stage_columns:
        column_name = str(column)
        if column_name not in column_types:
            con.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {quote_duckdb_name(column_name)} {stage_types[column_name]}")
            column_types[column_name] = stage_types[column_name]
            continue
        if _db_type_accepts_text(column_types[column_name]):
            continue
        if column_name.startswith("__"):
            continue
        if _stage_has_text_values(con, stage_table=stage_table, column=column_name):
            con.execute(f"ALTER TABLE {quoted_table} ALTER COLUMN {quote_duckdb_name(column_name)} TYPE VARCHAR")
            column_types[column_name] = "VARCHAR"
    return column_types


def _insert_stage_sql(*, quoted_table: str, stage_table: str, columns: list[str], target_types: dict[str, str], deduplicate: bool) -> str:
    quoted_columns = ", ".join(quote_duckdb_name(column) for column in columns)
    selected_columns = ", ".join(f"{_cast_for_target_type(column, target_types[column], table_alias='s')} AS {quote_duckdb_name(column)}" for column in columns)
    dedupe_sql = ""
    if deduplicate:
        dedupe_sql = f"""
        WHERE NOT EXISTS (
            SELECT 1
            FROM {quoted_table} t
            WHERE t.{quote_duckdb_name('__row_hash')} = s.{quote_duckdb_name('__row_hash')}
        )
        """
    return f"""
        INSERT INTO {quoted_table} ({quoted_columns})
        SELECT {selected_columns}
        FROM {quote_identifier(stage_table)} s
        {dedupe_sql}
    """


def _stage_count(con: Any, stage_table: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM {quote_identifier(stage_table)}").fetchone()
    return int(row[0]) if row else 0


def _csv_header_prefix(columns: list[str], *, delimiter: str = ";") -> str:
    buffer = io.StringIO()
    csv.writer(buffer, delimiter=delimiter, lineterminator="\n").writerow(columns)
    return "\ufeff" + buffer.getvalue()


def _clean_copy_query(query: str) -> str:
    cleaned = query.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()
    if not cleaned:
        raise ValueError("SQL-запрос для CSV-экспорта пуст.")
    return cleaned


def _csv_decimal_column_key(column: str) -> str:
    return str(column).strip().casefold()


def _csv_decimal_comma_expr(column: str) -> str:
    quoted = quote_duckdb_name(column)
    if _csv_decimal_column_key(column) in CSV_DECIMAL_COMMA_PROTECTED_COLUMNS:
        return f"{quoted} AS {quoted}"
    return (
        "CASE "
        f"WHEN {quoted} IS NULL THEN NULL "
        f"ELSE regexp_replace(CAST({quoted} AS VARCHAR), {sql_literal(r'([0-9])\.([0-9])')}, {sql_literal(r'\1,\2')}, 'g') "
        f"END AS {quoted}"
    )


def _csv_decimal_comma_query(query: str, columns: list[str]) -> str:
    select_sql = ", ".join(_csv_decimal_comma_expr(column) for column in columns)
    return f"SELECT {select_sql} FROM ({query}) AS csv_decimal_source"


def _copy_row_count(rows: list[Any]) -> int | None:
    if len(rows) != 1:
        return None
    row = rows[0]
    if not row:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def _copy_first_row_count(row: Any) -> int | None:
    if not row:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def _normalize_query_params(params: dict[str, Any] | list[Any] | tuple[Any, ...] | None) -> dict[str, Any] | list[Any]:
    if params is None:
        return []
    if isinstance(params, dict):
        return dict(params)
    return list(params)


def _validate_csv_delimiter(delimiter: str) -> str:
    clean = str(delimiter or ";")
    if len(clean) != 1 or clean in {"\n", "\r"}:
        raise ValueError("CSV-разделитель должен быть одним символом без перевода строки.")
    return clean


def _validate_sheet_name(sheet_name: str) -> str:
    clean = str(sheet_name or "Data").strip() or "Data"
    for forbidden in ("\\", "/", "?", "*", "[", "]", ":"):
        clean = clean.replace(forbidden, "_")
    return clean[:31] or "Data"


def _query_params_with_limit(
    params: dict[str, Any] | list[Any],
    *,
    limit: int,
    offset: int,
) -> tuple[str, dict[str, Any] | list[Any]]:
    if isinstance(params, dict):
        next_params = dict(params)
        next_params["__duckdb_limit"] = int(limit)
        next_params["__duckdb_offset"] = int(offset)
        return " LIMIT $__duckdb_limit OFFSET $__duckdb_offset", next_params
    return " LIMIT ? OFFSET ?", [*params, int(limit), int(offset)]


def _xlsx_cell_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


def _raw_export_column_expr(column: str) -> str:
    quoted = quote_duckdb_name(column)
    alias = quote_duckdb_name(column)
    lower = column.casefold()
    if column in RAW_EXPORT_INTEGER_COLUMNS or lower in {item.casefold() for item in RAW_EXPORT_INTEGER_COLUMNS}:
        return f"TRY_CAST({_number_expr(column)} AS BIGINT) AS {alias}"
    if column in RAW_EXPORT_DOUBLE_COLUMNS or lower in {item.casefold() for item in RAW_EXPORT_DOUBLE_COLUMNS}:
        return f"TRY_CAST({_number_expr(column)} AS DOUBLE) AS {alias}"
    if column in RAW_EXPORT_TIMESTAMP_COLUMNS or lower in {item.casefold() for item in RAW_EXPORT_TIMESTAMP_COLUMNS}:
        return f"TRY_CAST(NULLIF(TRIM(CAST({quoted} AS VARCHAR)), '') AS TIMESTAMP) AS {alias}"
    if column in RAW_EXPORT_DATE_COLUMNS or lower in {item.casefold() for item in RAW_EXPORT_DATE_COLUMNS} or ("дата" in lower and "время" not in lower):
        return f"NULLIF(TRIM(CAST({quoted} AS VARCHAR)), '') AS {alias}"
    return quoted


def _period_index_to_label(index: int) -> str:
    year = (index - 1) // 12
    month = (index - 1) % 12 + 1
    return f"{year}-{month:02d}"


def _max_iso(left: Any, right: Any) -> Any:
    if left is None:
        return right
    if right is None:
        return left
    return max(str(left), str(right))


class DuckDbAppRepository:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._lock = RLock()

    def _duckdb_temp_directory(self) -> Path | None:
        return resolve_duckdb_temp_directory(fallback_directory=self.settings.project_root / "data" / "duckdb_tmp")

    def ensure_ready(self) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            self._migrate_weight_columns(con, self.settings.products_table)

    def _migrate_weight_columns(self, con: Any, table_name: str) -> None:
        if not table_exists(con, table_name):
            return
        columns = _table_column_types(con, table_name)
        if "Вес, кг" not in columns:
            return
        quoted_table = quote_identifier(table_name)
        if "Вес, кг (ед.)" not in columns:
            con.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {quote_duckdb_name('Вес, кг (ед.)')} DOUBLE")
        if "Вес, кг (сумм.)" in columns:
            con.execute(
                f"""
                UPDATE {quoted_table}
                SET
                    {quote_duckdb_name('Вес, кг (ед.)')} = COALESCE({quote_duckdb_name('Вес, кг (ед.)')}, TRY_CAST({quote_duckdb_name('Вес, кг')} AS DOUBLE)),
                    {quote_duckdb_name('Вес, кг')} = COALESCE(TRY_CAST({quote_duckdb_name('Вес, кг (сумм.)')} AS DOUBLE), TRY_CAST({quote_duckdb_name('Вес, кг')} AS DOUBLE))
                WHERE {quote_duckdb_name('Вес, кг (сумм.)')} IS NOT NULL OR {quote_duckdb_name('Вес, кг (ед.)')} IS NULL
                """
            )
            con.execute(f"ALTER TABLE {quoted_table} DROP COLUMN {quote_duckdb_name('Вес, кг (сумм.)')}")

    def _fetch_records(
        self,
        query: str,
        params: list[Any] | None = None,
        *,
        read_only: bool = False,
        temp_directory: Path | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock, connect(self.settings.db_path, read_only=read_only, temp_directory=temp_directory) as con:
            if not read_only:
                apply_migrations(con)
            result = con.execute(query, params or [])
            columns = [col[0] for col in result.description]
            return clean_records([dict(zip(columns, row)) for row in result.fetchall()])

    def _fetch_one(
        self,
        query: str,
        params: list[Any] | None = None,
        *,
        read_only: bool = False,
        temp_directory: Path | None = None,
    ) -> dict[str, Any] | None:
        rows = self._fetch_records(query, params, read_only=read_only, temp_directory=temp_directory)
        return rows[0] if rows else None

    def create_run(
        self,
        *,
        run_id: str,
        project_name: str,
        steps: str,
        source: str,
        schedule_id: str | None,
        workdir: Path,
        config_path: Path,
        rules_path: Path,
        db_path: Path,
        products_table: str,
        write_xlsx: bool,
        max_weight_kg: float,
        fill_unclassified: dict[str, Any] | None,
    ) -> dict[str, Any]:
        payload = json.dumps(fill_unclassified, ensure_ascii=False) if fill_unclassified else None
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO app_runs (
                    run_id, project_name, steps, status, source, schedule_id, workdir,
                    config_path, rules_path, db_path, products_table, write_xlsx,
                    max_weight_kg, fill_unclassified_json
                )
                VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    project_name,
                    steps,
                    source,
                    schedule_id,
                    str(workdir),
                    str(config_path),
                    str(rules_path),
                    str(db_path),
                    products_table,
                    write_xlsx,
                    max_weight_kg,
                    payload,
                ],
            )
        self.add_event(run_id, "info", "Прогон поставлен в очередь", {"steps": steps, "source": source})
        return self.get_run(run_id) or {}

    def list_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._fetch_records(
            """
            SELECT *
            FROM app_runs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [int(limit)],
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._fetch_one("SELECT * FROM app_runs WHERE run_id = ?", [run_id])

    def set_setting(self, key: str, value: str | None) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (?, ?, now())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                """,
                [key, value],
            )

    def get_setting(self, key: str) -> str | None:
        row = self._fetch_one("SELECT value FROM app_settings WHERE key = ?", [key])
        return str(row["value"]) if row and row.get("value") is not None else None

    def list_project_database_summaries(self, *, table_name: str) -> list[dict[str, Any]]:
        names: set[str] = set()
        summaries: dict[str, dict[str, Any]] = {}

        def ensure(project_name: str) -> dict[str, Any]:
            if project_name not in summaries:
                summaries[project_name] = {
                    "project_name": project_name,
                    "pipeline_runs_count": 0,
                    "app_runs_count": 0,
                    "tasks_count": 0,
                    "cube_slices_count": 0,
                    "cube_rows_count": 0,
                    "product_rows_count": 0,
                    "schedules_count": 0,
                    "first_period": None,
                    "latest_period": None,
                    "latest_activity": None,
                }
            return summaries[project_name]

        current_project = self.get_setting("project_name")
        if current_project:
            names.add(current_project)

        for table, column in (
            ("pipeline_runs", "project_name"),
            ("download_tasks", "project_name"),
            ("cube_registry", "project_name"),
            ("app_runs", "project_name"),
            ("app_schedules", "project_name"),
            ("pipeline_loads", "project_name"),
        ):
            for row in self._fetch_records(
                f"""
                SELECT DISTINCT {column} AS project_name
                FROM {table}
                WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS VARCHAR)) <> ''
                """
            ):
                names.add(str(row["project_name"]))

        if self.table_exists(table_name):
            columns = self.table_columns(table_name)
            if "__project_name" in columns:
                quoted_table = quote_identifier(table_name)
                for row in self._fetch_records(
                    f"""
                    SELECT DISTINCT {quote_duckdb_name('__project_name')} AS project_name
                    FROM {quoted_table}
                    WHERE {quote_duckdb_name('__project_name')} IS NOT NULL
                      AND TRIM(CAST({quote_duckdb_name('__project_name')} AS VARCHAR)) <> ''
                    """
                ):
                    names.add(str(row["project_name"]))

        for name in names:
            ensure(name)

        for row in self._fetch_records(
            """
            SELECT project_name, COUNT(*) AS pipeline_runs_count, MAX(updated_at) AS latest_activity
            FROM pipeline_runs
            GROUP BY project_name
            """
        ):
            item = ensure(str(row["project_name"]))
            item["pipeline_runs_count"] = int(row["pipeline_runs_count"] or 0)
            item["latest_activity"] = row.get("latest_activity")

        for row in self._fetch_records(
            """
            SELECT project_name, COUNT(*) AS app_runs_count, MAX(COALESCE(finished_at, started_at, created_at)) AS latest_activity
            FROM app_runs
            GROUP BY project_name
            """
        ):
            item = ensure(str(row["project_name"]))
            item["app_runs_count"] = int(row["app_runs_count"] or 0)
            item["latest_activity"] = _max_iso(item.get("latest_activity"), row.get("latest_activity"))

        for row in self._fetch_records(
            """
            SELECT project_name, COUNT(*) AS tasks_count, MAX(updated_at) AS latest_activity
            FROM download_tasks
            GROUP BY project_name
            """
        ):
            item = ensure(str(row["project_name"]))
            item["tasks_count"] = int(row["tasks_count"] or 0)
            item["latest_activity"] = _max_iso(item.get("latest_activity"), row.get("latest_activity"))

        for row in self._fetch_records(
            """
            SELECT
                project_name,
                COUNT(*) AS cube_slices_count,
                SUM(rows_count) AS cube_rows_count,
                MIN(year * 12 + month) AS first_period,
                MAX(year * 12 + month) AS latest_period,
                MAX(saved_to_db_at) AS latest_activity
            FROM cube_registry
            GROUP BY project_name
            """
        ):
            item = ensure(str(row["project_name"]))
            item["cube_slices_count"] = int(row["cube_slices_count"] or 0)
            item["cube_rows_count"] = int(row["cube_rows_count"] or 0)
            item["first_period"] = _period_index_to_label(int(row["first_period"])) if row.get("first_period") else None
            item["latest_period"] = _period_index_to_label(int(row["latest_period"])) if row.get("latest_period") else None
            item["latest_activity"] = _max_iso(item.get("latest_activity"), row.get("latest_activity"))

        for row in self._fetch_records(
            """
            SELECT project_name, COUNT(*) AS schedules_count, MAX(updated_at) AS latest_activity
            FROM app_schedules
            GROUP BY project_name
            """
        ):
            item = ensure(str(row["project_name"]))
            item["schedules_count"] = int(row["schedules_count"] or 0)
            item["latest_activity"] = _max_iso(item.get("latest_activity"), row.get("latest_activity"))

        if self.table_exists(table_name):
            columns = self.table_columns(table_name)
            if "__project_name" in columns:
                quoted_table = quote_identifier(table_name)
                for row in self._fetch_records(
                    f"""
                    SELECT {quote_duckdb_name('__project_name')} AS project_name, COUNT(*) AS product_rows_count
                    FROM {quoted_table}
                    GROUP BY {quote_duckdb_name('__project_name')}
                    """
                ):
                    item = ensure(str(row["project_name"]))
                    item["product_rows_count"] = int(row["product_rows_count"] or 0)

        return sorted(summaries.values(), key=lambda item: str(item["project_name"]).casefold())

    def delete_project_records(self, *, project_name: str, table_name: str) -> dict[str, int]:
        counts: dict[str, int] = {
            "pipeline_runs": 0,
            "download_tasks": 0,
            "cube_registry": 0,
            "app_runs": 0,
            "app_run_steps": 0,
            "app_run_events": 0,
            "app_schedules": 0,
            "pipeline_loads": 0,
            "product_rows": 0,
        }

        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            counts["app_run_steps"] = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM app_run_steps
                    WHERE run_id IN (SELECT run_id FROM app_runs WHERE project_name = ?)
                    """,
                    [project_name],
                ).fetchone()[0]
            )
            counts["app_run_events"] = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM app_run_events
                    WHERE run_id IN (SELECT run_id FROM app_runs WHERE project_name = ?)
                    """,
                    [project_name],
                ).fetchone()[0]
            )
            for table in (
                "pipeline_runs",
                "download_tasks",
                "cube_registry",
                "app_runs",
                "app_schedules",
                "pipeline_loads",
            ):
                counts[table] = int(con.execute(f"SELECT COUNT(*) FROM {table} WHERE project_name = ?", [project_name]).fetchone()[0])

            quoted_table = quote_identifier(table_name)
            products_exists = bool(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name = ?
                    """,
                    [table_name],
                ).fetchone()[0]
            )
            if products_exists:
                product_columns = {
                    str(row[0])
                    for row in con.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'main' AND table_name = ?
                        """,
                        [table_name],
                    ).fetchall()
                }
                if "__project_name" in product_columns:
                    counts["product_rows"] = int(
                        con.execute(
                            f"SELECT COUNT(*) FROM {quoted_table} WHERE {quote_duckdb_name('__project_name')} = ?",
                            [project_name],
                        ).fetchone()[0]
                    )

            con.execute(
                """
                DELETE FROM app_run_steps
                WHERE run_id IN (SELECT run_id FROM app_runs WHERE project_name = ?)
                """,
                [project_name],
            )
            con.execute(
                """
                DELETE FROM app_run_events
                WHERE run_id IN (SELECT run_id FROM app_runs WHERE project_name = ?)
                """,
                [project_name],
            )
            for table in (
                "download_tasks",
                "pipeline_runs",
                "cube_registry",
                "app_schedules",
                "pipeline_loads",
                "app_runs",
            ):
                con.execute(f"DELETE FROM {table} WHERE project_name = ?", [project_name])

            if products_exists and counts["product_rows"]:
                con.execute(
                    f"DELETE FROM {quoted_table} WHERE {quote_duckdb_name('__project_name')} = ?",
                    [project_name],
                )

        return counts

    def delete_pipeline_run(self, run_id: str) -> dict[str, int]:
        counts = {"pipeline_runs": 0, "download_tasks": 0}
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            counts["pipeline_runs"] = int(con.execute("SELECT COUNT(*) FROM pipeline_runs WHERE id = ?", [run_id]).fetchone()[0])
            counts["download_tasks"] = int(con.execute("SELECT COUNT(*) FROM download_tasks WHERE run_id = ?", [run_id]).fetchone()[0])
            con.execute("DELETE FROM download_tasks WHERE run_id = ?", [run_id])
            con.execute("DELETE FROM pipeline_runs WHERE id = ?", [run_id])
        return counts

    def upsert_category(self, category: dict[str, Any]) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO app_category_catalog (
                    category_id, category_name, marketplace, mp_code, path,
                    filter_json, fbs, period_from, period_to, source_file, is_active, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (category_id) DO UPDATE SET
                    category_name = EXCLUDED.category_name,
                    marketplace = EXCLUDED.marketplace,
                    mp_code = EXCLUDED.mp_code,
                    path = EXCLUDED.path,
                    filter_json = EXCLUDED.filter_json,
                    fbs = EXCLUDED.fbs,
                    period_from = EXCLUDED.period_from,
                    period_to = EXCLUDED.period_to,
                    source_file = EXCLUDED.source_file,
                    is_active = EXCLUDED.is_active,
                    updated_at = now()
                """,
                [
                    category["category_id"],
                    category["category_name"],
                    category["marketplace"],
                    category["mp_code"],
                    category["path"],
                    category.get("filter_json"),
                    category.get("fbs"),
                    category.get("period_from"),
                    category.get("period_to"),
                    category.get("source_file"),
                    bool(category.get("is_active", True)),
                ],
            )

    def replace_categories(self, categories: list[dict[str, Any]], *, source_file: str) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute("UPDATE app_category_catalog SET is_active = false, updated_at = now()")
        for category in categories:
            self.upsert_category(category)

    def list_categories(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM app_category_catalog
        """
        if active_only:
            query += " WHERE is_active = true"
        query += " ORDER BY category_name, marketplace, period_from NULLS FIRST, period_to NULLS LAST, path"
        return self._fetch_records(query)

    def get_categories_by_ids(self, category_ids: list[str]) -> list[dict[str, Any]]:
        if not category_ids:
            return []
        placeholders = ", ".join("?" for _ in category_ids)
        return self._fetch_records(
            f"""
            SELECT *
            FROM app_category_catalog
            WHERE category_id IN ({placeholders})
            ORDER BY category_name, marketplace, period_from NULLS FIRST, period_to NULLS LAST, path
            """,
            list(category_ids),
        )

    def create_pipeline_run(
        self,
        *,
        run_id: str,
        project_name: str,
        run_type: str,
        period_from: str,
        period_to: str,
        selected_category_ids: list[str],
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO pipeline_runs (
                    id, project_name, run_type, period_from, period_to, status,
                    selected_category_ids_json, settings_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'planned', ?, ?, now())
                """,
                [
                    run_id,
                    project_name,
                    run_type,
                    period_from,
                    period_to,
                    json.dumps(selected_category_ids, ensure_ascii=False),
                    json.dumps(settings, ensure_ascii=False),
                ],
            )
        return self.get_pipeline_run(run_id) or {}

    def get_pipeline_run(self, run_id: str) -> dict[str, Any] | None:
        return self._fetch_one("SELECT * FROM pipeline_runs WHERE id = ?", [run_id])

    def list_pipeline_runs(self, *, project_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        query = "SELECT * FROM pipeline_runs"
        params: list[Any] = []
        if project_name:
            query += " WHERE project_name = ?"
            params.append(project_name)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        return self._fetch_records(query, params)

    def update_pipeline_run(self, run_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "status",
            "total_tasks",
            "completed_tasks",
            "failed_tasks",
            "current_step",
            "pause_requested",
            "stop_requested",
            "started_at",
            "finished_at",
        }
        assignments = [key for key in values if key in allowed]
        if not assignments:
            return self.get_pipeline_run(run_id)
        sql = ", ".join(f"{key} = ?" for key in assignments)
        params = [values[key] for key in assignments]
        params.append(run_id)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(f"UPDATE pipeline_runs SET {sql}, updated_at = now() WHERE id = ?", params)
        return self.get_pipeline_run(run_id)

    def request_pipeline_pause(self, run_id: str) -> dict[str, Any] | None:
        return self.update_pipeline_run(run_id, {"pause_requested": True, "status": "pausing"})

    def request_pipeline_stop(self, run_id: str) -> dict[str, Any] | None:
        return self.update_pipeline_run(run_id, {"stop_requested": True, "pause_requested": False, "status": "stopping"})

    def clear_pipeline_pause(self, run_id: str) -> dict[str, Any] | None:
        return self.update_pipeline_run(run_id, {"pause_requested": False})

    def clear_pipeline_control(self, run_id: str) -> dict[str, Any] | None:
        return self.update_pipeline_run(run_id, {"pause_requested": False, "stop_requested": False})

    def is_pipeline_pause_requested(self, run_id: str) -> bool:
        row = self._fetch_one("SELECT pause_requested FROM pipeline_runs WHERE id = ?", [run_id])
        return bool(row and row.get("pause_requested"))

    def is_pipeline_stop_requested(self, run_id: str) -> bool:
        row = self._fetch_one("SELECT stop_requested FROM pipeline_runs WHERE id = ?", [run_id])
        return bool(row and row.get("stop_requested"))

    def refresh_pipeline_run_counts(self, run_id: str) -> dict[str, Any] | None:
        row = self._fetch_one(
            """
            SELECT
                COUNT(*) AS total_tasks,
                SUM(CASE WHEN status IN ('saved_to_db', 'skipped', 'no_data') THEN 1 ELSE 0 END) AS completed_tasks,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_tasks
            FROM download_tasks
            WHERE run_id = ?
            """,
            [run_id],
        )
        return self.update_pipeline_run(
            run_id,
            {
                "total_tasks": int(row["total_tasks"] or 0) if row else 0,
                "completed_tasks": int(row["completed_tasks"] or 0) if row else 0,
                "failed_tasks": int(row["failed_tasks"] or 0) if row else 0,
            },
        )

    def upsert_download_task(self, task: dict[str, Any]) -> dict[str, Any]:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO download_tasks (
                    id, run_id, project_name, marketplace, marketplace_code,
                    category_name, category_path, category_id, category_key,
                    year, month, status, download_status, process_status,
                    classify_status, save_status, raw_file_path, processed_file_path,
                    classified_file_path, rows_count, error_message, task_hash, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (project_name, marketplace_code, category_key, year, month) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    marketplace = EXCLUDED.marketplace,
                    category_name = EXCLUDED.category_name,
                    category_path = EXCLUDED.category_path,
                    category_id = EXCLUDED.category_id,
                    status = EXCLUDED.status,
                    download_status = EXCLUDED.download_status,
                    process_status = EXCLUDED.process_status,
                    classify_status = EXCLUDED.classify_status,
                    save_status = EXCLUDED.save_status,
                    raw_file_path = EXCLUDED.raw_file_path,
                    processed_file_path = EXCLUDED.processed_file_path,
                    classified_file_path = EXCLUDED.classified_file_path,
                    rows_count = EXCLUDED.rows_count,
                    error_message = EXCLUDED.error_message,
                    task_hash = EXCLUDED.task_hash,
                    updated_at = now()
                """,
                [
                    task["id"],
                    task["run_id"],
                    task["project_name"],
                    task["marketplace"],
                    task["marketplace_code"],
                    task["category_name"],
                    task["category_path"],
                    task["category_id"],
                    task["category_key"],
                    int(task["year"]),
                    int(task["month"]),
                    task["status"],
                    task["download_status"],
                    task["process_status"],
                    task["classify_status"],
                    task["save_status"],
                    task.get("raw_file_path"),
                    task.get("processed_file_path"),
                    task.get("classified_file_path"),
                    int(task.get("rows_count") or 0),
                    task.get("error_message"),
                    task["task_hash"],
                ],
            )
        return self.get_download_task(str(task["id"])) or {}

    def get_download_task(self, task_id: str) -> dict[str, Any] | None:
        return self._fetch_one("SELECT * FROM download_tasks WHERE id = ?", [task_id])

    def list_download_tasks(self, *, run_id: str, task_filter: str = "all") -> list[dict[str, Any]]:
        query = "SELECT * FROM download_tasks WHERE run_id = ?"
        params: list[Any] = [run_id]
        if task_filter == "errors":
            query += " AND status = 'failed'"
        elif task_filter == "not_downloaded":
            query += " AND download_status NOT IN ('downloaded', 'skipped')"
        elif task_filter == "not_processed":
            query += " AND process_status <> 'processed'"
        elif task_filter == "not_saved":
            query += " AND save_status <> 'saved_to_db'"
        elif task_filter == "ready":
            query += " AND status IN ('processed', 'classified', 'saved_to_db')"
        query += " ORDER BY year, month, category_name, marketplace"
        return self._fetch_records(query, params)

    def list_project_download_tasks(self, *, project_name: str, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_records(
            """
            SELECT *
            FROM download_tasks
            WHERE project_name = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [project_name, int(limit)],
        )

    def update_download_task(self, task_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "status",
            "download_status",
            "process_status",
            "classify_status",
            "save_status",
            "raw_file_path",
            "processed_file_path",
            "classified_file_path",
            "rows_count",
            "error_message",
        }
        assignments = [key for key in values if key in allowed]
        if not assignments:
            return self.get_download_task(task_id)
        sql = ", ".join(f"{key} = ?" for key in assignments)
        params = [values[key] for key in assignments]
        params.append(task_id)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(f"UPDATE download_tasks SET {sql}, updated_at = now() WHERE id = ?", params)
        task = self.get_download_task(task_id)
        if task:
            self.refresh_pipeline_run_counts(str(task["run_id"]))
        return task

    def reset_failed_tasks(self, run_id: str) -> int:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            count = con.execute(
                "SELECT COUNT(*) FROM download_tasks WHERE run_id = ? AND status = 'failed'",
                [run_id],
            ).fetchone()[0]
            con.execute(
                """
                UPDATE download_tasks
                SET status = 'pending', error_message = NULL, updated_at = now()
                WHERE run_id = ? AND status = 'failed'
                """,
                [run_id],
            )
        self.refresh_pipeline_run_counts(run_id)
        return int(count)

    def reset_task_for_retry(self, task_id: str) -> dict[str, Any] | None:
        return self.update_download_task(task_id, {"status": "pending", "error_message": None})

    def get_cube_entry(
        self,
        *,
        project_name: str,
        year: int,
        month: int,
        marketplace_code: str,
        category_key: str,
    ) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT *
            FROM cube_registry
            WHERE project_name = ? AND year = ? AND month = ? AND marketplace_code = ? AND category_key = ?
            """,
            [project_name, int(year), int(month), marketplace_code, category_key],
        )

    def get_cube_entry_by_id(self, entry_id: str) -> dict[str, Any] | None:
        return self._fetch_one("SELECT * FROM cube_registry WHERE id = ?", [entry_id])

    def upsert_cube_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        entry_id = str(entry.get("id") or uuid4().hex)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO cube_registry (
                    id, project_name, year, month, marketplace, marketplace_code,
                    category_key, category_name, rows_count, saved_to_db_at,
                    source_processed_file_path, file_hash, days_loaded,
                    days_in_month, data_actual_until, data_mode, is_heavy,
                    heavy_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (project_name, year, month, marketplace_code, category_key) DO UPDATE SET
                    marketplace = EXCLUDED.marketplace,
                    category_name = EXCLUDED.category_name,
                    rows_count = EXCLUDED.rows_count,
                    saved_to_db_at = now(),
                    source_processed_file_path = EXCLUDED.source_processed_file_path,
                    file_hash = EXCLUDED.file_hash,
                    days_loaded = EXCLUDED.days_loaded,
                    days_in_month = EXCLUDED.days_in_month,
                    data_actual_until = EXCLUDED.data_actual_until,
                    data_mode = EXCLUDED.data_mode,
                    is_heavy = EXCLUDED.is_heavy,
                    heavy_reason = EXCLUDED.heavy_reason
                """,
                [
                    entry_id,
                    entry["project_name"],
                    int(entry["year"]),
                    int(entry["month"]),
                    entry["marketplace"],
                    entry["marketplace_code"],
                    entry["category_key"],
                    entry["category_name"],
                    int(entry.get("rows_count") or 0),
                    entry.get("source_processed_file_path"),
                    entry.get("file_hash"),
                    entry.get("days_loaded"),
                    entry.get("days_in_month"),
                    entry.get("data_actual_until"),
                    entry.get("data_mode") or "standard",
                    bool(entry.get("is_heavy", False)),
                    entry.get("heavy_reason"),
                ],
            )
        return self.get_cube_entry(
            project_name=str(entry["project_name"]),
            year=int(entry["year"]),
            month=int(entry["month"]),
            marketplace_code=str(entry["marketplace_code"]),
            category_key=str(entry["category_key"]),
        ) or {}

    def list_cube_registry(self, *, project_name: str, limit: int = 500) -> list[dict[str, Any]]:
        self.refresh_large_category_flags(project_name=project_name)
        return self._fetch_records(
            """
            SELECT *
            FROM cube_registry
            WHERE project_name = ?
            ORDER BY year DESC, month DESC, category_name, marketplace
            LIMIT ?
            """,
            [project_name, int(limit)],
        )

    def list_cube_registry_by_source_file(self, *, project_name: str, source_file_path: str) -> list[dict[str, Any]]:
        return self._fetch_records(
            """
            SELECT *
            FROM cube_registry
            WHERE project_name = ? AND source_processed_file_path = ?
            ORDER BY year DESC, month DESC, category_name, marketplace
            """,
            [project_name, source_file_path],
        )

    def delete_cube_entry(self, *, entry_id: str, table_name: str) -> dict[str, Any]:
        counts: dict[str, int] = {"cube_registry": 0, "product_rows": 0, "download_tasks": 0}
        run_ids: set[str] = set()
        deleted_entry: dict[str, Any] | None = None
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            result = con.execute("SELECT * FROM cube_registry WHERE id = ?", [entry_id])
            columns = [col[0] for col in result.description]
            row = result.fetchone()
            if not row:
                return {"entry_id": entry_id, "deleted": counts, "entry": None}
            deleted_entry = clean_record(dict(zip(columns, row)))
            project_name = str(deleted_entry["project_name"])
            year = int(deleted_entry["year"])
            month = int(deleted_entry["month"])
            marketplace_code = str(deleted_entry["marketplace_code"])
            category_key = str(deleted_entry["category_key"])

            products_exists = bool(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name = ?
                    """,
                    [table_name],
                ).fetchone()[0]
            )
            if products_exists:
                product_columns = {
                    str(product_column[0])
                    for product_column in con.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'main' AND table_name = ?
                        """,
                        [table_name],
                    ).fetchall()
                }
                required = {"__project_name", "__year", "__month", "__marketplace_code", "__category_key"}
                if required.issubset(product_columns):
                    quoted_table = quote_identifier(table_name)
                    where_sql = f"""
                        {quote_duckdb_name('__project_name')} = ?
                        AND CAST({quote_duckdb_name('__year')} AS INTEGER) = ?
                        AND CAST({quote_duckdb_name('__month')} AS INTEGER) = ?
                        AND {quote_duckdb_name('__marketplace_code')} = ?
                        AND {quote_duckdb_name('__category_key')} = ?
                    """
                    params = [project_name, year, month, marketplace_code, category_key]
                    counts["product_rows"] = int(
                        con.execute(f"SELECT COUNT(*) FROM {quoted_table} WHERE {where_sql}", params).fetchone()[0]
                    )
                    if counts["product_rows"]:
                        con.execute(f"DELETE FROM {quoted_table} WHERE {where_sql}", params)

            run_ids = {
                str(task_row[0])
                for task_row in con.execute(
                    """
                    SELECT DISTINCT run_id
                    FROM download_tasks
                    WHERE project_name = ?
                      AND year = ?
                      AND month = ?
                      AND marketplace_code = ?
                      AND category_key = ?
                    """,
                    [project_name, year, month, marketplace_code, category_key],
                ).fetchall()
                if task_row[0] is not None
            }
            counts["download_tasks"] = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM download_tasks
                    WHERE project_name = ?
                      AND year = ?
                      AND month = ?
                      AND marketplace_code = ?
                      AND category_key = ?
                    """,
                    [project_name, year, month, marketplace_code, category_key],
                ).fetchone()[0]
            )
            con.execute(
                """
                UPDATE download_tasks
                SET
                    save_status = 'pending',
                    status = CASE
                        WHEN classify_status = 'classified' THEN 'classified'
                        WHEN process_status = 'processed' THEN 'processed'
                        WHEN download_status = 'downloaded' THEN 'downloaded'
                        ELSE 'pending'
                    END,
                    error_message = NULL,
                    updated_at = now()
                WHERE project_name = ?
                  AND year = ?
                  AND month = ?
                  AND marketplace_code = ?
                  AND category_key = ?
                """,
                [project_name, year, month, marketplace_code, category_key],
            )
            counts["cube_registry"] = int(con.execute("SELECT COUNT(*) FROM cube_registry WHERE id = ?", [entry_id]).fetchone()[0])
            con.execute("DELETE FROM cube_registry WHERE id = ?", [entry_id])

        for run_id in run_ids:
            self.refresh_pipeline_run_counts(run_id)
        if deleted_entry:
            self.refresh_large_category_flags(
                project_name=str(deleted_entry["project_name"]),
                category_keys=[str(deleted_entry["category_key"])],
            )
        return {"entry_id": entry_id, "deleted": counts, "entry": deleted_entry}

    def mark_project_file_deleted(self, *, project_name: str, file_path: str, file_kind: str) -> dict[str, int]:
        if file_kind not in {"raw", "processed", "classified"}:
            return {"download_tasks": 0}

        path_column = {
            "raw": "raw_file_path",
            "processed": "processed_file_path",
            "classified": "classified_file_path",
        }[file_kind]
        run_ids: set[str] = set()
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            run_ids = {
                str(task_row[0])
                for task_row in con.execute(
                    f"""
                    SELECT DISTINCT run_id
                    FROM download_tasks
                    WHERE project_name = ? AND {path_column} = ?
                    """,
                    [project_name, file_path],
                ).fetchall()
                if task_row[0] is not None
            }
            count = int(
                con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM download_tasks
                    WHERE project_name = ? AND {path_column} = ?
                    """,
                    [project_name, file_path],
                ).fetchone()[0]
            )
            if not count:
                return {"download_tasks": 0}

            if file_kind == "raw":
                con.execute(
                    """
                    UPDATE download_tasks
                    SET
                        download_status = 'pending',
                        status = CASE
                            WHEN save_status = 'saved_to_db' THEN status
                            WHEN classify_status = 'classified' THEN status
                            WHEN process_status = 'processed' THEN status
                            ELSE 'pending'
                        END,
                        updated_at = now()
                    WHERE project_name = ? AND raw_file_path = ?
                    """,
                    [project_name, file_path],
                )
            elif file_kind == "processed":
                con.execute(
                    """
                    UPDATE download_tasks
                    SET
                        process_status = 'pending',
                        classify_status = CASE WHEN classify_status = 'classified' THEN classify_status ELSE 'pending' END,
                        save_status = CASE WHEN save_status = 'saved_to_db' THEN save_status ELSE 'pending' END,
                        status = CASE
                            WHEN save_status = 'saved_to_db' THEN status
                            WHEN classify_status = 'classified' THEN status
                            WHEN download_status = 'downloaded' THEN 'downloaded'
                            ELSE 'pending'
                        END,
                        updated_at = now()
                    WHERE project_name = ? AND processed_file_path = ?
                    """,
                    [project_name, file_path],
                )
            else:
                con.execute(
                    """
                    UPDATE download_tasks
                    SET
                        classify_status = 'pending',
                        save_status = 'pending',
                        status = CASE
                            WHEN process_status = 'processed' THEN 'processed'
                            WHEN download_status = 'downloaded' THEN 'downloaded'
                            ELSE 'pending'
                        END,
                        updated_at = now()
                    WHERE project_name = ? AND classified_file_path = ?
                    """,
                    [project_name, file_path],
                )

        for run_id in run_ids:
            self.refresh_pipeline_run_counts(run_id)
        return {"download_tasks": count}

    def latest_cube_month(self, *, project_name: str) -> tuple[int, int] | None:
        row = self._fetch_one(
            """
            SELECT year, month
            FROM cube_registry
            WHERE project_name = ?
            ORDER BY year DESC, month DESC
            LIMIT 1
            """,
            [project_name],
        )
        if not row:
            return None
        return int(row["year"]), int(row["month"])

    def latest_successful_run_id(self, *, project_name: str | None = None) -> str | None:
        app_where = "status = 'succeeded'"
        pipeline_where = "status IN ('succeeded', 'completed_with_errors')"
        params: list[Any] = []
        if project_name:
            app_where += " AND project_name = ?"
            pipeline_where += " AND project_name = ?"
            params.extend([project_name, project_name])
        row = self._fetch_one(
            f"""
            SELECT run_id
            FROM (
                SELECT run_id, finished_at, created_at
                FROM app_runs
                WHERE {app_where}
                UNION ALL
                SELECT id AS run_id, finished_at, created_at
                FROM pipeline_runs
                WHERE {pipeline_where}
            )
            ORDER BY finished_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            """,
            params,
        )
        return str(row["run_id"]) if row else None

    def mark_run_running(self, run_id: str) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                "UPDATE app_runs SET status = 'running', started_at = now() WHERE run_id = ?",
                [run_id],
            )

    def finish_run(self, run_id: str, status: str, *, error: str | None = None, manifest_path: str | None = None) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                UPDATE app_runs
                SET status = ?, error = ?, manifest_path = COALESCE(?, manifest_path), finished_at = now()
                WHERE run_id = ?
                """,
                [status, error, manifest_path, run_id],
            )

    def request_cancel(self, run_id: str) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute("UPDATE app_runs SET requested_cancel = true WHERE run_id = ?", [run_id])
        self.add_event(run_id, "warning", "Запрошена отмена прогона", None)

    def is_cancel_requested(self, run_id: str) -> bool:
        row = self._fetch_one("SELECT requested_cancel FROM app_runs WHERE run_id = ?", [run_id])
        return bool(row and row.get("requested_cancel"))

    def record_step(
        self,
        *,
        run_id: str,
        step_number: int,
        step_name: str,
        status: str,
        rows: int = 0,
        ok_count: int = 0,
        error_count: int = 0,
        skipped_count: int = 0,
        output: str | None = None,
        details: list[dict[str, Any]] | None = None,
        error: str | None = None,
        finished: bool = False,
    ) -> None:
        details_json = json.dumps(details or [], ensure_ascii=False)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                "DELETE FROM app_run_steps WHERE run_id = ? AND step_number = ?",
                [run_id, step_number],
            )
            con.execute(
                """
                INSERT INTO app_run_steps (
                    run_id, step_number, step_name, status, rows_loaded, ok_count,
                    error_count, skipped_count, output, details_json, error, finished_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? THEN now() ELSE NULL END)
                """,
                [
                    run_id,
                    step_number,
                    step_name,
                    status,
                    rows,
                    ok_count,
                    error_count,
                    skipped_count,
                    output,
                    details_json,
                    error,
                    finished,
                ],
            )

    def list_run_steps(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_records(
            """
            SELECT *
            FROM app_run_steps
            WHERE run_id = ?
            ORDER BY step_number
            """,
            [run_id],
        )

    def add_event(
        self,
        run_id: str,
        level: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event_id = uuid4().hex
        payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO app_run_events (event_id, run_id, level, message, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                [event_id, run_id, level, message, payload_json],
            )
        return self._fetch_one("SELECT * FROM app_run_events WHERE event_id = ?", [event_id]) or {}

    def list_run_events(self, run_id: str, *, after: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM app_run_events
            WHERE run_id = ?
        """
        params: list[Any] = [run_id]
        if after:
            query += " AND created_at > ?"
            params.append(after)
        query += " ORDER BY created_at ASC LIMIT ?"
        params.append(int(limit))
        return self._fetch_records(query, params)

    def table_columns(self, table_name: str) -> list[str]:
        quote_identifier(table_name)
        rows = self._fetch_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        return [str(row["column_name"]) for row in rows]

    def table_exists(self, table_name: str) -> bool:
        quote_identifier(table_name)
        row = self._fetch_one(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table_name],
        )
        return bool(row and int(row["cnt"]) > 0)

    def import_products_file(
        self,
        *,
        run_id: str,
        csv_path: Path,
        table_name: str,
        project_name: str,
        load_name: str | None = None,
    ) -> int:
        quoted_table = quote_identifier(table_name)
        stage_table = "_mpstats_app_products_stage"
        rows_loaded = 0
        with measure_duckdb_operation(
            "import_products_file",
            {"db_path": self.settings.db_path, "file_path": csv_path, "table_name": table_name, "project_name": project_name},
        ) as metrics:
            with self._lock, connect(self.settings.db_path, temp_directory=self._duckdb_temp_directory()) as con:
                apply_migrations(con)
                with duckdb_transaction(con):
                    stage_columns = _create_products_stage(
                        con,
                        csv_path=csv_path,
                        stage_table=stage_table,
                        run_id=run_id,
                        source_file=str(csv_path),
                        project_name=project_name,
                    )
                    rows_loaded = _stage_count(con, stage_table)
                    exists = table_exists(con, table_name)
                    if not exists:
                        con.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM {quote_identifier(stage_table)}")
                    else:
                        target_types = _ensure_table_accepts_stage_columns(
                            con,
                            table_name=table_name,
                            quoted_table=quoted_table,
                            stage_table=stage_table,
                            stage_columns=stage_columns,
                        )
                        con.execute(
                            _insert_stage_sql(
                                quoted_table=quoted_table,
                                stage_table=stage_table,
                                columns=stage_columns,
                                target_types=target_types,
                                deduplicate=False,
                            )
                        )

                    con.execute(
                        """
                        INSERT INTO pipeline_loads (
                            table_name, source_file, load_name, project_name, mode, rows_loaded
                        )
                        VALUES (?, ?, ?, ?, 'append', ?)
                        """,
                        [table_name, str(csv_path), load_name or f"app_run:{run_id}", project_name, rows_loaded],
                    )
            metrics["rows_affected"] = rows_loaded
        return rows_loaded

    def import_products_file_idempotent(
        self,
        *,
        run_id: str,
        csv_path: Path,
        table_name: str,
        project_name: str,
        year: int,
        month: int,
        marketplace_code: str,
        category_key: str,
        overwrite: bool = False,
        load_name: str | None = None,
    ) -> int:
        quoted_table = quote_identifier(table_name)
        inserted = 0
        stage_table = "_mpstats_app_products_stage"
        with measure_duckdb_operation(
            "import_products_file_idempotent",
            {
                "db_path": self.settings.db_path,
                "file_path": csv_path,
                "table_name": table_name,
                "project_name": project_name,
                "year": int(year),
                "month": int(month),
                "marketplace_code": marketplace_code,
                "category_key": category_key,
            },
        ) as metrics:
            with self._lock, connect(self.settings.db_path, temp_directory=self._duckdb_temp_directory()) as con:
                apply_migrations(con)
                with duckdb_transaction(con):
                    stage_columns = _create_products_stage(
                        con,
                        csv_path=csv_path,
                        stage_table=stage_table,
                        run_id=run_id,
                        source_file=str(csv_path),
                        project_name=project_name,
                        year=int(year),
                        month=int(month),
                        marketplace_code=marketplace_code,
                        category_key=category_key,
                    )
                    exists = table_exists(con, table_name)
                    if not exists:
                        con.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM {quote_identifier(stage_table)}")
                        inserted = _stage_count(con, stage_table)
                    else:
                        target_types = _ensure_table_accepts_stage_columns(
                            con,
                            table_name=table_name,
                            quoted_table=quoted_table,
                            stage_table=stage_table,
                            stage_columns=stage_columns,
                        )
                        con.execute(
                            f"""
                            DELETE FROM {quoted_table}
                            WHERE {quote_duckdb_name('__project_name')} = ?
                              AND CAST({quote_duckdb_name('__year')} AS INTEGER) = ?
                              AND CAST({quote_duckdb_name('__month')} AS INTEGER) = ?
                              AND {quote_duckdb_name('__marketplace_code')} = ?
                              AND {quote_duckdb_name('__category_key')} = ?
                            """,
                            [project_name, int(year), int(month), marketplace_code, category_key],
                        )
                        inserted = _stage_count(con, stage_table)
                        con.execute(
                            _insert_stage_sql(
                                quoted_table=quoted_table,
                                stage_table=stage_table,
                                columns=stage_columns,
                                target_types=target_types,
                                deduplicate=False,
                            )
                        )

                    con.execute(
                        """
                        INSERT INTO pipeline_loads (
                            table_name, source_file, load_name, project_name, mode, rows_loaded
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            table_name,
                            str(csv_path),
                            load_name or f"smart_pipeline:{run_id}",
                            project_name,
                            "replace" if overwrite else "replace_slice",
                            inserted,
                        ],
                    )
            metrics["rows_affected"] = inserted
        return inserted

    def search_products(
        self,
        *,
        table_name: str,
        query_text: str | None = None,
        project_name: str | None = None,
        run_id: str | None = None,
        marketplace: str | None = None,
        category: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        quote_identifier(table_name)
        if not self.table_exists(table_name):
            return {"columns": [], "rows": [], "total": 0, "run_id": run_id}

        columns = self.table_columns(table_name)
        quoted_table = quote_identifier(table_name)
        where: list[str] = []
        params: list[Any] = []

        effective_run_id = run_id
        if effective_run_id is None and "__run_id" in columns:
            effective_run_id = self.latest_successful_run_id(project_name=project_name)
        if effective_run_id and "__run_id" in columns:
            where.append(f"{quote_duckdb_name('__run_id')} = ?")
            params.append(effective_run_id)
        if project_name and "__project_name" in columns:
            where.append(f"{quote_duckdb_name('__project_name')} = ?")
            params.append(project_name)
        if marketplace and "Маркетплейс" in columns:
            where.append(f"{quote_duckdb_name('Маркетплейс')} = ?")
            params.append(marketplace)
        if category and "Категория" in columns:
            where.append(f"{quote_duckdb_name('Категория')} = ?")
            params.append(category)
        if query_text:
            searchable = [column for column in SEARCH_COLUMNS if column in columns]
            if searchable:
                needle = f"%{query_text.lower()}%"
                where.append(
                    "("
                    + " OR ".join(
                        f"lower(CAST({quote_duckdb_name(column)} AS VARCHAR)) LIKE ?" for column in searchable
                    )
                    + ")"
                )
                params.extend([needle] * len(searchable))

        where_sql = " WHERE " + " AND ".join(where) if where else ""
        order_sql = f" ORDER BY {quote_duckdb_name('__imported_at')} DESC" if "__imported_at" in columns else ""
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))

        count_row = self._fetch_one(f"SELECT COUNT(*) AS total FROM {quoted_table}{where_sql}", params, read_only=True)
        rows = self._fetch_records(
            f"SELECT * FROM {quoted_table}{where_sql}{order_sql} LIMIT {safe_limit} OFFSET {safe_offset}",
            params,
            read_only=True,
        )
        return {
            "columns": columns,
            "rows": rows,
            "total": int(count_row["total"]) if count_row else 0,
            "run_id": effective_run_id,
        }

    def refresh_large_category_flags(
        self,
        *,
        project_name: str,
        category_keys: list[str] | None = None,
        slice_limit: int = HEAVY_SLICE_ROWS_LIMIT,
        category_limit: int = HEAVY_CATEGORY_ROWS_LIMIT,
    ) -> list[dict[str, Any]]:
        where = ["project_name = ?"]
        params: list[Any] = [project_name]
        if category_keys:
            clean_keys = sorted({str(key) for key in category_keys if str(key).strip()})
            if clean_keys:
                placeholders = ", ".join("?" for _ in clean_keys)
                where.append(f"category_key IN ({placeholders})")
                params.extend(clean_keys)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            summaries = con.execute(
                f"""
                SELECT category_key, SUM(rows_count) AS category_rows_count
                FROM cube_registry
                WHERE {" AND ".join(where)}
                GROUP BY category_key
                """,
                params,
            ).fetchall()
            for category_key, total_rows in summaries:
                total = int(total_rows or 0)
                con.execute(
                    """
                    UPDATE cube_registry
                    SET
                        is_heavy = rows_count >= ? OR ? >= ?,
                        data_mode = CASE WHEN rows_count >= ? OR ? >= ? THEN 'heavy' ELSE 'standard' END,
                        heavy_reason = CASE
                            WHEN rows_count >= ? THEN 'Срез содержит ' || CAST(rows_count AS VARCHAR) || ' строк: raw XLSX отключён, используй агрегированные отчёты.'
                            WHEN ? >= ? THEN 'Категория содержит ' || CAST(? AS VARCHAR) || ' строк суммарно: используй агрегированные отчёты вместо raw XLSX.'
                            ELSE NULL
                        END
                    WHERE project_name = ? AND category_key = ?
                    """,
                    [
                        int(slice_limit),
                        total,
                        int(category_limit),
                        int(slice_limit),
                        total,
                        int(category_limit),
                        int(slice_limit),
                        total,
                        int(category_limit),
                        total,
                        project_name,
                        str(category_key),
                    ],
                )
        return self.large_category_summary(project_name=project_name)

    def large_category_summary(self, *, project_name: str) -> list[dict[str, Any]]:
        rows = self._fetch_records(
            """
            SELECT
                project_name,
                category_key,
                MIN(category_name) AS category_name,
                MIN(marketplace) AS marketplace,
                MIN(marketplace_code) AS marketplace_code,
                COUNT(*) AS slices_count,
                SUM(rows_count) AS rows_count,
                MAX(rows_count) AS max_slice_rows,
                BOOL_OR(COALESCE(is_heavy, false)) AS is_heavy,
                MAX(saved_to_db_at) AS latest_saved_at,
                MAX(reports_built_at) AS reports_built_at,
                MAX(heavy_reason) AS heavy_reason
            FROM cube_registry
            WHERE project_name = ?
            GROUP BY project_name, category_key
            ORDER BY is_heavy DESC, rows_count DESC, category_name, marketplace
            """,
            [project_name],
        )
        for row in rows:
            row["available_reports"] = ["category_month", "brand_month", "classification_month", "top_sku"]
        return rows

    def report_options(self, *, table_name: str, project_name: str) -> dict[str, Any]:
        if not self.table_exists(table_name):
            return {
                "categories": [],
                "period_from": None,
                "period_to": None,
                "columns": [],
                "warnings": [f"Таблица {table_name} не найдена."],
            }
        columns = self.table_columns(table_name)
        warnings: list[str] = []
        missing = [column for column in EXPORT_METADATA_COLUMNS if column not in columns]
        if missing:
            warnings.append(
                "Агрегированные отчёты доступны после сохранения данных через smart pipeline: не хватает "
                + ", ".join(missing)
                + "."
            )
            return {"categories": [], "period_from": None, "period_to": None, "columns": columns, "warnings": warnings}
        self.refresh_large_category_flags(project_name=project_name)
        period = self._export_period_from_cube(project_name=project_name)
        min_period = int(period["min_period"]) if period and period.get("min_period") is not None else None
        max_period = int(period["max_period"]) if period and period.get("max_period") is not None else None
        return {
            "categories": self.large_category_summary(project_name=project_name),
            "period_from": _period_index_to_label(min_period) if min_period else None,
            "period_to": _period_index_to_label(max_period) if max_period else None,
            "columns": [column for column in columns if not column.startswith("__")],
            "warnings": warnings,
        }

    def count_report_rows(
        self,
        *,
        table_name: str,
        project_name: str,
        report_type: str,
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
    ) -> int:
        query, params, _ = self._report_query_sql(
            table_name=table_name,
            project_name=project_name,
            report_type=report_type,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
        )
        row = self._fetch_one(
            f"SELECT COUNT(*) AS total FROM ({query}) report_rows",
            params,
            read_only=True,
            temp_directory=self._duckdb_temp_directory(),
        )
        return int(row["total"]) if row else 0

    def fetch_report_dataframe(
        self,
        *,
        table_name: str,
        project_name: str,
        report_type: str,
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        limit: int | None = 500,
        offset: int = 0,
    ) -> pd.DataFrame:
        query, params, _ = self._report_query_sql(
            table_name=table_name,
            project_name=project_name,
            report_type=report_type,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
        )
        if limit is not None:
            query = f"{query} LIMIT ? OFFSET ?"
            params = [*params, max(1, int(limit)), max(0, int(offset))]
        with self._lock, connect(self.settings.db_path, read_only=True, temp_directory=self._duckdb_temp_directory()) as con:
            return con.execute(query, params).fetchdf()

    def _load_excel_extension(self, con: Any) -> None:
        con.execute("INSTALL excel")
        con.execute("LOAD excel")

    def _export_flat_query_with_openpyxl(
        self,
        con: Any,
        *,
        query: str,
        output_path: Path,
        params: dict[str, Any] | list[Any],
        header: bool,
        sheet_name: str,
        row_count: int | None,
        batch_size: int = 50_000,
    ) -> int:
        try:
            from openpyxl import Workbook
        except ModuleNotFoundError as exc:
            raise ImportError("Для fallback XLSX нужен openpyxl. Установи зависимости проекта: pip install -r requirements.txt") from exc

        workbook = Workbook(write_only=True)
        worksheet = workbook.create_sheet(_validate_sheet_name(sheet_name))
        metadata = con.execute(f"SELECT * FROM ({query}) AS flat_export_source LIMIT 0", params)
        columns = [str(item[0]) for item in (metadata.description or [])]
        if header:
            worksheet.append(columns)

        written = 0
        total_rows = int(row_count or 0)
        while True:
            limit_sql, execute_params = _query_params_with_limit(params, limit=batch_size, offset=written)
            result = con.execute(
                f"SELECT * FROM ({query}) AS flat_export_source{limit_sql}",
                execute_params,
            )
            rows = result.fetchall()
            if not rows:
                break
            for row in rows:
                worksheet.append([_xlsx_cell_value(value) for value in row])
            written += len(rows)
            if total_rows and written >= total_rows:
                break

        workbook.save(output_path)
        return written

    def export_flat_query(
        self,
        query: str,
        output_path: Path,
        format: Literal["csv", "xlsx"],
        params: dict[str, Any] | list[Any] | None = None,
        delimiter: str = ";",
        header: bool = True,
        sheet_name: str = "Data",
        csv_decimal_separator: Literal["dot", "comma"] = "comma",
    ) -> ExportResult:
        target = Path(output_path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        clean_format = str(format).lower()
        if clean_format not in {"csv", "xlsx"}:
            raise ValueError("Формат flat export должен быть csv или xlsx.")
        clean_csv_decimal_separator = str(csv_decimal_separator).lower()
        if clean_csv_decimal_separator not in {"dot", "comma"}:
            raise ValueError("CSV decimal separator должен быть dot или comma.")
        clean_query = _clean_copy_query(query)
        clean_delimiter = _validate_csv_delimiter(delimiter)
        clean_sheet_name = _validate_sheet_name(sheet_name)
        execute_params = _normalize_query_params(params)
        started = time.perf_counter()
        row_count: int | None = None
        status = "success"
        fallback_error: str | None = None
        try:
            with measure_duckdb_operation(
                "export_flat_query",
                {"db_path": self.settings.db_path, "file_path": target, "format": clean_format},
            ) as metrics:
                with self._lock, connect(self.settings.db_path, read_only=True, temp_directory=self._duckdb_temp_directory()) as con:
                    if clean_format == "csv":
                        csv_query = clean_query
                        columns: list[str] | None = None
                        if header or clean_csv_decimal_separator == "comma":
                            metadata = con.execute(f"SELECT * FROM ({clean_query}) AS export_source LIMIT 0", execute_params)
                            columns = [str(item[0]) for item in (metadata.description or [])]
                        if clean_csv_decimal_separator == "comma":
                            csv_query = _csv_decimal_comma_query(clean_query, columns or [])
                        if header:
                            columns = columns or []
                            header_prefix = _csv_header_prefix(columns, delimiter=clean_delimiter)
                            options = (
                                f"(FORMAT csv, DELIMITER {sql_literal(clean_delimiter)}, HEADER false, "
                                f"PREFIX {sql_literal(header_prefix)}, SUFFIX '\n')"
                            )
                        else:
                            options = f"(FORMAT csv, DELIMITER {sql_literal(clean_delimiter)}, HEADER false)"
                        rows = con.execute(
                            f"COPY ({csv_query}) TO {sql_literal(str(target))} {options}",
                            execute_params,
                        ).fetchall()
                        row_count = _copy_row_count(rows)
                    else:
                        count_row = con.execute(f"SELECT COUNT(*) FROM ({clean_query}) AS flat_export_source", execute_params).fetchone()
                        row_count = int(count_row[0]) if count_row else 0
                        max_rows = XLSX_MAX_DATA_ROWS_WITH_HEADER if header else 1_048_576
                        if row_count > max_rows:
                            raise ValueError(f"XLSX row limit exceeded ({row_count} rows), use CSV.")
                        try:
                            self._load_excel_extension(con)
                            row = con.execute(
                                f"""
                                COPY ({clean_query}) TO {sql_literal(str(target))}
                                WITH (FORMAT xlsx, HEADER {str(bool(header)).lower()}, SHEET {sql_literal(clean_sheet_name)})
                                """,
                                execute_params,
                            ).fetchone()
                            row_count = _copy_first_row_count(row) or row_count
                        except Exception as exc:
                            fallback_error = f"{type(exc).__name__}: {exc}"
                            status = "fallback"
                            LOGGER.warning("DuckDB XLSX COPY failed, falling back to openpyxl: %s", fallback_error)
                            target.unlink(missing_ok=True)
                            row_count = self._export_flat_query_with_openpyxl(
                                con,
                                query=clean_query,
                                output_path=target,
                                params=execute_params,
                                header=header,
                                sheet_name=clean_sheet_name,
                                row_count=row_count,
                            )
                metrics["rows_exported"] = row_count
                metrics["file_size_bytes"] = target.stat().st_size if target.exists() else 0
        except Exception:
            LOGGER.exception("DuckDB flat export failed: %s", target)
            raise
        duration = time.perf_counter() - started
        return ExportResult(
            output_path=target,
            file_size_bytes=target.stat().st_size if target.exists() else 0,
            duration_seconds=duration,
            row_count=row_count,
            status=status,
            format=clean_format,
            error=fallback_error,
        )

    def export_query_to_csv(
        self,
        query: str,
        output_path: Path,
        params: dict[str, Any] | list[Any] | None = None,
        delimiter: str = ";",
        header: bool = True,
    ) -> ExportResult:
        try:
            return self.export_flat_query(query, output_path, "csv", params=params, delimiter=delimiter, header=header)
        except Exception:
            LOGGER.exception("DuckDB COPY CSV export failed: %s", output_path)
            raise

    def export_query_to_xlsx(
        self,
        query: str,
        output_path: Path,
        params: dict[str, Any] | list[Any] | None = None,
        header: bool = True,
        sheet_name: str = "Data",
    ) -> ExportResult:
        return self.export_flat_query(
            query,
            output_path,
            "xlsx",
            params=params,
            header=header,
            sheet_name=sheet_name,
        )

    def export_report_to_csv(
        self,
        *,
        table_name: str,
        target: str | Path,
        project_name: str,
        report_type: str,
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        limit: int | None = None,
    ) -> ExportResult:
        query, params, _ = self._report_query_sql(
            table_name=table_name,
            project_name=project_name,
            report_type=report_type,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
        )
        if limit is not None:
            query = f"{query} LIMIT ?"
            params = [*params, max(1, int(limit))]
        return self.export_flat_query(query, Path(target), "csv", params=params, delimiter=";", header=True, sheet_name="Data")

    def export_report_to_xlsx(
        self,
        *,
        table_name: str,
        target: str | Path,
        project_name: str,
        report_type: str,
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        limit: int | None = None,
    ) -> ExportResult:
        query, params, _ = self._report_query_sql(
            table_name=table_name,
            project_name=project_name,
            report_type=report_type,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
        )
        if limit is not None:
            query = f"{query} LIMIT ?"
            params = [*params, max(1, int(limit))]
        return self.export_flat_query(query, Path(target), "xlsx", params=params, header=True, sheet_name="Data")

    def mark_reports_built(self, *, project_name: str, category_keys: list[str] | None = None) -> None:
        where = ["project_name = ?"]
        params: list[Any] = [project_name]
        if category_keys:
            clean_keys = sorted({str(key) for key in category_keys if str(key).strip()})
            if clean_keys:
                placeholders = ", ".join("?" for _ in clean_keys)
                where.append(f"category_key IN ({placeholders})")
                params.extend(clean_keys)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(f"UPDATE cube_registry SET reports_built_at = now() WHERE {' AND '.join(where)}", params)

    def export_options(self, *, table_name: str, project_name: str) -> dict[str, Any]:
        quote_identifier(table_name)
        if not self.table_exists(table_name):
            return {
                "columns": [],
                "selected_columns": [],
                "categories": [],
                "period_from": None,
                "period_to": None,
                "warnings": [f"Таблица {table_name} не найдена."],
            }

        columns = self.table_columns(table_name)
        visible_columns = self.export_visible_columns(table_name=table_name)
        missing = [column for column in EXPORT_METADATA_COLUMNS if column not in columns]
        warnings: list[str] = []
        if missing:
            warnings.append(
                "В таблице нет metadata-колонок нового workflow: "
                + ", ".join(missing)
                + ". Выгрузка доступна после сохранения данных через smart pipeline."
            )
            return {
                "columns": visible_columns,
                "selected_columns": visible_columns,
                "categories": [],
                "period_from": None,
                "period_to": None,
                "warnings": warnings,
            }

        period = self._export_period_from_cube(project_name=project_name) or self._fetch_one(
            f"""
            SELECT
                MIN(CAST({quote_duckdb_name('__year')} AS INTEGER) * 12 + CAST({quote_duckdb_name('__month')} AS INTEGER)) AS min_period,
                MAX(CAST({quote_duckdb_name('__year')} AS INTEGER) * 12 + CAST({quote_duckdb_name('__month')} AS INTEGER)) AS max_period
            FROM {quote_identifier(table_name)}
            WHERE {quote_duckdb_name('__project_name')} = ?
            """,
            [project_name],
        )
        min_period = int(period["min_period"]) if period and period.get("min_period") is not None else None
        max_period = int(period["max_period"]) if period and period.get("max_period") is not None else None
        cube_categories = self._export_categories_from_cube(project_name=project_name)
        return {
            "columns": visible_columns,
            "selected_columns": visible_columns,
            "categories": cube_categories or self.export_categories(table_name=table_name, project_name=project_name),
            "period_from": _period_index_to_label(min_period) if min_period else None,
            "period_to": _period_index_to_label(max_period) if max_period else None,
            "warnings": warnings,
        }

    def export_visible_columns(self, *, table_name: str) -> list[str]:
        return [column for column in self.table_columns(table_name) if not column.startswith("__")]

    def export_categories(
        self,
        *,
        table_name: str,
        project_name: str,
        category_keys: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        quote_identifier(table_name)
        columns = self.table_columns(table_name)
        if any(column not in columns for column in ("__project_name", "__marketplace_code", "__category_key")):
            return []

        quoted_table = quote_identifier(table_name)
        category_name_expr = (
            f"MIN(CAST({quote_duckdb_name('Категория')} AS VARCHAR))"
            if "Категория" in columns
            else f"CAST({quote_duckdb_name('__category_key')} AS VARCHAR)"
        )
        marketplace_expr = (
            f"MIN(CAST({quote_duckdb_name('Маркетплейс')} AS VARCHAR))"
            if "Маркетплейс" in columns
            else f"CAST({quote_duckdb_name('__marketplace_code')} AS VARCHAR)"
        )
        where = [f"{quote_duckdb_name('__project_name')} = ?"]
        params: list[Any] = [project_name]
        if category_keys:
            placeholders = ", ".join("?" for _ in category_keys)
            where.append(f"{quote_duckdb_name('__category_key')} IN ({placeholders})")
            params.extend(category_keys)
        return self._fetch_records(
            f"""
            SELECT
                CAST({quote_duckdb_name('__category_key')} AS VARCHAR) AS category_key,
                {category_name_expr} AS category_name,
                CAST({quote_duckdb_name('__marketplace_code')} AS VARCHAR) AS marketplace_code,
                {marketplace_expr} AS marketplace,
                COUNT(*) AS rows_count
            FROM {quoted_table}
            WHERE {" AND ".join(where)}
            GROUP BY {quote_duckdb_name('__category_key')}, {quote_duckdb_name('__marketplace_code')}
            ORDER BY category_name, marketplace
            """,
            params,
            read_only=True,
            temp_directory=self._duckdb_temp_directory(),
        )

    def export_breakdown(
        self,
        *,
        table_name: str,
        project_name: str,
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        filters: list[dict[str, str]] | None = None,
        excluded_row_hashes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        columns = self.table_columns(table_name)
        self._require_export_metadata(columns)
        where_sql, params = self._export_where_sql(
            columns=columns,
            project_name=project_name,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
        )
        category_name_expr = (
            f"MIN(CAST({quote_duckdb_name('Категория')} AS VARCHAR))"
            if "Категория" in columns
            else f"CAST({quote_duckdb_name('__category_key')} AS VARCHAR)"
        )
        marketplace_expr = (
            f"MIN(CAST({quote_duckdb_name('Маркетплейс')} AS VARCHAR))"
            if "Маркетплейс" in columns
            else f"CAST({quote_duckdb_name('__marketplace_code')} AS VARCHAR)"
        )
        year_expr = f"CAST({quote_duckdb_name('__year')} AS INTEGER)"
        month_expr = f"CAST({quote_duckdb_name('__month')} AS INTEGER)"
        return self._fetch_records(
            f"""
            SELECT
                {year_expr} AS year,
                {month_expr} AS month,
                printf('%04d-%02d', {year_expr}, {month_expr}) AS period,
                CAST({quote_duckdb_name('__category_key')} AS VARCHAR) AS category_key,
                {category_name_expr} AS category_name,
                CAST({quote_duckdb_name('__marketplace_code')} AS VARCHAR) AS marketplace_code,
                {marketplace_expr} AS marketplace,
                COUNT(*) AS rows_count
            FROM {quote_identifier(table_name)}
            {where_sql}
            GROUP BY
                {quote_duckdb_name('__year')},
                {quote_duckdb_name('__month')},
                {quote_duckdb_name('__category_key')},
                {quote_duckdb_name('__marketplace_code')}
            ORDER BY year, month, category_name, marketplace
            """,
            params,
            read_only=True,
            temp_directory=self._duckdb_temp_directory(),
        )

    def count_export_products(
        self,
        *,
        table_name: str,
        project_name: str,
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        filters: list[dict[str, str]] | None = None,
        excluded_row_hashes: list[str] | None = None,
    ) -> int:
        columns = self.table_columns(table_name)
        self._require_export_metadata(columns)
        where_sql, params = self._export_where_sql(
            columns=columns,
            project_name=project_name,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
        )
        row = self._fetch_one(
            f"SELECT COUNT(*) AS total FROM {quote_identifier(table_name)}{where_sql}",
            params,
            read_only=True,
            temp_directory=self._duckdb_temp_directory(),
        )
        return int(row["total"]) if row else 0

    def fetch_export_products_dataframe(
        self,
        *,
        table_name: str,
        project_name: str,
        output_columns: list[str],
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        filters: list[dict[str, str]] | None = None,
        excluded_row_hashes: list[str] | None = None,
        sort_column: str | None = None,
        sort_direction: str = "asc",
        limit: int = 100,
        offset: int = 0,
        include_row_hash: bool = False,
        default_order: bool = True,
    ) -> pd.DataFrame:
        columns = self.table_columns(table_name)
        self._require_export_metadata(columns)
        selected_columns = self._safe_export_columns(columns, output_columns)
        if include_row_hash and "__row_hash" in columns and "__row_hash" not in selected_columns:
            selected_columns = [*selected_columns, "__row_hash"]
        select_sql = ", ".join(quote_duckdb_name(column) for column in selected_columns)
        where_sql, params = self._export_where_sql(
            columns=columns,
            project_name=project_name,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
        )
        order_sql = self._export_order_sql(
            columns=columns,
            sort_column=sort_column,
            sort_direction=sort_direction,
            default_order=default_order,
        )
        with self._lock, connect(self.settings.db_path, read_only=True, temp_directory=self._duckdb_temp_directory()) as con:
            return con.execute(
                f"""
                SELECT {select_sql}
                FROM {quote_identifier(table_name)}
                {where_sql}
                {order_sql}
                LIMIT ? OFFSET ?
                """,
                [*params, max(1, int(limit)), max(0, int(offset))],
            ).fetchdf()

    def _export_products_query_sql(
        self,
        *,
        table_name: str,
        project_name: str,
        output_columns: list[str],
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        filters: list[dict[str, str]] | None = None,
        excluded_row_hashes: list[str] | None = None,
        sort_column: str | None = None,
        sort_direction: str = "asc",
        default_order: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[str, list[Any], list[str]]:
        columns = self.table_columns(table_name)
        self._require_export_metadata(columns)
        selected_columns = self._safe_export_columns(columns, output_columns)
        select_sql = ", ".join(_raw_export_column_expr(column) for column in selected_columns)
        where_sql, params = self._export_where_sql(
            columns=columns,
            project_name=project_name,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
        )
        order_sql = self._export_order_sql(
            columns=columns,
            sort_column=sort_column,
            sort_direction=sort_direction,
            default_order=default_order,
        )
        query = f"""
            SELECT {select_sql}
            FROM {quote_identifier(table_name)}
            {where_sql}
            {order_sql}
        """
        if limit is not None:
            query = f"{query} LIMIT ? OFFSET ?"
            params = [*params, max(1, int(limit)), max(0, int(offset))]
        return query, params, selected_columns

    def export_products_flat(
        self,
        *,
        table_name: str,
        target: str | Path,
        format: Literal["csv", "xlsx"],
        project_name: str,
        output_columns: list[str],
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        filters: list[dict[str, str]] | None = None,
        excluded_row_hashes: list[str] | None = None,
        sort_column: str | None = None,
        sort_direction: str = "asc",
        default_order: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> ExportResult:
        query, params, _ = self._export_products_query_sql(
            table_name=table_name,
            project_name=project_name,
            output_columns=output_columns,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
            sort_column=sort_column,
            sort_direction=sort_direction,
            default_order=default_order,
            limit=limit,
            offset=offset,
        )
        return self.export_flat_query(
            query,
            Path(target),
            format,
            params=params,
            delimiter=";",
            header=True,
            sheet_name="Data",
        )

    def export_products_to_csv(
        self,
        *,
        table_name: str,
        target: str | Path,
        project_name: str,
        output_columns: list[str],
        category_keys: list[str] | None = None,
        period_from_index: int | None = None,
        period_to_index: int | None = None,
        filters: list[dict[str, str]] | None = None,
        excluded_row_hashes: list[str] | None = None,
        sort_column: str | None = None,
        sort_direction: str = "asc",
        default_order: bool = False,
    ) -> Path:
        result = self.export_products_flat(
            table_name=table_name,
            target=target,
            format="csv",
            project_name=project_name,
            output_columns=output_columns,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
            sort_column=sort_column,
            sort_direction=sort_direction,
            default_order=default_order,
        )
        return result.output_path

    def _report_query_sql(
        self,
        *,
        table_name: str,
        project_name: str,
        report_type: str,
        category_keys: list[str] | None,
        period_from_index: int | None,
        period_to_index: int | None,
    ) -> tuple[str, list[Any], list[str]]:
        columns = self.table_columns(table_name)
        self._require_export_metadata(columns)
        clean_report_type = report_type if report_type in {"category_month", "brand_month", "classification_month", "top_sku"} else "category_month"
        where_sql, params = self._export_where_sql(
            columns=columns,
            project_name=project_name,
            category_keys=category_keys,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            filters=[],
            excluded_row_hashes=[],
        )

        year_expr = f"CAST({quote_duckdb_name('__year')} AS INTEGER)"
        month_expr = f"CAST({quote_duckdb_name('__month')} AS INTEGER)"
        marketplace_expr = self._report_text_expr(columns, "Маркетплейс", fallback_column="__marketplace_code")
        category_expr = self._report_text_expr(columns, "Категория", fallback_column="__category_key")
        dimensions: list[tuple[str, str]] = [
            ("Период", f"printf('%04d-%02d', {year_expr}, {month_expr})"),
            ("Год", year_expr),
            ("Месяц", month_expr),
            ("Маркетплейс", marketplace_expr),
            ("Категория", category_expr),
        ]
        if clean_report_type == "brand_month":
            dimensions.extend(self._report_optional_dimensions(columns, ("Бренд",)))
        elif clean_report_type == "classification_month":
            dimensions.extend(self._report_optional_dimensions(columns, REPORT_CLASSIFICATION_COLUMNS))
        elif clean_report_type == "top_sku":
            dimensions.extend(self._report_optional_dimensions(columns, ("SKU", "Название", "Бренд", "Тип", "Подкатегория")))

        sales_expr = self._report_sum_expr(columns, CUBE_SALES_FILTER_COLUMNS)
        revenue_expr = self._report_sum_expr(columns, REPORT_REVENUE_COLUMNS)
        volume_expr = self._report_volume_kg_expr(columns)
        sku_expr = (
            f"COUNT(DISTINCT NULLIF(TRIM(CAST({quote_duckdb_name('SKU')} AS VARCHAR)), ''))"
            if "SKU" in columns
            else "CAST(0 AS BIGINT)"
        )
        select_parts = [f"{expr} AS {quote_duckdb_name(alias)}" for alias, expr in dimensions]
        metric_parts = [
            f"COUNT(*) AS {quote_duckdb_name('Строк')}",
            f"{sku_expr} AS {quote_duckdb_name('Уникальных SKU')}",
            f"{sales_expr} AS {quote_duckdb_name('Продажи, шт')}",
            f"{revenue_expr} AS {quote_duckdb_name('Выручка, руб')}",
            f"{volume_expr} AS {quote_duckdb_name('Объем, кг')}",
            (
                f"CASE WHEN {sales_expr} > 0 THEN {revenue_expr} / NULLIF({sales_expr}, 0) ELSE NULL END "
                f"AS {quote_duckdb_name('Средняя цена, руб')}"
            ),
            (
                f"CASE WHEN {volume_expr} > 0 THEN {revenue_expr} / NULLIF({volume_expr}, 0) ELSE NULL END "
                f"AS {quote_duckdb_name('Цена за кг')}"
            ),
        ]
        group_parts = [expr for _, expr in dimensions]
        group_sql = ", ".join(group_parts)
        order_sql = self._report_order_sql(clean_report_type, [alias for alias, _ in dimensions])
        query = f"""
            SELECT {", ".join(select_parts + metric_parts)}
            FROM {quote_identifier(table_name)}
            {where_sql}
            GROUP BY {group_sql}
            {order_sql}
        """
        return query, params, [alias for alias, _ in dimensions] + [
            "Строк",
            "Уникальных SKU",
            "Продажи, шт",
            "Выручка, руб",
            "Объем, кг",
            "Средняя цена, руб",
            "Цена за кг",
        ]

    def _report_optional_dimensions(self, columns: list[str], requested: tuple[str, ...]) -> list[tuple[str, str]]:
        return [(column, self._report_text_expr(columns, column)) for column in requested if column in columns]

    def _report_text_expr(self, columns: list[str], column: str, *, fallback_column: str | None = None) -> str:
        if column in columns:
            base = f"NULLIF(TRIM(CAST({quote_duckdb_name(column)} AS VARCHAR)), '')"
        else:
            base = "NULL"
        if fallback_column and fallback_column in columns:
            fallback = f"NULLIF(TRIM(CAST({quote_duckdb_name(fallback_column)} AS VARCHAR)), '')"
            return f"COALESCE({base}, {fallback}, 'Не заполнено')"
        return f"COALESCE({base}, 'Не заполнено')"

    def _report_sum_expr(self, columns: list[str], candidates: tuple[str, ...]) -> str:
        column = next((candidate for candidate in candidates if candidate in columns), None)
        if not column:
            return "CAST(0 AS DOUBLE)"
        return f"COALESCE(SUM({self._report_number_expr(column)}), 0)"

    def _report_volume_kg_expr(self, columns: list[str]) -> str:
        kg_column = next((candidate for candidate in REPORT_VOLUME_KG_COLUMNS if candidate in columns), None)
        if kg_column:
            return f"COALESCE(SUM({self._report_number_expr(kg_column)}), 0)"
        ton_column = next((candidate for candidate in REPORT_VOLUME_T_COLUMNS if candidate in columns), None)
        if ton_column:
            return f"COALESCE(SUM({self._report_number_expr(ton_column)} * 1000), 0)"
        return "CAST(0 AS DOUBLE)"

    @staticmethod
    def _report_number_expr(column: str) -> str:
        quoted = quote_duckdb_name(column)
        nbsp = "\u00a0"
        return (
            "TRY_CAST("
            f"REPLACE(REPLACE(REPLACE(CAST({quoted} AS VARCHAR), '{nbsp}', ''), ' ', ''), ',', '.') "
            "AS DOUBLE)"
        )

    @staticmethod
    def _report_order_sql(report_type: str, dimensions: list[str]) -> str:
        if report_type == "top_sku":
            return f"ORDER BY {quote_duckdb_name('Выручка, руб')} DESC NULLS LAST, {quote_duckdb_name('Продажи, шт')} DESC NULLS LAST"
        order_columns = [column for column in ("Год", "Месяц", "Категория", "Маркетплейс", "Бренд", "Тип", "Подкатегория") if column in dimensions]
        if not order_columns:
            return ""
        return "ORDER BY " + ", ".join(quote_duckdb_name(column) for column in order_columns)

    def _require_export_metadata(self, columns: list[str]) -> None:
        missing = [column for column in EXPORT_METADATA_COLUMNS if column not in columns]
        if missing:
            raise ValueError(
                "Для выгрузки нужны metadata-колонки нового workflow: "
                + ", ".join(missing)
                + ". Сохрани данные в БД через smart pipeline."
            )

    def _safe_export_columns(self, columns: list[str], requested: list[str] | None) -> list[str]:
        visible = [column for column in columns if not column.startswith("__")]
        selected = [column for column in (requested or visible) if column in visible]
        return selected or visible

    def _export_where_sql(
        self,
        *,
        columns: list[str],
        project_name: str,
        category_keys: list[str] | None,
        period_from_index: int | None,
        period_to_index: int | None,
        filters: list[dict[str, str]] | None,
        excluded_row_hashes: list[str] | None,
    ) -> tuple[str, list[Any]]:
        where = [f"{quote_duckdb_name('__project_name')} = ?"]
        params: list[Any] = [project_name]
        if category_keys:
            placeholders = ", ".join("?" for _ in category_keys)
            where.append(f"{quote_duckdb_name('__category_key')} IN ({placeholders})")
            params.extend(category_keys)
        period_expr = f"CAST({quote_duckdb_name('__year')} AS INTEGER) * 12 + CAST({quote_duckdb_name('__month')} AS INTEGER)"
        if period_from_index is not None:
            where.append(f"{period_expr} >= ?")
            params.append(int(period_from_index))
        if period_to_index is not None:
            where.append(f"{period_expr} <= ?")
            params.append(int(period_to_index))
        for item in filters or []:
            column = str(item.get("column") or "")
            value = str(item.get("value") or "").strip()
            match_type = str(item.get("match_type") or "contains")
            if not value or column not in columns or column.startswith("__"):
                continue
            column_expr = f"lower(CAST({quote_duckdb_name(column)} AS VARCHAR))"
            lowered = value.lower()
            if match_type == "equals":
                where.append(f"CAST({quote_duckdb_name(column)} AS VARCHAR) = ?")
                params.append(value)
            elif match_type == "not_contains":
                where.append(f"{column_expr} NOT LIKE ?")
                params.append(f"%{lowered}%")
            elif match_type == "startswith":
                where.append(f"{column_expr} LIKE ?")
                params.append(f"{lowered}%")
            elif match_type in {"gt", "gte", "lt", "lte"}:
                numeric_expr = f"TRY_CAST(REPLACE(CAST({quote_duckdb_name(column)} AS VARCHAR), ',', '.') AS DOUBLE)"
                op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[match_type]
                where.append(
                    f"{numeric_expr} IS NOT NULL AND {numeric_expr} {op} "
                    "TRY_CAST(REPLACE(CAST(? AS VARCHAR), ',', '.') AS DOUBLE)"
                )
                params.append(value)
            else:
                where.append(f"{column_expr} LIKE ?")
                params.append(f"%{lowered}%")
        if excluded_row_hashes:
            hashes = [item for item in excluded_row_hashes if item]
            if hashes:
                placeholders = ", ".join("?" for _ in hashes)
                where.append(f"{quote_duckdb_name('__row_hash')} NOT IN ({placeholders})")
                params.extend(hashes)
        return (" WHERE " + " AND ".join(where), params)

    def _export_period_from_cube(self, *, project_name: str) -> dict[str, Any] | None:
        row = self._fetch_one(
            """
            SELECT
                MIN(year * 12 + month) AS min_period,
                MAX(year * 12 + month) AS max_period,
                COUNT(*) AS slices_count
            FROM cube_registry
            WHERE project_name = ?
            """,
            [project_name],
        )
        if not row or not row.get("slices_count"):
            return None
        return row

    def _export_categories_from_cube(self, *, project_name: str, category_keys: list[str] | None = None) -> list[dict[str, Any]]:
        where = ["project_name = ?"]
        params: list[Any] = [project_name]
        if category_keys:
            placeholders = ", ".join("?" for _ in category_keys)
            where.append(f"category_key IN ({placeholders})")
            params.extend(category_keys)
        return self._fetch_records(
            f"""
            SELECT
                CAST(category_key AS VARCHAR) AS category_key,
                MIN(CAST(category_name AS VARCHAR)) AS category_name,
                CAST(marketplace_code AS VARCHAR) AS marketplace_code,
                MIN(CAST(marketplace AS VARCHAR)) AS marketplace,
                SUM(rows_count) AS rows_count
            FROM cube_registry
            WHERE {" AND ".join(where)}
            GROUP BY category_key, marketplace_code
            ORDER BY category_name, marketplace
            """,
            params,
        )

    def _export_order_sql(
        self,
        *,
        columns: list[str],
        sort_column: str | None,
        sort_direction: str,
        default_order: bool = True,
    ) -> str:
        direction = "DESC" if str(sort_direction).lower() == "desc" else "ASC"
        if sort_column and sort_column in columns and not sort_column.startswith("__"):
            return f" ORDER BY {quote_duckdb_name(sort_column)} {direction} NULLS LAST"
        if not default_order:
            return ""
        default_columns = [
            column
            for column in ("__year", "__month", "Категория", "Маркетплейс", "Название", "SKU")
            if column in columns
        ]
        if not default_columns:
            return ""
        return " ORDER BY " + ", ".join(quote_duckdb_name(column) for column in default_columns)

    def list_schedules(self) -> list[dict[str, Any]]:
        return self._fetch_records("SELECT * FROM app_schedules ORDER BY created_at DESC")

    def get_schedule(self, schedule_id: str) -> dict[str, Any] | None:
        return self._fetch_one("SELECT * FROM app_schedules WHERE schedule_id = ?", [schedule_id])

    def create_schedule(
        self,
        *,
        schedule_id: str,
        name: str,
        project_name: str,
        steps: str,
        enabled: bool,
        interval_minutes: int,
        next_run_at: datetime,
        write_xlsx: bool,
        max_weight_kg: float,
        fill_unclassified: dict[str, Any] | None,
    ) -> dict[str, Any]:
        payload = json.dumps(fill_unclassified, ensure_ascii=False) if fill_unclassified else None
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO app_schedules (
                    schedule_id, name, project_name, steps, enabled, interval_minutes,
                    next_run_at, write_xlsx, max_weight_kg, fill_unclassified_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    schedule_id,
                    name,
                    project_name,
                    steps,
                    enabled,
                    interval_minutes,
                    next_run_at,
                    write_xlsx,
                    max_weight_kg,
                    payload,
                ],
            )
        return self.get_schedule(schedule_id) or {}

    def update_schedule(self, schedule_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "name",
            "project_name",
            "steps",
            "enabled",
            "interval_minutes",
            "next_run_at",
            "last_run_at",
            "write_xlsx",
            "max_weight_kg",
            "fill_unclassified_json",
        }
        assignments = [key for key in values if key in allowed]
        if not assignments:
            return self.get_schedule(schedule_id)
        sql = ", ".join(f"{key} = ?" for key in assignments)
        params = [values[key] for key in assignments]
        params.append(schedule_id)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                f"UPDATE app_schedules SET {sql}, updated_at = now() WHERE schedule_id = ?",
                params,
            )
        return self.get_schedule(schedule_id)

    def delete_schedule(self, schedule_id: str) -> bool:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            before = con.execute("SELECT COUNT(*) FROM app_schedules WHERE schedule_id = ?", [schedule_id]).fetchone()[0]
            con.execute("DELETE FROM app_schedules WHERE schedule_id = ?", [schedule_id])
        return bool(before)

    def due_schedules(self, now: datetime) -> list[dict[str, Any]]:
        return self._fetch_records(
            """
            SELECT *
            FROM app_schedules
            WHERE enabled = true AND next_run_at <= ?
            ORDER BY next_run_at ASC
            """,
            [now],
        )

    def mark_schedule_triggered(self, schedule_id: str, *, now: datetime, interval_minutes: int) -> None:
        self.update_schedule(
            schedule_id,
            {
                "last_run_at": now,
                "next_run_at": now + timedelta(minutes=interval_minutes),
            },
        )
