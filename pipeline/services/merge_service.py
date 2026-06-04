from __future__ import annotations

import csv
from dataclasses import dataclass, field
import io
from pathlib import Path
import time
import traceback
from typing import Any

import duckdb
import pandas as pd

from pipeline.models import StepResult
from pipeline.repositories.file_repository import list_csv_files
from pipeline.repositories.sql_repository import sql_literal


MERGE_RENAME_COLUMNS = {
    "Продажи": "Продажи, шт",
    "Средняя цена": "Средняя цена, руб",
    "Выручка": "Выручка, руб",
}


@dataclass(frozen=True)
class MergeResult:
    output_path: Path
    duration_seconds: float
    rows_in: int
    rows_out: int
    duplicates_removed: int
    file_size_bytes: int
    input_files_count: int
    filtered_rows: int
    input_file_rows: list[dict[str, Any]] = field(default_factory=list)


def normalize_sales_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for old, new in {
        "Продажи": "Продажи, шт",
        "Средняя цена": "Средняя цена, руб",
        "Выручка": "Выручка, руб",
    }.items():
        if new not in out.columns and old in out.columns:
            out = out.rename(columns={old: new})

    if "Продажи, шт" not in out.columns:
        raise ValueError("В данных нет колонки 'Продажи, шт' или 'Продажи'.")

    out["Продажи, шт"] = (
        out["Продажи, шт"].astype(str).str.replace(" ", "", regex=False).str.replace("\u00a0", "", regex=False).str.replace(",", ".", regex=False)
    )
    out["Продажи, шт"] = pd.to_numeric(out["Продажи, шт"], errors="coerce").fillna(0)
    return out


def merge_dataframes(frames: list[pd.DataFrame], *, min_sales: float = 0, max_sales: float = 40_000) -> pd.DataFrame:
    if not frames:
        raise RuntimeError("Нет файлов для склейки.")
    result = pd.concat(frames, ignore_index=True)
    result = normalize_sales_column(result)
    return result[(result["Продажи, шт"] > min_sales) & (result["Продажи, шт"] < max_sales)].copy().drop_duplicates()


def merge_csv_files_with_duckdb(
    input_paths: list[Path],
    output_path: Path,
    dedup_columns: list[str] | None = None,
    delimiter: str = ";",
    encoding: str = "utf-8-sig",
    *,
    min_sales: float = 0,
    max_sales: float = 40_000,
) -> MergeResult:
    started = time.perf_counter()
    paths = [Path(path).expanduser().resolve() for path in input_paths]
    if not paths:
        raise RuntimeError("Нет файлов для склейки.")
    output = Path(output_path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)

    clean_delimiter = _validate_delimiter(delimiter)
    headers_by_path = [(path, _read_header(path, delimiter=clean_delimiter, encoding=encoding)) for path in paths]
    output_columns, rename_map = _normalized_output_columns([headers for _, headers in headers_by_path])
    if "Продажи, шт" not in output_columns:
        raise ValueError("В данных нет колонки 'Продажи, шт' или 'Продажи'.")

    effective_dedup_columns = output_columns if dedup_columns is None else list(dedup_columns)
    missing_dedup = [column for column in effective_dedup_columns if column not in output_columns]
    if missing_dedup:
        raise ValueError("Колонки дедупликации не найдены: " + ", ".join(missing_dedup))

    read_encoding = _duckdb_encoding(encoding)
    select_sql = "\nUNION ALL\n".join(
        _file_scan_sql(
            file_index=index,
            path=path,
            source_columns=headers,
            output_columns=output_columns,
            rename_map=rename_map,
            delimiter=clean_delimiter,
            encoding=read_encoding,
        )
        for index, (path, headers) in enumerate(headers_by_path)
    )
    quoted_output_columns = ", ".join(_quote_name(column) for column in output_columns)
    sales_column = _quote_name("Продажи, шт")
    filter_sql = f"COALESCE({sales_column}, 0) > ? AND COALESCE({sales_column}, 0) < ?"
    order_sql = "__source_file_index, __source_row_number"
    header_prefix = _csv_header_prefix(output_columns, delimiter=clean_delimiter)

    with duckdb.connect(":memory:") as con:
        con.execute("SET preserve_insertion_order = true")
        con.execute(f"CREATE TEMP TABLE merge_stage AS {select_sql}")
        rows_in = _fetch_int(con, "SELECT COUNT(*) FROM merge_stage")
        input_file_rows = [
            {
                "file": str(paths[int(row[0])]),
                "rows": int(row[1]),
            }
            for row in con.execute(
                """
                SELECT __source_file_index, COUNT(*) AS rows_count
                FROM merge_stage
                GROUP BY __source_file_index
                ORDER BY __source_file_index
                """
            ).fetchall()
        ]
        filtered_rows = _fetch_int(con, f"SELECT COUNT(*) FROM merge_stage WHERE {filter_sql}", [float(min_sales), float(max_sales)])
        if effective_dedup_columns:
            partition_sql = ", ".join(_quote_name(column) for column in effective_dedup_columns)
            con.execute(
                f"""
                CREATE TEMP TABLE merge_output AS
                WITH filtered AS (
                    SELECT *
                    FROM merge_stage
                    WHERE {filter_sql}
                ),
                ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {partition_sql}
                            ORDER BY {order_sql}
                        ) AS __dedup_rank
                    FROM filtered
                )
                SELECT *
                FROM ranked
                WHERE __dedup_rank = 1
                """,
                [float(min_sales), float(max_sales)],
            )
        else:
            con.execute(
                f"""
                CREATE TEMP TABLE merge_output AS
                SELECT *
                FROM merge_stage
                WHERE {filter_sql}
                """,
                [float(min_sales), float(max_sales)],
            )
        rows_out = _fetch_int(con, "SELECT COUNT(*) FROM merge_output")
        con.execute(
            f"""
            COPY (
                SELECT {quoted_output_columns}
                FROM merge_output
                ORDER BY {order_sql}
            ) TO {sql_literal(str(output))}
            (FORMAT csv, DELIMITER {sql_literal(clean_delimiter)}, HEADER false, PREFIX {sql_literal(header_prefix)}, SUFFIX '\n')
            """
        )

    return MergeResult(
        output_path=output,
        duration_seconds=time.perf_counter() - started,
        rows_in=rows_in,
        rows_out=rows_out,
        duplicates_removed=max(0, filtered_rows - rows_out),
        file_size_bytes=output.stat().st_size if output.exists() else 0,
        input_files_count=len(paths),
        filtered_rows=filtered_rows,
        input_file_rows=input_file_rows,
    )


def merge_directory(
    input_dir: str | Path,
    output_file: str | Path,
    *,
    min_sales: float = 0,
    max_sales: float = 40_000,
) -> tuple[MergeResult, StepResult]:
    result = StepResult(name="step5_merge", output=Path(output_file))
    input_paths = list_csv_files(input_dir)
    try:
        merged = merge_csv_files_with_duckdb(
            input_paths,
            Path(output_file),
            min_sales=min_sales,
            max_sales=max_sales,
        )
        result.ok = merged.input_files_count
        result.rows = merged.rows_out
        rows_by_file = {item["file"]: int(item["rows"]) for item in merged.input_file_rows}
        for file_path in input_paths:
            result.add_detail(status="ok", file=str(file_path), rows=rows_by_file.get(str(file_path.resolve()), 0))
        result.add_detail(
            status="merged",
            engine="duckdb",
            rows_in=merged.rows_in,
            filtered_rows=merged.filtered_rows,
            rows_out=merged.rows_out,
            duplicates_removed=merged.duplicates_removed,
            duration_seconds=round(merged.duration_seconds, 6),
            file_size_bytes=merged.file_size_bytes,
        )
        return merged, result
    except Exception as exc:
        result.errors += 1
        result.add_detail(status="error", file=str(input_dir), error=str(exc), trace=traceback.format_exc())
        raise


def _quote_name(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _validate_delimiter(delimiter: str) -> str:
    clean = str(delimiter or ";")
    if len(clean) != 1 or clean in {"\n", "\r"}:
        raise ValueError("CSV-разделитель должен быть одним символом без перевода строки.")
    return clean


def _read_header(path: Path, *, delimiter: str, encoding: str) -> list[str]:
    with path.open("r", encoding=encoding, newline="") as file:
        try:
            header = next(csv.reader(file, delimiter=delimiter))
        except StopIteration as exc:
            raise ValueError(f"CSV-файл пустой: {path}") from exc
    return [str(column) for column in header]


def _normalized_output_columns(headers: list[list[str]]) -> tuple[list[str], dict[str, str]]:
    columns: list[str] = []
    for header in headers:
        for column in header:
            if column not in columns:
                columns.append(column)
    rename_map = {
        old: new
        for old, new in MERGE_RENAME_COLUMNS.items()
        if old in columns and new not in columns
    }
    normalized: list[str] = []
    for column in columns:
        target = rename_map.get(column, column)
        if target not in normalized:
            normalized.append(target)
    return normalized, rename_map


def _duckdb_encoding(encoding: str) -> str:
    clean = str(encoding or "utf-8").lower().replace("_", "-")
    if clean == "utf-8-sig":
        return "utf-8"
    if clean in {"cp1251", "windows-1251"}:
        return "CP1251"
    return encoding


def _source_column_for_output(output_column: str, source_columns: list[str], rename_map: dict[str, str]) -> str | None:
    for old, new in rename_map.items():
        if new == output_column and old in source_columns:
            return old
    if output_column in source_columns:
        return output_column
    return None


def _file_scan_sql(
    *,
    file_index: int,
    path: Path,
    source_columns: list[str],
    output_columns: list[str],
    rename_map: dict[str, str],
    delimiter: str,
    encoding: str,
) -> str:
    select_parts = [
        f"{int(file_index)} AS __source_file_index",
        "ROW_NUMBER() OVER () AS __source_row_number",
    ]
    for output_column in output_columns:
        source_column = _source_column_for_output(output_column, source_columns, rename_map)
        if source_column is None:
            expr = "NULL"
        elif output_column == "Продажи, шт":
            expr = _sales_number_expr(source_column)
        else:
            expr = f"CAST({_quote_name(source_column)} AS VARCHAR)"
        select_parts.append(f"{expr} AS {_quote_name(output_column)}")
    return f"""
        SELECT {", ".join(select_parts)}
        FROM read_csv(
            {sql_literal(str(path))},
            delim={sql_literal(delimiter)},
            header=true,
            all_varchar=true,
            null_padding=true,
            ignore_errors=false,
            parallel=false,
            encoding={sql_literal(encoding)}
        )
    """


def _sales_number_expr(column: str) -> str:
    nbsp = "\u00a0"
    return (
        "COALESCE(TRY_CAST("
        f"REPLACE(REPLACE(REPLACE(CAST({_quote_name(column)} AS VARCHAR), {sql_literal(nbsp)}, ''), ' ', ''), ',', '.') "
        "AS DOUBLE), 0)"
    )


def _csv_header_prefix(columns: list[str], *, delimiter: str) -> str:
    buffer = io.StringIO()
    csv.writer(buffer, delimiter=delimiter, lineterminator="\n").writerow(columns)
    return "\ufeff" + buffer.getvalue()


def _fetch_int(con: duckdb.DuckDBPyConnection, query: str, params: list[Any] | None = None) -> int:
    row = con.execute(query, params or []).fetchone()
    return int(row[0]) if row else 0
