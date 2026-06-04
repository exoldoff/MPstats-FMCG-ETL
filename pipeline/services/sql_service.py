from __future__ import annotations

from pathlib import Path
import traceback

import pandas as pd

from pipeline.models import StepResult
from pipeline.repositories.file_repository import list_csv_files, read_semicolon_csv
from pipeline.repositories.sql_repository import (
    export_query_to_csv,
    export_table_to_csv,
    import_dataframe,
    list_tables,
    load_history,
    quote_identifier,
    query_row_count,
    query_to_dataframe,
    table_to_dataframe,
)


def import_csv_to_sql(
    csv_path: str | Path,
    *,
    db_path: str | Path,
    table_name: str,
    mode: str = "append",
    load_name: str | None = None,
    project_name: str | None = None,
) -> StepResult:
    path = Path(csv_path)
    df = read_semicolon_csv(path, low_memory=False)
    record = import_dataframe(
        db_path,
        table_name,
        df,
        mode=mode,
        source_file=str(path),
        load_name=load_name,
        project_name=project_name,
    )
    result = StepResult(name="sql_import", ok=1, rows=record.rows_loaded, output=Path(db_path))
    result.add_detail(
        table_name=record.table_name,
        source_file=record.source_file,
        load_name=record.load_name,
        project_name=record.project_name,
        mode=record.mode,
        rows_loaded=record.rows_loaded,
    )
    return result


def import_directory_to_sql(
    input_dir: str | Path,
    *,
    db_path: str | Path,
    table_name: str,
    mode: str = "append",
    load_name: str | None = None,
    project_name: str | None = None,
) -> StepResult:
    result = StepResult(name="sql_import_dir", output=Path(db_path))
    files = list_csv_files(input_dir)
    for index, file_path in enumerate(files):
        try:
            effective_mode = "replace" if mode == "replace" and index == 0 else "append"
            step = import_csv_to_sql(
                file_path,
                db_path=db_path,
                table_name=table_name,
                mode=effective_mode,
                load_name=load_name,
                project_name=project_name,
            )
            result.ok += 1
            result.rows += step.rows
            result.details.extend(step.details)
        except Exception as exc:
            result.errors += 1
            result.add_detail(status="error", file=str(file_path), error=str(exc), trace=traceback.format_exc())
    return result


def export_sql_to_csv(
    *,
    db_path: str | Path,
    output_file: str | Path,
    table_name: str | None = None,
    query: str | None = None,
) -> StepResult:
    if query:
        rows = query_row_count(db_path, query)
        out = export_query_to_csv(db_path, query, output_file)
    elif table_name:
        rows = query_row_count(db_path, f"SELECT * FROM {quote_identifier(table_name)}")
        out = export_table_to_csv(db_path, table_name, output_file)
    else:
        raise ValueError("Нужно передать table_name или query.")

    result = StepResult(name="sql_export", ok=1, rows=rows, output=out)
    result.add_detail(db_path=str(db_path), table_name=table_name, query=query, output_file=str(out), rows=rows)
    return result


def sql_query(db_path: str | Path, query: str) -> pd.DataFrame:
    return query_to_dataframe(db_path, query)


def sql_table(db_path: str | Path, table_name: str, *, limit: int | None = None) -> pd.DataFrame:
    return table_to_dataframe(db_path, table_name, limit=limit)


def sql_tables(db_path: str | Path, *, include_internal: bool = False) -> pd.DataFrame:
    return list_tables(db_path, include_internal=include_internal)


def sql_load_history(db_path: str | Path) -> pd.DataFrame:
    return load_history(db_path)
