from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import duckdb
import pandas as pd


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


@dataclass(frozen=True)
class SqlLoadRecord:
    table_name: str
    source_file: str | None
    load_name: str | None
    project_name: str | None
    mode: str
    rows_loaded: int


def quote_identifier(identifier: str) -> str:
    if not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Некорректное SQL-имя {identifier!r}. "
            "Используйте латиницу, цифры и подчёркивание; первый символ — буква или '_'."
        )
    return f'"{identifier}"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def apply_migrations(con: duckdb.DuckDBPyConnection) -> None:
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        con.execute(migration.read_text(encoding="utf-8"))


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    quote_identifier(table_name)
    rows = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(rows and rows[0])


def list_tables(db_path: str | Path, *, include_internal: bool = False) -> pd.DataFrame:
    with connect(db_path) as con:
        apply_migrations(con)
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        """
        if not include_internal:
            query += " AND table_name <> 'pipeline_loads'"
        query += " ORDER BY table_name"
        return con.execute(query).fetchdf()


def table_row_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    quoted = quote_identifier(table_name)
    row = con.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()
    return int(row[0]) if row else 0


def import_dataframe(
    db_path: str | Path,
    table_name: str,
    df: pd.DataFrame,
    *,
    mode: str = "append",
    source_file: str | None = None,
    load_name: str | None = None,
    project_name: str | None = None,
) -> SqlLoadRecord:
    if mode not in {"replace", "append"}:
        raise ValueError("mode должен быть 'replace' или 'append'.")

    quoted_table = quote_identifier(table_name)
    with connect(db_path) as con:
        apply_migrations(con)
        if mode == "replace" or not table_exists(con, table_name):
            con.register("_mpstats_import_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {quoted_table} AS SELECT * FROM _mpstats_import_df")
            con.unregister("_mpstats_import_df")
        else:
            con.append(table_name, df)

        record = SqlLoadRecord(
            table_name=table_name,
            source_file=source_file,
            load_name=load_name,
            project_name=project_name,
            mode=mode,
            rows_loaded=len(df),
        )
        con.execute(
            """
            INSERT INTO pipeline_loads (
                table_name, source_file, load_name, project_name, mode, rows_loaded
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                record.table_name,
                record.source_file,
                record.load_name,
                record.project_name,
                record.mode,
                record.rows_loaded,
            ],
        )
        return record


def query_to_dataframe(db_path: str | Path, query: str) -> pd.DataFrame:
    with connect(db_path) as con:
        apply_migrations(con)
        return con.execute(query).fetchdf()


def table_to_dataframe(db_path: str | Path, table_name: str, *, limit: int | None = None) -> pd.DataFrame:
    quoted = quote_identifier(table_name)
    query = f"SELECT * FROM {quoted}"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    return query_to_dataframe(db_path, query)


def export_query_to_csv(db_path: str | Path, query: str, output_file: str | Path) -> Path:
    out = Path(output_file)
    out.parent.mkdir(parents=True, exist_ok=True)
    clean_query = query.strip().rstrip(";")
    if not clean_query:
        raise ValueError("SQL query is empty.")
    with connect(db_path) as con:
        apply_migrations(con)
        con.execute(
            f"COPY ({clean_query}) TO {sql_literal(str(out))} WITH (HEADER, DELIMITER ';')"
        )
    return out


def export_table_to_csv(db_path: str | Path, table_name: str, output_file: str | Path) -> Path:
    quoted = quote_identifier(table_name)
    return export_query_to_csv(db_path, f"SELECT * FROM {quoted}", output_file)


def load_history(db_path: str | Path) -> pd.DataFrame:
    return query_to_dataframe(
        db_path,
        """
        SELECT loaded_at, table_name, source_file, load_name, project_name, mode, rows_loaded
        FROM pipeline_loads
        ORDER BY loaded_at DESC
        """,
    )
