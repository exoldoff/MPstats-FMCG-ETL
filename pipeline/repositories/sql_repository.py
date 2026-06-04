from __future__ import annotations

from pathlib import Path
import re

import duckdb


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


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
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name VARCHAR PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    applied = {
        str(row[0])
        for row in con.execute("SELECT name FROM schema_migrations").fetchall()
    }
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if migration.name in applied:
            continue
        con.execute(migration.read_text(encoding="utf-8"))
        con.execute("INSERT INTO schema_migrations (name) VALUES (?)", [migration.name])


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
