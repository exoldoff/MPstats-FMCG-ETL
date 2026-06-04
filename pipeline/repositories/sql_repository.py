from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import re
import time
from typing import Any

import duckdb


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
LOGGER = logging.getLogger(__name__)
DUCKDB_THREADS_ENV = "DUCKDB_THREADS"
DUCKDB_MEMORY_LIMIT_ENV = "DUCKDB_MEMORY_LIMIT"
DUCKDB_TEMP_DIRECTORY_ENV = "DUCKDB_TEMP_DIRECTORY"


def quote_identifier(identifier: str) -> str:
    if not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Некорректное SQL-имя {identifier!r}. "
            "Используйте латиницу, цифры и подчёркивание; первый символ — буква или '_'."
        )
    return f'"{identifier}"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_special_database_path(value: str) -> bool:
    return value in {":memory:", ":default:"}


def _env_threads() -> int | None:
    raw_value = os.environ.get(DUCKDB_THREADS_ENV)
    if raw_value is None or not raw_value.strip():
        return None
    try:
        threads = int(raw_value)
    except ValueError:
        LOGGER.warning("Ignoring invalid %s=%r: expected positive integer.", DUCKDB_THREADS_ENV, raw_value)
        return None
    if threads < 1:
        LOGGER.warning("Ignoring invalid %s=%r: expected positive integer.", DUCKDB_THREADS_ENV, raw_value)
        return None
    return threads


def _resolve_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def resolve_duckdb_temp_directory(
    temp_directory: str | Path | None = None,
    *,
    fallback_directory: str | Path | None = None,
    use_env: bool = True,
) -> Path | None:
    raw_directory = temp_directory
    if raw_directory is None and use_env:
        raw_directory = os.environ.get(DUCKDB_TEMP_DIRECTORY_ENV)
    if raw_directory is None:
        raw_directory = fallback_directory
    if raw_directory is None:
        return None
    path = _resolve_path(raw_directory)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _duckdb_config(
    *,
    threads: int | None = None,
    memory_limit: str | None = None,
    temp_directory: str | Path | None = None,
    use_env: bool = True,
) -> dict[str, str]:
    config: dict[str, str] = {}
    effective_threads = threads if threads is not None else _env_threads() if use_env else None
    if effective_threads is not None:
        config["threads"] = str(max(1, int(effective_threads)))

    effective_memory_limit = memory_limit
    if effective_memory_limit is None and use_env:
        effective_memory_limit = os.environ.get(DUCKDB_MEMORY_LIMIT_ENV)
    if effective_memory_limit:
        config["memory_limit"] = str(effective_memory_limit)

    effective_temp_directory = resolve_duckdb_temp_directory(temp_directory, use_env=use_env)
    if effective_temp_directory is not None:
        config["temp_directory"] = str(effective_temp_directory)
    return config


def get_duckdb_connection(
    db_path: str | Path,
    *,
    read_only: bool = False,
    threads: int | None = None,
    memory_limit: str | None = None,
    temp_directory: str | Path | None = None,
    use_env: bool = True,
) -> duckdb.DuckDBPyConnection:
    database = str(db_path)
    if not _is_special_database_path(database):
        path = Path(database).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        database = str(path)
    config = _duckdb_config(
        threads=threads,
        memory_limit=memory_limit,
        temp_directory=temp_directory,
        use_env=use_env,
    )
    if config:
        return duckdb.connect(database, read_only=read_only, config=config)
    return duckdb.connect(database, read_only=read_only)


@contextmanager
def duckdb_connection(
    db_path: str | Path,
    *,
    read_only: bool = False,
    threads: int | None = None,
    memory_limit: str | None = None,
    temp_directory: str | Path | None = None,
    use_env: bool = True,
) -> Iterator[duckdb.DuckDBPyConnection]:
    con = get_duckdb_connection(
        db_path,
        read_only=read_only,
        threads=threads,
        memory_limit=memory_limit,
        temp_directory=temp_directory,
        use_env=use_env,
    )
    try:
        yield con
    finally:
        con.close()


def connect(
    db_path: str | Path,
    *,
    read_only: bool = False,
    threads: int | None = None,
    memory_limit: str | None = None,
    temp_directory: str | Path | None = None,
    use_env: bool = True,
) -> duckdb.DuckDBPyConnection:
    return get_duckdb_connection(
        db_path,
        read_only=read_only,
        threads=threads,
        memory_limit=memory_limit,
        temp_directory=temp_directory,
        use_env=use_env,
    )


@contextmanager
def duckdb_transaction(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    con.execute("BEGIN")
    try:
        yield
    except Exception:
        con.execute("ROLLBACK")
        raise
    else:
        con.execute("COMMIT")


def _metadata_for_log(metadata: MutableMapping[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, Path):
            clean[key] = str(value)
        else:
            clean[key] = value
    file_path = clean.get("file_path") or clean.get("output_path")
    if file_path and "file_size_bytes" not in clean:
        path = Path(str(file_path))
        if path.exists():
            clean["file_size_bytes"] = path.stat().st_size
    return clean


@contextmanager
def measure_duckdb_operation(name: str, metadata: MutableMapping[str, Any] | None = None) -> Iterator[MutableMapping[str, Any]]:
    details: MutableMapping[str, Any] = dict(metadata or {})
    started = time.perf_counter()
    status = "success"
    try:
        yield details
    except Exception as exc:
        status = "error"
        details.setdefault("error", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        duration = time.perf_counter() - started
        details["duration_seconds"] = round(duration, 6)
        details["status"] = status
        LOGGER.info("DuckDB operation %s finished: %s", name, _metadata_for_log(details))


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
