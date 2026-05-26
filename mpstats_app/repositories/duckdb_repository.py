from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

import pandas as pd

from pipeline.repositories.file_repository import read_semicolon_csv
from pipeline.repositories.sql_repository import apply_migrations, connect, quote_identifier

from mpstats_app.config import AppSettings
from mpstats_app.utils import clean_record, clean_records, quote_duckdb_name


SEARCH_COLUMNS = ("SKU", "Артикул", "Название", "Бренд", "Категория")
EXPORT_METADATA_COLUMNS = ("__project_name", "__year", "__month", "__marketplace_code", "__category_key", "__row_hash")
TEXT_DB_TYPES = ("CHAR", "STRING", "TEXT", "VARCHAR")


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


def _series_has_text_values(series: pd.Series) -> bool:
    if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
        return False
    values = series.dropna()
    if values.empty:
        return False
    text_values = values.astype(str).str.strip()
    text_values = text_values[text_values != ""]
    if text_values.empty:
        return False
    numeric_values = pd.to_numeric(text_values.str.replace(",", ".", regex=False), errors="coerce")
    return bool(numeric_values.isna().any())


def _period_index_to_label(index: int) -> str:
    year = (index - 1) // 12
    month = (index - 1) % 12 + 1
    return f"{year}-{month:02d}"


def _ensure_table_accepts_source_columns(con: Any, *, table_name: str, quoted_table: str, df: pd.DataFrame) -> None:
    column_types = _table_column_types(con, table_name)
    for column in df.columns:
        column_name = str(column)
        if column_name not in column_types:
            con.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {quote_duckdb_name(column_name)} VARCHAR")
            column_types[column_name] = "VARCHAR"
            continue
        if _db_type_accepts_text(column_types[column_name]):
            continue
        if _series_has_text_values(df[column]):
            con.execute(f"ALTER TABLE {quoted_table} ALTER COLUMN {quote_duckdb_name(column_name)} TYPE VARCHAR")
            column_types[column_name] = "VARCHAR"


class DuckDbAppRepository:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._lock = RLock()

    def ensure_ready(self) -> None:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)

    def _fetch_records(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            result = con.execute(query, params or [])
            columns = [col[0] for col in result.description]
            return clean_records([dict(zip(columns, row)) for row in result.fetchall()])

    def _fetch_one(self, query: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        rows = self._fetch_records(query, params)
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

    def clear_pipeline_pause(self, run_id: str) -> dict[str, Any] | None:
        return self.update_pipeline_run(run_id, {"pause_requested": False})

    def is_pipeline_pause_requested(self, run_id: str) -> bool:
        row = self._fetch_one("SELECT pause_requested FROM pipeline_runs WHERE id = ?", [run_id])
        return bool(row and row.get("pause_requested"))

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

    def upsert_cube_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        entry_id = str(entry.get("id") or uuid4().hex)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.execute(
                """
                INSERT INTO cube_registry (
                    id, project_name, year, month, marketplace, marketplace_code,
                    category_key, category_name, rows_count, saved_to_db_at,
                    source_processed_file_path, file_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?, ?)
                ON CONFLICT (project_name, year, month, marketplace_code, category_key) DO UPDATE SET
                    marketplace = EXCLUDED.marketplace,
                    category_name = EXCLUDED.category_name,
                    rows_count = EXCLUDED.rows_count,
                    saved_to_db_at = now(),
                    source_processed_file_path = EXCLUDED.source_processed_file_path,
                    file_hash = EXCLUDED.file_hash
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
        df = read_semicolon_csv(csv_path, low_memory=False)
        df["__run_id"] = run_id
        df["__source_file"] = str(csv_path)
        df["__imported_at"] = datetime.now()

        quoted_table = quote_identifier(table_name)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.register("_mpstats_app_products_df", df)
            try:
                exists = bool(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = 'main' AND table_name = ?
                        """,
                        [table_name],
                    ).fetchone()[0]
                )
                if not exists:
                    con.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM _mpstats_app_products_df")
                else:
                    _ensure_table_accepts_source_columns(con, table_name=table_name, quoted_table=quoted_table, df=df)
                    quoted_columns = ", ".join(quote_duckdb_name(str(column)) for column in df.columns)
                    con.execute(
                        f"INSERT INTO {quoted_table} ({quoted_columns}) "
                        f"SELECT {quoted_columns} FROM _mpstats_app_products_df"
                    )
            finally:
                con.unregister("_mpstats_app_products_df")

            con.execute(
                """
                INSERT INTO pipeline_loads (
                    table_name, source_file, load_name, project_name, mode, rows_loaded
                )
                VALUES (?, ?, ?, ?, 'append', ?)
                """,
                [table_name, str(csv_path), load_name or f"app_run:{run_id}", project_name, len(df)],
            )
        return len(df)

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
        df = read_semicolon_csv(csv_path, low_memory=False)
        df["__run_id"] = run_id
        df["__source_file"] = str(csv_path)
        df["__imported_at"] = datetime.now()
        df["__project_name"] = project_name
        df["__year"] = int(year)
        df["__month"] = int(month)
        df["__marketplace_code"] = marketplace_code
        df["__category_key"] = category_key
        df["__row_hash"] = [
            hashlib.sha1(
                json.dumps(
                    {
                        "project_name": project_name,
                        "year": int(year),
                        "month": int(month),
                        "marketplace_code": marketplace_code,
                        "category_key": category_key,
                        "row": {str(key): None if pd.isna(value) else str(value) for key, value in row.items()},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            for row in df.drop(
                columns=[
                    "__run_id",
                    "__source_file",
                    "__imported_at",
                    "__project_name",
                    "__year",
                    "__month",
                    "__marketplace_code",
                    "__category_key",
                ]
            ).to_dict(orient="records")
        ]
        df = df.drop_duplicates(subset=["__row_hash"]).copy()

        quoted_table = quote_identifier(table_name)
        inserted = 0
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
            con.register("_mpstats_app_products_df", df)
            try:
                exists = bool(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = 'main' AND table_name = ?
                        """,
                        [table_name],
                    ).fetchone()[0]
                )
                if not exists:
                    con.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM _mpstats_app_products_df")
                    inserted = len(df)
                else:
                    _ensure_table_accepts_source_columns(con, table_name=table_name, quoted_table=quoted_table, df=df)
                    if overwrite:
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
                    quoted_columns = ", ".join(quote_duckdb_name(str(column)) for column in df.columns)
                    selected_columns = ", ".join(f"d.{quote_duckdb_name(str(column))}" for column in df.columns)
                    inserted = int(
                        con.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM _mpstats_app_products_df d
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM {quoted_table} t
                                WHERE t.{quote_duckdb_name('__row_hash')} = d.{quote_duckdb_name('__row_hash')}
                            )
                            """
                        ).fetchone()[0]
                    )
                    con.execute(
                        f"""
                        INSERT INTO {quoted_table} ({quoted_columns})
                        SELECT {selected_columns}
                        FROM _mpstats_app_products_df d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {quoted_table} t
                            WHERE t.{quote_duckdb_name('__row_hash')} = d.{quote_duckdb_name('__row_hash')}
                        )
                        """
                    )
            finally:
                con.unregister("_mpstats_app_products_df")

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
                    "replace" if overwrite else "append_dedup",
                    inserted,
                ],
            )
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

        count_row = self._fetch_one(f"SELECT COUNT(*) AS total FROM {quoted_table}{where_sql}", params)
        rows = self._fetch_records(
            f"SELECT * FROM {quoted_table}{where_sql}{order_sql} LIMIT {safe_limit} OFFSET {safe_offset}",
            params,
        )
        return {
            "columns": columns,
            "rows": rows,
            "total": int(count_row["total"]) if count_row else 0,
            "run_id": effective_run_id,
        }

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

        period = self._fetch_one(
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
        return {
            "columns": visible_columns,
            "selected_columns": visible_columns,
            "categories": self.export_categories(table_name=table_name, project_name=project_name),
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
        order_sql = self._export_order_sql(columns=columns, sort_column=sort_column, sort_direction=sort_direction)
        with self._lock, connect(self.settings.db_path) as con:
            apply_migrations(con)
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

    def _export_order_sql(self, *, columns: list[str], sort_column: str | None, sort_direction: str) -> str:
        direction = "DESC" if str(sort_direction).lower() == "desc" else "ASC"
        if sort_column and sort_column in columns and not sort_column.startswith("__"):
            return f" ORDER BY {quote_duckdb_name(sort_column)} {direction} NULLS LAST"
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
