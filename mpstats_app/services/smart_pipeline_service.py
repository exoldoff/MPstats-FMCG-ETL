from __future__ import annotations

import calendar
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
from threading import RLock, Thread
import time
from typing import Any
from uuid import uuid4

from pipeline.repositories.file_repository import read_csv_auto, read_semicolon_csv, write_semicolon_csv
from pipeline.services.classification_service import classify_file
from pipeline.services.export_service import (
    SOURCE_TYPE_SUBJECT,
    ExportSettings,
    build_api_session,
    build_session,
    export_one_month,
    normalize_source_type,
)
from pipeline.services.standardize_service import standardize_dataframe
from pipeline.services.weight_parser_service import parse_weights_dataframe

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.services.category_catalog_service import CategoryCatalogService


FINAL_STATUSES = {"saved_to_db", "skipped", "no_data"}


DEFAULT_PIPELINE_SETTINGS: dict[str, Any] = {
    "overwrite_raw": False,
    "overwrite_processed": False,
    "overwrite_db": False,
    "max_parallel_downloads": 1,
    "retry_count": 1,
    "timeout_seconds": 300,
    "pause_between_requests": 2.0,
    "max_weight_kg": 40.0,
}


def month_iter(start_year: int, start_month: int, end_year: int, end_month: int) -> list[tuple[int, int]]:
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("Начальный месяц больше конечного.")
    out: list[tuple[int, int]] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        out.append((year, month))
        month += 1
        if month > 12:
            year += 1
            month = 1
    return out


def next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def ym(year: int, month: int) -> str:
    return f"{year}-{month:02d}"


def month_day_coverage(year: int, month: int, *, today: date | None = None) -> dict[str, Any]:
    current = today or date.today()
    days_in_month = calendar.monthrange(year, month)[1]
    target_index = year * 12 + month
    current_index = current.year * 12 + current.month
    if target_index < current_index:
        days_loaded = days_in_month
    elif target_index == current_index:
        days_loaded = min(current.day, days_in_month)
    else:
        days_loaded = 0
    data_actual_until = date(year, month, days_loaded).isoformat() if days_loaded else None
    return {
        "days_loaded": days_loaded,
        "days_in_month": days_in_month,
        "data_actual_until": data_actual_until,
    }


def category_active_in_month(category: dict[str, Any], year: int, month: int) -> bool:
    current = ym(year, month)
    period_from = str(category.get("period_from") or "")
    period_to = str(category.get("period_to") or "")
    if period_from and current < period_from:
        return False
    if period_to and current > period_to:
        return False
    return True


def category_uses_fbs(category: dict[str, Any]) -> bool:
    if category.get("fbs") is None:
        return str(category.get("mp_code") or "") == "oz"
    return bool(category.get("fbs"))


def category_source_type(category: dict[str, Any]) -> str:
    return normalize_source_type(category.get("source_type"))


def safe_segment(value: str) -> str:
    segment = re.sub(r"[^\w_.-]+", "_", value.strip(), flags=re.UNICODE)
    return segment.strip("._") or "mpstats"


def file_sha1(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_timestamp(path: Path) -> datetime | None:
    return datetime.fromtimestamp(path.stat().st_mtime) if path.is_file() else None


class SmartPipelineService:
    def __init__(
        self,
        *,
        settings: AppSettings,
        repository: DuckDbAppRepository,
        catalog_service: CategoryCatalogService,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.catalog_service = catalog_service
        self._lock = RLock()
        self._threads: dict[str, Thread] = {}
        self._operation_progress: dict[str, dict[str, Any]] = {}

    def get_pipeline_settings(self) -> dict[str, Any]:
        raw = self.repository.get_setting("pipeline_settings_json")
        if not raw:
            return dict(DEFAULT_PIPELINE_SETTINGS)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return dict(DEFAULT_PIPELINE_SETTINGS)
        return {**DEFAULT_PIPELINE_SETTINGS, **parsed}

    def save_pipeline_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        settings = {**self.get_pipeline_settings(), **payload}
        settings["overwrite_raw"] = bool(settings.get("overwrite_raw"))
        settings["overwrite_processed"] = bool(settings.get("overwrite_processed"))
        settings["overwrite_db"] = bool(settings.get("overwrite_db"))
        settings["max_parallel_downloads"] = max(1, int(settings.get("max_parallel_downloads") or 1))
        settings["retry_count"] = max(0, int(settings.get("retry_count") or 0))
        settings["timeout_seconds"] = max(30, int(settings.get("timeout_seconds") or 300))
        settings["pause_between_requests"] = max(0.0, float(settings.get("pause_between_requests") or 0.0))
        settings["max_weight_kg"] = max(1.0, float(settings.get("max_weight_kg") or 40.0))
        self.repository.set_setting("pipeline_settings_json", json.dumps(settings, ensure_ascii=False))
        return settings

    def create_plan(
        self,
        *,
        project_name: str,
        run_type: str,
        category_ids: list[str],
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        categories = self.repository.get_categories_by_ids(category_ids)
        if not categories:
            raise ValueError("Выбери хотя бы одну категорию.")
        effective_settings = self.save_pipeline_settings(settings or {})
        months = month_iter(start_year, start_month, end_year, end_month)
        planned_tasks = [
            (category, year, month)
            for category in categories
            for year, month in months
            if category_active_in_month(category, year, month)
        ]
        if not planned_tasks:
            raise ValueError("Для выбранных категорий нет активных путей в указанном периоде.")
        run_id = uuid4().hex
        self.repository.create_pipeline_run(
            run_id=run_id,
            project_name=project_name,
            run_type=run_type,
            period_from=ym(start_year, start_month),
            period_to=ym(end_year, end_month),
            selected_category_ids=category_ids,
            settings=effective_settings,
        )
        for category, year, month in planned_tasks:
            self.repository.upsert_download_task(
                self._build_task(
                    run_id=run_id,
                    project_name=project_name,
                    category=category,
                    year=year,
                    month=month,
                    settings=effective_settings,
                )
            )
        self.repository.refresh_pipeline_run_counts(run_id)
        return self.get_run(run_id)

    def create_monthly_sync_plan(
        self,
        *,
        project_name: str,
        settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        latest = self.repository.latest_cube_month(project_name=project_name)
        if latest:
            year, month = next_month(*latest)
        else:
            now = datetime.now()
            year, month = now.year, now.month
        categories = self.repository.list_categories()
        return self.create_plan(
            project_name=project_name,
            run_type="monthly_sync",
            category_ids=[str(category["category_id"]) for category in categories],
            start_year=year,
            start_month=month,
            end_year=year,
            end_month=month,
            settings=settings,
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self.repository.refresh_pipeline_run_counts(run_id) or self.repository.get_pipeline_run(run_id)
        if not run:
            raise KeyError(f"Запуск не найден: {run_id}")
        tasks = self.repository.list_download_tasks(run_id=run_id)
        months = {(int(task["year"]), int(task["month"])) for task in tasks}
        categories = {str(task["category_id"]) for task in tasks}
        total = int(run.get("total_tasks") or len(tasks))
        completed = int(run.get("completed_tasks") or 0)
        failed = int(run.get("failed_tasks") or 0)
        remaining = max(0, total - completed - failed)
        progress = round((completed / total) * 100, 1) if total else 0.0
        return {
            **run,
            "tasks_preview": tasks[:20],
            "category_count": len(categories),
            "month_count": len(months),
            "remaining_tasks": remaining,
            "progress": progress,
            "is_active": self._is_thread_active(str(run["id"])),
            "operation_progress": self._operation_progress_snapshot(run_id),
        }

    def list_runs(self, *, project_name: str | None = None) -> dict[str, Any]:
        return {"runs": self.repository.list_pipeline_runs(project_name=project_name)}

    def list_tasks(self, *, run_id: str, task_filter: str = "all") -> dict[str, Any]:
        return {"tasks": self.repository.list_download_tasks(run_id=run_id, task_filter=task_filter)}

    def list_files(self, *, project_name: str) -> dict[str, Any]:
        root = self._project_data_root(project_name)
        files: list[dict[str, Any]] = []
        for path in sorted(root.rglob("*")) if root.exists() else []:
            if not path.is_file():
                continue
            relative = path.relative_to(root)
            kind = self._project_file_kind(relative)
            if kind is None:
                continue
            files.append(
                {
                    "path": str(path),
                    "relative_path": str(relative),
                    "kind": kind,
                    "size": path.stat().st_size,
                    "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                }
            )
        return {"root": str(root), "files": files[:1000]}

    def list_cube(self, *, project_name: str) -> dict[str, Any]:
        return {
            "items": self.repository.list_cube_registry(project_name=project_name),
            "total": self.repository.count_cube_registry(project_name=project_name),
        }

    def delete_run(self, *, run_id: str) -> dict[str, Any]:
        run = self.repository.get_pipeline_run(run_id)
        if not run:
            raise KeyError(f"План не найден: {run_id}")
        if self._is_thread_active(run_id) or str(run.get("status") or "") in {"running", "pausing"}:
            raise ValueError("Нельзя удалить план, пока он выполняется. Сначала поставь его на паузу или дождись завершения.")
        with self._lock:
            self._operation_progress.pop(run_id, None)
        deleted = self.repository.delete_pipeline_run(run_id)
        return {"run_id": run_id, "project_name": run["project_name"], "deleted": deleted}

    def delete_file(self, *, project_name: str, path: str, delete_cube: bool = False) -> dict[str, Any]:
        target, relative_path, kind = self._resolve_project_file(project_name=project_name, path=path)
        cube_entries = self.repository.list_cube_registry_by_source_file(
            project_name=project_name,
            source_file_path=str(target),
        )
        cube_deletions: list[dict[str, Any]] = []
        if delete_cube:
            for entry in cube_entries:
                cube_deletions.append(self.repository.delete_cube_entry(entry_id=str(entry["id"]), table_name=self.settings.products_table))

        target.unlink()
        if kind == "classified" and cube_entries and not delete_cube:
            task_updates = {"download_tasks": 0}
        else:
            task_updates = self.repository.mark_project_file_deleted(project_name=project_name, file_path=str(target), file_kind=kind)
        return {
            "project_name": project_name,
            "path": str(target),
            "relative_path": str(relative_path),
            "kind": kind,
            "deleted": {"files": 1, **task_updates},
            "cube_deletions": cube_deletions,
        }

    def delete_cube_entry(self, *, entry_id: str) -> dict[str, Any]:
        entry = self.repository.get_cube_entry_by_id(entry_id)
        if not entry:
            raise KeyError(f"Срез куба не найден: {entry_id}")
        return self.repository.delete_cube_entry(entry_id=entry_id, table_name=self.settings.products_table)

    def start_run(
        self,
        *,
        run_id: str,
        task_filter: str = "all",
        task_ids: list[str] | None = None,
        rebuild_only: bool = False,
        force_reclassify: bool = False,
        force_reprocess: bool = False,
        force_db_overwrite: bool = False,
        wait: bool = False,
    ) -> dict[str, Any]:
        operation_kind = self._operation_kind(
            rebuild_only=rebuild_only,
            force_reclassify=force_reclassify,
            force_reprocess=force_reprocess,
        )
        with self._lock:
            if self._is_thread_active(run_id):
                return self.get_run(run_id)
            self.repository.clear_pipeline_control(run_id)
            if operation_kind:
                self._begin_operation_progress(
                    run_id,
                    kind=operation_kind,
                    tasks=self._operation_tasks(run_id=run_id, task_filter=task_filter, task_ids=task_ids),
                )
            else:
                self._operation_progress.pop(run_id, None)
            if not wait:
                self.repository.update_pipeline_run(
                    run_id,
                    {"status": "running", "current_step": "Старт", "started_at": datetime.now(), "finished_at": None},
                )
                thread = Thread(
                    target=self._run,
                    kwargs={
                        "run_id": run_id,
                        "task_filter": task_filter,
                        "task_ids": task_ids,
                        "rebuild_only": rebuild_only,
                        "force_reclassify": force_reclassify,
                        "force_reprocess": force_reprocess,
                        "force_db_overwrite": force_db_overwrite,
                    },
                    daemon=True,
                )
                self._threads[run_id] = thread
                thread.start()
                return self.get_run(run_id)
        if wait:
            self._run(
                run_id=run_id,
                task_filter=task_filter,
                task_ids=task_ids,
                rebuild_only=rebuild_only,
                force_reclassify=force_reclassify,
                force_reprocess=force_reprocess,
                force_db_overwrite=force_db_overwrite,
            )
            return self.get_run(run_id)
        return self.get_run(run_id)

    def pause_run(self, *, run_id: str) -> dict[str, Any]:
        run = self.repository.get_pipeline_run(run_id)
        if not run:
            raise KeyError(f"Запуск не найден: {run_id}")
        if str(run.get("status") or "") == "paused":
            return self.get_run(run_id)
        self.repository.request_pipeline_pause(run_id)
        if not self._is_thread_active(run_id):
            self.repository.update_pipeline_run(run_id, {"status": "paused", "current_step": "Пауза"})
        return self.get_run(run_id)

    def stop_run(self, *, run_id: str) -> dict[str, Any]:
        run = self.repository.get_pipeline_run(run_id)
        if not run:
            raise KeyError(f"Запуск не найден: {run_id}")
        if str(run.get("status") or "") == "stopped":
            return self.get_run(run_id)
        self.repository.request_pipeline_stop(run_id)
        if not self._is_thread_active(run_id):
            self.repository.update_pipeline_run(
                run_id,
                {"status": "stopped", "current_step": "Остановлено", "finished_at": datetime.now()},
            )
        return self.get_run(run_id)

    def resume_run(self, *, run_id: str, wait: bool = False) -> dict[str, Any]:
        return self.start_run(run_id=run_id, wait=wait)

    def retry_errors(self, *, run_id: str, wait: bool = False) -> dict[str, Any]:
        failed_ids = [str(task["id"]) for task in self.repository.list_download_tasks(run_id=run_id, task_filter="errors")]
        if not failed_ids:
            return self.get_run(run_id)
        self.repository.reset_failed_tasks(run_id)
        return self.start_run(run_id=run_id, task_ids=failed_ids, wait=wait)

    def retry_task(self, *, task_id: str, wait: bool = False) -> dict[str, Any]:
        task = self.repository.reset_task_for_retry(task_id)
        if not task:
            raise KeyError(f"Задача не найдена: {task_id}")
        return self.start_run(run_id=str(task["run_id"]), task_ids=[task_id], wait=wait)

    def rebuild_cube(self, *, run_id: str, wait: bool = False) -> dict[str, Any]:
        return self.start_run(run_id=run_id, rebuild_only=True, wait=wait)

    def reclassify_cube(self, *, run_id: str, wait: bool = False) -> dict[str, Any]:
        return self.start_run(
            run_id=run_id,
            rebuild_only=True,
            force_reclassify=True,
            force_db_overwrite=True,
            wait=wait,
        )

    def reprocess_sources(self, *, run_id: str, wait: bool = False) -> dict[str, Any]:
        return self.start_run(
            run_id=run_id,
            force_reprocess=True,
            force_db_overwrite=True,
            wait=wait,
        )

    def _operation_tasks(self, *, run_id: str, task_filter: str = "all", task_ids: list[str] | None = None) -> list[dict[str, Any]]:
        tasks = self.repository.list_download_tasks(run_id=run_id, task_filter=task_filter)
        if not task_ids:
            return tasks
        wanted = set(task_ids)
        return [task for task in tasks if str(task["id"]) in wanted]

    @staticmethod
    def _operation_kind(*, rebuild_only: bool, force_reclassify: bool, force_reprocess: bool) -> str | None:
        if force_reprocess:
            return "reprocess"
        if force_reclassify:
            return "reclassify"
        if rebuild_only:
            return "rebuild"
        return None

    def _begin_operation_progress(self, run_id: str, *, kind: str, tasks: list[dict[str, Any]]) -> None:
        with self._lock:
            self._operation_progress[run_id] = {
                "kind": kind,
                "total_files": len(tasks),
                "completed_files": 0,
                "failed_files": 0,
                "remaining_files": len(tasks),
                "progress": 0.0,
                "status": "running",
            }

    def _mark_operation_file(self, run_id: str, *, success: bool) -> None:
        with self._lock:
            progress = self._operation_progress.get(run_id)
            if not progress:
                return
            key = "completed_files" if success else "failed_files"
            progress[key] = int(progress.get(key) or 0) + 1
            self._refresh_operation_progress(progress)

    def _finish_operation_progress(self, run_id: str, *, status: str) -> None:
        with self._lock:
            progress = self._operation_progress.get(run_id)
            if not progress:
                return
            progress["status"] = status
            self._refresh_operation_progress(progress)

    def _operation_progress_snapshot(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            progress = self._operation_progress.get(run_id)
            return dict(progress) if progress else None

    @staticmethod
    def _refresh_operation_progress(progress: dict[str, Any]) -> None:
        total = int(progress.get("total_files") or 0)
        completed = int(progress.get("completed_files") or 0)
        failed = int(progress.get("failed_files") or 0)
        done = min(total, completed + failed)
        progress["remaining_files"] = max(0, total - done)
        progress["progress"] = round((done / total) * 100, 1) if total else 0.0

    def _run(
        self,
        *,
        run_id: str,
        task_filter: str = "all",
        task_ids: list[str] | None = None,
        rebuild_only: bool = False,
        force_reclassify: bool = False,
        force_reprocess: bool = False,
        force_db_overwrite: bool = False,
    ) -> None:
        run = self.repository.get_pipeline_run(run_id)
        if not run:
            return
        operation_kind = self._operation_kind(
            rebuild_only=rebuild_only,
            force_reclassify=force_reclassify,
            force_reprocess=force_reprocess,
        )
        settings = {**self.get_pipeline_settings(), **self._json_dict(run.get("settings_json"))}
        if force_reclassify or force_reprocess:
            settings["overwrite_processed"] = True
        if force_db_overwrite:
            settings["overwrite_db"] = True
        self.repository.update_pipeline_run(
            run_id,
            {"status": "running", "current_step": "Старт", "started_at": datetime.now(), "finished_at": None},
        )
        tasks = self._operation_tasks(run_id=run_id, task_filter=task_filter, task_ids=task_ids)
        if operation_kind:
            self._begin_operation_progress(run_id, kind=operation_kind, tasks=tasks)
        try:
            for task in tasks:
                self._check_control(run_id)
                if str(task["status"]) in FINAL_STATUSES and not settings.get("overwrite_db"):
                    self._mark_operation_file(run_id, success=True)
                    continue
                success = self._execute_task(
                    task,
                    settings=settings,
                    rebuild_only=rebuild_only,
                    force_reclassify=force_reclassify,
                    force_reprocess=force_reprocess,
                )
                self._mark_operation_file(run_id, success=success)
                self._check_control(run_id)
            self._finish_run(run_id)
        except _PipelinePaused:
            self.repository.update_pipeline_run(run_id, {"status": "paused", "current_step": "Пауза"})
            self._finish_operation_progress(run_id, status="paused")
        except _PipelineStopped:
            self.repository.update_pipeline_run(
                run_id,
                {"status": "stopped", "current_step": "Остановлено", "finished_at": datetime.now()},
            )
            self._finish_operation_progress(run_id, status="stopped")
        except Exception as exc:
            self.repository.update_pipeline_run(
                run_id,
                {"status": "failed", "current_step": str(exc), "finished_at": datetime.now()},
            )
            self._finish_operation_progress(run_id, status="failed")

    def _finish_run(self, run_id: str) -> None:
        run = self.repository.refresh_pipeline_run_counts(run_id)
        total = int(run.get("total_tasks") or 0) if run else 0
        completed = int(run.get("completed_tasks") or 0) if run else 0
        failed = int(run.get("failed_tasks") or 0) if run else 0
        remaining = max(0, total - completed - failed)
        if failed:
            status = "completed_with_errors"
        elif remaining:
            status = "paused"
        else:
            status = "succeeded"
        self.repository.update_pipeline_run(
            run_id,
            {"status": status, "current_step": "Готово", "finished_at": datetime.now()},
        )
        self._finish_operation_progress(run_id, status=status)

    def _execute_task(
        self,
        task: dict[str, Any],
        *,
        settings: dict[str, Any],
        rebuild_only: bool = False,
        force_reclassify: bool = False,
        force_reprocess: bool = False,
    ) -> bool:
        task_id = str(task["id"])
        try:
            self._check_control(str(task["run_id"]))
            if force_reprocess:
                self._prepare_reprocess_task(task_id)
                self._process_task(task_id, settings=settings)
                self._check_control(str(task["run_id"]))
                self._classify_task(task_id, settings=settings, force_reclassify=True)
                self._check_control(str(task["run_id"]))
                self._save_task(task_id, settings=settings)
                return True
            fresh = self.repository.get_download_task(task_id) or task
            classified_exists = Path(str(fresh["classified_file_path"])).exists()
            processed_exists = Path(str(fresh["processed_file_path"])).exists()
            if not force_reclassify and not settings.get("overwrite_processed") and (classified_exists or processed_exists):
                self._classify_task(task_id, settings=settings, force_reclassify=force_reclassify)
                self._check_control(str(task["run_id"]))
                self._save_task(task_id, settings=settings)
                return True
            if not rebuild_only:
                self._download_task(task, settings=settings)
                self._check_control(str(task["run_id"]))
                self._process_task(task_id, settings=settings)
                self._check_control(str(task["run_id"]))
            self._classify_task(task_id, settings=settings, force_reclassify=force_reclassify)
            self._check_control(str(task["run_id"]))
            self._save_task(task_id, settings=settings)
            return True
        except _PipelineInterrupted:
            raise
        except Exception as exc:
            self.repository.update_download_task(
                task_id,
                {"status": "failed", "error_message": str(exc)},
            )
            return False

    def _prepare_reprocess_task(self, task_id: str) -> None:
        task = self.repository.get_download_task(task_id)
        if not task:
            raise KeyError(task_id)
        raw_path = Path(str(task["raw_file_path"]))
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw-файл для переобработки не найден: {raw_path}")
        for key in ("processed_file_path", "classified_file_path"):
            path = Path(str(task[key]))
            if path.exists():
                path.unlink()
        self.repository.update_download_task(
            task_id,
            {
                "status": "downloaded",
                "download_status": "downloaded",
                "process_status": "pending",
                "classify_status": "pending",
                "save_status": "pending",
                "error_message": None,
            },
        )

    def _download_task(self, task: dict[str, Any], *, settings: dict[str, Any]) -> None:
        task_id = str(task["id"])
        run_id = str(task["run_id"])
        self._check_control(run_id)
        raw_path = Path(str(task["raw_file_path"]))
        if raw_path.exists() and not settings.get("overwrite_raw"):
            self.repository.update_download_task(task_id, {"status": "downloaded", "download_status": "downloaded", "error_message": None})
            return
        source_type = normalize_source_type(task.get("source_type"))
        cookie = self.repository.get_setting("mpstats_cookie") or ""
        api_token = self.repository.get_setting("mpstats_api_token") or ""
        if source_type == SOURCE_TYPE_SUBJECT:
            if not api_token.strip():
                raise ValueError("MPStats API token пустой. Сохрани token перед выгрузкой по предмету.")
        elif not cookie.strip():
            raise ValueError("Cookie MPStats пустой. Сохрани доступ перед запуском.")
        self.repository.update_pipeline_run(str(task["run_id"]), {"current_step": f"Скачивание {task['category_name']} {ym(int(task['year']), int(task['month']))}"})
        self.repository.update_download_task(task_id, {"status": "downloading", "download_status": "downloading", "error_message": None})
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if settings.get("overwrite_raw") and raw_path.exists():
            raw_path.unlink()
        export_settings = ExportSettings(
            export_months_by_year={int(task["year"]): (int(task["month"]),)},
            save_dir=raw_path.parent,
            skip_if_exists=not bool(settings.get("overwrite_raw")),
            extract_zip=True,
            cookie=cookie,
            tasks=[self._task_to_export_task(task)],
            api_token=api_token,
        )
        session = build_api_session(api_token) if source_type == SOURCE_TYPE_SUBJECT else build_session(cookie)
        last_error: Exception | None = None
        for attempt in range(int(settings.get("retry_count") or 0) + 1):
            self._check_control(run_id)
            try:
                downloaded = export_one_month(
                    session,
                    export_settings,
                    export_settings.tasks[0],
                    year=int(task["year"]),
                    month=int(task["month"]),
                    max_wait_sec=int(settings.get("timeout_seconds") or 300),
                    request_timeout=int(settings.get("timeout_seconds") or 300),
                )
                if downloaded != raw_path:
                    shutil.move(str(downloaded), raw_path)
                break
            except Exception as exc:
                last_error = exc
                if attempt >= int(settings.get("retry_count") or 0):
                    raise
                self._sleep_with_control(run_id, float(settings.get("pause_between_requests") or 0.0))
        if last_error and not raw_path.exists():
            raise last_error
        if not raw_path.exists() or raw_path.stat().st_size == 0:
            self.repository.update_download_task(task_id, {"status": "no_data", "download_status": "no_data"})
            return
        self.repository.update_download_task(task_id, {"status": "downloaded", "download_status": "downloaded", "raw_file_path": str(raw_path)})
        pause = float(settings.get("pause_between_requests") or 0.0)
        if pause:
            self._sleep_with_control(run_id, pause)

    def _process_task(self, task_id: str, *, settings: dict[str, Any]) -> None:
        task = self.repository.get_download_task(task_id)
        if not task:
            raise KeyError(task_id)
        self._check_control(str(task["run_id"]))
        processed_path = Path(str(task["processed_file_path"]))
        if processed_path.exists() and not settings.get("overwrite_processed"):
            rows = len(read_semicolon_csv(processed_path, low_memory=False))
            self.repository.update_download_task(
                task_id,
                {"status": "processed", "process_status": "processed", "processed_file_path": str(processed_path), "rows_count": rows},
            )
            return
        raw_path = Path(str(task["raw_file_path"]))
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw-файл не найден: {raw_path}")
        self.repository.update_pipeline_run(str(task["run_id"]), {"current_step": f"Обработка {task['category_name']} {ym(int(task['year']), int(task['month']))}"})
        self.repository.update_download_task(task_id, {"status": "processing", "process_status": "processing", "error_message": None})
        raw_df = read_csv_auto(raw_path, low_memory=False)
        if raw_df.empty:
            self.repository.update_download_task(task_id, {"status": "no_data", "process_status": "no_data", "rows_count": 0})
            return
        for column in ("Дата", "Маркетплейс", "Категория"):
            if column in raw_df.columns:
                raw_df = raw_df.drop(columns=[column])
        raw_df.insert(0, "Дата", f"01.{int(task['month']):02d}.{int(task['year'])}")
        raw_df.insert(1, "Маркетплейс", task["marketplace"])
        raw_df.insert(2, "Категория", task["category_name"])
        marketplace_type = {"oz": "ozon", "wb": "wb", "ym": "ym"}[str(task["marketplace_code"])]
        standardized = standardize_dataframe(raw_df, marketplace_type)
        parsed = parse_weights_dataframe(standardized, max_weight_kg=float(settings.get("max_weight_kg") or 40.0))
        processed_path.parent.mkdir(parents=True, exist_ok=True)
        write_semicolon_csv(parsed, processed_path)
        self.repository.update_download_task(
            task_id,
            {"status": "processed", "process_status": "processed", "processed_file_path": str(processed_path), "rows_count": len(parsed)},
        )

    def _classify_task(self, task_id: str, *, settings: dict[str, Any], force_reclassify: bool = False) -> None:
        task = self.repository.get_download_task(task_id)
        if not task:
            raise KeyError(task_id)
        self._check_control(str(task["run_id"]))
        classified_path = Path(str(task["classified_file_path"]))
        if classified_path.exists() and not force_reclassify and not settings.get("overwrite_processed"):
            rows = len(read_semicolon_csv(classified_path, low_memory=False))
            self.repository.update_download_task(
                task_id,
                {"status": "classified", "classify_status": "classified", "classified_file_path": str(classified_path), "rows_count": rows},
            )
            return
        processed_path = Path(str(task["processed_file_path"]))
        if not processed_path.exists():
            if force_reclassify:
                raise FileNotFoundError(f"Processed-файл для повторной классификации не найден: {processed_path}")
            if classified_path.exists():
                return
            raise FileNotFoundError(f"Processed-файл не найден: {processed_path}")
        self.repository.update_pipeline_run(str(task["run_id"]), {"current_step": f"Классификация {task['category_name']} {ym(int(task['year']), int(task['month']))}"})
        self.repository.update_download_task(task_id, {"status": "classifying", "classify_status": "classifying", "error_message": None})
        _, _, result = classify_file(
            processed_path,
            classified_path,
            rules_path=self.settings.rules_path,
            write_xlsx=False,
            manual_overrides_path=self.settings.manual_overrides_path,
        )
        self.repository.update_download_task(
            task_id,
            {
                "status": "classified",
                "classify_status": "classified",
                "classified_file_path": str(classified_path),
                "rows_count": result.rows,
            },
        )

    def _save_task(self, task_id: str, *, settings: dict[str, Any]) -> None:
        task = self.repository.get_download_task(task_id)
        if not task:
            raise KeyError(task_id)
        self._check_control(str(task["run_id"]))
        existing = self.repository.get_cube_entry(
            project_name=str(task["project_name"]),
            year=int(task["year"]),
            month=int(task["month"]),
            marketplace_code=str(task["marketplace_code"]),
            category_key=str(task["category_key"]),
        )
        existing_by_category = self.repository.get_cube_entry_by_natural_key(
            project_name=str(task["project_name"]),
            year=int(task["year"]),
            month=int(task["month"]),
            marketplace_code=str(task["marketplace_code"]),
            category_name=str(task["category_name"]),
        )
        existing = existing_by_category or existing
        if existing and not settings.get("overwrite_db"):
            self.repository.update_download_task(
                task_id,
                {
                    "status": "saved_to_db",
                    "save_status": "saved_to_db",
                    "rows_count": int(existing.get("rows_count") or 0),
                    "error_message": None,
                },
            )
            return
        classified_path = Path(str(task["classified_file_path"]))
        if not classified_path.exists():
            raise FileNotFoundError(f"Classified-файл не найден: {classified_path}")
        if (
            existing_by_category
            and str(existing_by_category.get("category_key") or "") != str(task["category_key"])
            and settings.get("overwrite_db")
        ):
            self.repository.delete_cube_entry(entry_id=str(existing_by_category["id"]), table_name=self.settings.products_table)
        self.repository.update_pipeline_run(str(task["run_id"]), {"current_step": f"Сохранение {task['category_name']} {ym(int(task['year']), int(task['month']))}"})
        self.repository.update_download_task(task_id, {"status": "saving_to_db", "save_status": "saving_to_db", "error_message": None})
        inserted = self.repository.import_products_file_idempotent(
            run_id=str(task["run_id"]),
            csv_path=classified_path,
            table_name=self.settings.products_table,
            project_name=str(task["project_name"]),
            year=int(task["year"]),
            month=int(task["month"]),
            marketplace_code=str(task["marketplace_code"]),
            category_key=str(task["category_key"]),
            category_name=str(task["category_name"]),
            source_type=normalize_source_type(task.get("source_type")),
            overwrite=bool(settings.get("overwrite_db")),
        )
        raw_path = Path(str(task.get("raw_file_path") or ""))
        self.repository.upsert_cube_entry(
            {
                "project_name": task["project_name"],
                "year": int(task["year"]),
                "month": int(task["month"]),
                "marketplace": task["marketplace"],
                "marketplace_code": task["marketplace_code"],
                "source_type": normalize_source_type(task.get("source_type")),
                "category_key": task["category_key"],
                "category_name": task["category_name"],
                "rows_count": inserted,
                "exported_at": file_timestamp(raw_path) or file_timestamp(classified_path),
                "source_processed_file_path": str(classified_path),
                "file_hash": file_sha1(classified_path),
                **month_day_coverage(int(task["year"]), int(task["month"])),
            }
        )
        self.repository.refresh_large_category_flags(
            project_name=str(task["project_name"]),
            category_keys=[str(task["category_key"])],
        )
        self.repository.update_download_task(
            task_id,
            {"status": "saved_to_db", "save_status": "saved_to_db", "rows_count": inserted, "error_message": None},
        )
        if inserted == 0:
            return

    @staticmethod
    def _project_file_kind(relative_path: Path) -> str | None:
        top = relative_path.parts[0] if relative_path.parts else ""
        if top == "merged":
            return None
        if top == "raw":
            return "raw"
        if top == "processed":
            return "classified" if relative_path.name.endswith("_classified.csv") else "processed"
        if top == "exports":
            return "export"
        return "other"

    def _build_task(
        self,
        *,
        run_id: str,
        project_name: str,
        category: dict[str, Any],
        year: int,
        month: int,
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        category_key = str(category["category_id"])
        source_type = category_source_type(category)
        task_hash = hashlib.sha1(
            json.dumps(
                {
                    "project_name": project_name,
                    "mp": category["mp_code"],
                    "source_type": source_type,
                    "category_key": category_key,
                    "year": year,
                    "month": month,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        paths = self._task_paths(project_name, year, month, str(category["mp_code"]), category_key)
        statuses = self._initial_statuses(
            project_name=project_name,
            year=year,
            month=month,
            marketplace_code=str(category["mp_code"]),
            category_key=category_key,
            category_name=str(category["category_name"]),
            raw_path=paths["raw"],
            processed_path=paths["processed"],
            classified_path=paths["classified"],
            overwrite_db=bool(settings.get("overwrite_db")),
        )
        return {
            "id": task_hash[:24],
            "run_id": run_id,
            "project_name": project_name,
            "marketplace": category["marketplace"],
            "marketplace_code": category["mp_code"],
            "source_type": source_type,
            "category_name": category["category_name"],
            "category_path": category["path"],
            "category_id": category["category_id"],
            "category_key": category_key,
            "year": year,
            "month": month,
            "raw_file_path": str(paths["raw"]),
            "processed_file_path": str(paths["processed"]),
            "classified_file_path": str(paths["classified"]),
            "task_hash": task_hash,
            **statuses,
        }

    def _initial_statuses(
        self,
        *,
        project_name: str,
        year: int,
        month: int,
        marketplace_code: str,
        category_key: str,
        category_name: str,
        raw_path: Path,
        processed_path: Path,
        classified_path: Path,
        overwrite_db: bool,
    ) -> dict[str, Any]:
        existing = self.repository.get_cube_entry(
            project_name=project_name,
            year=year,
            month=month,
            marketplace_code=marketplace_code,
            category_key=category_key,
        )
        existing_by_category = self.repository.get_cube_entry_by_natural_key(
            project_name=project_name,
            year=year,
            month=month,
            marketplace_code=marketplace_code,
            category_name=category_name,
        )
        existing = existing_by_category or existing
        if existing and not overwrite_db:
            return {
                "status": "saved_to_db",
                "download_status": "downloaded",
                "process_status": "processed",
                "classify_status": "classified",
                "save_status": "saved_to_db",
                "rows_count": int(existing.get("rows_count") or 0),
                "error_message": None,
            }
        if classified_path.exists():
            return {
                "status": "classified",
                "download_status": "downloaded" if raw_path.exists() else "skipped",
                "process_status": "processed",
                "classify_status": "classified",
                "save_status": "pending",
                "rows_count": 0,
                "error_message": None,
            }
        if processed_path.exists():
            return {
                "status": "processed",
                "download_status": "downloaded" if raw_path.exists() else "skipped",
                "process_status": "processed",
                "classify_status": "pending",
                "save_status": "pending",
                "rows_count": 0,
                "error_message": None,
            }
        if raw_path.exists():
            return {
                "status": "downloaded",
                "download_status": "downloaded",
                "process_status": "pending",
                "classify_status": "pending",
                "save_status": "pending",
                "rows_count": 0,
                "error_message": None,
            }
        return {
            "status": "pending",
            "download_status": "pending",
            "process_status": "pending",
            "classify_status": "pending",
            "save_status": "pending",
            "rows_count": 0,
            "error_message": None,
        }

    def _task_paths(self, project_name: str, year: int, month: int, mp_code: str, category_key: str) -> dict[str, Path]:
        root = self._project_data_root(project_name)
        month_dir = ym(year, month)
        return {
            "raw": root / "raw" / month_dir / mp_code / f"{category_key}.csv",
            "processed": root / "processed" / month_dir / mp_code / f"{category_key}.csv",
            "classified": root / "processed" / month_dir / mp_code / f"{category_key}_classified.csv",
        }

    def _project_data_root(self, project_name: str) -> Path:
        return self.settings.project_root / "data" / "projects" / safe_segment(project_name)

    def _resolve_project_file(self, *, project_name: str, path: str) -> tuple[Path, Path, str]:
        root = self._project_data_root(project_name).resolve()
        raw_path = Path(path)
        target = (raw_path if raw_path.is_absolute() else root / raw_path).resolve()
        try:
            relative_path = target.relative_to(root)
        except ValueError as exc:
            raise ValueError("Можно удалять только файлы внутри папки выбранного проекта.") from exc
        if not target.is_file():
            raise FileNotFoundError(f"Файл не найден: {target}")
        kind = self._project_file_kind(relative_path)
        if kind is None:
            raise ValueError("Legacy merged-файлы не удаляются из web-flow.")
        return target, relative_path, kind

    def _task_to_export_task(self, task: dict[str, Any]) -> dict[str, Any]:
        export_task: dict[str, Any] = {
            "mp": task["marketplace_code"],
            "path": task["category_path"],
            "cat": task["category_name"],
            "source_type": normalize_source_type(task.get("source_type")),
        }
        category = self.repository.get_categories_by_ids([str(task["category_id"])])
        if category and category_uses_fbs(category[0]):
            export_task["fbs"] = 1
        if category and category[0].get("filter_json"):
            export_task["filterModel"] = json.loads(str(category[0]["filter_json"]))
        return export_task

    def _check_control(self, run_id: str) -> None:
        if self.repository.is_pipeline_stop_requested(run_id):
            raise _PipelineStopped()
        if self.repository.is_pipeline_pause_requested(run_id):
            raise _PipelinePaused()

    def _sleep_with_control(self, run_id: str, seconds: float) -> None:
        deadline = time.monotonic() + max(0.0, seconds)
        while True:
            self._check_control(run_id)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.2, remaining))

    def _is_thread_active(self, run_id: str) -> bool:
        thread = self._threads.get(run_id)
        return bool(thread and thread.is_alive())

    @staticmethod
    def _json_dict(value: Any) -> dict[str, Any]:
        if not value:
            return {}
        if isinstance(value, dict):
            return value
        try:
            parsed = json.loads(str(value))
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}


class _PipelineInterrupted(Exception):
    pass


class _PipelinePaused(_PipelineInterrupted):
    pass


class _PipelineStopped(_PipelineInterrupted):
    pass
