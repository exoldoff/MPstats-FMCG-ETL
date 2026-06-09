from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository


SMART_PLAN_STATUSES = ("ready", "missing", "stale", "failed", "incomplete")


class SmartPlanService:
    def __init__(self, *, settings: AppSettings, repository: DuckDbAppRepository) -> None:
        self.settings = settings
        self.repository = repository

    def get_run_plan(self, *, run_id: str, status: str = "all") -> dict[str, Any]:
        if status != "all" and status not in SMART_PLAN_STATUSES:
            raise ValueError("Неизвестный фильтр умного плана.")

        run = self.repository.refresh_pipeline_run_counts(run_id) or self.repository.get_pipeline_run(run_id)
        if not run:
            raise KeyError(f"Запуск не найден: {run_id}")

        items = [self._inspect_task(task) for task in self.repository.list_download_tasks(run_id=run_id)]
        summary = self._summary(items)
        filtered_items = items if status == "all" else [item for item in items if item["smart_status"] == status]
        return {
            "run_id": run_id,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "summary": summary,
            "recommended_action": self._recommended_action(summary),
            "tasks": filtered_items,
        }

    def _inspect_task(self, task: dict[str, Any]) -> dict[str, Any]:
        raw_file = self._file_state(task.get("raw_file_path"))
        processed_file = self._file_state(task.get("processed_file_path"))
        classified_file = self._file_state(task.get("classified_file_path"))
        cube_entry = self.repository.get_cube_entry(
            project_name=str(task["project_name"]),
            year=int(task["year"]),
            month=int(task["month"]),
            marketplace_code=str(task["marketplace_code"]),
            category_key=str(task["category_key"]),
        )

        status, reason, action = self._task_status(
            task=task,
            raw_file=raw_file,
            processed_file=processed_file,
            classified_file=classified_file,
            cube_entry=cube_entry,
        )
        self._drop_internal_file_fields(raw_file, processed_file, classified_file)
        return {
            "task_id": task["id"],
            "run_id": task["run_id"],
            "project_name": task["project_name"],
            "marketplace": task["marketplace"],
            "marketplace_code": task["marketplace_code"],
            "source_type": task.get("source_type") or "category",
            "category_name": task["category_name"],
            "category_path": task["category_path"],
            "category_id": task["category_id"],
            "category_key": task["category_key"],
            "year": int(task["year"]),
            "month": int(task["month"]),
            "pipeline_status": task["status"],
            "download_status": task["download_status"],
            "process_status": task["process_status"],
            "classify_status": task["classify_status"],
            "save_status": task["save_status"],
            "rows_count": int(task.get("rows_count") or 0),
            "error_message": task.get("error_message"),
            "smart_status": status,
            "reason": reason,
            "recommended_action": action,
            "has_cube": bool(cube_entry),
            "cube_rows_count": int(cube_entry.get("rows_count") or 0) if cube_entry else 0,
            "cube_saved_at": self._iso(cube_entry.get("saved_to_db_at")) if cube_entry else None,
            "raw_file": raw_file,
            "processed_file": processed_file,
            "classified_file": classified_file,
        }

    def _task_status(
        self,
        *,
        task: dict[str, Any],
        raw_file: dict[str, Any],
        processed_file: dict[str, Any],
        classified_file: dict[str, Any],
        cube_entry: dict[str, Any] | None,
    ) -> tuple[str, str, str]:
        if str(task.get("status")) == "failed" or task.get("error_message"):
            reason = str(task.get("error_message") or "Последний запуск задачи завершился ошибкой.")
            return "failed", reason, "Повторить задачу"

        if str(task.get("status")) == "no_data":
            return "ready", "MPStats вернул пустой результат; задача завершена без данных.", "Ничего не требуется"

        raw_ready = self._file_ready(raw_file)
        processed_ready = self._file_ready(processed_file)
        classified_ready = self._file_ready(classified_file)
        stale_reason = self._stale_reason(
            raw_file=raw_file,
            processed_file=processed_file,
            classified_file=classified_file,
            cube_entry=cube_entry,
        )
        if stale_reason:
            return "stale", stale_reason, "Пересобрать устаревший результат"

        if cube_entry:
            return "ready", "Срез уже сохранён в БД / Куб.", "Ничего не требуется"

        if classified_ready:
            return "ready", "Classified-файл готов, но ещё не сохранён в БД.", "Собрать куб из готовых файлов"

        if raw_ready or processed_ready:
            missing = []
            if not processed_ready:
                missing.append("processed")
            if not classified_ready:
                missing.append("classified")
            return "incomplete", f"Есть часть локальных файлов, не хватает: {', '.join(missing)}.", "Достроить задачу"

        return "missing", "Для задачи не найдено готовых локальных файлов.", "Запустить скачивание"

    def _stale_reason(
        self,
        *,
        raw_file: dict[str, Any],
        processed_file: dict[str, Any],
        classified_file: dict[str, Any],
        cube_entry: dict[str, Any] | None,
    ) -> str | None:
        if self._newer(raw_file, processed_file):
            return "Raw-файл новее processed-файла."
        if self._newer(processed_file, classified_file):
            return "Processed-файл новее classified-файла."
        if cube_entry and self._file_ready(classified_file):
            saved_at = self._coerce_datetime(cube_entry.get("saved_to_db_at"))
            classified_mtime = classified_file.get("_updated_dt")
            if saved_at and classified_mtime and classified_mtime > saved_at:
                return "Classified-файл новее сохранённого среза БД."
            stored_hash = str(cube_entry.get("file_hash") or "")
            path = classified_file.get("path")
            if stored_hash and path and Path(str(path)).exists() and self._file_sha1(Path(str(path))) != stored_hash:
                return "Хэш classified-файла отличается от сохранённого в registry."
        return None

    def _summary(self, items: list[dict[str, Any]]) -> dict[str, int]:
        summary = {status: 0 for status in SMART_PLAN_STATUSES}
        summary.update({"total": len(items), "saved_to_db": 0, "ready_for_db": 0})
        for item in items:
            summary[str(item["smart_status"])] += 1
            if item["has_cube"]:
                summary["saved_to_db"] += 1
            elif item["smart_status"] == "ready":
                summary["ready_for_db"] += 1
        return summary

    @staticmethod
    def _recommended_action(summary: dict[str, int]) -> dict[str, str]:
        if summary["failed"]:
            return {
                "key": "retry_failed",
                "label": "Повторить ошибки",
                "detail": f"{summary['failed']} задач упали при прошлом запуске.",
            }
        if summary["stale"]:
            return {
                "key": "rebuild_stale",
                "label": "Пересобрать устаревшие",
                "detail": f"{summary['stale']} задач имеют более свежий upstream-файл или classified новее БД.",
            }
        if summary["incomplete"]:
            return {
                "key": "continue_incomplete",
                "label": "Достроить неполные",
                "detail": f"{summary['incomplete']} задач имеют только часть локальных файлов.",
            }
        if summary["missing"]:
            return {
                "key": "start_missing",
                "label": "Запустить скачивание",
                "detail": f"{summary['missing']} задач пока без локальных файлов.",
            }
        if summary["ready_for_db"]:
            return {
                "key": "rebuild_cube",
                "label": "Собрать куб из готовых файлов",
                "detail": f"{summary['ready_for_db']} задач готовы локально, но ещё не сохранены в БД.",
            }
        return {"key": "none", "label": "Ничего не требуется", "detail": "Все задачи плана готовы."}

    @staticmethod
    def _file_state(path_value: Any) -> dict[str, Any]:
        if not path_value:
            return {"path": None, "exists": False, "size": 0, "updated_at": None}
        path = Path(str(path_value))
        if not path.is_file():
            return {"path": str(path), "exists": False, "size": 0, "updated_at": None}
        stat = path.stat()
        updated = datetime.fromtimestamp(stat.st_mtime)
        return {
            "path": str(path),
            "exists": True,
            "size": int(stat.st_size),
            "updated_at": updated.isoformat(timespec="seconds"),
            "_updated_dt": updated,
        }

    @staticmethod
    def _drop_internal_file_fields(*files: dict[str, Any]) -> None:
        for file in files:
            file.pop("_updated_dt", None)

    @staticmethod
    def _file_ready(file: dict[str, Any]) -> bool:
        return bool(file.get("exists")) and int(file.get("size") or 0) > 0

    def _newer(self, source: dict[str, Any], target: dict[str, Any]) -> bool:
        if not self._file_ready(source) or not self._file_ready(target):
            return False
        source_updated = source.get("_updated_dt")
        target_updated = target.get("_updated_dt")
        return bool(source_updated and target_updated and source_updated > target_updated)

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _iso(value: Any) -> str | None:
        parsed = SmartPlanService._coerce_datetime(value)
        return parsed.isoformat(timespec="seconds") if parsed else None

    @staticmethod
    def _file_sha1(path: Path) -> str:
        digest = hashlib.sha1()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
