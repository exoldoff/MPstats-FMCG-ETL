from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
import shutil
from typing import Any

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository


def _safe_segment(value: str) -> str:
    segment = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return segment.strip("._") or "mpstats"


class ProjectService:
    def __init__(self, *, settings: AppSettings, repository: DuckDbAppRepository) -> None:
        self.settings = settings
        self.repository = repository

    def list_projects(self) -> dict[str, Any]:
        db_items = {
            str(item["project_name"]): dict(item)
            for item in self.repository.list_project_database_summaries(table_name=self.settings.products_table)
        }
        for project_dir in self._project_dirs():
            item = db_items.setdefault(
                project_dir.name,
                {
                    "project_name": project_dir.name,
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
                },
            )
            self._merge_file_stats(item, project_dir)

        for project_name, item in db_items.items():
            if "data_path" not in item:
                self._merge_file_stats(item, self._preferred_project_dir(project_name))
            item["total_runs_count"] = int(item.get("pipeline_runs_count") or 0) + int(item.get("app_runs_count") or 0)
            item["is_current"] = project_name == (self.repository.get_setting("project_name") or "mpstats")

        return {"projects": sorted(db_items.values(), key=lambda item: str(item["project_name"]).casefold())}

    def delete_project(self, *, project_name: str, delete_files: bool = False) -> dict[str, Any]:
        normalized = project_name.strip()
        if not normalized:
            raise ValueError("Название проекта не заполнено.")

        deleted = self.repository.delete_project_records(
            project_name=normalized,
            table_name=self.settings.products_table,
        )

        deleted_file_paths: list[str] = []
        skipped_file_paths: list[str] = []
        if delete_files:
            for project_dir in self._delete_candidate_dirs(normalized):
                if not project_dir.exists():
                    continue
                if not self._is_inside_projects_root(project_dir):
                    skipped_file_paths.append(str(project_dir))
                    continue
                shutil.rmtree(project_dir)
                deleted_file_paths.append(str(project_dir))

        if self.repository.get_setting("project_name") == normalized:
            self.repository.set_setting("project_name", "mpstats")

        return {
            "project_name": normalized,
            "deleted": deleted,
            "deleted_file_paths": deleted_file_paths,
            "skipped_file_paths": skipped_file_paths,
        }

    @property
    def projects_root(self) -> Path:
        return self.settings.project_root / "data" / "projects"

    def _project_dirs(self) -> list[Path]:
        if not self.projects_root.exists():
            return []
        return sorted(path for path in self.projects_root.iterdir() if path.is_dir())

    def _preferred_project_dir(self, project_name: str) -> Path:
        exact = self.projects_root / project_name
        if exact.exists():
            return exact
        return self.projects_root / _safe_segment(project_name)

    def _delete_candidate_dirs(self, project_name: str) -> list[Path]:
        exact = self.projects_root / project_name
        safe = self.projects_root / _safe_segment(project_name)
        if exact == safe:
            return [exact]
        if exact.exists():
            return [exact]
        # Avoid deleting a shared fallback path such as data/projects/mpstats for
        # a non-ASCII project name that was sanitized to "mpstats" by legacy code.
        if safe.name == project_name:
            return [safe]
        return []

    def _merge_file_stats(self, item: dict[str, Any], project_dir: Path) -> None:
        files_count = 0
        files_size = 0
        latest_file_activity: str | None = None
        if project_dir.exists():
            for path in project_dir.rglob("*"):
                if not path.is_file():
                    continue
                stat = path.stat()
                files_count += 1
                files_size += stat.st_size
                updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
                latest_file_activity = max(latest_file_activity or "", updated_at) or None

        item["data_path"] = str(project_dir)
        item["files_count"] = files_count
        item["files_size"] = files_size
        item["has_files"] = files_count > 0
        if latest_file_activity:
            item["latest_activity"] = max(str(item.get("latest_activity") or ""), latest_file_activity)

    def _is_inside_projects_root(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.projects_root.resolve())
        except ValueError:
            return False
        return path.resolve() != self.projects_root.resolve()
