from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

import pandas as pd

from pipeline.models import PipelinePaths, StepResult
from pipeline.repositories.file_repository import read_csv_auto
from pipeline.services.classification_service import classify_file
from pipeline.services.enrich_service import enrich_directory
from pipeline.services.export_service import ExportSettings, run_export
from pipeline.services.merge_service import merge_directory
from pipeline.services.standardize_service import standardize_directory
from pipeline.services.weight_parser_service import parse_weights_directory

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.services.category_catalog_service import CategoryCatalogService
from mpstats_app.utils import clean_records


CLASSIFIER_UPLOAD_SUFFIXES = {".csv", ".xlsx"}


def month_range_by_year(start_year: int, start_month: int, end_year: int, end_month: int) -> dict[int, tuple[int, ...]]:
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("Начальный период больше конечного.")
    out: dict[int, list[int]] = {}
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        out.setdefault(year, []).append(month)
        month += 1
        if month > 12:
            year += 1
            month = 1
    return {year: tuple(months) for year, months in out.items()}


class WorkflowService:
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

    def get_settings(self) -> dict[str, Any]:
        return {
            "cookie": self.repository.get_setting("mpstats_cookie") or "",
            "project_name": self.repository.get_setting("project_name") or "mpstats",
            "workflow_mode": self.repository.get_setting("workflow_mode") or "historical_backfill",
            "start_year": self._optional_int_setting("start_year"),
            "start_month": self._optional_int_setting("start_month"),
            "end_year": self._optional_int_setting("end_year"),
            "end_month": self._optional_int_setting("end_month"),
        }

    def save_settings(
        self,
        *,
        cookie: str,
        project_name: str,
        workflow_mode: str = "historical_backfill",
        start_year: int | None = None,
        start_month: int | None = None,
        end_year: int | None = None,
        end_month: int | None = None,
    ) -> dict[str, Any]:
        self.repository.set_setting("mpstats_cookie", cookie)
        self.repository.set_setting("project_name", project_name.strip() or "mpstats")
        self.repository.set_setting("workflow_mode", workflow_mode if workflow_mode in {"historical_backfill", "monthly_sync"} else "historical_backfill")
        for key, value in {
            "start_year": start_year,
            "start_month": start_month,
            "end_year": end_year,
            "end_month": end_month,
        }.items():
            if value is not None:
                self.repository.set_setting(key, str(value))
        return self.get_settings()

    def _optional_int_setting(self, key: str) -> int | None:
        raw = self.repository.get_setting(key)
        if raw in (None, ""):
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    def download(
        self,
        *,
        project_name: str,
        cookie: str,
        category_ids: list[str],
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        skip_if_exists: bool = True,
    ) -> dict[str, Any]:
        tasks = self.catalog_service.selected_tasks(category_ids)
        if not tasks:
            raise ValueError("Выбери хотя бы одну категорию.")
        self.save_settings(cookie=cookie, project_name=project_name)
        paths = PipelinePaths.create(project_root=self.settings.project_root, workdir=self.settings.workdir, project_name=project_name)
        paths.ensure_dirs()
        run_id = self._start_action(project_name, "download")
        self.repository.mark_run_running(run_id)
        try:
            settings = ExportSettings(
                export_months_by_year=month_range_by_year(start_year, start_month, end_year, end_month),
                save_dir=paths.step1_raw_dir,
                skip_if_exists=skip_if_exists,
                extract_zip=True,
                cookie=cookie,
                tasks=tasks,
            )
            result = run_export(settings, log_dir=paths.logs_dir)
            self._record_action_step(run_id, 1, result)
            self.repository.finish_run(run_id, "failed" if result.failed else "succeeded")
            return self._action_payload(run_id, result)
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", error=str(exc))
            raise

    def process(self, *, project_name: str, max_weight_kg: float = 40.0) -> dict[str, Any]:
        paths = PipelinePaths.create(project_root=self.settings.project_root, workdir=self.settings.workdir, project_name=project_name)
        paths.ensure_dirs()
        run_id = self._start_action(project_name, "process")
        self.repository.mark_run_running(run_id)
        results: list[StepResult] = []
        try:
            steps = [
                (2, enrich_directory(paths.step1_raw_dir, paths.step2_enriched_dir)),
                (3, standardize_directory(paths.step2_enriched_dir, paths.step3_standardized_dir)),
                (4, parse_weights_directory(paths.step3_standardized_dir, paths.step4_parsed_dir, max_weight_kg=max_weight_kg)),
            ]
            merged, merge_result = merge_directory(paths.step4_parsed_dir, paths.merged_csv)
            steps.append((5, merge_result))
            for step_number, result in steps:
                results.append(result)
                self._record_action_step(run_id, step_number, result)
                if result.failed:
                    self.repository.finish_run(run_id, "failed", error=f"{result.name} failed")
                    return {"run_id": run_id, "status": "failed", "results": [self._result_dict(item) for item in results]}
            self.repository.finish_run(run_id, "succeeded")
            return {
                "run_id": run_id,
                "status": "succeeded",
                "output_file": str(paths.merged_csv),
                "rows": len(merged),
                "preview": self.preview_file(paths.merged_csv),
                "results": [self._result_dict(item) for item in results],
            }
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", error=str(exc))
            raise

    def classify(
        self,
        *,
        project_name: str,
        input_file: str | None = None,
        overwrite_input: bool = False,
        write_xlsx: bool = False,
    ) -> dict[str, Any]:
        paths = PipelinePaths.create(project_root=self.settings.project_root, workdir=self.settings.workdir, project_name=project_name)
        source = Path(input_file).expanduser() if input_file else paths.merged_csv
        if not source.exists():
            raise FileNotFoundError(f"Файл для классификации не найден: {source}")
        output = source if overwrite_input else paths.classified_csv
        return self._classify_source(project_name=project_name, source=source, output=output, write_xlsx=write_xlsx, action="classify")

    def classify_uploaded_file(
        self,
        *,
        project_name: str,
        filename: str,
        content: bytes,
        write_xlsx: bool = False,
    ) -> dict[str, Any]:
        if not content:
            raise ValueError("Файл пустой. Выбери CSV или XLSX с данными для классификации.")
        source_name = _safe_filename(filename)
        suffix = Path(source_name).suffix.lower()
        if suffix not in CLASSIFIER_UPLOAD_SUFFIXES:
            allowed = ", ".join(sorted(CLASSIFIER_UPLOAD_SUFFIXES))
            raise ValueError(f"Неподдерживаемый формат файла. Можно загрузить: {allowed}.")

        paths = PipelinePaths.create(project_root=self.settings.project_root, workdir=self.settings.workdir, project_name=project_name)
        paths.ensure_dirs()
        upload_dir = paths.workdir / "external_classification"
        upload_dir.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_prefix = f"{stamp}_{uuid4().hex[:8]}"
        source = upload_dir / f"{unique_prefix}_{source_name}"
        source.write_bytes(content)
        output = upload_dir / f"{source.stem}_classified.csv"

        return self._classify_source(
            project_name=project_name,
            source=source,
            output=output,
            write_xlsx=write_xlsx,
            action="classify_external",
        )

    def _classify_source(
        self,
        *,
        project_name: str,
        source: Path,
        output: Path,
        write_xlsx: bool,
        action: str,
    ) -> dict[str, Any]:
        run_id = self._start_action(project_name, action)
        self.repository.mark_run_running(run_id)
        try:
            _, _, result = classify_file(source, output, rules_path=self.settings.rules_path, write_xlsx=write_xlsx)
            self._record_action_step(run_id, 6, result)
            self.repository.finish_run(run_id, "failed" if result.failed else "succeeded")
            payload = {
                "run_id": run_id,
                "status": "failed" if result.failed else "succeeded",
                "input_file": str(source),
                "output_file": str(output),
                "preview": self.preview_file(output),
                "result": self._result_dict(result),
            }
            if write_xlsx and output.with_suffix(".xlsx").exists():
                payload["output_xlsx"] = str(output.with_suffix(".xlsx"))
            return payload
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", error=str(exc))
            raise

    def preview(self, *, project_name: str, file_kind: str = "classified", file_path: str | None = None) -> dict[str, Any]:
        paths = PipelinePaths.create(project_root=self.settings.project_root, workdir=self.settings.workdir, project_name=project_name)
        if file_path:
            target = Path(file_path).expanduser()
        elif file_kind == "merged":
            target = paths.merged_csv
        else:
            target = paths.classified_csv if paths.classified_csv.exists() else paths.merged_csv
        return self.preview_file(target)

    def preview_file(self, path: str | Path, *, limit: int = 50) -> dict[str, Any]:
        target = Path(path)
        if not target.exists():
            return {"file": str(target), "columns": [], "rows": [], "total": 0}
        df = read_csv_auto(target, low_memory=False)
        return {
            "file": str(target),
            "columns": [str(column) for column in df.columns],
            "rows": clean_records(df.head(limit).where(pd.notna(df), None).to_dict(orient="records")),
            "total": len(df),
        }

    def save_to_db(self, *, project_name: str, file_path: str | None = None) -> dict[str, Any]:
        paths = PipelinePaths.create(project_root=self.settings.project_root, workdir=self.settings.workdir, project_name=project_name)
        target = Path(file_path).expanduser() if file_path else (paths.classified_csv if paths.classified_csv.exists() else paths.merged_csv)
        if not target.exists():
            raise FileNotFoundError(f"Файл для сохранения в БД не найден: {target}")
        run_id = self._start_action(project_name, "save_to_db")
        rows = self.repository.import_products_file(
            run_id=run_id,
            csv_path=target,
            table_name=self.settings.products_table,
            project_name=project_name,
            load_name=f"workflow:{run_id}",
        )
        self.repository.finish_run(run_id, "succeeded")
        return {"run_id": run_id, "table": self.settings.products_table, "rows": rows, "file": str(target)}

    def resolve_download_file(self, file_path: str) -> Path:
        target = Path(file_path).expanduser().resolve()
        allowed_roots = [
            self.settings.workdir.resolve(),
            (self.settings.project_root / "data" / "projects").resolve(),
        ]
        if not any(target.is_relative_to(root) for root in allowed_roots):
            raise ValueError("Можно скачать только файлы, созданные внутри рабочего каталога приложения.")
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"Файл не найден: {target}")
        return target

    def _start_action(self, project_name: str, action: str) -> str:
        run_id = uuid4().hex
        self.repository.create_run(
            run_id=run_id,
            project_name=project_name,
            steps=action,
            source="workflow",
            schedule_id=None,
            workdir=self.settings.workdir,
            config_path=self.settings.config_path,
            rules_path=self.settings.rules_path,
            db_path=self.settings.db_path,
            products_table=self.settings.products_table,
            write_xlsx=True,
            max_weight_kg=40.0,
            fill_unclassified=None,
        )
        return run_id

    def _record_action_step(self, run_id: str, step_number: int, result: StepResult) -> None:
        self.repository.record_step(
            run_id=run_id,
            step_number=step_number,
            step_name=result.name,
            status="failed" if result.failed else "succeeded",
            rows=result.rows,
            ok_count=result.ok,
            error_count=result.errors,
            skipped_count=result.skipped,
            output=str(result.output) if result.output else None,
            details=result.details,
            error="step failed" if result.failed else None,
            finished=True,
        )

    def _action_payload(self, run_id: str, result: StepResult) -> dict[str, Any]:
        return {"run_id": run_id, "status": "failed" if result.failed else "succeeded", "result": self._result_dict(result)}

    @staticmethod
    def _result_dict(result: StepResult) -> dict[str, Any]:
        payload = asdict(result)
        if result.output is not None:
            payload["output"] = str(result.output)
        return payload


def _safe_filename(filename: str) -> str:
    source = Path(filename or "external.csv").name
    suffix = Path(source).suffix
    stem = Path(source).stem or "external"
    safe_stem = re.sub(r"[^A-Za-z0-9А-Яа-яЁё_. -]+", "_", stem).strip(" ._")
    return f"{safe_stem or 'external'}{suffix.lower()}"
