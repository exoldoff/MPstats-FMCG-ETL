from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, date
import json
import math
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

import pandas as pd

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.utils import clean_records, clean_value


EXCEL_MAX_DATA_ROWS = 1_048_575
EXPORT_BATCH_SIZE = 50_000
LARGE_EXPORT_FILE_WARNING = 10
EXPORT_MATCH_TYPES = {"contains", "not_contains", "equals", "startswith", "gt", "gte", "lt", "lte"}
EXPORT_TEMPLATES_SETTING = "export_templates_json"


@dataclass(frozen=True)
class ExportSpec:
    project_name: str
    category_keys: list[str]
    period_from: str | None
    period_to: str | None
    period_from_index: int | None
    period_to_index: int | None
    selected_columns: list[str]
    filters: list[dict[str, str]]
    excluded_row_hashes: list[str]
    sort_column: str | None
    sort_direction: str
    split_by_category: bool


class ExportService:
    def __init__(self, *, settings: AppSettings, repository: DuckDbAppRepository) -> None:
        self.settings = settings
        self.repository = repository

    def options(self, *, project_name: str) -> dict[str, Any]:
        project = _clean_project_name(project_name)
        payload = self.repository.export_options(table_name=self.settings.products_table, project_name=project)
        payload.update(
            {
                "project_name": project,
                "default_output_dir": str(self.default_output_dir(project)),
                "excel_max_rows": EXCEL_MAX_DATA_ROWS,
            }
        )
        return payload

    def list_templates(self, *, project_name: str) -> dict[str, Any]:
        project = _clean_project_name(project_name)
        templates = [
            template
            for template in self._read_templates()
            if str(template.get("project_name") or "") == project
        ]
        return {"templates": sorted(templates, key=lambda item: str(item.get("updated_at") or ""), reverse=True)}

    def save_template(
        self,
        *,
        name: str,
        project_name: str,
        category_keys: list[str],
        period_from: str | None,
        period_to: str | None,
        selected_columns: list[str],
        filters: list[dict[str, str]],
        sort_column: str | None,
        sort_direction: str,
        split_by_category: bool,
        output_dir: str | None,
    ) -> dict[str, Any]:
        clean_name = name.strip()
        if not clean_name:
            raise ValueError("Название шаблона не заполнено.")
        spec = self._spec(
            project_name=project_name,
            category_keys=category_keys,
            period_from=period_from,
            period_to=period_to,
            selected_columns=selected_columns,
            filters=filters,
            excluded_row_hashes=[],
            sort_column=sort_column,
            sort_direction=sort_direction,
            split_by_category=split_by_category,
        )
        if not spec.category_keys:
            raise ValueError("Выбери хотя бы одну категорию для шаблона.")
        now = datetime.now().isoformat(timespec="seconds")
        templates = self._read_templates()
        existing = next(
            (
                template
                for template in templates
                if str(template.get("project_name") or "") == spec.project_name
                and str(template.get("name") or "").casefold() == clean_name.casefold()
            ),
            None,
        )
        template = {
            "id": str(existing.get("id")) if existing else uuid4().hex,
            "name": clean_name,
            "project_name": spec.project_name,
            "category_keys": spec.category_keys,
            "period_from": spec.period_from,
            "period_to": spec.period_to,
            "selected_columns": spec.selected_columns,
            "filters": spec.filters,
            "sort_column": spec.sort_column,
            "sort_direction": spec.sort_direction,
            "split_by_category": spec.split_by_category,
            "output_dir": (output_dir or "").strip() or None,
            "created_at": str(existing.get("created_at")) if existing else now,
            "updated_at": now,
        }
        templates = [item for item in templates if str(item.get("id") or "") != template["id"]]
        templates.append(template)
        self._write_templates(templates)
        return template

    def delete_template(self, *, template_id: str, project_name: str) -> dict[str, Any]:
        clean_id = template_id.strip()
        project = _clean_project_name(project_name)
        templates = self._read_templates()
        kept = [
            template
            for template in templates
            if not (str(template.get("id") or "") == clean_id and str(template.get("project_name") or "") == project)
        ]
        if len(kept) == len(templates):
            raise ValueError("Шаблон выгрузки не найден.")
        self._write_templates(kept)
        return {"template_id": clean_id, "project_name": project, "deleted": True}

    def preview(
        self,
        *,
        project_name: str,
        category_keys: list[str],
        period_from: str | None,
        period_to: str | None,
        selected_columns: list[str],
        filters: list[dict[str, str]],
        excluded_row_hashes: list[str],
        sort_column: str | None,
        sort_direction: str,
        split_by_category: bool,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        spec = self._spec(
            project_name=project_name,
            category_keys=category_keys,
            period_from=period_from,
            period_to=period_to,
            selected_columns=selected_columns,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
            sort_column=sort_column,
            sort_direction=sort_direction,
            split_by_category=split_by_category,
        )
        total = self._count(spec)
        df = self.repository.fetch_export_products_dataframe(
            table_name=self.settings.products_table,
            project_name=spec.project_name,
            output_columns=spec.selected_columns,
            category_keys=spec.category_keys,
            period_from_index=spec.period_from_index,
            period_to_index=spec.period_to_index,
            filters=spec.filters,
            excluded_row_hashes=spec.excluded_row_hashes,
            sort_column=spec.sort_column,
            sort_direction=spec.sort_direction,
            limit=limit,
            offset=offset,
            include_row_hash=True,
        )
        estimated_files = self._estimate_file_count(spec, total)
        breakdown = self._breakdown(spec)
        return {
            "columns": spec.selected_columns,
            "rows": clean_records(df.where(pd.notna(df), None).to_dict(orient="records")),
            "total": total,
            "estimated_files": estimated_files,
            "breakdown": breakdown,
            "warnings": self._warnings_for_estimate(estimated_files),
        }

    def build(
        self,
        *,
        project_name: str,
        category_keys: list[str],
        period_from: str | None,
        period_to: str | None,
        selected_columns: list[str],
        filters: list[dict[str, str]],
        excluded_row_hashes: list[str],
        sort_column: str | None,
        sort_direction: str,
        split_by_category: bool,
        output_dir: str | None,
        confirm_large_export: bool,
    ) -> dict[str, Any]:
        spec = self._spec(
            project_name=project_name,
            category_keys=category_keys,
            period_from=period_from,
            period_to=period_to,
            selected_columns=selected_columns,
            filters=filters,
            excluded_row_hashes=excluded_row_hashes,
            sort_column=sort_column,
            sort_direction=sort_direction,
            split_by_category=split_by_category,
        )
        total = self._count(spec)
        if total <= 0:
            raise ValueError("Нет строк для выгрузки. Проверь категории, период и фильтры.")

        estimated_files = self._estimate_file_count(spec, total)
        if estimated_files > LARGE_EXPORT_FILE_WARNING and not confirm_large_export:
            raise ValueError(
                f"Выгрузка создаст {estimated_files} файлов. Подтверди большую выгрузку и запусти экспорт ещё раз."
            )

        target_dir = self.resolve_output_dir(output_dir, spec.project_name)
        target_dir.mkdir(parents=True, exist_ok=True)
        self.repository.set_setting("export_output_dir", str(target_dir))

        artifacts: list[dict[str, Any]] = []
        if spec.split_by_category:
            categories = self._selected_categories(spec)
            for category in categories:
                category_spec = replace(spec, category_keys=[str(category["category_key"])])
                category_total = self._count(category_spec)
                if category_total <= 0:
                    continue
                artifacts.extend(
                    self._write_parts(
                        spec=category_spec,
                        total_rows=category_total,
                        output_dir=target_dir,
                        category=category,
                    )
                )
        else:
            artifacts.extend(self._write_parts(spec=spec, total_rows=total, output_dir=target_dir, category=None))

        return {
            "artifacts": artifacts,
            "total": sum(int(item["rows"]) for item in artifacts),
            "estimated_files": estimated_files,
            "output_dir": str(target_dir),
            "split_by_category": spec.split_by_category,
            "breakdown": self._breakdown(spec),
            "warnings": self._warnings_for_estimate(estimated_files),
        }

    def resolve_output_dir(self, output_dir: str | None, project_name: str) -> Path:
        project = _clean_project_name(project_name)
        raw = (output_dir or "").strip()
        path = Path(raw).expanduser() if raw else self.default_output_dir(project)
        if not path.is_absolute():
            path = self.settings.project_root / path
        if raw:
            path = _with_project_segment(path, project)
        return path.resolve()

    def default_output_dir(self, project_name: str) -> Path:
        return (self.settings.project_root / "data" / "projects" / _safe_segment(project_name) / "exports").resolve()

    def resolve_export_file(self, file_path: str) -> Path:
        target = Path(file_path).expanduser().resolve()
        allowed_roots = [(self.settings.project_root / "data" / "projects").resolve()]
        last_output_dir = self.repository.get_setting("export_output_dir")
        if last_output_dir:
            allowed_roots.append(Path(last_output_dir).expanduser().resolve())
        if not any(target.is_relative_to(root) for root in allowed_roots):
            raise ValueError("Можно скачать только файлы, созданные вкладкой выгрузки.")
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"Файл не найден: {target}")
        return target

    def _spec(
        self,
        *,
        project_name: str,
        category_keys: list[str],
        period_from: str | None,
        period_to: str | None,
        selected_columns: list[str],
        filters: list[dict[str, str]],
        excluded_row_hashes: list[str],
        sort_column: str | None,
        sort_direction: str,
        split_by_category: bool,
    ) -> ExportSpec:
        visible_columns = self.repository.export_visible_columns(table_name=self.settings.products_table)
        selected = [column for column in selected_columns if column in visible_columns]
        if not selected:
            selected = visible_columns
        normalized_filters = []
        for item in filters:
            column = str(item.get("column") or "")
            value = str(item.get("value") or "").strip()
            match_type = str(item.get("match_type") or "contains")
            if column in visible_columns and value:
                clean_match_type = match_type if match_type in EXPORT_MATCH_TYPES else "contains"
                if clean_match_type in {"gt", "gte", "lt", "lte"}:
                    _parse_filter_number(value, column)
                normalized_filters.append(
                    {
                        "column": column,
                        "value": value,
                        "match_type": clean_match_type,
                    }
                )
        period_from_index = _period_label_to_index(period_from)
        period_to_index = _period_label_to_index(period_to)
        if period_from_index is not None and period_to_index is not None and period_from_index > period_to_index:
            raise ValueError("Начальный период выгрузки больше конечного.")
        clean_category_keys = sorted({str(key) for key in category_keys if str(key).strip()})
        clean_hashes = sorted({str(row_hash) for row_hash in excluded_row_hashes if str(row_hash).strip()})
        clean_sort_column = sort_column if sort_column in visible_columns else None
        return ExportSpec(
            project_name=_clean_project_name(project_name),
            category_keys=clean_category_keys,
            period_from=period_from,
            period_to=period_to,
            period_from_index=period_from_index,
            period_to_index=period_to_index,
            selected_columns=selected,
            filters=normalized_filters,
            excluded_row_hashes=clean_hashes,
            sort_column=clean_sort_column,
            sort_direction="desc" if str(sort_direction).lower() == "desc" else "asc",
            split_by_category=bool(split_by_category),
        )

    def _count(self, spec: ExportSpec) -> int:
        return self.repository.count_export_products(
            table_name=self.settings.products_table,
            project_name=spec.project_name,
            category_keys=spec.category_keys,
            period_from_index=spec.period_from_index,
            period_to_index=spec.period_to_index,
            filters=spec.filters,
            excluded_row_hashes=spec.excluded_row_hashes,
        )

    def _breakdown(self, spec: ExportSpec) -> list[dict[str, Any]]:
        return self.repository.export_breakdown(
            table_name=self.settings.products_table,
            project_name=spec.project_name,
            category_keys=spec.category_keys,
            period_from_index=spec.period_from_index,
            period_to_index=spec.period_to_index,
            filters=spec.filters,
            excluded_row_hashes=spec.excluded_row_hashes,
        )

    def _estimate_file_count(self, spec: ExportSpec, total: int) -> int:
        if total <= 0:
            return 0
        if not spec.split_by_category:
            return math.ceil(total / EXCEL_MAX_DATA_ROWS)
        estimated = 0
        for category in self._selected_categories(spec):
            category_spec = replace(spec, category_keys=[str(category["category_key"])])
            category_total = self._count(category_spec)
            if category_total > 0:
                estimated += math.ceil(category_total / EXCEL_MAX_DATA_ROWS)
        return estimated

    def _selected_categories(self, spec: ExportSpec) -> list[dict[str, Any]]:
        return self.repository.export_categories(
            table_name=self.settings.products_table,
            project_name=spec.project_name,
            category_keys=spec.category_keys or None,
        )

    def _write_parts(
        self,
        *,
        spec: ExportSpec,
        total_rows: int,
        output_dir: Path,
        category: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        parts = max(1, math.ceil(total_rows / EXCEL_MAX_DATA_ROWS))
        artifacts: list[dict[str, Any]] = []
        for part_index in range(parts):
            base_offset = part_index * EXCEL_MAX_DATA_ROWS
            rows_in_part = min(EXCEL_MAX_DATA_ROWS, total_rows - base_offset)
            filename = self._filename(
                spec=spec,
                total_rows=total_rows,
                category=category,
                part_index=part_index + 1,
                parts=parts,
            )
            target = _unique_path(output_dir / filename)
            written = self._write_xlsx(
                target=target,
                spec=spec,
                rows_in_part=rows_in_part,
                base_offset=base_offset,
            )
            artifacts.append(
                {
                    "path": str(target),
                    "filename": target.name,
                    "rows": written,
                    "part": part_index + 1,
                    "parts": parts,
                    "category_key": category.get("category_key") if category else None,
                    "category_name": category.get("category_name") if category else None,
                    "marketplace": category.get("marketplace") if category else None,
                }
            )
        return artifacts

    def _write_xlsx(self, *, target: Path, spec: ExportSpec, rows_in_part: int, base_offset: int) -> int:
        try:
            from openpyxl import Workbook
            from openpyxl.utils import get_column_letter
        except ModuleNotFoundError as exc:
            raise ImportError("Для выгрузки XLSX нужен openpyxl. Установи зависимости проекта: pip install -r requirements.txt") from exc

        workbook = Workbook(write_only=True)
        worksheet = workbook.create_sheet("Данные")
        worksheet.freeze_panes = "A2"
        worksheet.append(spec.selected_columns)
        written = 0
        while written < rows_in_part:
            batch_size = min(EXPORT_BATCH_SIZE, rows_in_part - written)
            df = self.repository.fetch_export_products_dataframe(
                table_name=self.settings.products_table,
                project_name=spec.project_name,
                output_columns=spec.selected_columns,
                category_keys=spec.category_keys,
                period_from_index=spec.period_from_index,
                period_to_index=spec.period_to_index,
                filters=spec.filters,
                excluded_row_hashes=spec.excluded_row_hashes,
                sort_column=spec.sort_column,
                sort_direction=spec.sort_direction,
                limit=batch_size,
                offset=base_offset + written,
                include_row_hash=False,
            )
            if df.empty:
                break
            for row in df.itertuples(index=False, name=None):
                worksheet.append([_excel_value(value) for value in row])
            written += len(df)
        if spec.selected_columns:
            worksheet.auto_filter.ref = f"A1:{get_column_letter(len(spec.selected_columns))}{written + 1}"
        workbook.save(target)
        return written

    def _filename(
        self,
        *,
        spec: ExportSpec,
        total_rows: int,
        category: dict[str, Any] | None,
        part_index: int,
        parts: int,
    ) -> str:
        stamp = datetime.now().strftime("%Y%m%d-%H%M")
        period_from = spec.period_from or "все-периоды"
        period_to = spec.period_to or "все-периоды"
        if category:
            base = (
                f"MPStats_{spec.project_name}_{category.get('category_name') or 'категория'}_"
                f"{category.get('marketplace') or category.get('marketplace_code') or 'mp'}_"
                f"{period_from}_{period_to}_{total_rows}стр_{stamp}"
            )
        else:
            base = f"MPStats_{spec.project_name}_все-категории_{period_from}_{period_to}_{total_rows}стр_{stamp}"
        if parts > 1:
            base += f"_part{part_index:02d}-of{parts:02d}"
        return f"{_safe_filename(base)}.xlsx"

    @staticmethod
    def _warnings_for_estimate(estimated_files: int) -> list[str]:
        if estimated_files <= 1:
            return []
        return [f"Выгрузка будет разбита на {estimated_files} файлов."]

    def _read_templates(self) -> list[dict[str, Any]]:
        raw = self.repository.get_setting(EXPORT_TEMPLATES_SETTING)
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return []
        templates = payload.get("templates") if isinstance(payload, dict) else payload
        if not isinstance(templates, list):
            return []
        return [item for item in templates if isinstance(item, dict)]

    def _write_templates(self, templates: list[dict[str, Any]]) -> None:
        self.repository.set_setting(
            EXPORT_TEMPLATES_SETTING,
            json.dumps({"version": 1, "templates": templates}, ensure_ascii=False),
        )


def _clean_project_name(value: str) -> str:
    return value.strip() or "mpstats"


def _safe_segment(value: str) -> str:
    segment = re.sub(r"[^\w_.-]+", "_", value.strip(), flags=re.UNICODE)
    return segment.strip("._") or "mpstats"


def _with_project_segment(path: Path, project_name: str) -> Path:
    project_segment = _safe_segment(project_name)
    if project_segment in path.parts or project_name in path.parts:
        return path
    return path / project_segment


def _parse_filter_number(value: str, column: str) -> float:
    text = value.strip().replace(",", ".")
    try:
        number = float(text)
    except ValueError as exc:
        raise ValueError(f"Фильтр {column}: для числового условия нужно число.") from exc
    if not math.isfinite(number):
        raise ValueError(f"Фильтр {column}: для числового условия нужно конечное число.")
    return number


def _period_label_to_index(value: str | None) -> int | None:
    text = (value or "").strip()
    if not text:
        return None
    match = re.fullmatch(r"(\d{4})-(\d{1,2})", text)
    if not match:
        raise ValueError(f"Некорректный период {value!r}. Используй формат YYYY-MM.")
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError(f"Некорректный месяц в периоде {value!r}.")
    return year * 12 + month


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[/\\:]+", "_", value)
    cleaned = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._ -]+", "_", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ._")
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned[:180] or "MPStats_export"


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(2, 1000):
        candidate = path.with_name(f"{path.stem}_v{index}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise FileExistsError(f"Не удалось подобрать свободное имя файла для {path}")


def _excel_value(value: Any) -> Any:
    if value is pd.NA:
        return None
    cleaned = clean_value(value)
    if isinstance(cleaned, (datetime, date)):
        return cleaned
    return cleaned
