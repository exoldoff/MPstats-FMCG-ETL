from __future__ import annotations

from pathlib import Path
import time
from typing import Callable, TypeVar

import pandas as pd

from classifiers import engine as classifier_engine
from pipeline.models import StepResult
from pipeline.repositories.file_repository import read_csv_auto, write_semicolon_csv
from pipeline.services.manual_override_service import apply_manual_overrides


SERVICE_COLUMNS_TO_DROP = ["Вес, кг сырой", "Вес аномалия", "Вес причина", "Объем, кг"]
EXCEL_SUFFIXES = {".xlsx"}
MONTH_LABELS = {
    1: "янв.",
    2: "фев.",
    3: "мар.",
    4: "апр.",
    5: "май",
    6: "июн.",
    7: "июл.",
    8: "авг.",
    9: "сен.",
    10: "окт.",
    11: "ноя.",
    12: "дек.",
}

T = TypeVar("T")


def _time_call(timings: dict[str, float], key: str, callback: Callable[[], T]) -> T:
    started = time.perf_counter()
    try:
        return callback()
    finally:
        timings[key] = timings.get(key, 0.0) + (time.perf_counter() - started)


def read_classification_input(input_file: str | Path) -> pd.DataFrame:
    path = Path(input_file)
    if path.suffix.lower() in EXCEL_SUFFIXES:
        try:
            return pd.read_excel(path)
        except ImportError as exc:
            raise ImportError("Для XLSX нужен openpyxl. Установи зависимости проекта: pip install -r requirements.txt") from exc
    return read_csv_auto(path, low_memory=False)


def prepare_for_classification(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Название" not in out.columns:
        if {"SKU", "Артикул"}.issubset(out.columns):
            return out.rename(columns={"SKU": "Название", "Артикул": "SKU"})
        raise KeyError("Для классификации нужен столбец 'Название'. Если данные уже переименованы, нужны колонки 'SKU' и 'Артикул'.")
    return out


def postprocess_classified(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], dict[str, str]]:
    out = df.copy()
    dropped_columns = [column for column in SERVICE_COLUMNS_TO_DROP if column in out.columns]
    if dropped_columns:
        out = out.drop(columns=dropped_columns)
    out = add_month_column(out)

    rename_map: dict[str, str] = {}
    if "SKU" in out.columns:
        rename_map["SKU"] = "Артикул"
    if "Название" in out.columns:
        rename_map["Название"] = "SKU"
    if rename_map:
        out = out.rename(columns=rename_map)
    return out, dropped_columns, rename_map


def add_month_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Дата" not in df.columns:
        return df
    out = df.copy()
    parsed_month = pd.to_datetime(out["Дата"], errors="coerce", dayfirst=True).dt.month
    month_values = parsed_month.map(lambda value: MONTH_LABELS.get(int(value)) if pd.notna(value) else pd.NA)
    if "месяц" in out.columns:
        out = out.drop(columns=["месяц"])
    out.insert(out.columns.get_loc("Дата") + 1, "месяц", month_values)
    return out


def classify_dataframe(
    df: pd.DataFrame,
    *,
    rules_path: str | Path,
    fill_unclassified: dict[str, object] | None = None,
    manual_overrides_path: str | Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    timings: dict[str, float] = {}
    prepared = _time_call(timings, "prepare_input_seconds", lambda: prepare_for_classification(df))
    result, report = _time_call(
        timings,
        "apply_rules_seconds",
        lambda: classifier_engine.apply_classifiers(
            prepared,
            rules_path=rules_path,
            fill_unclassified=fill_unclassified,
        ),
    )
    result, dropped_columns, rename_map = _time_call(timings, "postprocess_seconds", lambda: postprocess_classified(result))
    manual_report = pd.DataFrame()
    if manual_overrides_path is not None:
        result, manual_report = _time_call(
            timings,
            "manual_overrides_seconds",
            lambda: apply_manual_overrides(result, overrides_path=manual_overrides_path),
        )
    else:
        timings["manual_overrides_seconds"] = 0.0
    active_report = report[report["active"] == True].copy() if "active" in report.columns else report.copy()
    updated_rows = int(active_report["applied_rows"].sum()) if "applied_rows" in active_report.columns else 0
    manual_updated_rows = int(manual_report["applied_rows"].sum()) if "applied_rows" in manual_report.columns else 0
    meta = {
        "active_rules": len(active_report),
        "updated_rows": updated_rows,
        "manual_overrides": len(manual_report),
        "manual_updated_rows": manual_updated_rows,
        "dropped_columns": dropped_columns,
        "renamed_columns": rename_map,
        "timings": timings,
    }
    return result, report, meta


def classify_file(
    input_file: str | Path,
    output_file: str | Path,
    *,
    rules_path: str | Path,
    write_xlsx: bool = False,
    fill_unclassified: dict[str, object] | None = None,
    manual_overrides_path: str | Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, StepResult]:
    total_started = time.perf_counter()
    timings: dict[str, float] = {}
    df = _time_call(timings, "read_input_seconds", lambda: read_classification_input(input_file))
    result_df, report, meta = classify_dataframe(
        df,
        rules_path=rules_path,
        fill_unclassified=fill_unclassified,
        manual_overrides_path=manual_overrides_path,
    )
    timings.update(meta.pop("timings", {}))
    out_path = _time_call(timings, "write_output_seconds", lambda: write_semicolon_csv(result_df, output_file))
    if write_xlsx:
        try:
            _time_call(
                timings,
                "write_xlsx_seconds",
                lambda: result_df.to_excel(out_path.with_suffix(".xlsx"), index=False),
            )
        except ImportError as exc:
            raise ImportError("Для сохранения XLSX нужен openpyxl. Установи зависимости проекта: pip install -r requirements.txt") from exc
    else:
        timings["write_xlsx_seconds"] = 0.0
    timings["total_seconds"] = time.perf_counter() - total_started

    step = StepResult(name="step6_classify", ok=1, rows=len(result_df), output=out_path)
    step.add_detail(
        input=str(input_file),
        output=str(out_path),
        report_rows=len(report),
        timings={key: round(value, 4) for key, value in timings.items()},
        **meta,
    )
    return result_df, report, step
