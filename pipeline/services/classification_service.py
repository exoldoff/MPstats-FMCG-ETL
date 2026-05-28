from __future__ import annotations

from pathlib import Path
import importlib

import pandas as pd

from classifiers import engine as classifier_engine
from pipeline.models import StepResult
from pipeline.repositories.file_repository import read_csv_auto, write_semicolon_csv


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
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    importlib.reload(classifier_engine)
    result, report = classifier_engine.apply_classifiers(
        prepare_for_classification(df),
        rules_path=rules_path,
        fill_unclassified=fill_unclassified,
    )
    result, dropped_columns, rename_map = postprocess_classified(result)
    active_report = report[report["active"] == True].copy() if "active" in report.columns else report.copy()
    updated_rows = int(active_report["applied_rows"].sum()) if "applied_rows" in active_report.columns else 0
    meta = {
        "active_rules": len(active_report),
        "updated_rows": updated_rows,
        "dropped_columns": dropped_columns,
        "renamed_columns": rename_map,
    }
    return result, report, meta


def classify_file(
    input_file: str | Path,
    output_file: str | Path,
    *,
    rules_path: str | Path,
    write_xlsx: bool = False,
    fill_unclassified: dict[str, object] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, StepResult]:
    df = read_classification_input(input_file)
    result_df, report, meta = classify_dataframe(df, rules_path=rules_path, fill_unclassified=fill_unclassified)
    out_path = write_semicolon_csv(result_df, output_file)
    if write_xlsx:
        try:
            result_df.to_excel(out_path.with_suffix(".xlsx"), index=False)
        except ImportError as exc:
            raise ImportError("Для сохранения XLSX нужен openpyxl. Установи зависимости проекта: pip install -r requirements.txt") from exc

    step = StepResult(name="step6_classify", ok=1, rows=len(result_df), output=out_path)
    step.add_detail(input=str(input_file), output=str(out_path), report_rows=len(report), **meta)
    return result_df, report, step
