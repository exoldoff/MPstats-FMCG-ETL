from __future__ import annotations

from pathlib import Path
import traceback

import pandas as pd

from pipeline.models import StepResult
from pipeline.repositories.file_repository import list_csv_files, read_semicolon_csv, write_semicolon_csv


def normalize_sales_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for old, new in {
        "Продажи": "Продажи, шт",
        "Средняя цена": "Средняя цена, руб",
        "Выручка": "Выручка, руб",
    }.items():
        if new not in out.columns and old in out.columns:
            out = out.rename(columns={old: new})

    if "Продажи, шт" not in out.columns:
        raise ValueError("В данных нет колонки 'Продажи, шт' или 'Продажи'.")

    out["Продажи, шт"] = (
        out["Продажи, шт"].astype(str).str.replace(" ", "", regex=False).str.replace("\u00a0", "", regex=False).str.replace(",", ".", regex=False)
    )
    out["Продажи, шт"] = pd.to_numeric(out["Продажи, шт"], errors="coerce").fillna(0)
    return out


def merge_dataframes(frames: list[pd.DataFrame], *, min_sales: float = 0, max_sales: float = 40_000) -> pd.DataFrame:
    if not frames:
        raise RuntimeError("Нет файлов для склейки.")
    result = pd.concat(frames, ignore_index=True)
    result = normalize_sales_column(result)
    return result[(result["Продажи, шт"] > min_sales) & (result["Продажи, шт"] < max_sales)].copy().drop_duplicates()


def merge_directory(
    input_dir: str | Path,
    output_file: str | Path,
    *,
    min_sales: float = 0,
    max_sales: float = 40_000,
) -> tuple[pd.DataFrame, StepResult]:
    result = StepResult(name="step5_merge", output=Path(output_file))
    frames: list[pd.DataFrame] = []
    for file_path in list_csv_files(input_dir):
        try:
            frame = read_semicolon_csv(file_path, low_memory=False)
            frames.append(frame)
            result.ok += 1
            result.add_detail(status="ok", file=str(file_path), rows=len(frame))
        except Exception as exc:
            result.errors += 1
            result.add_detail(status="error", file=str(file_path), error=str(exc), trace=traceback.format_exc())

    merged = merge_dataframes(frames, min_sales=min_sales, max_sales=max_sales)
    result.rows = len(merged)
    write_semicolon_csv(merged, output_file)
    return merged, result
