from __future__ import annotations

from pathlib import Path
import traceback

import pandas as pd

from pipeline.models import StepResult
from pipeline.repositories.file_repository import list_csv_files, write_semicolon_csv


CANON_COLUMNS = [
    "Дата",
    "Маркетплейс",
    "Категория",
    "SKU",
    "Бренд",
    "Название",
    "Продажи",
    "Продавец",
    "Средняя цена",
    "Выручка",
]


def detect_marketplace_type(filename: str) -> str:
    lowered = filename.lower()
    if "ozon" in lowered:
        return "ozon"
    if "wildberries" in lowered or "wb" in lowered:
        return "wb"
    if "яндекс" in lowered or "yandex" in lowered or "маркет" in lowered or "ym" in lowered:
        return "ym"
    return "unknown"


def read_csv_simple(path: str | Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path, sep=";", encoding="utf-8", engine="python", on_bad_lines="skip")


def standardize_dataframe(df: pd.DataFrame, marketplace_type: str) -> pd.DataFrame:
    if marketplace_type in {"ozon", "wb"}:
        df = df.rename(
            columns={
                "id": "SKU",
                "Brand": "Бренд",
                "brand": "Бренд",
                "Name": "Название",
                "name": "Название",
                "Sales": "Продажи",
                "sales": "Продажи",
                "Seller": "Продавец",
                "seller": "Продавец",
                "Average price": "Средняя цена",
                "average_price": "Средняя цена",
                "final_price_average": "Средняя цена",
                "Revenue": "Выручка",
                "revenue": "Выручка",
            }
        )
    elif marketplace_type == "ym":
        rename_map: dict[object, str] = {"Продаж": "Продажи"}
        for column in df.columns:
            if str(column).strip().lower() == "sku" and str(column).strip() != "SKU":
                rename_map[column] = "SKU"
                break
        for src, dst in (
            ("Brand", "Бренд"),
            ("brand", "Бренд"),
            ("Name", "Название"),
            ("name", "Название"),
            ("Sales", "Продажи"),
            ("sales", "Продажи"),
            ("Revenue", "Выручка"),
            ("revenue", "Выручка"),
            ("Price", "Средняя цена"),
            ("price", "Средняя цена"),
            ("Average price", "Средняя цена"),
            ("Seller", "Продавец"),
            ("seller", "Продавец"),
        ):
            if src in df.columns:
                rename_map[src] = dst
        revenue_col = next((c for c in df.columns if str(c).strip().startswith("Выручка")), None)
        if revenue_col is not None:
            rename_map[revenue_col] = "Выручка"
        df = df.rename(columns=rename_map)
        if "Выручка" not in df.columns:
            raise ValueError("Не найдена колонка выручки (Выручка / Revenue / revenue).")
    else:
        raise ValueError(f"Unknown marketplace type: {marketplace_type}")

    for column in CANON_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    return df[CANON_COLUMNS]


def standardize_file(input_path: str | Path, output_dir: str | Path) -> Path | None:
    in_path = Path(input_path)
    marketplace_type = detect_marketplace_type(in_path.name)
    if marketplace_type == "unknown":
        return None

    df = read_csv_simple(in_path)
    out_df = standardize_dataframe(df, marketplace_type)
    out_path = Path(output_dir) / in_path.name
    write_semicolon_csv(out_df, out_path)
    return out_path


def standardize_directory(input_dir: str | Path, output_dir: str | Path) -> StepResult:
    result = StepResult(name="step3_standardize")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for file_path in list_csv_files(input_dir):
        try:
            out = standardize_file(file_path, output_dir)
            if out is None:
                result.skipped += 1
                result.add_detail(status="skip", file=str(file_path), reason="unknown_marketplace")
                continue
            result.ok += 1
            result.output = Path(output_dir)
            result.add_detail(status="ok", file=str(file_path), output=str(out))
        except Exception as exc:
            result.errors += 1
            result.add_detail(status="error", file=str(file_path), error=str(exc), trace=traceback.format_exc())
    return result
