from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
import traceback

import pandas as pd

from pipeline.models import StepResult
from pipeline.repositories.file_repository import detect_encoding_and_sep, list_csv_files


def extract_first_date_from_filename(filename: str) -> datetime | None:
    candidates: list[tuple[int, datetime]] = []

    for match in re.finditer(r"(?P<y>20\d{2}|19\d{2})[.\-_](?P<mo>\d{2})[.\-_](?P<d>\d{2})", filename):
        try:
            candidates.append((match.start(), datetime(int(match["y"]), int(match["mo"]), int(match["d"]))))
        except ValueError:
            pass

    for match in re.finditer(r"(?P<d>\d{2})[.\-_](?P<mo>\d{2})[.\-_](?P<y>20\d{2}|19\d{2})", filename):
        try:
            candidates.append((match.start(), datetime(int(match["y"]), int(match["mo"]), int(match["d"]))))
        except ValueError:
            pass

    for match in re.finditer(r"\b(?P<yyyymmdd>(?:19|20)\d{2}\d{2}\d{2})\b", filename):
        try:
            candidates.append((match.start(), datetime.strptime(match["yyyymmdd"], "%Y%m%d")))
        except ValueError:
            pass

    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[0][1]


def extract_marketplace_from_filename(filename: str) -> str:
    lowered = filename.lower()
    if "ozon" in lowered:
        return "Ozon"
    if "wildberries" in lowered or re.search(r"(^|[\s\-_])wb([\s\-_]|$)", lowered):
        return "Wildberries"
    if "яндекс" in lowered or "yandex" in lowered or "маркет" in lowered or "ym" in lowered:
        return "Яндекс.Маркет"
    return "Unknown"


def extract_category_from_filename(filename: str) -> str:
    if "__" not in filename:
        return "Unknown"
    name_no_ext = filename.rsplit(".", 1)[0]
    category = name_no_ext.rsplit("__", 1)[-1]
    return category or "Unknown"


def upsert_column(
    df: pd.DataFrame,
    *,
    column_name: str,
    value: object,
    position: int,
    overwrite_existing: bool,
) -> pd.DataFrame:
    if column_name in df.columns:
        if not overwrite_existing:
            return df
        df = df.drop(columns=[column_name])

    insert_at = min(max(position, 0), len(df.columns))
    df.insert(insert_at, column_name, value)
    return df


def enrich_file(
    input_path: str | Path,
    output_dir: str | Path,
    *,
    date_col_name: str = "Дата",
    marketplace_col_name: str = "Маркетплейс",
    category_col_name: str = "Категория",
    date_output_format: str = "%d.%m.%Y",
    overwrite_existing_col: bool = False,
    chunksize: int | None = None,
) -> Path | None:
    in_path = Path(input_path)
    output = Path(output_dir)
    marketplace = extract_marketplace_from_filename(in_path.name)
    dt = extract_first_date_from_filename(in_path.name)
    if dt is None:
        return None
    date_str = dt.strftime(date_output_format)
    category = extract_category_from_filename(in_path.name)
    enc, sep = detect_encoding_and_sep(in_path)

    output.mkdir(parents=True, exist_ok=True)
    out_path = output / in_path.name

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        df = upsert_column(
            df,
            column_name=date_col_name,
            value=date_str,
            position=0,
            overwrite_existing=overwrite_existing_col,
        )
        df = upsert_column(
            df,
            column_name=marketplace_col_name,
            value=marketplace,
            position=1,
            overwrite_existing=overwrite_existing_col,
        )
        return upsert_column(
            df,
            column_name=category_col_name,
            value=category,
            position=2,
            overwrite_existing=overwrite_existing_col,
        )

    if chunksize:
        first = True
        for chunk in pd.read_csv(in_path, sep=sep, encoding=enc, low_memory=False, chunksize=chunksize):
            transform(chunk).to_csv(
                out_path,
                sep=sep,
                index=False,
                encoding="utf-8-sig",
                mode="w" if first else "a",
                header=first,
            )
            first = False
    else:
        df = pd.read_csv(in_path, sep=sep, encoding=enc, low_memory=False)
        transform(df).to_csv(out_path, sep=sep, index=False, encoding="utf-8-sig")

    return out_path


def enrich_directory(input_dir: str | Path, output_dir: str | Path, **kwargs: object) -> StepResult:
    result = StepResult(name="step2_enrich")
    files = list_csv_files(input_dir)
    for file_path in files:
        try:
            out = enrich_file(file_path, output_dir, **kwargs)
            if out is None:
                result.skipped += 1
                result.add_detail(status="skip", file=str(file_path), reason="date_not_found")
                continue
            result.ok += 1
            result.output = Path(output_dir)
            result.add_detail(status="ok", file=str(file_path), output=str(out))
        except Exception as exc:
            result.errors += 1
            result.add_detail(status="error", file=str(file_path), error=str(exc), trace=traceback.format_exc())
    return result
