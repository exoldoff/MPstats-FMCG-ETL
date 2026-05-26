from __future__ import annotations

from pathlib import Path
import re
import traceback

import numpy as np
import pandas as pd

from pipeline.models import StepResult
from pipeline.repositories.file_repository import list_csv_files, read_semicolon_csv, write_semicolon_csv


MG_UNITS = r"(?:мг|mg|milligram(?:s)?|milligramme(?:s)?)"
G_UNITS = r"(?:г|гр|g|gram(?:s)?|gramme(?:s)?|грамм(?:а|ов)?)"
KG_UNITS = r"(?:кг|kg|kilogram(?:s)?|kilogramme(?:s)?|килограмм(?:а|ов)?)"
ML_UNITS = r"(?:мл|ml|milliliter(?:s)?|millilitre(?:s)?|миллилитр(?:а|ов)?)"
L_UNITS = r"(?:л|l|литр(?:а|ов)?|liter(?:s)?|litre(?:s)?)"

UNIT = rf"(?:{MG_UNITS}|{G_UNITS}|{KG_UNITS}|{ML_UNITS}|{L_UNITS})"
MUL = r"(?:x|х|×|\*)"
NUM = r"\d+(?:\.\d+)?"
COUNT_WORD = (
    r"(?:шт\.?|штука?|штуки?|штучк(?:а|и)?"
    r"|pcs\.?|piece(?:s)?"
    r"|уп\.?|упак\.?|пак\.?|pack\.?|packs\.?"
    r"|ед\.?|ед\b|единиц(?:а|ы)?"
    r"|кусок|куска?|кусков|кусочков?"
    r"|набор(?:а|ов)?|наб\.?)"
)

PACK_PRE = re.compile(rf"(?P<count>\d+)\s*(?:{COUNT_WORD})?\s*{MUL}\s*(?P<qty>{NUM})\s*(?P<unit>{UNIT})(?=\W|$)", re.IGNORECASE)
PACK_POST = re.compile(rf"(?P<qty>{NUM})\s*(?P<unit>{UNIT})\s*{MUL}\s*(?P<count>\d+)\s*(?:{COUNT_WORD})?(?=\W|$)", re.IGNORECASE)
PACK_PO = re.compile(rf"(?P<count>\d+)\s*(?:{COUNT_WORD})?\s*по\s*(?P<qty>{NUM})\s*(?P<unit>{UNIT})(?=\W|$)", re.IGNORECASE)
PACK_COUNT_DASH = re.compile(rf"(?P<count>\d+)\s*(?:{COUNT_WORD})\s*[-–]\s*(?P<qty>{NUM})\s*(?P<unit>{UNIT})(?!\w)(?=\W|$)", re.IGNORECASE)
PACK_SUFFIX = re.compile(rf"(?P<qty>{NUM})\s*(?P<unit>{UNIT})(?!\w)[^;\n]{{0,80}}?(?P<count>\d{{1,4}})\s*(?:{COUNT_WORD})(?=\W|$)", re.IGNORECASE)
SINGLE = re.compile(rf"(?P<qty>{NUM})\s*(?P<unit>{UNIT})(?=\W|$)", re.IGNORECASE)

RX_L_BIG = re.compile(rf"\b(\d{{3,4}})\s*({L_UNITS})\b", re.IGNORECASE)
RX_ML_LONG = re.compile(rf"\b(\d{{6,12}})\s*({ML_UNITS})\b", re.IGNORECASE)

STEP4_IN_CANON = [
    "Дата",
    "Маркетплейс",
    "Категория",
    "SKU",
    "Бренд",
    "Название",
    "Продажи, шт",
    "Продавец",
    "Средняя цена, руб",
    "Выручка, руб",
]
STEP4_OUT_ORDER = STEP4_IN_CANON + [
    "Вес, кг сырой",
    "Вес, кг",
    "Вес аномалия",
    "Вес причина",
    "Объем, кг",
    "Объем, т",
    "Год",
    "Цена за кг",
]


def normalize_text(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"(?<=\d),(?=\d)", ".", text)
    return text.replace("\u00a0", " ")


def unit_to_kg(qty: float, unit: str) -> float:
    normalized = unit.lower().strip().strip(".")
    if re.fullmatch(KG_UNITS, normalized, flags=re.IGNORECASE):
        return qty
    if re.fullmatch(G_UNITS, normalized, flags=re.IGNORECASE):
        return qty / 1000.0
    if re.fullmatch(MG_UNITS, normalized, flags=re.IGNORECASE):
        return qty / 1_000_000.0
    if re.fullmatch(L_UNITS, normalized, flags=re.IGNORECASE):
        return qty
    if re.fullmatch(ML_UNITS, normalized, flags=re.IGNORECASE):
        return qty / 1000.0
    return 0.0


def extract_weight_kg_from_name(name: str | None) -> float | None:
    if name is None:
        return None
    text = normalize_text(str(name))
    if not text:
        return None

    pack_total_kg = 0.0

    def consume(pattern: re.Pattern[str], source: str) -> tuple[float, str]:
        subtotal = 0.0
        spans: list[tuple[int, int]] = []
        for match in pattern.finditer(source):
            qty = float(match.group("qty"))
            unit = match.group("unit")
            count = int(match.group("count"))
            subtotal += unit_to_kg(qty, unit) * count
            spans.append((match.start(), match.end()))
        if not spans:
            return 0.0, source
        out = source
        for start, end in reversed(spans):
            out = out[:start] + " " + out[end:]
        return subtotal, out

    subtotal, text = consume(PACK_PRE, text)
    pack_total_kg += subtotal
    subtotal, text = consume(PACK_POST, text)
    pack_total_kg += subtotal
    subtotal, text = consume(PACK_PO, text)
    pack_total_kg += subtotal
    subtotal, text = consume(PACK_COUNT_DASH, text)
    pack_total_kg += subtotal
    subtotal, text = consume(PACK_SUFFIX, text)
    pack_total_kg += subtotal

    singles = [unit_to_kg(float(match.group("qty")), match.group("unit")) for match in SINGLE.finditer(text)]
    result = max(pack_total_kg, max(singles) if singles else 0.0)
    return None if result <= 0 else result


def fix_liters_decimal_shift(num: int) -> float:
    digits = len(str(num))
    return num / (10 ** (digits - 1))


def fix_long_ml(num: int) -> float:
    text = str(num)
    tail4 = int(text[-4:])
    tail3 = int(text[-3:])
    if 1000 <= tail4 <= 5000:
        return tail4 / 1000.0
    if 50 <= tail3 <= 999:
        return tail3 / 1000.0
    return np.nan


def sanitize_weight_kg(name: str, w_kg: float | None, max_kg: float) -> tuple[float, bool, str]:
    if w_kg is None or not np.isfinite(w_kg) or w_kg <= 0:
        return np.nan, True, "empty_or_bad"

    text = normalize_text(str(name))
    if w_kg > max_kg:
        liters = RX_L_BIG.search(text)
        if liters:
            num = int(liters.group(1))
            fixed = fix_liters_decimal_shift(num)
            if 0 < fixed <= max_kg:
                return fixed, True, f"fixed_liters_{num}-> {fixed}"

        ml = RX_ML_LONG.search(text)
        if ml:
            num = int(ml.group(1))
            fixed = fix_long_ml(num)
            if np.isfinite(fixed) and 0 < fixed <= max_kg:
                return fixed, True, f"fixed_long_ml_{num}-> {fixed}kg"

        return np.nan, True, f"too_big>{max_kg}kg_no_fix"

    return float(w_kg), False, "ok"


def money_to_float(series: pd.Series) -> pd.Series:
    text = series.astype("string")
    text = text.str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(text, errors="coerce")


def varchar_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("")


def sku_varchar(series: pd.Series) -> pd.Series:
    return varchar_series(series).str.replace(r"^(\d+)\.0+$", r"\1", regex=True)


def apply_step4_output_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Продажи, шт" not in out.columns and "Продажи" in out.columns:
        out["Продажи, шт"] = out["Продажи"]
    if "Средняя цена, руб" not in out.columns and "Средняя цена" in out.columns:
        out["Средняя цена, руб"] = out["Средняя цена"]
    if "Выручка, руб" not in out.columns and "Выручка" in out.columns:
        out["Выручка, руб"] = out["Выручка"]

    for column in STEP4_IN_CANON:
        if column not in out.columns:
            out[column] = pd.NA

    out["Дата"] = varchar_series(out["Дата"])
    out["Маркетплейс"] = varchar_series(out["Маркетплейс"])
    out["Категория"] = varchar_series(out["Категория"])
    out["SKU"] = sku_varchar(out["SKU"])
    out["Бренд"] = varchar_series(out["Бренд"]).str.strip().str.lower()
    out["Название"] = varchar_series(out["Название"])
    out["Продавец"] = varchar_series(out["Продавец"]).str.strip().str.lower()
    out["Вес причина"] = varchar_series(out["Вес причина"])
    out["Продажи, шт"] = pd.to_numeric(out["Продажи, шт"], errors="coerce").fillna(0.0).astype("float64")
    out["Выручка, руб"] = money_to_float(out["Выручка, руб"]).astype("float64")
    out["Средняя цена, руб"] = pd.to_numeric(out["Средняя цена, руб"], errors="coerce").astype("float64")
    out["Вес, кг сырой"] = pd.to_numeric(out["Вес, кг сырой"], errors="coerce").astype("float64")
    out["Вес, кг"] = pd.to_numeric(out["Вес, кг"], errors="coerce").astype("float64")
    out["Вес аномалия"] = out["Вес аномалия"].fillna(False).astype(bool)
    out["Объем, кг"] = pd.to_numeric(out["Объем, кг"], errors="coerce").fillna(0.0).astype("float64")
    out["Объем, т"] = pd.to_numeric(out["Объем, т"], errors="coerce").fillna(0.0).astype("float64")
    out["Год"] = pd.to_numeric(out["Год"], errors="coerce").astype("Int64")
    out["Цена за кг"] = pd.to_numeric(out["Цена за кг"], errors="coerce").astype("float64")
    return out.loc[:, STEP4_OUT_ORDER]


def parse_weights_dataframe(df: pd.DataFrame, *, max_weight_kg: float = 40.0) -> pd.DataFrame:
    required = ["Дата", "Название", "Продажи", "SKU", "Категория", "Бренд", "Средняя цена"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Не найдены колонки {missing}. Сейчас есть: {list(df.columns)}")

    out = df.copy()
    out["Продажи"] = (
        out["Продажи"].astype(str).str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    )
    out["Продажи"] = pd.to_numeric(out["Продажи"], errors="coerce").fillna(0)
    out["Вес, кг сырой"] = out["Название"].apply(extract_weight_kg_from_name)

    fixes = out.apply(lambda row: sanitize_weight_kg(row["Название"], row["Вес, кг сырой"], max_weight_kg), axis=1)
    out["Вес, кг"] = fixes.apply(lambda value: value[0])
    out["Вес аномалия"] = fixes.apply(lambda value: value[1])
    out["Вес причина"] = fixes.apply(lambda value: value[2])
    out["Объем, кг"] = out["Продажи"] * out["Вес, кг"].fillna(0)
    out["Объем, т"] = out["Объем, кг"] / 1000.0
    out["Год"] = pd.to_datetime(out["Дата"], errors="coerce", dayfirst=True).dt.year
    out["Средняя цена"] = money_to_float(out["Средняя цена"])
    out["Цена за кг"] = np.where(
        out["Вес, кг"].notna() & (out["Вес, кг"] > 0) & out["Средняя цена"].notna(),
        out["Средняя цена"] / out["Вес, кг"],
        np.nan,
    )
    return apply_step4_output_schema(out)


def parse_weights_file(input_path: str | Path, output_dir: str | Path, *, max_weight_kg: float = 40.0) -> Path:
    in_path = Path(input_path)
    df = read_semicolon_csv(in_path, low_memory=False)
    out_df = parse_weights_dataframe(df, max_weight_kg=max_weight_kg)
    out_path = Path(output_dir) / in_path.name
    write_semicolon_csv(out_df, out_path)
    return out_path


def parse_weights_directory(input_dir: str | Path, output_dir: str | Path, *, max_weight_kg: float = 40.0) -> StepResult:
    result = StepResult(name="step4_parse_weights")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for file_path in list_csv_files(input_dir):
        try:
            out = parse_weights_file(file_path, output_dir, max_weight_kg=max_weight_kg)
            parsed = read_semicolon_csv(out, low_memory=False)
            result.ok += 1
            result.rows += len(parsed)
            result.output = Path(output_dir)
            result.add_detail(
                status="ok",
                file=str(file_path),
                output=str(out),
                rows=len(parsed),
                anomalies=int(parsed["Вес аномалия"].sum()) if "Вес аномалия" in parsed.columns else 0,
            )
        except Exception as exc:
            result.errors += 1
            result.add_detail(status="error", file=str(file_path), error=str(exc), trace=traceback.format_exc())
    return result
