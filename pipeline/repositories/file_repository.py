from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_CSV_ENCODING = "utf-8-sig"
DEFAULT_CSV_SEP = ";"


def list_csv_files(directory: str | Path) -> list[Path]:
    path = Path(directory)
    if not path.exists():
        return []
    return sorted(p for p in path.glob("*.csv") if p.is_file())


def detect_encoding_and_sep(csv_path: str | Path) -> tuple[str, str]:
    path = Path(csv_path)
    raw = path.read_bytes()
    used_enc = "utf-8"
    text: str | None = None

    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            text = raw.decode(enc)
            used_enc = enc
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        text = raw.decode("utf-8", errors="replace")

    sample = text[:50_000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return used_enc, dialect.delimiter
    except Exception:
        candidates = [
            (",", sample.count(",")),
            (";", sample.count(";")),
            ("\t", sample.count("\t")),
            ("|", sample.count("|")),
        ]
        return used_enc, max(candidates, key=lambda item: item[1])[0]


def read_csv_auto(csv_path: str | Path, *, low_memory: bool = False) -> pd.DataFrame:
    path = Path(csv_path)
    enc, sep = detect_encoding_and_sep(path)
    return pd.read_csv(path, sep=sep, encoding=enc, low_memory=low_memory)


def read_semicolon_csv(csv_path: str | Path, *, low_memory: bool = False) -> pd.DataFrame:
    return pd.read_csv(csv_path, sep=DEFAULT_CSV_SEP, encoding=DEFAULT_CSV_ENCODING, low_memory=low_memory)


def write_semicolon_csv(df: pd.DataFrame, csv_path: str | Path) -> Path:
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=DEFAULT_CSV_SEP, index=False, encoding=DEFAULT_CSV_ENCODING)
    return path


def write_json(data: dict[str, Any], path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out
