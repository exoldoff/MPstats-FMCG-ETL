from __future__ import annotations

from pathlib import Path

import pandas as pd


MANUAL_OVERRIDE_COLUMNS = (
    "active",
    "priority",
    "match_field",
    "match_value",
    "target_column",
    "set_value",
    "mode",
    "comment",
)


def empty_manual_overrides_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(MANUAL_OVERRIDE_COLUMNS))


def read_manual_overrides(path: str | Path) -> pd.DataFrame:
    target = Path(path)
    if not target.exists():
        return empty_manual_overrides_frame()

    frame = pd.read_csv(target, sep=";", dtype=str).fillna("")
    for column in MANUAL_OVERRIDE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, list(MANUAL_OVERRIDE_COLUMNS)]


def write_manual_overrides(path: str | Path, frame: pd.DataFrame) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    out = frame.copy()
    for column in MANUAL_OVERRIDE_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    out.loc[:, list(MANUAL_OVERRIDE_COLUMNS)].to_csv(target, sep=";", index=False, encoding="utf-8-sig")
    return target
