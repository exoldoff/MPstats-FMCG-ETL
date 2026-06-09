from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.repositories.manual_override_repository import MANUAL_OVERRIDE_COLUMNS, read_manual_overrides


ALLOWED_MANUAL_OVERRIDE_MODES = {"fill_empty", "overwrite"}


def _to_bool(value: object) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on", "да"}


def _is_empty_series(series: pd.Series) -> pd.Series:
    text = series.astype("string")
    return series.isna() | text.str.strip().fillna("").eq("")


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.casefold()


def _validate_and_prepare_overrides(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    missing = [column for column in MANUAL_OVERRIDE_COLUMNS if column not in out.columns]
    if missing:
        raise ValueError(f"Manual overrides file is missing required columns: {missing}")

    out = out.loc[:, list(MANUAL_OVERRIDE_COLUMNS)].fillna("")
    for column in MANUAL_OVERRIDE_COLUMNS:
        out[column] = out[column].astype(str).str.strip()

    out["row_num"] = range(2, len(out) + 2)
    out["active"] = out["active"].map(_to_bool).astype(bool)
    out["priority"] = pd.to_numeric(out["priority"], errors="coerce").fillna(9999).astype(int)
    out["mode"] = out["mode"].str.lower().replace("", "overwrite")

    bad_mode = out.loc[~out["mode"].isin(ALLOWED_MANUAL_OVERRIDE_MODES), ["row_num", "mode"]]
    if not bad_mode.empty:
        raise ValueError(
            "Invalid mode in manual override rows: "
            + ", ".join(f"{int(row.row_num)}='{row.mode}'" for row in bad_mode.itertuples())
        )

    active_rows = out[out["active"]]
    for column in ("match_field", "match_value", "target_column", "set_value"):
        bad_rows = active_rows.loc[active_rows[column].eq(""), "row_num"].tolist()
        if bad_rows:
            raise ValueError(f"Column '{column}' must be non-empty in active manual override rows: {bad_rows}")

    return out.sort_values(["priority", "row_num"], kind="stable").reset_index(drop=True)


def apply_manual_overrides(
    df: pd.DataFrame,
    *,
    overrides_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    overrides = _validate_and_prepare_overrides(read_manual_overrides(overrides_path))
    out = df.copy()
    report_rows: list[dict[str, Any]] = []

    for override in overrides.itertuples(index=False):
        if not override.active:
            report_rows.append(
                {
                    "row_num": int(override.row_num),
                    "active": False,
                    "candidate_rows": 0,
                    "applied_rows": 0,
                    "reason": "inactive",
                    "match_field": override.match_field,
                    "match_value": override.match_value,
                    "target_column": override.target_column,
                    "set_value": override.set_value,
                    "mode": override.mode,
                    "comment": override.comment,
                }
            )
            continue

        if override.match_field not in out.columns:
            report_rows.append(
                {
                    "row_num": int(override.row_num),
                    "active": True,
                    "candidate_rows": 0,
                    "applied_rows": 0,
                    "reason": f"missing_match_field:{override.match_field}",
                    "match_field": override.match_field,
                    "match_value": override.match_value,
                    "target_column": override.target_column,
                    "set_value": override.set_value,
                    "mode": override.mode,
                    "comment": override.comment,
                }
            )
            continue

        if override.target_column not in out.columns:
            out[override.target_column] = pd.NA

        match_mask = _normalize_text(out[override.match_field]).eq(str(override.match_value).strip().casefold())
        if override.mode == "fill_empty":
            write_mask = match_mask & _is_empty_series(out[override.target_column])
        else:
            write_mask = match_mask

        if write_mask.any():
            out.loc[write_mask, override.target_column] = override.set_value

        report_rows.append(
            {
                "row_num": int(override.row_num),
                "active": True,
                "priority": int(override.priority),
                "candidate_rows": int(match_mask.sum()),
                "applied_rows": int(write_mask.sum()),
                "reason": "applied",
                "match_field": override.match_field,
                "match_value": override.match_value,
                "target_column": override.target_column,
                "set_value": override.set_value,
                "mode": override.mode,
                "comment": override.comment,
            }
        )

    return out, pd.DataFrame(report_rows)
