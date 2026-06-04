from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Iterable, Tuple

import pandas as pd

REQUIRED_RULE_COLUMNS: Tuple[str, ...] = (
    "active",
    "priority",
    "category",
    "target_column",
    "match_field",
    "match_type",
    "pattern",
    "set_value",
    "mode",
)

ALLOWED_MATCH_TYPES = {"contains", "not_contains", "regex", "equals", "startswith", "otherwise"}
ALLOWED_MODES = {"fill_empty", "overwrite"}
ALLOWED_LOGIC_OPERATORS = {"and", "or"}
CONDITIONS_COLUMN = "conditions_json"


def default_rules_path(base_dir: str | Path | None = None) -> Path:
    """Return default rules file path."""
    if base_dir is None:
        return Path(__file__).resolve().parent / "rules.csv"
    return Path(base_dir) / "rules.csv"


def load_rules(rules_path: str | Path) -> pd.DataFrame:
    """Load classifier rules from CSV or Excel."""
    path = Path(rules_path)
    if not path.exists():
        raise FileNotFoundError(f"Rules file not found: {path}")

    if path.suffix.lower() == ".csv":
        # Prefer deterministic separators first. `sep=None` can mis-detect when
        # conditions_json contains commas inside JSON payload.
        for sep in (";", ","):
            candidate = pd.read_csv(path, sep=sep, dtype=str).fillna("")
            if all(col in candidate.columns for col in REQUIRED_RULE_COLUMNS):
                return candidate
        return pd.read_csv(path, sep=None, engine="python", dtype=str).fillna("")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str).fillna("")
    raise ValueError(f"Unsupported rules format: {path.suffix}. Use .csv/.xlsx/.xls")


def _to_bool(value: object) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on", "да"}


def _normalize_string_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        out[col] = out[col].astype(str).str.strip()
    return out


def _parse_conditions_json(raw_value: object, row_num: int) -> list[dict[str, str]]:
    text = str(raw_value).strip()
    if text == "":
        return []

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Rule row {row_num} has invalid {CONDITIONS_COLUMN}: {exc}") from exc

    if isinstance(parsed, dict):
        parsed = parsed.get("conditions")

    if not isinstance(parsed, list) or not parsed:
        raise ValueError(
            f"Rule row {row_num} must provide a non-empty JSON array in {CONDITIONS_COLUMN}."
        )

    normalized: list[dict[str, str]] = []
    for idx, condition in enumerate(parsed, start=1):
        if not isinstance(condition, dict):
            raise ValueError(f"Rule row {row_num} condition #{idx} must be a JSON object.")

        join_with_prev = str(condition.get("join_with_prev", "and")).strip().lower() or "and"
        if join_with_prev not in ALLOWED_LOGIC_OPERATORS:
            raise ValueError(
                f"Rule row {row_num} condition #{idx} has invalid join_with_prev "
                f"'{join_with_prev}'. Use 'and' or 'or'."
            )

        match_field = str(condition.get("match_field", "")).strip()
        match_type = str(condition.get("match_type", "")).strip().lower()
        pattern = str(condition.get("pattern", "")).strip()

        if match_type != "otherwise" and match_field == "":
            raise ValueError(f"Rule row {row_num} condition #{idx} has empty match_field.")
        if match_type not in ALLOWED_MATCH_TYPES:
            raise ValueError(
                f"Rule row {row_num} condition #{idx} has invalid match_type '{match_type}'."
            )
        if match_type != "otherwise" and pattern == "":
            raise ValueError(f"Rule row {row_num} condition #{idx} has empty pattern.")

        normalized.append(
            {
                "join_with_prev": join_with_prev,
                "match_field": match_field,
                "match_type": match_type,
                "pattern": pattern,
            }
        )
    return normalized


def _validate_and_prepare_rules(rules_df: pd.DataFrame) -> pd.DataFrame:
    out = rules_df.copy()
    missing = [c for c in REQUIRED_RULE_COLUMNS if c not in out.columns]
    if missing:
        raise ValueError(f"Rules file is missing required columns: {missing}")

    if CONDITIONS_COLUMN not in out.columns:
        out[CONDITIONS_COLUMN] = ""

    out["row_num"] = range(2, len(out) + 2)  # +2 because header is row 1.
    out = _normalize_string_columns(
        out,
        [
            "active",
            "priority",
            "category",
            "target_column",
            "match_field",
            "match_type",
            "pattern",
            "set_value",
            "mode",
            CONDITIONS_COLUMN,
        ],
    )

    out["active"] = out["active"].map(_to_bool)
    out["priority"] = pd.to_numeric(out["priority"], errors="coerce").fillna(9999).astype(int)
    out["match_type"] = out["match_type"].str.lower()
    out["mode"] = out["mode"].str.lower().replace("", "fill_empty")
    has_conditions = out[CONDITIONS_COLUMN].ne("")
    bad_match = out.loc[
        (~has_conditions) & (~out["match_type"].isin(ALLOWED_MATCH_TYPES)),
        ["row_num", "match_type"],
    ]
    if not bad_match.empty:
        raise ValueError(
            "Invalid match_type in rules rows: "
            + ", ".join(f"{int(r.row_num)}='{r.match_type}'" for r in bad_match.itertuples())
        )

    bad_mode = out.loc[~out["mode"].isin(ALLOWED_MODES), ["row_num", "mode"]]
    if not bad_mode.empty:
        raise ValueError(
            "Invalid mode in rules rows: "
            + ", ".join(f"{int(r.row_num)}='{r.mode}'" for r in bad_mode.itertuples())
        )

    active_rules = out[out["active"]]
    bad_target = active_rules.loc[active_rules["target_column"].eq(""), "row_num"].tolist()
    if bad_target:
        raise ValueError(f"Column 'target_column' must be non-empty in active rules rows: {bad_target}")
    bad_set_value = active_rules.loc[active_rules["set_value"].eq(""), "row_num"].tolist()
    if bad_set_value:
        raise ValueError(f"Column 'set_value' must be non-empty in active rules rows: {bad_set_value}")

    parsed_conditions: list[list[dict[str, str]]] = []
    bad_match_field_rows: list[int] = []
    bad_pattern_rows: list[int] = []
    for row in out.itertuples(index=False):
        row_num = int(row.row_num)
        conditions = _parse_conditions_json(getattr(row, CONDITIONS_COLUMN, ""), row_num)
        parsed_conditions.append(conditions)

        if row.active and not conditions and str(row.match_type).strip().lower() != "otherwise":
            if str(row.match_field).strip() == "":
                bad_match_field_rows.append(row_num)
            if str(row.pattern).strip() == "":
                bad_pattern_rows.append(row_num)

    if bad_match_field_rows:
        raise ValueError(
            "Column 'match_field' must be non-empty in active rules rows without conditions_json: "
            f"{bad_match_field_rows}"
        )
    if bad_pattern_rows:
        raise ValueError(
            "Column 'pattern' must be non-empty in active rules rows without conditions_json: "
            f"{bad_pattern_rows}"
        )

    out["parsed_conditions"] = parsed_conditions
    return out.sort_values(["priority", "row_num"], kind="stable").reset_index(drop=True)


def _is_empty_series(series: pd.Series) -> pd.Series:
    text = series.astype("string")
    return series.isna() | text.str.strip().fillna("").eq("")


def _normalize_fill_unclassified(
    fill_unclassified: dict[str, object] | None,
) -> dict[str, str]:
    if fill_unclassified is None:
        return {}
    if not isinstance(fill_unclassified, dict):
        raise ValueError("fill_unclassified must be a dict {column_name: fill_value}.")

    normalized: dict[str, str] = {}
    for raw_column, raw_value in fill_unclassified.items():
        column_name = str(raw_column).strip()
        if column_name == "":
            raise ValueError("fill_unclassified contains an empty column name.")
        fill_value = str(raw_value).strip()
        if fill_value == "":
            raise ValueError(
                f"fill_unclassified for column '{column_name}' has empty fill_value."
            )
        normalized[column_name] = fill_value
    return normalized


def _build_match_mask(series: pd.Series, match_type: str, pattern: str) -> pd.Series:
    text = series.fillna("").astype(str)
    if match_type == "contains":
        return text.str.contains(pattern, case=False, regex=False, na=False)
    if match_type == "not_contains":
        return ~text.str.contains(re.escape(pattern), case=False, regex=True, na=False)
    if match_type == "regex":
        try:
            return text.str.contains(pattern, case=False, regex=True, na=False)
        except re.error as exc:
            raise ValueError(f"Invalid regex pattern '{pattern}': {exc}") from exc
    if match_type == "equals":
        return text.str.strip().str.casefold().eq(pattern.strip().casefold())
    if match_type == "startswith":
        return text.str.startswith(pattern, na=False)
    raise ValueError(f"Unsupported match_type: {match_type}")


def _build_condition_mask(
    df: pd.DataFrame,
    *,
    row_num: int,
    match_field: str,
    match_type: str,
    pattern: str,
) -> pd.Series:
    if match_type == "otherwise":
        return pd.Series(True, index=df.index)
    if match_field not in df.columns:
        raise ValueError(f"Rule row {row_num} refers to missing column '{match_field}'")
    return _build_match_mask(df[match_field], match_type, pattern)


def _build_rule_mask(
    df: pd.DataFrame,
    *,
    row_num: int,
    conditions: list[dict[str, str]],
    match_field: str,
    match_type: str,
    pattern: str,
) -> pd.Series:
    has_primary_condition = match_type == "otherwise" or all(
        str(value).strip() != "" for value in (match_field, match_type, pattern)
    )

    if has_primary_condition:
        mask = _build_condition_mask(
            df,
            row_num=row_num,
            match_field=match_field,
            match_type=match_type,
            pattern=pattern,
        )
        for condition in conditions:
            cond_mask = _build_condition_mask(
                df,
                row_num=row_num,
                match_field=condition["match_field"],
                match_type=condition["match_type"],
                pattern=condition["pattern"],
            )
            if condition["join_with_prev"] == "or":
                mask = mask | cond_mask
            else:
                mask = mask & cond_mask
        return mask

    if conditions:
        first = conditions[0]
        mask = _build_condition_mask(
            df,
            row_num=row_num,
            match_field=first["match_field"],
            match_type=first["match_type"],
            pattern=first["pattern"],
        )
        for condition in conditions[1:]:
            cond_mask = _build_condition_mask(
                df,
                row_num=row_num,
                match_field=condition["match_field"],
                match_type=condition["match_type"],
                pattern=condition["pattern"],
            )
            if condition["join_with_prev"] == "or":
                mask = mask | cond_mask
            else:
                mask = mask & cond_mask
        return mask

    return _build_condition_mask(
        df,
        row_num=row_num,
        match_field=match_field,
        match_type=match_type,
        pattern=pattern,
    )


def _rule_uses_otherwise(match_type: str, conditions: list[dict[str, str]]) -> bool:
    return match_type == "otherwise" or any(condition["match_type"] == "otherwise" for condition in conditions)


def _build_category_mask(df: pd.DataFrame, *, row_num: int, category_column: str, rule_category: str) -> pd.Series:
    if category_column not in df.columns:
        raise ValueError(
            f"Rule row {row_num} has category filter '{rule_category}', "
            f"but dataframe has no '{category_column}' column."
        )
    return (
        df[category_column]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.casefold()
        .eq(rule_category.casefold())
    )


def apply_classifiers(
    df: pd.DataFrame,
    rules_path: str | Path | None = None,
    *,
    category_column: str = "Категория",
    fill_unclassified: dict[str, object] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply CSV/Excel rules to dataframe and return:
      1) transformed dataframe
      2) per-rule execution report

    Optional:
      - fill_unclassified: dict {target_column: fill_value}
        Applied after all rules; fills only empty values in selected columns.
    """
    if rules_path is None:
        rules_path = default_rules_path()

    out = df.copy()
    rules = _validate_and_prepare_rules(load_rules(rules_path))
    fill_unclassified_map = _normalize_fill_unclassified(fill_unclassified)
    report_rows: list[dict[str, object]] = []

    for rule in rules.itertuples(index=False):
        conditions = list(getattr(rule, "parsed_conditions", []))
        if not rule.active:
            report_rows.append(
                {
                    "row_num": int(rule.row_num),
                    "active": False,
                    "applied_rows": 0,
                    "candidate_rows": 0,
                    "reason": "inactive",
                    "target_column": rule.target_column,
                    "comment": getattr(rule, "comment", ""),
                    "conditions_count": len(conditions),
                    CONDITIONS_COLUMN: getattr(rule, CONDITIONS_COLUMN, ""),
                }
            )
            continue

        row_num = int(rule.row_num)
        if rule.target_column not in out.columns:
            out[rule.target_column] = pd.NA

        rule_category = str(rule.category).strip()
        if rule_category and rule_category != "*":
            category_mask = _build_category_mask(
                out,
                row_num=row_num,
                category_column=category_column,
                rule_category=rule_category,
            )
            if category_mask.any():
                match_mask = _build_rule_mask(
                    out.loc[category_mask],
                    row_num=row_num,
                    conditions=conditions,
                    match_field=rule.match_field,
                    match_type=rule.match_type,
                    pattern=rule.pattern,
                )
                mask = pd.Series(False, index=out.index)
                mask.loc[category_mask] = match_mask.to_numpy(dtype=bool)
            else:
                mask = pd.Series(False, index=out.index)
        else:
            mask = _build_rule_mask(
                out,
                row_num=row_num,
                conditions=conditions,
                match_field=rule.match_field,
                match_type=rule.match_type,
                pattern=rule.pattern,
            )

        candidate_rows = int(mask.sum())
        if _rule_uses_otherwise(rule.match_type, conditions):
            target_empty = _is_empty_series(out[rule.target_column])
            write_mask = mask & target_empty
        elif rule.mode == "fill_empty":
            target_empty = _is_empty_series(out[rule.target_column])
            write_mask = mask & target_empty
        else:
            write_mask = mask

        applied_rows = int(write_mask.sum())
        if applied_rows > 0:
            out.loc[write_mask, rule.target_column] = rule.set_value

        report_rows.append(
            {
                "row_num": row_num,
                "active": True,
                "priority": int(rule.priority),
                "category": rule.category,
                "target_column": rule.target_column,
                "match_field": rule.match_field,
                "match_type": rule.match_type,
                "pattern": rule.pattern,
                "set_value": rule.set_value,
                "mode": rule.mode,
                "candidate_rows": candidate_rows,
                "applied_rows": applied_rows,
                "comment": getattr(rule, "comment", ""),
                "conditions_count": len(conditions),
                CONDITIONS_COLUMN: getattr(rule, CONDITIONS_COLUMN, ""),
            }
        )

    for column_name, fill_value in fill_unclassified_map.items():
        if column_name not in out.columns:
            out[column_name] = pd.NA
        empty_mask = _is_empty_series(out[column_name])
        if empty_mask.any():
            out.loc[empty_mask, column_name] = fill_value

    report = pd.DataFrame(report_rows)
    return out, report
