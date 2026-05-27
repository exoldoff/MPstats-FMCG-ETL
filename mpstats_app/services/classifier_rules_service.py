from __future__ import annotations

import json
from typing import Any

import pandas as pd

from classifiers.engine import ALLOWED_MATCH_TYPES, ALLOWED_MODES, CONDITIONS_COLUMN, REQUIRED_RULE_COLUMNS, load_rules
from mpstats_app.config import AppSettings


COMMENT_COLUMN = "comment"
RULE_COLUMNS = list(REQUIRED_RULE_COLUMNS) + [COMMENT_COLUMN, CONDITIONS_COLUMN]


def _bool_text(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "да"}


def _condition() -> dict[str, str]:
    return {"join_with_prev": "and", "match_field": "", "match_type": "contains", "pattern": ""}


def _rule_id(index: int) -> str:
    return f"rule-{index + 1}"


class ClassifierRulesService:
    def __init__(self, *, settings: AppSettings) -> None:
        self.settings = settings

    def list_rules(self) -> dict[str, Any]:
        path = self.settings.rules_path
        if not path.exists():
            return {"path": str(path), "rules": []}
        frame = load_rules(path)
        return {"path": str(path), "rules": self._rules_from_frame(frame)}

    def save_rules(self, rules: list[dict[str, Any]]) -> dict[str, Any]:
        frame = self._frame_from_rules(rules)
        self.settings.rules_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(self.settings.rules_path, sep=";", index=False, encoding="utf-8-sig")
        return self.list_rules()

    def _rules_from_frame(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        normalized = frame.fillna("").copy()
        if COMMENT_COLUMN not in normalized.columns:
            normalized[COMMENT_COLUMN] = ""
        if CONDITIONS_COLUMN not in normalized.columns:
            normalized[CONDITIONS_COLUMN] = ""

        rules: list[dict[str, Any]] = []
        for index, row in normalized.iterrows():
            primary = {
                "join_with_prev": "and",
                "match_field": str(row.get("match_field", "")).strip(),
                "match_type": str(row.get("match_type", "contains")).strip().lower() or "contains",
                "pattern": str(row.get("pattern", "")).strip(),
            }
            conditions = [primary]
            raw_conditions = str(row.get(CONDITIONS_COLUMN, "")).strip()
            if raw_conditions:
                conditions.extend(self._parse_extra_conditions(raw_conditions, int(index) + 2))
            if not any(
                condition["match_type"] == "otherwise" or condition["match_field"] or condition["pattern"]
                for condition in conditions
            ):
                conditions = [_condition()]

            rules.append(
                {
                    "id": _rule_id(int(index)),
                    "active": _bool_text(row.get("active", "")),
                    "priority": int(str(row.get("priority", "") or "9999")),
                    "category": str(row.get("category", "")).strip() or "*",
                    "target_column": str(row.get("target_column", "")).strip(),
                    "set_value": str(row.get("set_value", "")).strip(),
                    "mode": str(row.get("mode", "fill_empty")).strip().lower() or "fill_empty",
                    "comment": str(row.get(COMMENT_COLUMN, "")).strip(),
                    "conditions": conditions,
                }
            )
        return rules

    def _frame_from_rules(self, rules: list[dict[str, Any]]) -> pd.DataFrame:
        if not rules:
            raise ValueError("Добавь хотя бы одно правило классификатора.")
        rows: list[dict[str, str]] = []
        for index, rule in enumerate(rules, start=2):
            rows.append(self._row_from_rule(index, rule))
        return pd.DataFrame(rows, columns=RULE_COLUMNS)

    def _row_from_rule(self, row_num: int, rule: dict[str, Any]) -> dict[str, str]:
        priority = int(rule.get("priority") or 9999)
        mode = str(rule.get("mode") or "fill_empty").strip().lower()
        if mode not in ALLOWED_MODES:
            raise ValueError(f"Правило {row_num}: неизвестный режим {mode!r}.")

        conditions = self._normalize_conditions(rule.get("conditions"), row_num)
        first = conditions[0]
        extra = conditions[1:]
        active = bool(rule.get("active"))
        target_column = str(rule.get("target_column") or "").strip()
        if active and not target_column:
            raise ValueError(f"Правило {row_num}: у активного правила должна быть целевая колонка.")
        set_value = str(rule.get("set_value") or "").strip()
        if active and not set_value:
            raise ValueError(f"Правило {row_num}: у активного правила должно быть значение для записи.")

        return {
            "active": "1" if active else "0",
            "priority": str(priority),
            "category": str(rule.get("category") or "*").strip() or "*",
            "target_column": target_column,
            "match_field": first["match_field"],
            "match_type": first["match_type"],
            "pattern": first["pattern"],
            "set_value": set_value,
            "mode": mode,
            COMMENT_COLUMN: str(rule.get("comment") or "").strip(),
            CONDITIONS_COLUMN: json.dumps(extra, ensure_ascii=False) if extra else "",
        }

    def _normalize_conditions(self, value: object, row_num: int, *, force_first_join: bool = True) -> list[dict[str, str]]:
        if not isinstance(value, list) or not value:
            raise ValueError(f"Правило {row_num}: добавь хотя бы одно условие.")
        normalized: list[dict[str, str]] = []
        for index, raw_condition in enumerate(value, start=1):
            if not isinstance(raw_condition, dict):
                raise ValueError(f"Правило {row_num}, условие {index}: неверный формат.")
            join_with_prev = str(raw_condition.get("join_with_prev") or "and").strip().lower()
            if index == 1 and force_first_join:
                join_with_prev = "and"
            if join_with_prev not in {"and", "or"}:
                raise ValueError(f"Правило {row_num}, условие {index}: связка должна быть and/or.")
            match_field = str(raw_condition.get("match_field") or "").strip()
            match_type = str(raw_condition.get("match_type") or "contains").strip().lower()
            pattern = str(raw_condition.get("pattern") or "").strip()
            if match_type != "otherwise" and not match_field:
                raise ValueError(f"Правило {row_num}, условие {index}: поле не заполнено.")
            if match_type not in ALLOWED_MATCH_TYPES:
                raise ValueError(f"Правило {row_num}, условие {index}: неизвестный тип {match_type!r}.")
            if match_type != "otherwise" and not pattern:
                raise ValueError(f"Правило {row_num}, условие {index}: шаблон не заполнен.")
            normalized.append(
                {
                    "join_with_prev": join_with_prev,
                    "match_field": "" if match_type == "otherwise" else match_field,
                    "match_type": match_type,
                    "pattern": "" if match_type == "otherwise" else pattern,
                }
            )
        return normalized

    def _parse_extra_conditions(self, payload: str, row_num: int) -> list[dict[str, str]]:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Правило {row_num}: неверные дополнительные условия: {exc}") from exc
        if isinstance(parsed, dict):
            parsed = parsed.get("conditions")
        return self._normalize_conditions(parsed, row_num, force_first_join=False) if parsed else []
