from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pandas as pd

try:
    from .engine import (
        ALLOWED_MATCH_TYPES,
        ALLOWED_MODES,
        CONDITIONS_COLUMN,
        REQUIRED_RULE_COLUMNS,
        default_rules_path,
        load_rules,
    )
except ImportError:
    from engine import (  # type: ignore
        ALLOWED_MATCH_TYPES,
        ALLOWED_MODES,
        CONDITIONS_COLUMN,
        REQUIRED_RULE_COLUMNS,
        default_rules_path,
        load_rules,
    )


COMMENT_COLUMN = "comment"
LOGIC_CHOICES = ("and", "or")
ACTIVE_TRUE = {"1", "true", "yes", "y", "on", "да"}
RULE_TREE_HEADINGS = {
    "active": "Активна",
    "priority": "Приоритет",
    "category": "Категория",
    "target_column": "Целевая колонка",
    "set_value": "Значение",
    "mode": "Режим",
    COMMENT_COLUMN: "Комментарий",
    "conditions_count": "Условий",
}
CONDITION_TREE_HEADINGS = {
    "join_with_prev": "Связка",
    "match_field": "Поле",
    "match_type": "Тип совпадения",
    "pattern": "Шаблон",
}
RULE_FORM_LABELS = {
    "active": "Активна",
    "priority": "Приоритет",
    "category": "Категория",
    "target_column": "Целевая колонка",
    "set_value": "Значение",
    "mode": "Режим",
    COMMENT_COLUMN: "Комментарий",
}
CONDITION_FORM_LABELS = {
    "join_with_prev": "Связка",
    "match_field": "Поле",
    "match_type": "Тип совпадения",
    "pattern": "Шаблон",
}
RULE_TREE_COLUMNS = (
    "active",
    "priority",
    "category",
    "target_column",
    "set_value",
    "mode",
    COMMENT_COLUMN,
    "conditions_count",
)
CONDITION_TREE_COLUMNS = ("join_with_prev", "match_field", "match_type", "pattern")


def _to_bool_text(value: str) -> bool:
    return value.strip().lower() in ACTIVE_TRUE


def _normalize_active(value: str) -> str:
    return "1" if _to_bool_text(value) else "0"


def _default_condition() -> dict[str, str]:
    return {
        "join_with_prev": "and",
        "match_field": "",
        "match_type": "contains",
        "pattern": "",
    }


def _empty_rule() -> dict[str, object]:
    return {
        "active": "1",
        "priority": "100",
        "category": "*",
        "target_column": "",
        "match_field": "",
        "match_type": "contains",
        "pattern": "",
        "set_value": "",
        "mode": "fill_empty",
        COMMENT_COLUMN: "",
        CONDITIONS_COLUMN: "",
        "conditions": [_default_condition()],
    }


class RulesEditorApp(tk.Tk):
    def __init__(self, initial_path: str | Path | None = None) -> None:
        super().__init__()
        self.title("Редактор правил классификатора")
        self.geometry("1450x900")
        self.minsize(1100, 700)

        self.file_path: Path | None = None
        self.rules: list[dict[str, object]] = []
        self.current_rule_idx: int | None = None
        self.current_condition_idx: int | None = None
        self._syncing_ui = False

        self.rule_vars: dict[str, tk.StringVar] = {
            "active": tk.StringVar(value="1"),
            "priority": tk.StringVar(value="100"),
            "category": tk.StringVar(value="*"),
            "target_column": tk.StringVar(value=""),
            "set_value": tk.StringVar(value=""),
            "mode": tk.StringVar(value="fill_empty"),
            COMMENT_COLUMN: tk.StringVar(value=""),
        }
        self.condition_vars: dict[str, tk.StringVar] = {
            "join_with_prev": tk.StringVar(value="and"),
            "match_field": tk.StringVar(value=""),
            "match_type": tk.StringVar(value="contains"),
            "pattern": tk.StringVar(value=""),
        }

        self._build_ui()

        start_path = Path(initial_path) if initial_path else default_rules_path()
        if start_path.exists():
            self.load_file(start_path)
        else:
            self.rules = [_empty_rule()]
            self._refresh_rules_tree()
            self._select_rule(0)

    @contextmanager
    def _ui_lock(self):
        previous_state = self._syncing_ui
        self._syncing_ui = True
        try:
            yield
        finally:
            self._syncing_ui = previous_state

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=10)
        container.pack(fill="both", expand=True)

        toolbar = ttk.Frame(container)
        toolbar.pack(fill="x", pady=(0, 8))

        ttk.Button(toolbar, text="Открыть", command=self.choose_open_file).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Сохранить", command=self.save_current).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Сохранить как", command=self.save_as).pack(side="left", padx=(0, 12))
        ttk.Button(toolbar, text="Добавить правило", command=self.add_rule).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Удалить правило", command=self.delete_rule).pack(side="left", padx=(0, 12))

        self.path_label = ttk.Label(toolbar, text="Файл не выбран")
        self.path_label.pack(side="left", fill="x", expand=True)

        top_frame = ttk.LabelFrame(container, text="Правила")
        top_frame.pack(fill="both", expand=True, pady=(0, 8))
        top_frame.rowconfigure(0, weight=1)
        top_frame.columnconfigure(0, weight=1)

        self.rules_tree = ttk.Treeview(
            top_frame,
            columns=RULE_TREE_COLUMNS,
            show="headings",
            height=10,
        )
        widths = {
            "active": 70,
            "priority": 70,
            "category": 140,
            "target_column": 140,
            "set_value": 200,
            "mode": 100,
            COMMENT_COLUMN: 260,
            "conditions_count": 90,
        }
        for col in RULE_TREE_COLUMNS:
            self.rules_tree.heading(col, text=RULE_TREE_HEADINGS[col])
            self.rules_tree.column(col, width=widths[col], anchor="w")
        self.rules_tree.grid(row=0, column=0, sticky="nsew")
        self.rules_tree.bind("<<TreeviewSelect>>", self._on_rule_select)

        rules_scroll = ttk.Scrollbar(top_frame, orient="vertical", command=self.rules_tree.yview)
        rules_scroll.grid(row=0, column=1, sticky="ns")
        self.rules_tree.configure(yscrollcommand=rules_scroll.set)

        rule_form = ttk.Frame(top_frame, padding=(0, 8, 0, 0))
        rule_form.grid(row=1, column=0, columnspan=2, sticky="ew")
        for idx in range(8):
            rule_form.columnconfigure(idx, weight=1)

        self._add_rule_form_field(rule_form, 0, "active", ttk.Combobox, values=("1", "0"))
        self._add_rule_form_field(rule_form, 1, "priority", ttk.Entry)
        self._add_rule_form_field(rule_form, 2, "category", ttk.Entry)
        self._add_rule_form_field(rule_form, 3, "target_column", ttk.Entry)
        self._add_rule_form_field(rule_form, 4, "set_value", ttk.Entry)
        self._add_rule_form_field(rule_form, 5, "mode", ttk.Combobox, values=tuple(sorted(ALLOWED_MODES)))
        self._add_rule_form_field(rule_form, 6, COMMENT_COLUMN, ttk.Entry)

        ttk.Button(rule_form, text="Применить поля правила", command=self.apply_rule_form).grid(
            row=0, column=7, padx=(8, 0), sticky="ew"
        )

        cond_frame = ttk.LabelFrame(container, text="Условия выбранного правила")
        cond_frame.pack(fill="both", expand=True)
        cond_frame.rowconfigure(1, weight=1)
        cond_frame.columnconfigure(0, weight=1)

        cond_btns = ttk.Frame(cond_frame)
        cond_btns.grid(row=0, column=0, sticky="w", pady=(0, 6))

        ttk.Button(cond_btns, text="Добавить условие", command=self.add_condition).pack(side="left", padx=(0, 6))
        ttk.Button(cond_btns, text="Удалить условие", command=self.delete_condition).pack(
            side="left", padx=(0, 6)
        )
        ttk.Button(cond_btns, text="Вверх", command=lambda: self.move_condition(-1)).pack(
            side="left", padx=(0, 6)
        )
        ttk.Button(cond_btns, text="Вниз", command=lambda: self.move_condition(1)).pack(
            side="left", padx=(0, 6)
        )

        self.conditions_tree = ttk.Treeview(
            cond_frame,
            columns=CONDITION_TREE_COLUMNS,
            show="headings",
            height=8,
        )
        cond_widths = {
            "join_with_prev": 90,
            "match_field": 260,
            "match_type": 140,
            "pattern": 520,
        }
        for col in CONDITION_TREE_COLUMNS:
            self.conditions_tree.heading(col, text=CONDITION_TREE_HEADINGS[col])
            self.conditions_tree.column(col, width=cond_widths[col], anchor="w")
        self.conditions_tree.grid(row=1, column=0, sticky="nsew")
        self.conditions_tree.bind("<<TreeviewSelect>>", self._on_condition_select)

        cond_scroll = ttk.Scrollbar(cond_frame, orient="vertical", command=self.conditions_tree.yview)
        cond_scroll.grid(row=1, column=1, sticky="ns")
        self.conditions_tree.configure(yscrollcommand=cond_scroll.set)

        cond_form = ttk.Frame(cond_frame, padding=(0, 8, 0, 0))
        cond_form.grid(row=2, column=0, columnspan=2, sticky="ew")
        for idx in range(5):
            cond_form.columnconfigure(idx, weight=1)

        self._add_condition_form_field(
            cond_form,
            0,
            "join_with_prev",
            ttk.Combobox,
            values=LOGIC_CHOICES,
        )
        self._add_condition_form_field(cond_form, 1, "match_field", ttk.Entry)
        self._add_condition_form_field(
            cond_form,
            2,
            "match_type",
            ttk.Combobox,
            values=tuple(sorted(ALLOWED_MATCH_TYPES)),
        )
        self._add_condition_form_field(cond_form, 3, "pattern", ttk.Entry)
        ttk.Button(cond_form, text="Применить условие", command=self.apply_condition_form).grid(
            row=0, column=4, padx=(8, 0), sticky="ew"
        )

    def _add_rule_form_field(
        self,
        parent: ttk.Frame,
        col_idx: int,
        name: str,
        widget_cls: type[ttk.Entry] | type[ttk.Combobox],
        **kwargs: object,
    ) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=0, column=col_idx, sticky="ew", padx=(0, 6))
        ttk.Label(frame, text=RULE_FORM_LABELS.get(name, name)).pack(anchor="w")
        widget = widget_cls(frame, textvariable=self.rule_vars[name], **kwargs)
        widget.pack(fill="x")

    def _add_condition_form_field(
        self,
        parent: ttk.Frame,
        col_idx: int,
        name: str,
        widget_cls: type[ttk.Entry] | type[ttk.Combobox],
        **kwargs: object,
    ) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=0, column=col_idx, sticky="ew", padx=(0, 6))
        ttk.Label(frame, text=CONDITION_FORM_LABELS.get(name, name)).pack(anchor="w")
        widget = widget_cls(frame, textvariable=self.condition_vars[name], **kwargs)
        widget.pack(fill="x")

    def _select_tree_index(self, tree: ttk.Treeview, idx: int) -> None:
        iid = str(idx)
        if not tree.exists(iid):
            return
        with self._ui_lock():
            tree.selection_set(iid)
            tree.focus(iid)
            tree.see(iid)

    def choose_open_file(self) -> None:
        path_str = filedialog.askopenfilename(
            title="Открыть файл правил",
            filetypes=[
                ("Файлы правил", "*.csv *.xlsx *.xls"),
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls"),
                ("Все файлы", "*.*"),
            ],
        )
        if not path_str:
            return
        self.load_file(Path(path_str))

    def load_file(self, path: Path) -> None:
        if not self._commit_forms():
            return
        try:
            df = load_rules(path)
            rules = self._rules_from_dataframe(df)
        except Exception as exc:
            messagebox.showerror("Ошибка открытия", str(exc))
            return

        self.rules = rules if rules else [_empty_rule()]
        self.file_path = path
        self._set_path_label()
        self._refresh_rules_tree()
        self._select_rule(0)

    def _rules_from_dataframe(self, df: pd.DataFrame) -> list[dict[str, object]]:
        missing = [col for col in REQUIRED_RULE_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Rules file is missing required columns: {missing}")

        out: list[dict[str, object]] = []
        normalized = df.fillna("").copy()
        if COMMENT_COLUMN not in normalized.columns:
            normalized[COMMENT_COLUMN] = ""
        if CONDITIONS_COLUMN not in normalized.columns:
            normalized[CONDITIONS_COLUMN] = ""

        for _, row in normalized.iterrows():
            rule = _empty_rule()
            for key in REQUIRED_RULE_COLUMNS + (COMMENT_COLUMN, CONDITIONS_COLUMN):
                rule[key] = str(row.get(key, "")).strip()

            conditions = self._load_conditions_for_rule(rule)
            rule["conditions"] = conditions
            self._sync_legacy_fields(rule)
            out.append(rule)
        return out

    def _load_conditions_for_rule(self, rule: dict[str, object]) -> list[dict[str, str]]:
        fallback_field = str(rule.get("match_field", "")).strip()
        fallback_type = str(rule.get("match_type", "contains")).strip().lower() or "contains"
        fallback_pattern = str(rule.get("pattern", "")).strip()
        if fallback_type not in ALLOWED_MATCH_TYPES:
            fallback_type = "contains"

        base_condition: dict[str, str] | None = None
        if fallback_type == "otherwise" or fallback_field or fallback_pattern:
            base_condition = {
                "join_with_prev": "and",
                "match_field": "" if fallback_type == "otherwise" else fallback_field,
                "match_type": fallback_type,
                "pattern": "" if fallback_type == "otherwise" else fallback_pattern,
            }

        raw = str(rule.get(CONDITIONS_COLUMN, "")).strip()
        if raw:
            parsed = self._parse_conditions_payload(raw)
            if parsed:
                if base_condition is not None:
                    return [base_condition, *parsed]
                parsed[0]["join_with_prev"] = "and"
                return parsed

        if base_condition is not None:
            return [base_condition]
        return [_default_condition()]

    def _parse_conditions_payload(self, payload: str) -> list[dict[str, str]]:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid {CONDITIONS_COLUMN}: {exc}") from exc

        if isinstance(parsed, dict):
            parsed = parsed.get("conditions")
        if not isinstance(parsed, list):
            raise ValueError(f"{CONDITIONS_COLUMN} must be a JSON array.")

        conditions: list[dict[str, str]] = []
        for idx, cond in enumerate(parsed, start=1):
            if not isinstance(cond, dict):
                raise ValueError(f"{CONDITIONS_COLUMN} item #{idx} must be JSON object.")
            join = str(cond.get("join_with_prev", "and")).strip().lower() or "and"
            if join not in LOGIC_CHOICES:
                raise ValueError(
                    f"{CONDITIONS_COLUMN} item #{idx} has invalid join_with_prev '{join}'."
                )
            match_type = str(cond.get("match_type", "")).strip().lower()
            if match_type not in ALLOWED_MATCH_TYPES:
                raise ValueError(
                    f"{CONDITIONS_COLUMN} item #{idx} has invalid match_type '{match_type}'."
                )
            conditions.append(
                {
                    "join_with_prev": join,
                    "match_field": str(cond.get("match_field", "")).strip(),
                    "match_type": match_type,
                    "pattern": str(cond.get("pattern", "")).strip(),
                }
            )
        return conditions

    def _set_path_label(self) -> None:
        if self.file_path is None:
            self.path_label.configure(text="Файл не выбран")
            return
        self.path_label.configure(text=str(self.file_path))

    def _refresh_rules_tree(self) -> None:
        with self._ui_lock():
            self.rules_tree.delete(*self.rules_tree.get_children())
            for idx, rule in enumerate(self.rules):
                conditions = list(rule.get("conditions", []))
                values = (
                    _normalize_active(str(rule.get("active", "0"))),
                    str(rule.get("priority", "")),
                    str(rule.get("category", "")),
                    str(rule.get("target_column", "")),
                    str(rule.get("set_value", "")),
                    str(rule.get("mode", "")),
                    str(rule.get(COMMENT_COLUMN, "")),
                    str(len(conditions)),
                )
                self.rules_tree.insert("", "end", iid=str(idx), values=values)

    def _refresh_conditions_tree(self) -> None:
        with self._ui_lock():
            self.conditions_tree.delete(*self.conditions_tree.get_children())
            if self.current_rule_idx is None or self.current_rule_idx >= len(self.rules):
                return
            rule = self.rules[self.current_rule_idx]
            conditions: list[dict[str, str]] = list(rule.get("conditions", []))
            if not conditions:
                conditions = [_default_condition()]
                rule["conditions"] = conditions

            for idx, cond in enumerate(conditions):
                join = cond.get("join_with_prev", "and")
                if idx == 0:
                    join = "and"
                    cond["join_with_prev"] = "and"
                values = (
                    join,
                    cond.get("match_field", ""),
                    cond.get("match_type", "contains"),
                    cond.get("pattern", ""),
                )
                self.conditions_tree.insert("", "end", iid=str(idx), values=values)

    def _select_rule(self, idx: int) -> None:
        if idx < 0 or idx >= len(self.rules):
            return
        self.current_rule_idx = idx
        self._select_tree_index(self.rules_tree, idx)
        self._load_rule_form()
        self._refresh_conditions_tree()
        self._select_condition(0)

    def _select_condition(self, idx: int) -> None:
        if self.current_rule_idx is None:
            return
        rule = self.rules[self.current_rule_idx]
        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        if not conditions:
            return
        if idx < 0:
            idx = 0
        if idx >= len(conditions):
            idx = len(conditions) - 1
        self.current_condition_idx = idx
        self._select_tree_index(self.conditions_tree, idx)
        self._load_condition_form()

    def _on_rule_select(self, _: object) -> None:
        if self._syncing_ui:
            return
        if not self._commit_forms(silent=True):
            return
        selected = self.rules_tree.selection()
        if not selected:
            self.current_rule_idx = None
            self.current_condition_idx = None
            return
        self.current_rule_idx = int(selected[0])
        self.current_condition_idx = None
        self._load_rule_form()
        self._refresh_conditions_tree()
        self._select_condition(0)

    def _on_condition_select(self, _: object) -> None:
        if self._syncing_ui:
            return
        if not self.apply_condition_form(silent=True, refresh_ui=False):
            return
        selected = self.conditions_tree.selection()
        if not selected:
            self.current_condition_idx = None
            return
        self.current_condition_idx = int(selected[0])
        self._load_condition_form()

    def _load_rule_form(self) -> None:
        with self._ui_lock():
            if self.current_rule_idx is None:
                return
            rule = self.rules[self.current_rule_idx]
            for key, var in self.rule_vars.items():
                var.set(str(rule.get(key, "")))

    def _load_condition_form(self) -> None:
        with self._ui_lock():
            if self.current_rule_idx is None or self.current_condition_idx is None:
                return
            rule = self.rules[self.current_rule_idx]
            conditions: list[dict[str, str]] = list(rule.get("conditions", []))
            if self.current_condition_idx >= len(conditions):
                return
            condition = conditions[self.current_condition_idx]
            for key, var in self.condition_vars.items():
                value = str(condition.get(key, ""))
                if key == "join_with_prev" and self.current_condition_idx == 0:
                    value = "and"
                var.set(value)

    def _commit_forms(self, silent: bool = False) -> bool:
        if not self.apply_rule_form(silent=silent, refresh_ui=False):
            return False
        if not self.apply_condition_form(silent=silent, refresh_ui=False):
            return False
        return True

    def apply_rule_form(self, silent: bool = False, *, refresh_ui: bool = True) -> bool:
        if self.current_rule_idx is None or self.current_rule_idx >= len(self.rules):
            return True
        try:
            rule = self.rules[self.current_rule_idx]
            rule["active"] = _normalize_active(self.rule_vars["active"].get())
            rule["priority"] = self.rule_vars["priority"].get().strip() or "9999"
            rule["category"] = self.rule_vars["category"].get().strip()
            rule["target_column"] = self.rule_vars["target_column"].get().strip()
            rule["set_value"] = self.rule_vars["set_value"].get().strip()
            mode = self.rule_vars["mode"].get().strip().lower() or "fill_empty"
            if mode not in ALLOWED_MODES:
                raise ValueError(f"Invalid mode '{mode}'.")
            rule["mode"] = mode
            rule[COMMENT_COLUMN] = self.rule_vars[COMMENT_COLUMN].get().strip()
            int(str(rule["priority"]))
        except Exception as exc:
            if not silent:
                messagebox.showerror("Ошибка правила", str(exc))
            return False

        if refresh_ui:
            self._refresh_rules_tree()
            self._select_tree_index(self.rules_tree, self.current_rule_idx)
        return True

    def apply_condition_form(self, silent: bool = False, *, refresh_ui: bool = True) -> bool:
        if self.current_rule_idx is None or self.current_condition_idx is None:
            return True
        if self.current_rule_idx >= len(self.rules):
            return True
        rule = self.rules[self.current_rule_idx]
        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        if self.current_condition_idx >= len(conditions):
            return True

        try:
            join = self.condition_vars["join_with_prev"].get().strip().lower() or "and"
            if self.current_condition_idx == 0:
                join = "and"
            if join not in LOGIC_CHOICES:
                raise ValueError(f"Invalid join_with_prev '{join}'.")

            match_type = self.condition_vars["match_type"].get().strip().lower()
            if match_type not in ALLOWED_MATCH_TYPES:
                raise ValueError(f"Invalid match_type '{match_type}'.")

            conditions[self.current_condition_idx] = {
                "join_with_prev": join,
                "match_field": self.condition_vars["match_field"].get().strip(),
                "match_type": match_type,
                "pattern": self.condition_vars["pattern"].get().strip(),
            }
        except Exception as exc:
            if not silent:
                messagebox.showerror("Ошибка условия", str(exc))
            return False

        rule["conditions"] = conditions
        self._sync_legacy_fields(rule)
        if refresh_ui:
            self._refresh_conditions_tree()
            self._select_tree_index(self.conditions_tree, self.current_condition_idx)
            self._refresh_rules_tree()
            self._select_tree_index(self.rules_tree, self.current_rule_idx)
        return True

    def add_rule(self) -> None:
        if not self._commit_forms():
            return
        self.rules.append(_empty_rule())
        self._refresh_rules_tree()
        self._select_rule(len(self.rules) - 1)

    def delete_rule(self) -> None:
        if self.current_rule_idx is None or self.current_rule_idx >= len(self.rules):
            return
        if len(self.rules) == 1:
            messagebox.showwarning("Удаление правила", "Должно остаться хотя бы одно правило.")
            return
        del self.rules[self.current_rule_idx]
        self._refresh_rules_tree()
        self._select_rule(max(0, self.current_rule_idx - 1))

    def add_condition(self) -> None:
        if self.current_rule_idx is None or self.current_rule_idx >= len(self.rules):
            return
        if not self._commit_forms():
            return
        rule = self.rules[self.current_rule_idx]
        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        new_condition = _default_condition()
        if conditions:
            new_condition["join_with_prev"] = "and"
        conditions.append(new_condition)
        rule["conditions"] = conditions
        self._refresh_conditions_tree()
        self._select_condition(len(conditions) - 1)
        self._refresh_rules_tree()
        self._select_tree_index(self.rules_tree, self.current_rule_idx)

    def delete_condition(self) -> None:
        if self.current_rule_idx is None or self.current_rule_idx >= len(self.rules):
            return
        if self.current_condition_idx is None:
            return
        rule = self.rules[self.current_rule_idx]
        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        if len(conditions) <= 1:
            messagebox.showwarning("Удаление условия", "У правила должно остаться хотя бы одно условие.")
            return
        if self.current_condition_idx >= len(conditions):
            return
        del conditions[self.current_condition_idx]
        if conditions:
            conditions[0]["join_with_prev"] = "and"
        rule["conditions"] = conditions
        self._sync_legacy_fields(rule)
        self._refresh_conditions_tree()
        self._select_condition(max(0, self.current_condition_idx - 1))
        self._refresh_rules_tree()
        self._select_tree_index(self.rules_tree, self.current_rule_idx)

    def move_condition(self, offset: int) -> None:
        if self.current_rule_idx is None or self.current_condition_idx is None:
            return
        rule = self.rules[self.current_rule_idx]
        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        src = self.current_condition_idx
        dst = src + offset
        if src < 0 or src >= len(conditions) or dst < 0 or dst >= len(conditions):
            return
        if not self._commit_forms():
            return

        conditions[src], conditions[dst] = conditions[dst], conditions[src]
        if conditions:
            conditions[0]["join_with_prev"] = "and"
        rule["conditions"] = conditions
        self._sync_legacy_fields(rule)
        self._refresh_conditions_tree()
        self._select_condition(dst)
        self._refresh_rules_tree()
        self._select_tree_index(self.rules_tree, self.current_rule_idx)

    def _sync_legacy_fields(self, rule: dict[str, object]) -> None:
        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        if not conditions:
            rule["match_field"] = ""
            rule["match_type"] = "contains"
            rule["pattern"] = ""
            rule[CONDITIONS_COLUMN] = ""
            return
        first = conditions[0]
        first["join_with_prev"] = "and"
        rule["match_field"] = first.get("match_field", "")
        rule["match_type"] = first.get("match_type", "contains")
        rule["pattern"] = first.get("pattern", "")
        extra_conditions = conditions[1:]
        if extra_conditions:
            rule[CONDITIONS_COLUMN] = json.dumps(extra_conditions, ensure_ascii=False)
        else:
            rule[CONDITIONS_COLUMN] = ""

    def _validate_rule_for_save(self, idx: int, rule: dict[str, object]) -> None:
        row_num = idx + 2
        priority = str(rule.get("priority", "")).strip()
        if priority == "":
            raise ValueError(f"Rule row {row_num}: priority is required.")
        int(priority)

        mode = str(rule.get("mode", "")).strip().lower()
        if mode not in ALLOWED_MODES:
            raise ValueError(f"Rule row {row_num}: invalid mode '{mode}'.")

        conditions: list[dict[str, str]] = list(rule.get("conditions", []))
        if not conditions:
            raise ValueError(f"Rule row {row_num}: at least one condition is required.")
        conditions[0]["join_with_prev"] = "and"

        for c_idx, cond in enumerate(conditions, start=1):
            join = str(cond.get("join_with_prev", "and")).strip().lower() or "and"
            if c_idx == 1:
                join = "and"
                cond["join_with_prev"] = "and"
            if join not in LOGIC_CHOICES:
                raise ValueError(
                    f"Rule row {row_num}, condition #{c_idx}: invalid join_with_prev '{join}'."
                )
            match_type = str(cond.get("match_type", "")).strip().lower()
            if match_type not in ALLOWED_MATCH_TYPES:
                raise ValueError(
                    f"Rule row {row_num}, condition #{c_idx}: invalid match_type '{match_type}'."
                )
            field = str(cond.get("match_field", "")).strip()
            if match_type != "otherwise" and field == "":
                raise ValueError(f"Rule row {row_num}, condition #{c_idx}: match_field is empty.")
            pattern = str(cond.get("pattern", "")).strip()
            if match_type != "otherwise" and pattern == "":
                raise ValueError(f"Rule row {row_num}, condition #{c_idx}: pattern is empty.")
            if match_type == "otherwise":
                cond["match_field"] = ""
                cond["pattern"] = ""

        if _to_bool_text(str(rule.get("active", ""))):
            target_column = str(rule.get("target_column", "")).strip()
            if target_column == "":
                raise ValueError(f"Rule row {row_num}: target_column is required for active rules.")

    def _build_dataframe_for_save(self) -> pd.DataFrame:
        rows: list[dict[str, str]] = []
        for idx, raw_rule in enumerate(self.rules):
            rule = dict(raw_rule)
            self._validate_rule_for_save(idx, rule)

            self._sync_legacy_fields(rule)

            row = {
                "active": _normalize_active(str(rule.get("active", "0"))),
                "priority": str(rule.get("priority", "")).strip(),
                "category": str(rule.get("category", "")).strip(),
                "target_column": str(rule.get("target_column", "")).strip(),
                "match_field": str(rule.get("match_field", "")).strip(),
                "match_type": str(rule.get("match_type", "")).strip().lower(),
                "pattern": str(rule.get("pattern", "")).strip(),
                "set_value": str(rule.get("set_value", "")).strip(),
                "mode": str(rule.get("mode", "")).strip().lower(),
                COMMENT_COLUMN: str(rule.get(COMMENT_COLUMN, "")).strip(),
                CONDITIONS_COLUMN: str(rule.get(CONDITIONS_COLUMN, "")).strip(),
            }
            rows.append(row)

        ordered_columns = list(REQUIRED_RULE_COLUMNS) + [COMMENT_COLUMN, CONDITIONS_COLUMN]
        return pd.DataFrame(rows, columns=ordered_columns)

    def save_current(self) -> None:
        if not self._commit_forms():
            return
        if self.file_path is None:
            self.save_as()
            return
        self._save_to_path(self.file_path)

    def save_as(self) -> None:
        if not self._commit_forms():
            return
        path_str = filedialog.asksaveasfilename(
            title="Сохранить файл правил",
            defaultextension=".csv",
            filetypes=[
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls"),
                ("Все файлы", "*.*"),
            ],
        )
        if not path_str:
            return
        self._save_to_path(Path(path_str))

    def _save_to_path(self, path: Path) -> None:
        try:
            df = self._build_dataframe_for_save()
            suffix = path.suffix.lower()
            if suffix in {"", ".csv"}:
                out_path = path if suffix else path.with_suffix(".csv")
                df.to_csv(out_path, sep=";", index=False, encoding="utf-8-sig")
            elif suffix in {".xlsx", ".xls"}:
                out_path = path
                df.to_excel(out_path, index=False)
            else:
                raise ValueError(
                    f"Unsupported file extension '{path.suffix}'. Use .csv/.xlsx/.xls."
                )
        except Exception as exc:
            messagebox.showerror("Ошибка сохранения", str(exc))
            return

        self.file_path = out_path
        self._set_path_label()
        self._refresh_rules_tree()
        if self.current_rule_idx is not None:
            self._select_tree_index(self.rules_tree, self.current_rule_idx)
        messagebox.showinfo("Сохранено", f"Правила сохранены в:\n{out_path}")


def main() -> None:
    app = RulesEditorApp()
    app.mainloop()


if __name__ == "__main__":
    main()
