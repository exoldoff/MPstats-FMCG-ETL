from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from .step1_config import (
        archive_tasks_path,
        default_config_path,
        load_step1_config,
        load_tasks_from_archive,
        normalize_task,
        save_config,
    )
except ImportError:
    from step1_config import (  # type: ignore
        archive_tasks_path,
        default_config_path,
        load_step1_config,
        load_tasks_from_archive,
        normalize_task,
        save_config,
    )


TASK_TREE_COLUMNS = ("active", "mp", "cat", "path", "fbs", "has_filter")
TASK_TREE_HEADINGS = {
    "active": "Активна",
    "mp": "МП",
    "cat": "Категория",
    "path": "Путь",
    "fbs": "FBS",
    "has_filter": "Фильтр",
}


def _default_task() -> dict[str, object]:
    return {
        "active": True,
        "mp": "oz",
        "cat": "",
        "path": "",
        "fbs": 1,
        "filterModel": {},
    }


def _build_export_months_by_year(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> dict[str, list[int]]:
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("Начальный период не может быть больше конечного.")

    out: dict[str, list[int]] = {}
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        key = str(year)
        out.setdefault(key, []).append(month)
        month += 1
        if month > 12:
            month = 1
            year += 1
    return out


def _period_from_export_months(export_months_by_year: dict[str, list[int]]) -> tuple[int, int, int, int]:
    if not export_months_by_year:
        return 2025, 1, 2025, 12

    pairs: list[tuple[int, int]] = []
    for year_raw, months in export_months_by_year.items():
        if not str(year_raw).isdigit():
            continue
        year = int(year_raw)
        for month in months:
            month_int = int(month)
            if 1 <= month_int <= 12:
                pairs.append((year, month_int))

    if not pairs:
        return 2025, 1, 2025, 12

    pairs.sort()
    start_year, start_month = pairs[0]
    end_year, end_month = pairs[-1]
    return start_year, start_month, end_year, end_month


class Step1ConfigGui(tk.Tk):
    def __init__(self, config_path: Path, archive_path: Path) -> None:
        super().__init__()
        self.title("MPStats: настройки шага 1")
        self.geometry("1550x980")
        self.minsize(1200, 780)

        self.config_path = config_path
        self.archive_path = archive_path
        self.tasks: list[dict[str, object]] = []
        self.current_task_idx: int | None = None
        self._syncing_ui = False

        self.start_year_var = tk.StringVar(value="2025")
        self.start_month_var = tk.StringVar(value="1")
        self.end_year_var = tk.StringVar(value="2025")
        self.end_month_var = tk.StringVar(value="12")
        self.save_dir_var = tk.StringVar(value="")
        self.skip_if_exists_var = tk.BooleanVar(value=True)
        self.extract_zip_var = tk.BooleanVar(value=True)

        self.task_active_var = tk.BooleanVar(value=True)
        self.task_mp_var = tk.StringVar(value="oz")
        self.task_cat_var = tk.StringVar(value="")
        self.task_path_var = tk.StringVar(value="")
        self.task_fbs_var = tk.StringVar(value="1")

        self._build_ui()
        self._load_from_config(config_path)

    @contextmanager
    def _ui_lock(self):
        previous = self._syncing_ui
        self._syncing_ui = True
        try:
            yield
        finally:
            self._syncing_ui = previous

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=10)
        container.pack(fill="both", expand=True)

        toolbar = ttk.Frame(container)
        toolbar.pack(fill="x", pady=(0, 8))

        ttk.Button(toolbar, text="Открыть конфиг", command=self.open_config).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Сохранить", command=self.save_current).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Сохранить как", command=self.save_as).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Импорт из архива", command=self.import_archive_tasks).pack(
            side="left", padx=(0, 12)
        )
        ttk.Button(toolbar, text="Добавить задачу", command=self.add_task).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Клонировать задачу", command=self.clone_task).pack(side="left", padx=(0, 6))
        ttk.Button(toolbar, text="Удалить задачу", command=self.delete_task).pack(side="left", padx=(0, 6))

        self.config_label = ttk.Label(toolbar, text=str(self.config_path))
        self.config_label.pack(side="left", fill="x", expand=True)

        settings_frame = ttk.LabelFrame(container, text="Настройки шага 1")
        settings_frame.pack(fill="x", pady=(0, 8))
        for col in range(10):
            settings_frame.columnconfigure(col, weight=1)

        ttk.Label(settings_frame, text="Начальный год").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.start_year_var).grid(
            row=0, column=1, sticky="ew", padx=4, pady=4
        )
        ttk.Label(settings_frame, text="Начальный месяц").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        ttk.Combobox(
            settings_frame,
            textvariable=self.start_month_var,
            values=[str(i) for i in range(1, 13)],
            state="readonly",
        ).grid(row=0, column=3, sticky="ew", padx=4, pady=4)

        ttk.Label(settings_frame, text="Конечный год").grid(row=0, column=4, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.end_year_var).grid(
            row=0, column=5, sticky="ew", padx=4, pady=4
        )
        ttk.Label(settings_frame, text="Конечный месяц").grid(row=0, column=6, sticky="w", padx=4, pady=4)
        ttk.Combobox(
            settings_frame,
            textvariable=self.end_month_var,
            values=[str(i) for i in range(1, 13)],
            state="readonly",
        ).grid(row=0, column=7, sticky="ew", padx=4, pady=4)

        ttk.Checkbutton(
            settings_frame, text="Пропускать существующие", variable=self.skip_if_exists_var
        ).grid(row=0, column=8, sticky="w", padx=4, pady=4)
        ttk.Checkbutton(
            settings_frame, text="Распаковывать ZIP", variable=self.extract_zip_var
        ).grid(row=0, column=9, sticky="w", padx=4, pady=4)

        ttk.Label(settings_frame, text="Папка сохранения").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.save_dir_var).grid(
            row=1, column=1, columnspan=8, sticky="ew", padx=4, pady=4
        )
        ttk.Button(settings_frame, text="Выбрать", command=self.choose_save_dir).grid(
            row=1, column=9, sticky="ew", padx=4, pady=4
        )

        ttk.Label(settings_frame, text="Cookie").grid(row=2, column=0, sticky="nw", padx=4, pady=4)
        self.cookie_text = tk.Text(settings_frame, height=5, wrap="word")
        self.cookie_text.grid(row=2, column=1, columnspan=9, sticky="nsew", padx=4, pady=4)

        task_frame = ttk.LabelFrame(container, text="Задачи")
        task_frame.pack(fill="both", expand=True)
        task_frame.rowconfigure(0, weight=1)
        task_frame.columnconfigure(0, weight=1)

        self.task_tree = ttk.Treeview(task_frame, columns=TASK_TREE_COLUMNS, show="headings", height=12)
        widths = {"active": 70, "mp": 60, "cat": 180, "path": 620, "fbs": 60, "has_filter": 90}
        for col in TASK_TREE_COLUMNS:
            self.task_tree.heading(col, text=TASK_TREE_HEADINGS[col])
            self.task_tree.column(col, width=widths[col], anchor="w")
        self.task_tree.grid(row=0, column=0, sticky="nsew")
        self.task_tree.bind("<<TreeviewSelect>>", self._on_task_select)

        task_scroll = ttk.Scrollbar(task_frame, orient="vertical", command=self.task_tree.yview)
        task_scroll.grid(row=0, column=1, sticky="ns")
        self.task_tree.configure(yscrollcommand=task_scroll.set)

        detail_frame = ttk.LabelFrame(container, text="Выбранная задача")
        detail_frame.pack(fill="both", expand=True, pady=(8, 0))
        for col in range(7):
            detail_frame.columnconfigure(col, weight=1)

        ttk.Checkbutton(detail_frame, text="Активна", variable=self.task_active_var).grid(
            row=0, column=0, sticky="w", padx=4, pady=4
        )

        ttk.Label(detail_frame, text="МП").grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Combobox(
            detail_frame, textvariable=self.task_mp_var, values=("oz", "wb", "ym"), state="readonly"
        ).grid(row=0, column=2, sticky="ew", padx=4, pady=4)

        ttk.Label(detail_frame, text="FBS").grid(row=0, column=3, sticky="w", padx=4, pady=4)
        ttk.Combobox(detail_frame, textvariable=self.task_fbs_var, values=("", "0", "1")).grid(
            row=0, column=4, sticky="ew", padx=4, pady=4
        )

        ttk.Button(detail_frame, text="Вверх", command=lambda: self.move_task(-1)).grid(
            row=0, column=5, sticky="ew", padx=4, pady=4
        )
        ttk.Button(detail_frame, text="Вниз", command=lambda: self.move_task(1)).grid(
            row=0, column=6, sticky="ew", padx=4, pady=4
        )

        ttk.Label(detail_frame, text="Категория").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(detail_frame, textvariable=self.task_cat_var).grid(
            row=1, column=1, columnspan=6, sticky="ew", padx=4, pady=4
        )

        ttk.Label(detail_frame, text="Путь").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(detail_frame, textvariable=self.task_path_var).grid(
            row=2, column=1, columnspan=6, sticky="ew", padx=4, pady=4
        )

        ttk.Label(detail_frame, text="Фильтр (JSON)").grid(
            row=3, column=0, sticky="nw", padx=4, pady=4
        )
        self.filter_text = tk.Text(detail_frame, height=8, wrap="word")
        self.filter_text.grid(row=3, column=1, columnspan=6, sticky="nsew", padx=4, pady=4)

        ttk.Button(detail_frame, text="Применить изменения задачи", command=self.apply_task_form).grid(
            row=4, column=6, sticky="e", padx=4, pady=8
        )

    def choose_save_dir(self) -> None:
        selected = filedialog.askdirectory(title="Выберите папку сохранения")
        if selected:
            self.save_dir_var.set(selected)

    def _set_config_label(self) -> None:
        self.config_label.configure(text=str(self.config_path))

    def _refresh_task_tree(self) -> None:
        with self._ui_lock():
            self.task_tree.delete(*self.task_tree.get_children())
            for idx, task in enumerate(self.tasks):
                values = (
                    "1" if bool(task.get("active", True)) else "0",
                    str(task.get("mp", "")),
                    str(task.get("cat", "")),
                    str(task.get("path", "")),
                    "" if "fbs" not in task else str(task.get("fbs")),
                    "да" if task.get("filterModel") else "",
                )
                self.task_tree.insert("", "end", iid=str(idx), values=values)

    def _select_task(self, idx: int) -> None:
        if idx < 0 or idx >= len(self.tasks):
            return
        self.current_task_idx = idx
        iid = str(idx)
        if not self.task_tree.exists(iid):
            return
        with self._ui_lock():
            self.task_tree.selection_set(iid)
            self.task_tree.focus(iid)
            self.task_tree.see(iid)
        self._load_task_form()

    def _load_task_form(self) -> None:
        if self.current_task_idx is None or self.current_task_idx >= len(self.tasks):
            return
        task = self.tasks[self.current_task_idx]
        with self._ui_lock():
            self.task_active_var.set(bool(task.get("active", True)))
            self.task_mp_var.set(str(task.get("mp", "oz")))
            self.task_cat_var.set(str(task.get("cat", "")))
            self.task_path_var.set(str(task.get("path", "")))
            fbs_value = "" if "fbs" not in task else str(task.get("fbs"))
            self.task_fbs_var.set(fbs_value)
            self.filter_text.delete("1.0", "end")
            filter_model = task.get("filterModel", {})
            if filter_model:
                self.filter_text.insert("1.0", json.dumps(filter_model, ensure_ascii=False, indent=2))

    def _on_task_select(self, _: object) -> None:
        if self._syncing_ui:
            return
        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        selected = self.task_tree.selection()
        if not selected:
            self.current_task_idx = None
            return
        self.current_task_idx = int(selected[0])
        self._load_task_form()

    def _collect_settings(self) -> dict[str, object]:
        start_year = int(self.start_year_var.get().strip())
        start_month = int(self.start_month_var.get().strip())
        end_year = int(self.end_year_var.get().strip())
        end_month = int(self.end_month_var.get().strip())
        if not (1 <= start_month <= 12 and 1 <= end_month <= 12):
            raise ValueError("Месяцы должны быть в диапазоне 1..12.")

        export_months_by_year = _build_export_months_by_year(
            start_year, start_month, end_year, end_month
        )

        return {
            "export_months_by_year": export_months_by_year,
            "save_dir": self.save_dir_var.get().strip(),
            "skip_if_exists": bool(self.skip_if_exists_var.get()),
            "extract_zip": bool(self.extract_zip_var.get()),
            "cookie": self.cookie_text.get("1.0", "end-1c"),
            "tasks": [dict(task) for task in self.tasks],
        }

    def _load_settings(self, config_data: dict[str, object]) -> None:
        export_months = config_data.get("export_months_by_year", {})
        start_year, start_month, end_year, end_month = _period_from_export_months(
            export_months if isinstance(export_months, dict) else {}
        )
        with self._ui_lock():
            self.start_year_var.set(str(start_year))
            self.start_month_var.set(str(start_month))
            self.end_year_var.set(str(end_year))
            self.end_month_var.set(str(end_month))
            self.save_dir_var.set(str(config_data.get("save_dir", "")))
            self.skip_if_exists_var.set(bool(config_data.get("skip_if_exists", True)))
            self.extract_zip_var.set(bool(config_data.get("extract_zip", True)))
            self.cookie_text.delete("1.0", "end")
            self.cookie_text.insert("1.0", str(config_data.get("cookie", "")))

        tasks_raw = config_data.get("tasks", [])
        tasks: list[dict[str, object]] = []
        if isinstance(tasks_raw, list):
            for task in tasks_raw:
                try:
                    tasks.append(normalize_task(task))
                except Exception:
                    continue
        self.tasks = tasks or [_default_task()]
        self._refresh_task_tree()
        self._select_task(0)

    def apply_task_form(self, silent: bool = False, *, refresh_ui: bool = True) -> bool:
        if self.current_task_idx is None or self.current_task_idx >= len(self.tasks):
            return True

        try:
            raw_filter = self.filter_text.get("1.0", "end-1c").strip()
            filter_model: dict[str, object] = {}
            if raw_filter:
                loaded = json.loads(raw_filter)
                if not isinstance(loaded, dict):
                    raise ValueError("filterModel должен быть JSON-объектом.")
                filter_model = loaded

            payload: dict[str, object] = {
                "active": bool(self.task_active_var.get()),
                "mp": self.task_mp_var.get().strip().lower(),
                "cat": self.task_cat_var.get().strip(),
                "path": self.task_path_var.get().strip(),
            }
            fbs_text = self.task_fbs_var.get().strip()
            if fbs_text != "":
                payload["fbs"] = int(fbs_text)
            if filter_model:
                payload["filterModel"] = filter_model

            self.tasks[self.current_task_idx] = normalize_task(payload)
        except Exception as exc:
            if not silent:
                messagebox.showerror("Ошибка задачи", str(exc))
            return False

        if refresh_ui:
            self._refresh_task_tree()
            self._select_task(self.current_task_idx)
        return True

    def add_task(self) -> None:
        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        self.tasks.append(_default_task())
        self._refresh_task_tree()
        self._select_task(len(self.tasks) - 1)

    def clone_task(self) -> None:
        if self.current_task_idx is None or self.current_task_idx >= len(self.tasks):
            return
        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        src = dict(self.tasks[self.current_task_idx])
        src["active"] = False
        self.tasks.insert(self.current_task_idx + 1, src)
        self._refresh_task_tree()
        self._select_task(self.current_task_idx + 1)

    def delete_task(self) -> None:
        if self.current_task_idx is None or self.current_task_idx >= len(self.tasks):
            return
        if len(self.tasks) == 1:
            messagebox.showwarning("Удаление задачи", "Должна остаться хотя бы одна задача.")
            return
        del self.tasks[self.current_task_idx]
        self._refresh_task_tree()
        self._select_task(max(0, self.current_task_idx - 1))

    def move_task(self, offset: int) -> None:
        if self.current_task_idx is None or self.current_task_idx >= len(self.tasks):
            return
        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        src = self.current_task_idx
        dst = src + offset
        if dst < 0 or dst >= len(self.tasks):
            return
        self.tasks[src], self.tasks[dst] = self.tasks[dst], self.tasks[src]
        self._refresh_task_tree()
        self._select_task(dst)

    def import_archive_tasks(self) -> None:
        path_str = filedialog.askopenfilename(
            title="Выберите файл архива",
            initialfile=self.archive_path.name,
            initialdir=str(self.archive_path.parent),
            filetypes=[("Markdown", "*.md"), ("Все файлы", "*.*")],
        )
        if not path_str:
            return
        path = Path(path_str)
        try:
            imported = load_tasks_from_archive(path)
        except Exception as exc:
            messagebox.showerror("Ошибка импорта", str(exc))
            return

        if not imported:
            messagebox.showwarning("Импорт", "В архиве не найдено валидных задач.")
            return

        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        existing_keys = {
            json.dumps(
                {
                    "mp": t.get("mp"),
                    "path": t.get("path"),
                    "cat": t.get("cat"),
                    "fbs": t.get("fbs"),
                    "filterModel": t.get("filterModel", {}),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            for t in self.tasks
        }
        added = 0
        for task in imported:
            key = json.dumps(
                {
                    "mp": task.get("mp"),
                    "path": task.get("path"),
                    "cat": task.get("cat"),
                    "fbs": task.get("fbs"),
                    "filterModel": task.get("filterModel", {}),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            if key in existing_keys:
                continue
            existing_keys.add(key)
            self.tasks.append(task)
            added += 1

        self._refresh_task_tree()
        self._select_task(max(0, len(self.tasks) - 1))
        messagebox.showinfo(
            "Импорт завершен", f"Импортировано задач: {len(imported)}\nДобавлено новых: {added}"
        )

    def _load_from_config(self, path: Path) -> None:
        try:
            config = load_step1_config(path)
        except Exception as exc:
            messagebox.showwarning(
                "Предупреждение загрузки конфига",
                f"Не удалось загрузить конфиг {path}.\nБудет создан шаблон после Save.\n\nПричина: {exc}",
            )
            config = {
                "export_months_by_year": {"2025": list(range(1, 13))},
                "save_dir": "",
                "skip_if_exists": True,
                "extract_zip": True,
                "cookie": "",
                "tasks": [_default_task()],
            }
        self.config_path = path
        self._set_config_label()
        self._load_settings(config)

    def open_config(self) -> None:
        path_str = filedialog.askopenfilename(
            title="Открыть конфиг шага 1",
            initialfile=self.config_path.name,
            initialdir=str(self.config_path.parent),
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        if not path_str:
            return
        self._load_from_config(Path(path_str))

    def save_current(self) -> None:
        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        self._save_to(self.config_path)

    def save_as(self) -> None:
        if not self.apply_task_form(silent=True, refresh_ui=False):
            return
        path_str = filedialog.asksaveasfilename(
            title="Сохранить конфиг шага 1",
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        if not path_str:
            return
        self._save_to(Path(path_str))

    def _save_to(self, path: Path) -> None:
        try:
            config_data = self._collect_settings()
            save_config(config_data, path)
        except Exception as exc:
            messagebox.showerror("Ошибка сохранения", str(exc))
            return
        self.config_path = path
        self._set_config_label()
        messagebox.showinfo("Сохранено", f"Конфиг сохранен в:\n{path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 1 MPStats config GUI")
    parser.add_argument("--config", type=str, default=None, help="Path to step1 JSON config")
    parser.add_argument(
        "--archive", type=str, default=None, help="Path to 'справочник tasks архив.md'"
    )
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else default_config_path()
    archive_path = Path(args.archive) if args.archive else archive_tasks_path()

    app = Step1ConfigGui(config_path=config_path, archive_path=archive_path)
    app.mainloop()


if __name__ == "__main__":
    main()
