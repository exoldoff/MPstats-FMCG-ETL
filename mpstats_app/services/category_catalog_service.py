from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.repositories.file_repository import detect_encoding_and_sep

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository


MP_ALIASES = {
    "озон": ("Ozon", "oz"),
    "ozon": ("Ozon", "oz"),
    "oz": ("Ozon", "oz"),
    "wb": ("WB", "wb"),
    "wildberries": ("WB", "wb"),
    "вб": ("WB", "wb"),
    "ям": ("Яндекс.Маркет", "ym"),
    "яндекс": ("Яндекс.Маркет", "ym"),
    "яндекс маркет": ("Яндекс.Маркет", "ym"),
    "яндекс.маркет": ("Яндекс.Маркет", "ym"),
    "ym": ("Яндекс.Маркет", "ym"),
}

SOURCE_TYPE_CATEGORY = "category"
SOURCE_TYPE_SUBJECT = "subject"

SOURCE_COLUMNS = [
    "Чек",
    "Категория",
    "МП",
    "FBS",
    "Тип выгрузки",
    "От",
    "До",
    "Комментарий",
    "Путь",
    "Фильтр",
    "Путь2",
    "Фильтр2",
    "Актуализация",
]

MONTH_ALIASES = {
    "янв": 1,
    "январь": 1,
    "фев": 2,
    "февраль": 2,
    "мар": 3,
    "март": 3,
    "апр": 4,
    "апрель": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июнь": 6,
    "июл": 7,
    "июль": 7,
    "авг": 8,
    "август": 8,
    "сен": 9,
    "сент": 9,
    "сентябрь": 9,
    "окт": 10,
    "октябрь": 10,
    "ноя": 11,
    "ноябрь": 11,
    "дек": 12,
    "декабрь": 12,
}

NO_PERIOD_VALUES = {"", "нд", "n/a", "na", "-", "нет", "не указано"}


class CategoryCatalogService:
    def __init__(self, *, settings: AppSettings, repository: DuckDbAppRepository) -> None:
        self.settings = settings
        self.repository = repository
        self._last_source_state: tuple[str, str] | None = None

    def ensure_seeded(self) -> dict[str, Any]:
        source = self.find_source()
        if source is not None:
            source_state = self._source_state(source)
            if self._last_source_state != source_state or not self.repository.list_categories(active_only=False):
                return self.import_from_file(source)
            return {"imported": 0, "source": str(source)}
        if self.repository.list_categories(active_only=False):
            return {"imported": 0, "source": None}
        return {"imported": 0, "source": None}

    def find_source(self) -> Path | None:
        candidates = sorted(self.settings.project_root.glob("Справочник категори*MP STATS.csv"))
        if not candidates:
            return None
        return max(candidates, key=self._source_score)

    def import_from_file(self, source: str | Path) -> dict[str, Any]:
        path = Path(source)
        df = self._read_source(path)
        categories = self._categories_from_frame(df, path)
        self.repository.replace_categories(categories, source_file=str(path))
        self._last_source_state = self._source_state(path)
        imported = sum(1 for category in categories if category.get("is_active", True))
        return {"imported": imported, "source": str(path)}

    def list_categories(self) -> list[dict[str, Any]]:
        self.ensure_seeded()
        return self.repository.list_categories()

    def list_source_rows(self) -> dict[str, Any]:
        source = self.find_source()
        if source is None:
            source = self.settings.project_root / "Справочник категорий MP STATS.csv"
        if not source.exists():
            return {"path": str(source), "rows": []}
        frame = self._read_source(source)
        return {"path": str(source), "rows": self._source_rows_from_frame(frame)}

    def save_source_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        source = self.find_source() or (self.settings.project_root / "Справочник категорий MP STATS.csv")
        normalized_rows = [self._normalize_source_row(row, index) for index, row in enumerate(rows, start=1)]
        frame = pd.DataFrame(normalized_rows, columns=SOURCE_COLUMNS)
        source.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(source, sep=";", index=False, encoding="utf-8-sig")
        imported = self.import_from_file(source)
        return {"path": str(source), "rows": self._source_rows_from_frame(frame), "imported": imported["imported"]}

    def selected_tasks(self, category_ids: list[str]) -> list[dict[str, Any]]:
        categories = self.repository.get_categories_by_ids(category_ids)
        tasks: list[dict[str, Any]] = []
        for category in categories:
            task: dict[str, Any] = {
                "mp": category["mp_code"],
                "path": category["path"],
                "cat": category["category_name"],
                "source_type": category.get("source_type") or SOURCE_TYPE_CATEGORY,
            }
            if self._category_uses_fbs(category):
                task["fbs"] = 1
            if category.get("filter_json"):
                task["filterModel"] = json.loads(str(category["filter_json"]))
            tasks.append(task)
        return tasks

    def _read_source(self, path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".csv":
            enc, sep = detect_encoding_and_sep(path)
            return pd.read_csv(path, sep=sep, encoding=enc)
        raise ValueError(f"Справочник категорий должен быть CSV: {path}")

    def _categories_from_frame(self, df: pd.DataFrame, source: Path) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            name = self._cell(row, "Категория")
            marketplace_raw = self._cell(row, "МП")
            if not name or not marketplace_raw:
                continue
            marketplace, mp_code = self._normalize_marketplace(marketplace_raw)
            is_active = self._is_active(self._cell(row, "Чек"))
            fbs = self._row_uses_fbs(row, mp_code)
            source_type = self._normalize_source_type(self._cell(row, "Тип выгрузки"), mp_code=mp_code)
            period_from = self._parse_period_month(self._cell(row, "От"), boundary="from")
            period_to = self._parse_period_month(self._cell(row, "До"), boundary="to")
            if period_from and period_to and period_from > period_to:
                raise ValueError(f"Период справочника некорректен: {period_from} больше {period_to}.")
            for path_column, filter_column in (("Путь", "Фильтр"), ("Путь2", "Фильтр2")):
                category_path = self._cell(row, path_column)
                if not category_path or category_path.strip().lower() in {"нд", "n/a", "na", "-"}:
                    continue
                filter_json = self._parse_filter(self._cell(row, filter_column))
                category_id = self._category_id(name, mp_code, category_path, filter_json, period_from, period_to, fbs, source_type)
                out.append(
                    {
                        "category_id": category_id,
                        "category_name": name,
                        "marketplace": marketplace,
                        "mp_code": mp_code,
                        "source_type": source_type,
                        "path": category_path,
                        "filter_json": filter_json,
                        "fbs": fbs,
                        "period_from": period_from,
                        "period_to": period_to,
                        "source_file": str(source),
                        "is_active": is_active,
                    }
                )
        return out

    def _source_rows_from_frame(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        source_has_fbs = "FBS" in df.columns
        normalized = df.fillna("").copy()
        for column in SOURCE_COLUMNS:
            if column not in normalized.columns:
                normalized[column] = ""
        rows: list[dict[str, Any]] = []
        for index, row in normalized.iterrows():
            rows.append(
                {
                    "id": f"category-row-{index + 1}",
                    "active": self._is_active(self._cell(row, "Чек")),
                    "category_name": self._cell(row, "Категория"),
                    "marketplace": self._cell(row, "МП"),
                    "fbs": self._source_row_uses_fbs(row, source_has_fbs),
                    "source_type": self._normalize_source_type(self._cell(row, "Тип выгрузки"), raw_marketplace=self._cell(row, "МП")),
                    "period_from": self._cell(row, "От"),
                    "period_to": self._cell(row, "До"),
                    "comment": self._cell(row, "Комментарий"),
                    "path": self._cell(row, "Путь"),
                    "filter_text": self._cell(row, "Фильтр"),
                    "path2": self._cell(row, "Путь2"),
                    "filter2_text": self._cell(row, "Фильтр2"),
                    "actualization": self._cell(row, "Актуализация"),
                }
            )
        return rows

    def _normalize_source_row(self, row: dict[str, Any], index: int) -> dict[str, str]:
        category_name = str(row.get("category_name") or "").strip()
        marketplace = str(row.get("marketplace") or "").strip()
        if category_name or marketplace or str(row.get("path") or "").strip() or str(row.get("path2") or "").strip():
            if not category_name:
                raise ValueError(f"Строка справочника {index}: категория не заполнена.")
            if not marketplace:
                raise ValueError(f"Строка справочника {index}: маркетплейс не заполнен.")
            _, mp_code = self._normalize_marketplace(marketplace)
        else:
            mp_code = ""
        fbs = False if mp_code == "ym" else True if row.get("fbs") is None else bool(row.get("fbs", False))
        source_type = self._normalize_source_type(row.get("source_type"), mp_code=mp_code) if mp_code else SOURCE_TYPE_CATEGORY
        return {
            "Чек": "1" if bool(row.get("active", True)) else "0",
            "Категория": category_name,
            "МП": marketplace,
            "FBS": "1" if fbs else "",
            "Тип выгрузки": "Предмет" if source_type == SOURCE_TYPE_SUBJECT else "Категория",
            "От": str(row.get("period_from") or "").strip(),
            "До": str(row.get("period_to") or "").strip(),
            "Комментарий": str(row.get("comment") or "").strip(),
            "Путь": str(row.get("path") or "").strip(),
            "Фильтр": str(row.get("filter_text") or "").strip(),
            "Путь2": str(row.get("path2") or "").strip(),
            "Фильтр2": str(row.get("filter2_text") or "").strip(),
            "Актуализация": str(row.get("actualization") or "").strip(),
        }

    @staticmethod
    def _cell(row: pd.Series, column: str) -> str:
        if column not in row:
            return ""
        value = row[column]
        if pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_marketplace(raw: str) -> tuple[str, str]:
        key = raw.strip().lower()
        if key not in MP_ALIASES:
            raise ValueError(f"Неизвестный маркетплейс в справочнике: {raw!r}")
        return MP_ALIASES[key]

    @staticmethod
    def _is_active(raw: str) -> bool:
        text = raw.strip().lower()
        return text not in {"0", "false", "no", "n", "off", "нет", "неактивна", "skip"}

    @staticmethod
    def _is_truthy(raw: str) -> bool:
        return raw.strip().lower() in {"1", "1.0", "true", "yes", "y", "on", "да", "д", "истина"}

    def _row_uses_fbs(self, row: pd.Series, mp_code: str) -> bool:
        if mp_code == "ym":
            return False
        if "FBS" not in row:
            return True
        return self._is_truthy(self._cell(row, "FBS"))

    def _source_row_uses_fbs(self, row: pd.Series, source_has_fbs: bool) -> bool:
        try:
            _, mp_code = self._normalize_marketplace(self._cell(row, "МП"))
        except ValueError:
            return False
        if mp_code == "ym":
            return False
        if source_has_fbs:
            return self._is_truthy(self._cell(row, "FBS"))
        return True

    @staticmethod
    def _normalize_source_type(
        raw: Any,
        *,
        mp_code: str | None = None,
        raw_marketplace: str | None = None,
    ) -> str:
        resolved_mp = mp_code
        if resolved_mp is None and raw_marketplace:
            try:
                _, resolved_mp = CategoryCatalogService._normalize_marketplace(str(raw_marketplace))
            except ValueError:
                resolved_mp = None
        text = str(raw or "").strip().lower()
        source_type = SOURCE_TYPE_SUBJECT if text in {"subject", "предмет", "по предмету"} else SOURCE_TYPE_CATEGORY
        if resolved_mp == "ym":
            return SOURCE_TYPE_CATEGORY
        return source_type

    @staticmethod
    def _category_uses_fbs(category: dict[str, Any]) -> bool:
        if str(category.get("mp_code") or "") == "ym":
            return False
        if category.get("fbs") is None:
            return True
        return bool(category.get("fbs"))

    def _source_score(self, path: Path) -> tuple[int, int, float]:
        try:
            frame = self._read_source(path).fillna("")
        except Exception:
            return (0, 0, path.stat().st_mtime if path.exists() else 0.0)
        path_count = 0
        for column in ("Путь", "Путь2"):
            if column in frame.columns:
                path_count += int(frame[column].astype(str).str.strip().ne("").sum())
        return (path_count, len(frame), path.stat().st_mtime)

    @staticmethod
    def _source_state(path: Path) -> tuple[str, str]:
        return (str(path.resolve()), hashlib.sha256(path.read_bytes()).hexdigest())

    @staticmethod
    def _parse_filter(raw: str) -> str | None:
        if not raw:
            return None
        text = raw.strip()
        parsed: Any
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                parsed = text
        if isinstance(parsed, dict):
            return json.dumps(parsed, ensure_ascii=False, sort_keys=True)
        if isinstance(parsed, str):
            parsed_filter = CategoryCatalogService._parse_filter_text(parsed)
            return json.dumps(parsed_filter, ensure_ascii=False, sort_keys=True) if parsed_filter else None
        raise ValueError("Фильтр должен быть JSON object или строкой поиска")

    @staticmethod
    def _parse_period_month(raw: str, *, boundary: str) -> str | None:
        text = raw.strip().lower().replace("ё", "е")
        if text in NO_PERIOD_VALUES:
            return None

        year_match = re.fullmatch(r"(20\d{2}|\d{2})", text)
        if year_match:
            year = CategoryCatalogService._normalize_year(year_match.group(1))
            month = 1 if boundary == "from" else 12
            return f"{year:04d}-{month:02d}"

        iso_match = re.fullmatch(r"(20\d{2})[-/.](0?[1-9]|1[0-2])", text)
        if iso_match:
            return f"{int(iso_match.group(1)):04d}-{int(iso_match.group(2)):02d}"

        dot_match = re.fullmatch(r"(0?[1-9]|1[0-2])[-/.](20\d{2}|\d{2})", text)
        if dot_match:
            year = CategoryCatalogService._normalize_year(dot_match.group(2))
            return f"{year:04d}-{int(dot_match.group(1)):02d}"

        month_match = re.fullmatch(r"([а-я]+)\.?\s*(20\d{2}|\d{2})", text)
        if month_match:
            month_name = month_match.group(1)
            if month_name not in MONTH_ALIASES:
                raise ValueError(f"Неизвестный месяц в справочнике: {raw!r}")
            year = CategoryCatalogService._normalize_year(month_match.group(2))
            return f"{year:04d}-{MONTH_ALIASES[month_name]:02d}"

        raise ValueError(f"Не удалось распознать период справочника: {raw!r}")

    @staticmethod
    def _normalize_year(raw: str) -> int:
        year = int(raw)
        if year < 100:
            year += 2000
        return year

    @staticmethod
    def _parse_filter_text(text: str) -> dict[str, Any] | None:
        tokens = CategoryCatalogService._filter_tokens(text)
        if not tokens:
            return None
        if len(tokens) > 2:
            raise ValueError("Фильтр MPStats поддерживает не больше двух условий.")
        operators = {token["operator"] for token in tokens[1:] if token.get("operator")}
        if len(operators) > 1:
            raise ValueError("В одном фильтре нельзя смешивать И и ИЛИ.")

        def condition(token: dict[str, str]) -> dict[str, str]:
            return {
                "filterType": "text",
                "type": "notContains" if token.get("negate") == "1" else "contains",
                "filter": token["value"],
            }

        if len(tokens) == 1:
            return {"name": condition(tokens[0])}
        return {
            "name": {
                "filterType": "text",
                "operator": next(iter(operators), "AND"),
                "condition1": condition(tokens[0]),
                "condition2": condition(tokens[1]),
            }
        }

    @staticmethod
    def _filter_tokens(text: str) -> list[dict[str, str]]:
        stripped = text.strip().strip('"').strip("'").strip()
        if not stripped:
            return []

        token_pattern = re.compile(r"(?P<separator>[|&])?\s*(?P<negate>NOT)?\s*(?P<quote>[\"'])(?P<value>.*?)(?P=quote)", re.IGNORECASE)
        matches = list(token_pattern.finditer(text.strip()))
        if matches:
            tokens: list[dict[str, str]] = []
            for match in matches:
                value = match.group("value").strip()
                if not value:
                    continue
                separator = match.group("separator")
                tokens.append(
                    {
                        "operator": "OR" if separator == "|" else "AND" if separator == "&" else "",
                        "negate": "1" if match.group("negate") else "",
                        "value": value,
                    }
                )
            return tokens

        separator = "|" if "|" in stripped else "&" if "&" in stripped else ""
        parts = stripped.split(separator) if separator else [stripped]
        tokens = []
        for index, part in enumerate(parts):
            value = part.strip().strip('"').strip("'").strip()
            negate = False
            if value.upper().startswith("NOT"):
                negate = True
                value = value[3:].strip().strip('"').strip("'").strip()
            if not value:
                continue
            tokens.append(
                {
                    "operator": "OR" if separator == "|" and index > 0 else "AND" if separator == "&" and index > 0 else "",
                    "negate": "1" if negate else "",
                    "value": value,
                }
            )
        return tokens

    @staticmethod
    def _category_id(
        name: str,
        mp_code: str,
        path: str,
        filter_json: str | None,
        period_from: str | None,
        period_to: str | None,
        fbs: bool,
        source_type: str = SOURCE_TYPE_CATEGORY,
    ) -> str:
        payload: dict[str, Any] = {
            "name": name,
            "mp": mp_code,
            "path": path,
            "filter": filter_json or "",
            "period_from": period_from or "",
            "period_to": period_to or "",
            "fbs": fbs,
        }
        if source_type == SOURCE_TYPE_SUBJECT:
            payload["source_type"] = source_type
        digest = hashlib.sha1(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return digest[:16]
