from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from pipeline.repositories.data_quality_repository import DataQualityRepository, QualityDataSource


MAX_REASONABLE_WEIGHT_KG = 40.0
MAX_REASONABLE_VOLUME_KG = 100_000.0
MIN_SUSPICIOUS_WEIGHT_KG = 0.001
WARNING_SHARE_THRESHOLD = 0.05
EXAMPLE_LIMIT = 8

PRODUCT_NAME_COLUMNS = ("Название", "Название товара", "Наименование", "Product Name", "product_name", "name")
MARKETPLACE_COLUMNS = ("Маркетплейс", "Marketplace", "МП", "marketplace")
CATEGORY_COLUMNS = ("Категория", "Category", "category")
CLASSIFICATION_COLUMNS = (
    "Подкатегория",
    "Тип",
    "Вид",
    "Вид мяса",
    "Сегмент",
    "Класс",
    "Классификация",
    "classification",
    "classification_result",
)
IDENTIFIER_COLUMNS = (
    "Артикул",
    "SKU",
    "sku",
    "ID товара",
    "id товара",
    "product_id",
    "offer_id",
    "nmId",
    "nm_id",
)
WEIGHT_COLUMNS = ("Вес, кг", "Вес кг", "weight_kg", "parsed_weight_kg")
RAW_WEIGHT_COLUMNS = ("Вес, кг сырой", "raw_weight_kg")
VOLUME_COLUMNS = ("Объем, кг", "Объём, кг", "Объем, т", "Объём, т", "Объем, л", "Объём, л", "volume_kg", "volume_l")


@dataclass(frozen=True)
class QualityProblem:
    type: str
    count: int
    share: float
    comment: str


@dataclass(frozen=True)
class SkippedCheck:
    check: str
    reason: str


class DataQualityService:
    def __init__(self, repository: DataQualityRepository) -> None:
        self.repository = repository

    def list_projects(self) -> dict[str, object]:
        return {"projects": self.repository.list_projects()}

    def build_report(self, project_name: str) -> dict[str, object]:
        source = self.repository.resolve_source(project_name)
        try:
            df = self.repository.read_dataframe(source)
        except EmptyDataError:
            return self._fail_report(source, "Файл пустой или не содержит заголовков.")
        except Exception as exc:
            return self._fail_report(source, f"Файл не читается: {exc}")

        total_rows = int(len(df))
        if total_rows == 0:
            return self._fail_report(source, "В итоговом файле 0 строк.", total_rows=0)

        problems: list[QualityProblem] = []
        skipped: list[SkippedCheck] = []
        warnings: list[str] = []

        if source.fallback_used:
            warnings.append("Classified CSV не найден, используется merged CSV.")

        key_metrics = self._empty_key_metrics(df, total_rows, problems, skipped)
        weight_metrics = self._weight_metrics(df, total_rows, problems, skipped)
        anomaly_metrics = self._anomaly_metrics(df, total_rows, problems, skipped)
        classification_metrics = self._classification_metrics(df, total_rows, problems, skipped)
        duplicate_metrics = self._duplicate_metrics(df, total_rows, problems, skipped)

        warning_reasons = self._warning_reasons(
            total_rows=total_rows,
            key_metrics=key_metrics,
            weight_metrics=weight_metrics,
            anomaly_metrics=anomaly_metrics,
            classification_metrics=classification_metrics,
            duplicate_metrics=duplicate_metrics,
            skipped=skipped,
        )
        warnings.extend(warning_reasons)
        status = "WARNING" if warning_reasons else "OK"

        report = {
            "project_name": source.project_name,
            "status": status,
            "status_comment": "Есть замечания к итоговым данным." if status == "WARNING" else "Критичных проблем не найдено.",
            "source": self._source_payload(source),
            "total_rows": total_rows,
            "metrics": {
                "empty_key_fields": key_metrics,
                "weight_volume": weight_metrics,
                "anomalies": anomaly_metrics,
                "classification": classification_metrics,
                "duplicates": duplicate_metrics,
            },
            "problems": [asdict(problem) for problem in problems],
            "skipped_checks": [asdict(item) for item in skipped],
            "examples": {
                "unclassified": self._examples(df, classification_metrics.get("unclassified_mask"), classification_metrics.get("columns", [])),
                "missing_weight_volume": self._examples(df, weight_metrics.get("missing_mask"), weight_metrics.get("columns", [])),
                "anomalies": self._examples(df, anomaly_metrics.get("anomaly_mask"), anomaly_metrics.get("columns", [])),
                "duplicates": self._examples(df, duplicate_metrics.get("duplicate_mask"), duplicate_metrics.get("columns", [])),
            },
            "warnings": warnings,
        }
        self._strip_internal_masks(report)
        report["summary"] = self._summary(report)
        return report

    def _empty_key_metrics(
        self,
        df: pd.DataFrame,
        total_rows: int,
        problems: list[QualityProblem],
        skipped: list[SkippedCheck],
    ) -> dict[str, object]:
        checks: list[tuple[str, list[str], str]] = [
            ("Название товара", _present_columns(df, PRODUCT_NAME_COLUMNS), "Не найдено название товара"),
            ("Маркетплейс", _present_columns(df, MARKETPLACE_COLUMNS), "Не найден маркетплейс"),
            ("Категория", _present_columns(df, CATEGORY_COLUMNS), "Не найдена категория"),
        ]
        classification_columns = _present_columns(df, CLASSIFICATION_COLUMNS)
        checks.extend((f"Классификация: {column}", [column], f"Пустая классификация: {column}") for column in classification_columns)

        row_with_empty = pd.Series(False, index=df.index)
        fields: list[dict[str, object]] = []
        for label, columns, comment in checks:
            if not columns:
                skipped.append(SkippedCheck(check=label, reason="Не проверялось: колонка не найдена."))
                continue
            column = columns[0]
            mask = _empty_mask(df[column])
            count = int(mask.sum())
            row_with_empty |= mask
            fields.append({"field": label, "column": column, "empty_count": count, "share": _share(count, total_rows)})
            if count:
                problems.append(QualityProblem(type=comment, count=count, share=_share(count, total_rows), comment=f"Колонка: {column}"))

        total_empty = int(row_with_empty.sum())
        return {
            "rows_with_empty": total_empty,
            "share": _share(total_empty, total_rows),
            "fields": fields,
        }

    def _weight_metrics(
        self,
        df: pd.DataFrame,
        total_rows: int,
        problems: list[QualityProblem],
        skipped: list[SkippedCheck],
    ) -> dict[str, object]:
        columns = _present_columns(df, WEIGHT_COLUMNS + VOLUME_COLUMNS)
        if not columns:
            skipped.append(SkippedCheck(check="Вес/объём", reason="Не проверялось: не найдены колонки веса или объёма."))
            return {"columns": [], "parsed_count": 0, "missing_count": 0, "coverage_share": 0.0, "missing_share": 0.0}

        parsed_mask = pd.Series(False, index=df.index)
        for column in columns:
            numeric = _numeric_series(df[column])
            parsed_mask |= numeric.gt(0).fillna(False)

        parsed_count = int(parsed_mask.sum())
        missing_mask = ~parsed_mask
        missing_count = int(missing_mask.sum())
        if missing_count:
            problems.append(
                QualityProblem(
                    type="Не найден вес/объём",
                    count=missing_count,
                    share=_share(missing_count, total_rows),
                    comment="Нет положительного значения в колонках веса или объёма.",
                )
            )
        return {
            "columns": columns,
            "parsed_count": parsed_count,
            "missing_count": missing_count,
            "coverage_share": _share(parsed_count, total_rows),
            "missing_share": _share(missing_count, total_rows),
            "missing_mask": missing_mask,
        }

    def _anomaly_metrics(
        self,
        df: pd.DataFrame,
        total_rows: int,
        problems: list[QualityProblem],
        skipped: list[SkippedCheck],
    ) -> dict[str, object]:
        columns = _present_columns(df, WEIGHT_COLUMNS + RAW_WEIGHT_COLUMNS + VOLUME_COLUMNS)
        if not columns:
            skipped.append(SkippedCheck(check="Аномалии веса/объёма", reason="Не проверялось: не найдены числовые колонки веса или объёма."))
            return {"columns": [], "count": 0, "zero_or_negative": 0, "too_large": 0, "suspicious": 0}

        zero_or_negative = pd.Series(False, index=df.index)
        too_large = pd.Series(False, index=df.index)
        suspicious = pd.Series(False, index=df.index)
        for column in columns:
            numeric = _numeric_series(df[column])
            present = numeric.notna()
            zero_or_negative |= (present & numeric.lt(0)).fillna(False)
            if _normalized(column) in {_normalized(item) for item in VOLUME_COLUMNS}:
                too_large |= numeric.gt(MAX_REASONABLE_VOLUME_KG).fillna(False)
            else:
                too_large |= numeric.gt(MAX_REASONABLE_WEIGHT_KG).fillna(False)
                suspicious |= (numeric.gt(0) & numeric.lt(MIN_SUSPICIOUS_WEIGHT_KG)).fillna(False)

        reason_column = _first_present_column(df, ("Вес причина", "weight_reason"))
        if reason_column:
            reason = df[reason_column].astype("string").fillna("").str.strip().str.lower()
            suspicious |= (~reason.isin({"", "ok", "empty_or_bad"}) & reason.notna()).fillna(False)

        anomaly_mask = zero_or_negative | too_large | suspicious
        zero_count = int(zero_or_negative.sum())
        large_count = int(too_large.sum())
        suspicious_count = int(suspicious.sum())
        total_anomalies = int(anomaly_mask.sum())

        if zero_count:
            problems.append(QualityProblem(type="Отрицательный вес/объём", count=zero_count, share=_share(zero_count, total_rows), comment="Нулевой вес не считается аномалией, отрицательное значение нужно проверить вручную."))
        if large_count:
            problems.append(QualityProblem(type="Слишком большой вес/объём", count=large_count, share=_share(large_count, total_rows), comment="Проверь единицы измерения и десятичные разделители."))
        if suspicious_count:
            problems.append(QualityProblem(type="Подозрительный вес/объём", count=suspicious_count, share=_share(suspicious_count, total_rows), comment="Значение выглядит необычно или отмечено парсером."))

        return {
            "columns": columns,
            "count": total_anomalies,
            "zero_or_negative": zero_count,
            "too_large": large_count,
            "suspicious": suspicious_count,
            "anomaly_mask": anomaly_mask,
        }

    def _classification_metrics(
        self,
        df: pd.DataFrame,
        total_rows: int,
        problems: list[QualityProblem],
        skipped: list[SkippedCheck],
    ) -> dict[str, object]:
        columns = _present_columns(df, CLASSIFICATION_COLUMNS)
        if not columns:
            skipped.append(SkippedCheck(check="Классификация", reason="Не проверялось: не найдены классификационные колонки."))
            return {"columns": [], "classified_count": 0, "unclassified_count": 0, "coverage_share": 0.0, "unclassified_share": 0.0}

        classified_mask = pd.Series(False, index=df.index)
        for column in columns:
            classified_mask |= ~_empty_mask(df[column])
        classified_count = int(classified_mask.sum())
        unclassified_mask = ~classified_mask
        unclassified_count = int(unclassified_mask.sum())
        if unclassified_count:
            problems.append(
                QualityProblem(
                    type="Не классифицировано",
                    count=unclassified_count,
                    share=_share(unclassified_count, total_rows),
                    comment=f"Пусто во всех колонках: {', '.join(columns)}",
                )
            )
        return {
            "columns": columns,
            "classified_count": classified_count,
            "unclassified_count": unclassified_count,
            "coverage_share": _share(classified_count, total_rows),
            "unclassified_share": _share(unclassified_count, total_rows),
            "unclassified_mask": unclassified_mask,
        }

    def _duplicate_metrics(
        self,
        df: pd.DataFrame,
        total_rows: int,
        problems: list[QualityProblem],
        skipped: list[SkippedCheck],
    ) -> dict[str, object]:
        columns = [str(column) for column in df.columns if not str(column).startswith("__quality_")]
        if not columns:
            skipped.append(SkippedCheck(check="Дубли строк", reason="Не проверялось: нет пользовательских колонок."))
            return {"checked": False, "identifier_column": None, "columns": [], "duplicate_rows": 0, "duplicate_keys": 0, "share": 0.0}

        normalized = df[columns].copy()
        for column in columns:
            normalized[column] = normalized[column].astype("string").fillna("").str.strip()
        non_empty = normalized.ne("").any(axis=1)
        duplicate_mask = normalized.duplicated(keep=False) & non_empty
        duplicate_rows = int(duplicate_mask.sum())
        duplicate_keys = int(normalized.loc[duplicate_mask, columns].drop_duplicates().shape[0]) if duplicate_rows else 0
        if duplicate_rows:
            problems.append(
                QualityProblem(
                    type="Полные дубли строк",
                    count=duplicate_rows,
                    share=_share(duplicate_rows, total_rows),
                    comment=f"Строка полностью повторяется по {len(columns)} пользовательским колонкам. Групп дублей: {duplicate_keys}.",
                )
            )
        return {
            "checked": True,
            "identifier_column": None,
            "columns": columns,
            "duplicate_rows": duplicate_rows,
            "duplicate_keys": duplicate_keys,
            "share": _share(duplicate_rows, total_rows),
            "duplicate_mask": duplicate_mask,
        }

    def _warning_reasons(
        self,
        *,
        total_rows: int,
        key_metrics: dict[str, object],
        weight_metrics: dict[str, object],
        anomaly_metrics: dict[str, object],
        classification_metrics: dict[str, object],
        duplicate_metrics: dict[str, object],
        skipped: list[SkippedCheck],
    ) -> list[str]:
        reasons: list[str] = []
        if float(key_metrics.get("share") or 0) >= WARNING_SHARE_THRESHOLD:
            reasons.append("Есть существенные пустые значения в ключевых полях.")
        if float(weight_metrics.get("missing_share") or 0) >= WARNING_SHARE_THRESHOLD:
            reasons.append("У заметной части строк не найден вес/объём.")
        if int(anomaly_metrics.get("count") or 0) > 0:
            reasons.append("Найдены аномалии веса/объёма.")
        if float(classification_metrics.get("unclassified_share") or 0) >= WARNING_SHARE_THRESHOLD:
            reasons.append("Есть неклассифицированные строки.")
        if int(duplicate_metrics.get("duplicate_rows") or 0) > 0:
            reasons.append("Найдены полностью одинаковые строки.")
        return reasons

    def _fail_report(self, source: QualityDataSource, reason: str, *, total_rows: int = 0) -> dict[str, object]:
        report = {
            "project_name": source.project_name,
            "status": "FAIL",
            "status_comment": reason,
            "source": self._source_payload(source),
            "total_rows": total_rows,
            "metrics": {
                "empty_key_fields": {"rows_with_empty": 0, "share": 0.0, "fields": []},
                "weight_volume": {"columns": [], "parsed_count": 0, "missing_count": 0, "coverage_share": 0.0, "missing_share": 0.0},
                "anomalies": {"columns": [], "count": 0, "zero_or_negative": 0, "too_large": 0, "suspicious": 0},
                "classification": {"columns": [], "classified_count": 0, "unclassified_count": 0, "coverage_share": 0.0, "unclassified_share": 0.0},
                "duplicates": {"checked": False, "identifier_column": None, "columns": [], "duplicate_rows": 0, "duplicate_keys": 0, "share": 0.0},
            },
            "problems": [asdict(QualityProblem(type="Файл не проверен", count=0, share=0.0, comment=reason))],
            "skipped_checks": [],
            "examples": {"unclassified": [], "missing_weight_volume": [], "anomalies": [], "duplicates": []},
            "warnings": [reason],
        }
        report["summary"] = self._summary(report)
        return report

    @staticmethod
    def _source_payload(source: QualityDataSource) -> dict[str, object]:
        primary = source.primary_path
        return {
            "kind": source.source_kind,
            "scope": source.source_scope,
            "path": str(primary) if primary else "",
            "paths": [str(path) for path in source.paths[:10]],
            "file_count": source.file_count,
            "fallback_used": source.fallback_used,
        }

    @staticmethod
    def _examples(df: pd.DataFrame, mask: object, extra_columns: object) -> list[dict[str, object]]:
        if not isinstance(mask, pd.Series) or not bool(mask.any()):
            return []
        base_columns = [
            _first_present_column(df, IDENTIFIER_COLUMNS),
            _first_present_column(df, PRODUCT_NAME_COLUMNS),
            _first_present_column(df, MARKETPLACE_COLUMNS),
            _first_present_column(df, CATEGORY_COLUMNS),
        ]
        columns = [column for column in base_columns if column]
        for column in extra_columns if isinstance(extra_columns, list) else []:
            if isinstance(column, str) and column and column in df.columns and column not in columns:
                columns.append(column)
        source_column = "__quality_source_file"
        if source_column in df.columns and source_column not in columns:
            columns.append(source_column)
        if not columns:
            columns = list(df.columns[:5])
        records = df.loc[mask, columns].head(EXAMPLE_LIMIT).to_dict(orient="records")
        return [_clean_record(record) for record in records]

    @staticmethod
    def _strip_internal_masks(value: object) -> None:
        if isinstance(value, dict):
            for key in list(value.keys()):
                if key.endswith("_mask"):
                    value.pop(key)
                else:
                    DataQualityService._strip_internal_masks(value[key])
        elif isinstance(value, list):
            for item in value:
                DataQualityService._strip_internal_masks(item)

    @staticmethod
    def _summary(report: dict[str, object]) -> str:
        metrics = report.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
        weight = metrics.get("weight_volume", {}) if isinstance(metrics.get("weight_volume"), dict) else {}
        classification = metrics.get("classification", {}) if isinstance(metrics.get("classification"), dict) else {}
        anomalies = metrics.get("anomalies", {}) if isinstance(metrics.get("anomalies"), dict) else {}
        duplicates = metrics.get("duplicates", {}) if isinstance(metrics.get("duplicates"), dict) else {}
        return (
            f"{report.get('project_name')}: {report.get('status')}. "
            f"Строк: {report.get('total_rows')}. "
            f"Вес/объём найден: {weight.get('parsed_count', 0)}. "
            f"Классифицировано: {classification.get('classified_count', 0)}. "
            f"Аномалий: {anomalies.get('count', 0)}. "
            f"Полных дублей строк: {duplicates.get('duplicate_rows', 0)}."
        )


def _present_columns(df: pd.DataFrame, candidates: tuple[str, ...]) -> list[str]:
    lookup = {_normalized(column): str(column) for column in df.columns}
    out: list[str] = []
    for candidate in candidates:
        column = lookup.get(_normalized(candidate))
        if column and column not in out:
            out.append(column)
    return out


def _first_present_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    columns = _present_columns(df, candidates)
    return columns[0] if columns else None


def _empty_mask(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip().str.lower()
    return series.isna() | text.isin({"", "nan", "none", "null", "<na>", "-", "—"})


def _numeric_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(text, errors="coerce")


def _share(count: int, total: int) -> float:
    return round(float(count) / float(total), 4) if total else 0.0


def _normalized(value: object) -> str:
    text = str(value).strip().lower().replace("ё", "е")
    return "".join(char for char in text if char.isalnum())


def _clean_record(record: dict[str, Any]) -> dict[str, object]:
    return {str(key): _clean_value(value) for key, value in record.items()}


def _clean_value(value: Any) -> object:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, Path):
        return str(value)
    return value
