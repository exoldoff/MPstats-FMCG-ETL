from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

from pandas.errors import EmptyDataError

from pipeline.data_quality.checks import (
    run_consistency_checks,
    run_duplicate_checks,
    run_market_share_checks,
    run_period_checks,
    run_price_checks,
    run_revenue_checks,
    run_sales_checks,
    run_sku_lifecycle_checks,
)
from pipeline.data_quality.config import DataQualityConfig
from pipeline.data_quality.models import QualityContext, QualityIssue, SkippedCheck, issues_to_payload
from pipeline.data_quality.sql import prepare_quality_tables, prepare_quality_tables_from_cube
from pipeline.repositories.data_quality_repository import QualityDataSource
from pipeline.repositories.sql_repository import duckdb_connection


CheckFn = Callable[[QualityContext], list[QualityIssue]]


CHECKS: tuple[CheckFn, ...] = (
    run_sales_checks,
    run_revenue_checks,
    run_price_checks,
    run_market_share_checks,
    run_duplicate_checks,
    run_period_checks,
    run_consistency_checks,
    run_sku_lifecycle_checks,
)


class DataQualityRunner:
    def __init__(self, *, config: DataQualityConfig | None = None, temp_directory: str | Path | None = None) -> None:
        self.config = config or DataQualityConfig()
        self.temp_directory = Path(temp_directory) if temp_directory else None

    def run(self, source: QualityDataSource) -> dict[str, Any]:
        if not source.paths:
            return self._fail_report(source, "Источник качества для проекта не найден.")

        try:
            db_path = source.primary_path if source.source_kind == "cube" else None
            connection_path = db_path if db_path else ":memory:"
            with duckdb_connection(connection_path, temp_directory=self.temp_directory) as con:
                if source.source_kind == "cube":
                    if not source.table_name or not db_path:
                        return self._fail_report(source, "Источник куба не настроен.")
                    prepared = prepare_quality_tables_from_cube(
                        con,
                        table_name=source.table_name,
                        project_name=source.project_name,
                        db_path=db_path,
                    )
                else:
                    prepared = prepare_quality_tables(con, source.paths)
                return self._run_checks(source, prepared, con)
        except EmptyDataError:
            return self._fail_report(source, "Источник пустой или не содержит заголовков.")
        except Exception as exc:
            return self._fail_report(source, f"Источник не читается или не проверяется: {exc}")

    def _run_checks(self, source: QualityDataSource, prepared: Any, con: Any) -> dict[str, Any]:
        if prepared.total_rows == 0:
            return self._fail_report(source, "В источнике данных 0 строк.", total_rows=0)

        skipped: list[SkippedCheck] = []
        ctx = QualityContext(con=con, config=self.config, prepared=prepared, skipped_checks=skipped)
        issues: list[QualityIssue] = []
        for check in CHECKS:
            issues.extend(check(ctx))
        return self._report(source=source, total_rows=prepared.total_rows, issues=issues, skipped=skipped, columns=prepared.columns)

    def _report(
        self,
        *,
        source: QualityDataSource,
        total_rows: int,
        issues: list[QualityIssue],
        skipped: list[SkippedCheck],
        columns: dict[str, str | None],
    ) -> dict[str, Any]:
        payload_issues = issues_to_payload(issues)
        severity = Counter(issue.severity for issue in issues)
        has_problem = severity["CRITICAL"] > 0 or severity["WARNING"] > 0
        status = "WARNING" if has_problem else "OK"
        warning_messages: list[str] = []
        if source.fallback_used:
            warning_messages.append("Куб DuckDB не найден, используется legacy CSV fallback.")
        if severity["CRITICAL"]:
            warning_messages.append(f"Найдены CRITICAL-предупреждения: {severity['CRITICAL']}.")
        if severity["WARNING"]:
            warning_messages.append(f"Найдены WARNING-предупреждения: {severity['WARNING']}.")

        metrics = {
            "summary_by_severity": {
                "CRITICAL": severity["CRITICAL"],
                "WARNING": severity["WARNING"],
                "INFO": severity["INFO"],
                "total": len(issues),
            },
            "summary_by_category": _summary_by(payload_issues, "category"),
            "summary_by_period": _summary_by(payload_issues, "period"),
            "summary_by_check": _summary_by(payload_issues, "check_id"),
            "top_suspicious_skus": _top_skus(payload_issues),
            "top_problem_categories": _top_categories(payload_issues),
            "detected_columns": {key: value for key, value in columns.items() if value},
            "checks": {
                "sales_anomalies": _count_checks(payload_issues, {"sku_sales_spike", "sku_sales_drop", "new_sku_high_sales"}),
                "revenue_anomalies": _count_checks(payload_issues, {"category_revenue_spike", "category_revenue_drop", "brand_revenue_share_spike"}),
                "price_anomalies": _count_checks(payload_issues, {"zero_or_negative_price", "sku_price_change", "unit_price_category_outlier"}),
                "market_share_anomalies": _count_checks(payload_issues, {"sku_category_share_spike", "top_sku_dominance"}),
                "duplicates": _count_checks(payload_issues, {"duplicate_sku_period_count", "duplicate_metric_rows"}),
                "period_completeness": _count_checks(payload_issues, {"missing_period", "category_period_row_spike", "category_period_row_drop", "period_mostly_zero_metrics"}),
                "metric_consistency": _count_checks(payload_issues, {"revenue_price_sales_mismatch", "sales_with_zero_price", "sales_price_with_zero_revenue", "negative_sales", "negative_revenue", "negative_stock", "negative_acb"}),
                "sku_lifecycle": _count_checks(payload_issues, {"sku_sales_drop_to_zero", "sku_returned_after_gap", "new_sku_high_sales"}),
            },
            # Старые ключи оставлены пустыми, чтобы внешний код не падал на переходе.
            "empty_key_fields": {"rows_with_empty": 0, "share": 0.0, "fields": []},
            "weight_volume": {"columns": [], "parsed_count": 0, "missing_count": 0, "coverage_share": 0.0, "missing_share": 0.0},
            "anomalies": {"columns": [], "count": len(issues), "zero_or_negative": 0, "too_large": 0, "suspicious": len(issues)},
            "classification": {"columns": [], "classified_count": 0, "unclassified_count": 0, "coverage_share": 0.0, "unclassified_share": 0.0},
            "duplicates": {"checked": True, "identifier_column": "SKU", "columns": [], "duplicate_rows": _count_checks(payload_issues, {"duplicate_sku_period_count", "duplicate_metric_rows"}), "duplicate_keys": 0, "share": 0.0},
        }

        report = {
            "project_name": source.project_name,
            "status": status,
            "status_comment": _status_comment(status, severity),
            "source": self._source_payload(source),
            "total_rows": total_rows,
            "metrics": metrics,
            "issues": payload_issues,
            "critical_issues": [item for item in payload_issues if item["severity"] == "CRITICAL"],
            "warning_issues": [item for item in payload_issues if item["severity"] == "WARNING"],
            "business_changes": [
                item
                for item in payload_issues
                if item["check_id"] in {"new_sku_high_sales", "sku_sales_drop_to_zero", "sku_returned_after_gap"}
            ],
            "problems": _legacy_problems(payload_issues, total_rows),
            "skipped_checks": [asdict(item) for item in skipped],
            "examples": {"unclassified": [], "missing_weight_volume": [], "anomalies": payload_issues[:8], "duplicates": []},
            "warnings": warning_messages,
        }
        report["summary"] = _summary_text(report)
        return report

    def _fail_report(self, source: QualityDataSource, reason: str, *, total_rows: int = 0) -> dict[str, Any]:
        report = {
            "project_name": source.project_name,
            "status": "FAIL",
            "status_comment": reason,
            "source": self._source_payload(source),
            "total_rows": total_rows,
            "metrics": {
                "summary_by_severity": {"CRITICAL": 0, "WARNING": 0, "INFO": 0, "total": 0},
                "summary_by_category": [],
                "summary_by_period": [],
                "summary_by_check": [],
                "top_suspicious_skus": [],
                "top_problem_categories": [],
                "detected_columns": {},
                "checks": {},
                "empty_key_fields": {"rows_with_empty": 0, "share": 0.0, "fields": []},
                "weight_volume": {"columns": [], "parsed_count": 0, "missing_count": 0, "coverage_share": 0.0, "missing_share": 0.0},
                "anomalies": {"columns": [], "count": 0, "zero_or_negative": 0, "too_large": 0, "suspicious": 0},
                "classification": {"columns": [], "classified_count": 0, "unclassified_count": 0, "coverage_share": 0.0, "unclassified_share": 0.0},
                "duplicates": {"checked": False, "identifier_column": None, "columns": [], "duplicate_rows": 0, "duplicate_keys": 0, "share": 0.0},
            },
            "issues": [],
            "critical_issues": [],
            "warning_issues": [],
            "business_changes": [],
            "problems": [{"type": "Файл не проверен", "count": 0, "share": 0.0, "comment": reason}],
            "skipped_checks": [],
            "examples": {"unclassified": [], "missing_weight_volume": [], "anomalies": [], "duplicates": []},
            "warnings": [reason],
        }
        report["summary"] = _summary_text(report)
        return report

    @staticmethod
    def _source_payload(source: QualityDataSource) -> dict[str, Any]:
        primary = source.primary_path
        return {
            "kind": source.source_kind,
            "scope": source.source_scope,
            "path": str(primary) if primary else "",
            "paths": [str(path) for path in source.paths[:10]],
            "file_count": source.file_count,
            "fallback_used": source.fallback_used,
            "table_name": source.table_name,
            "row_count": source.row_count,
            "slice_count": source.slice_count,
        }


def _status_comment(status: str, severity: Counter[str]) -> str:
    if status == "OK":
        if severity["INFO"]:
            return "Критичных проблем не найдено, есть информационные бизнес-изменения."
        return "Подозрительные бизнес-аномалии не найдены."
    parts = []
    if severity["CRITICAL"]:
        parts.append(f"CRITICAL: {severity['CRITICAL']}")
    if severity["WARNING"]:
        parts.append(f"WARNING: {severity['WARNING']}")
    return "Есть бизнес-предупреждения по данным: " + ", ".join(parts) + "."


def _summary_by(issues: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    counters: dict[str, Counter[str]] = defaultdict(Counter)
    for item in issues:
        key = str(item.get(field) or "Не указано")
        counters[key][str(item.get("severity") or "INFO")] += 1
        counters[key]["total"] += 1
    rows = [
        {"key": key, "total": counter["total"], "critical": counter["CRITICAL"], "warning": counter["WARNING"], "info": counter["INFO"]}
        for key, counter in counters.items()
    ]
    return sorted(rows, key=lambda row: (-int(row["critical"]), -int(row["warning"]), -int(row["total"]), str(row["key"])))[:50]


def _top_skus(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scores: dict[str, dict[str, Any]] = {}
    for item in issues:
        if item.get("entity_type") != "sku":
            continue
        sku = str(item.get("entity_id") or "")
        if not sku:
            continue
        current = scores.setdefault(
            sku,
            {"sku": sku, "category": item.get("category"), "issues": 0, "critical": 0, "warning": 0, "max_relative_delta": None},
        )
        current["issues"] += 1
        if item.get("severity") == "CRITICAL":
            current["critical"] += 1
        if item.get("severity") == "WARNING":
            current["warning"] += 1
        rel = item.get("relative_delta")
        if isinstance(rel, (int, float)):
            current["max_relative_delta"] = max(float(rel), float(current["max_relative_delta"] or 0))
    return sorted(scores.values(), key=lambda row: (-row["critical"], -row["warning"], -row["issues"], -(row["max_relative_delta"] or 0)))[:20]


def _top_categories(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in _summary_by(issues, "category") if row["key"] != "Не указано"]
    return rows[:20]


def _count_checks(issues: list[dict[str, Any]], check_ids: set[str]) -> int:
    return sum(1 for item in issues if item.get("check_id") in check_ids)


def _legacy_problems(issues: list[dict[str, Any]], total_rows: int) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in issues:
        key = str(item.get("check_name") or item.get("check_id"))
        current = grouped.setdefault(
            key,
            {"type": key, "count": 0, "share": 0.0, "comment": str(item.get("suggested_action") or item.get("message") or "")},
        )
        current["count"] += 1
    for row in grouped.values():
        row["share"] = round(float(row["count"]) / float(total_rows), 4) if total_rows else 0.0
    return sorted(grouped.values(), key=lambda row: -int(row["count"]))


def _summary_text(report: dict[str, Any]) -> str:
    severity = report.get("metrics", {}).get("summary_by_severity", {})
    return (
        f"{report.get('project_name')}: {report.get('status')}. "
        f"Строк: {report.get('total_rows')}. "
        f"CRITICAL: {severity.get('CRITICAL', 0)}. "
        f"WARNING: {severity.get('WARNING', 0)}. "
        f"INFO: {severity.get('INFO', 0)}."
    )
