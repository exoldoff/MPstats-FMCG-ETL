from __future__ import annotations

from typing import Any

from pipeline.data_quality.models import QualityIssue, QualityContext, Severity, clean_issue
from pipeline.data_quality.sql import fetch_dicts


def query_rows(ctx: QualityContext, sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
    return fetch_dicts(ctx.con, sql, params)


def limit(ctx: QualityContext) -> int:
    return max(1, int(ctx.config.max_issues_per_check))


def issue(
    *,
    check_id: str,
    check_name: str,
    severity: Severity,
    entity_type: str,
    entity_id: object,
    category: object = None,
    period: object = None,
    metric_name: str,
    current_value: object = None,
    previous_value: object = None,
    baseline_value: object = None,
    absolute_delta: object = None,
    relative_delta: object = None,
    message: str,
    details: dict[str, Any] | None = None,
    suggested_action: str,
) -> QualityIssue:
    return QualityIssue(
        check_id=check_id,
        check_name=check_name,
        severity=severity,
        entity_type=entity_type,
        entity_id=str(clean_issue(entity_id) or ""),
        category=str(category) if category is not None else None,
        period=str(period) if period is not None else None,
        metric_name=metric_name,
        current_value=clean_issue(current_value),
        previous_value=clean_issue(previous_value),
        baseline_value=clean_issue(baseline_value),
        absolute_delta=clean_issue(absolute_delta),
        relative_delta=clean_issue(relative_delta),
        message=message,
        details={str(key): clean_issue(value) for key, value in (details or {}).items()},
        suggested_action=suggested_action,
    )


def fmt(value: object, suffix: str = "") -> str:
    cleaned = clean_issue(value)
    if cleaned is None:
        return "нет данных"
    if isinstance(cleaned, float):
        text = f"{cleaned:,.2f}".replace(",", " ")
        if text.endswith(".00"):
            text = text[:-3]
        return f"{text}{suffix}"
    return f"{cleaned}{suffix}"


def pct(value: object) -> str:
    cleaned = clean_issue(value)
    if cleaned is None:
        return "нет данных"
    try:
        return f"{float(cleaned) * 100:.1f}%"
    except (TypeError, ValueError):
        return str(cleaned)
