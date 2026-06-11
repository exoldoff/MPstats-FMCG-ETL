from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from pipeline.data_quality.config import DataQualityConfig


Severity = Literal["CRITICAL", "WARNING", "INFO"]


@dataclass(frozen=True)
class SkippedCheck:
    check: str
    reason: str


@dataclass(frozen=True)
class QualityIssue:
    check_id: str
    check_name: str
    severity: Severity
    entity_type: str
    entity_id: str
    category: str | None
    period: str | None
    metric_name: str
    current_value: float | int | str | None = None
    previous_value: float | int | str | None = None
    baseline_value: float | int | str | None = None
    absolute_delta: float | int | None = None
    relative_delta: float | int | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    suggested_action: str = ""

    def to_dict(self) -> dict[str, Any]:
        return _clean_payload(asdict(self))


@dataclass(frozen=True)
class QualityDataSourcePayload:
    kind: str
    scope: str
    path: str
    paths: list[str]
    file_count: int
    fallback_used: bool


@dataclass
class PreparedQualityData:
    total_rows: int
    columns: dict[str, str | None]
    raw_columns: list[str]
    latest_period: str | None
    source_paths: tuple[Path, ...]

    def has(self, *field_names: str) -> bool:
        return all(self.columns.get(field_name) for field_name in field_names)


@dataclass
class QualityContext:
    con: Any
    config: DataQualityConfig
    prepared: PreparedQualityData
    skipped_checks: list[SkippedCheck]

    def has(self, *field_names: str) -> bool:
        return self.prepared.has(*field_names)

    def skip(self, check: str, reason: str) -> None:
        self.skipped_checks.append(SkippedCheck(check=check, reason=reason))


def clean_issue(value: Any) -> Any:
    return _clean_value(value)


def issues_to_payload(issues: list[QualityIssue]) -> list[dict[str, Any]]:
    return [issue.to_dict() for issue in issues]


def _clean_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _clean_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_payload(item) for item in value]
    return _clean_value(value)


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            return _clean_value(value.item())
        except Exception:
            pass
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            return None
        return round(value, 6)
    if isinstance(value, Path):
        return str(value)
    return value
