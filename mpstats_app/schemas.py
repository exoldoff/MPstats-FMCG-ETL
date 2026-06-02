from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, field_validator

from pipeline.services.run_service import parse_steps


class RunCreate(BaseModel):
    project_name: str = Field(default="mpstats", min_length=1)
    steps: str = "2-6"
    write_xlsx: bool = True
    max_weight_kg: float = 40.0
    fill_unclassified: dict[str, Any] | None = None

    @field_validator("steps")
    @classmethod
    def validate_steps(cls, value: str) -> str:
        parse_steps(value)
        return value


class ScheduleCreate(BaseModel):
    name: str = Field(min_length=1)
    project_name: str = Field(default="mpstats", min_length=1)
    steps: str = "2-6"
    enabled: bool = True
    interval_minutes: int = Field(default=1440, ge=1)
    next_run_at: datetime | None = None
    write_xlsx: bool = True
    max_weight_kg: float = 40.0
    fill_unclassified: dict[str, Any] | None = None

    @field_validator("steps")
    @classmethod
    def validate_steps(cls, value: str) -> str:
        parse_steps(value)
        return value

    def resolved_next_run_at(self) -> datetime:
        return self.next_run_at or datetime.now() + timedelta(minutes=self.interval_minutes)


class ScheduleUpdate(ScheduleCreate):
    pass


class TextPayload(BaseModel):
    content: str


class ClassifierConditionPayload(BaseModel):
    join_with_prev: str = "and"
    match_field: str = ""
    match_type: str = "contains"
    pattern: str = ""


class ClassifierRulePayload(BaseModel):
    id: str | None = None
    active: bool = True
    priority: int = 100
    category: str = "*"
    target_column: str = ""
    set_value: str = ""
    mode: str = "fill_empty"
    comment: str = ""
    conditions: list[ClassifierConditionPayload] = Field(default_factory=list)


class ClassifierRulesPayload(BaseModel):
    rules: list[ClassifierRulePayload]


class CategorySourceRowPayload(BaseModel):
    id: str | None = None
    active: bool = True
    category_name: str = ""
    marketplace: str = ""
    fbs: bool = False
    period_from: str = ""
    period_to: str = ""
    comment: str = ""
    path: str = ""
    filter_text: str = ""
    path2: str = ""
    filter2_text: str = ""
    actualization: str = ""


class CategorySourcePayload(BaseModel):
    rows: list[CategorySourceRowPayload]


class AppSettingsPayload(BaseModel):
    cookie: str = ""
    project_name: str = "mpstats"
    workflow_mode: str = "historical_backfill"
    start_year: int | None = None
    start_month: int | None = Field(default=None, ge=1, le=12)
    end_year: int | None = None
    end_month: int | None = Field(default=None, ge=1, le=12)


class DownloadPayload(BaseModel):
    project_name: str = "mpstats"
    cookie: str
    category_ids: list[str]
    start_year: int
    start_month: int = Field(ge=1, le=12)
    end_year: int
    end_month: int = Field(ge=1, le=12)
    skip_if_exists: bool = True


class ProjectPayload(BaseModel):
    project_name: str = "mpstats"


class ProcessPayload(ProjectPayload):
    max_weight_kg: float = 40.0


class ClassifyPayload(ProjectPayload):
    input_file: str | None = None
    overwrite_input: bool = False
    write_xlsx: bool = False


class PreviewPayload(ProjectPayload):
    file_kind: str = "classified"
    file_path: str | None = None


class SaveToDbPayload(ProjectPayload):
    file_path: str | None = None


class PipelineSettingsPayload(BaseModel):
    overwrite_raw: bool = False
    overwrite_processed: bool = False
    overwrite_db: bool = False
    max_parallel_downloads: int = Field(default=1, ge=1, le=8)
    retry_count: int = Field(default=1, ge=0, le=10)
    timeout_seconds: int = Field(default=300, ge=30, le=3600)
    pause_between_requests: float = Field(default=2.0, ge=0, le=120)
    max_weight_kg: float = Field(default=40.0, ge=1, le=1000)


class PipelinePlanPayload(ProjectPayload):
    run_type: str = "historical_backfill"
    category_ids: list[str]
    start_year: int
    start_month: int = Field(ge=1, le=12)
    end_year: int
    end_month: int = Field(ge=1, le=12)
    settings: PipelineSettingsPayload | None = None


class PipelineActionPayload(BaseModel):
    wait: bool = False


class MonthlySyncPayload(ProjectPayload):
    settings: PipelineSettingsPayload | None = None
    start_immediately: bool = True
    wait: bool = False


class ExportColumnFilterPayload(BaseModel):
    column: str = ""
    match_type: str = "contains"
    value: str = ""


class ExportPreviewPayload(ProjectPayload):
    category_keys: list[str] = Field(default_factory=list)
    period_from: str | None = None
    period_to: str | None = None
    selected_columns: list[str] = Field(default_factory=list)
    filters: list[ExportColumnFilterPayload] = Field(default_factory=list)
    excluded_row_hashes: list[str] = Field(default_factory=list)
    sort_column: str | None = None
    sort_direction: str = "asc"
    split_by_category: bool = False
    export_format: str = Field(default="xlsx", pattern="^(xlsx|csv)$")
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class ExportBuildPayload(ExportPreviewPayload):
    output_dir: str | None = None
    confirm_large_export: bool = False


class ExportTemplatePayload(ProjectPayload):
    name: str = Field(min_length=1)
    category_keys: list[str] = Field(default_factory=list)
    period_from: str | None = None
    period_to: str | None = None
    selected_columns: list[str] = Field(default_factory=list)
    filters: list[ExportColumnFilterPayload] = Field(default_factory=list)
    sort_column: str | None = None
    sort_direction: str = "asc"
    split_by_category: bool = False
    export_format: str = Field(default="xlsx", pattern="^(xlsx|csv)$")
    output_dir: str | None = None


class ReportPreviewPayload(ProjectPayload):
    report_type: str = Field(default="category_month", pattern="^(category_month|brand_month|classification_month|top_sku)$")
    category_keys: list[str] = Field(default_factory=list)
    period_from: str | None = None
    period_to: str | None = None
    export_format: str = Field(default="xlsx", pattern="^(xlsx|csv)$")
    max_rows: int = Field(default=5000, ge=100, le=100_000)
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class ReportBuildPayload(ReportPreviewPayload):
    output_dir: str | None = None
