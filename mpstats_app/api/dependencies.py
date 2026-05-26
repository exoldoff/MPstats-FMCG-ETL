from __future__ import annotations

from fastapi import Request

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.services.job_service import JobService
from mpstats_app.services.scheduler_service import SchedulerService
from mpstats_app.services.category_catalog_service import CategoryCatalogService
from mpstats_app.services.classifier_rules_service import ClassifierRulesService
from mpstats_app.services.export_service import ExportService
from mpstats_app.services.smart_plan_service import SmartPlanService
from mpstats_app.services.smart_pipeline_service import SmartPipelineService
from mpstats_app.services.workflow_service import WorkflowService
from pipeline.services.data_quality_service import DataQualityService


def get_settings(request: Request) -> AppSettings:
    return request.app.state.settings


def get_repository(request: Request) -> DuckDbAppRepository:
    return request.app.state.repository


def get_job_service(request: Request) -> JobService:
    return request.app.state.job_service


def get_scheduler_service(request: Request) -> SchedulerService:
    return request.app.state.scheduler_service


def get_catalog_service(request: Request) -> CategoryCatalogService:
    return request.app.state.catalog_service


def get_classifier_rules_service(request: Request) -> ClassifierRulesService:
    return request.app.state.classifier_rules_service


def get_workflow_service(request: Request) -> WorkflowService:
    return request.app.state.workflow_service


def get_smart_pipeline_service(request: Request) -> SmartPipelineService:
    return request.app.state.smart_pipeline_service


def get_smart_plan_service(request: Request) -> SmartPlanService:
    return request.app.state.smart_plan_service


def get_export_service(request: Request) -> ExportService:
    return request.app.state.export_service


def get_data_quality_service(request: Request) -> DataQualityService:
    return request.app.state.data_quality_service
