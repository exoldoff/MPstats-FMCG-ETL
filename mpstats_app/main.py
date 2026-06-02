from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Iterator

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from mpstats_app.api.exports import router as exports_router
from mpstats_app.api.health import router as health_router
from mpstats_app.api.products import router as products_router
from mpstats_app.api.projects import router as projects_router
from mpstats_app.api.quality import router as quality_router
from mpstats_app.api.reports import router as reports_router
from mpstats_app.api.runs import router as runs_router
from mpstats_app.api.schedules import router as schedules_router
from mpstats_app.api.settings import router as settings_router
from mpstats_app.api.workflow import router as workflow_router
from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.services.category_catalog_service import CategoryCatalogService
from mpstats_app.services.classifier_rules_service import ClassifierRulesService
from mpstats_app.services.export_service import ExportService
from mpstats_app.services.job_service import JobService
from mpstats_app.services.project_service import ProjectService
from mpstats_app.services.report_service import ReportService
from mpstats_app.services.scheduler_service import SchedulerService
from mpstats_app.services.smart_plan_service import SmartPlanService
from mpstats_app.services.smart_pipeline_service import SmartPipelineService
from mpstats_app.services.workflow_service import WorkflowService
from pipeline.repositories.data_quality_repository import DataQualityRepository
from pipeline.services.data_quality_service import DataQualityService


SPA_INDEX_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def create_app(settings: AppSettings | None = None, *, start_workers: bool = True) -> FastAPI:
    app_settings = settings or AppSettings.create()
    repository = DuckDbAppRepository(app_settings)
    catalog_service = CategoryCatalogService(settings=app_settings, repository=repository)
    classifier_rules_service = ClassifierRulesService(settings=app_settings)
    workflow_service = WorkflowService(settings=app_settings, repository=repository, catalog_service=catalog_service)
    smart_pipeline_service = SmartPipelineService(settings=app_settings, repository=repository, catalog_service=catalog_service)
    smart_plan_service = SmartPlanService(settings=app_settings, repository=repository)
    project_service = ProjectService(settings=app_settings, repository=repository)
    export_service = ExportService(settings=app_settings, repository=repository)
    report_service = ReportService(settings=app_settings, repository=repository)
    job_service = JobService(settings=app_settings, repository=repository)
    scheduler_service = SchedulerService(settings=app_settings, repository=repository, job_service=job_service)
    data_quality_service = DataQualityService(
        DataQualityRepository(project_root=app_settings.project_root, workdir=app_settings.workdir)
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> Iterator[None]:
        repository.ensure_ready()
        catalog_service.ensure_seeded()
        if start_workers:
            job_service.start()
            scheduler_service.start()
        yield
        scheduler_service.stop()
        job_service.stop()

    app = FastAPI(title="MPStats Local App", version="0.1.0", lifespan=lifespan)
    app.state.settings = app_settings
    app.state.repository = repository
    app.state.catalog_service = catalog_service
    app.state.classifier_rules_service = classifier_rules_service
    app.state.workflow_service = workflow_service
    app.state.smart_pipeline_service = smart_pipeline_service
    app.state.smart_plan_service = smart_plan_service
    app.state.project_service = project_service
    app.state.export_service = export_service
    app.state.report_service = report_service
    app.state.job_service = job_service
    app.state.scheduler_service = scheduler_service
    app.state.data_quality_service = data_quality_service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health_router)
    app.include_router(runs_router)
    app.include_router(products_router)
    app.include_router(projects_router)
    app.include_router(exports_router)
    app.include_router(reports_router)
    app.include_router(settings_router)
    app.include_router(schedules_router)
    app.include_router(workflow_router)
    app.include_router(quality_router)

    @app.get("/docs/USER_GUIDE.md", include_in_schema=False)
    def user_guide() -> FileResponse:
        guide_path = app_settings.project_root / "docs" / "USER_GUIDE.md"
        if not guide_path.exists():
            raise HTTPException(status_code=404, detail="User guide not found")
        return FileResponse(guide_path, media_type="text/markdown; charset=utf-8")

    static_dir = app_settings.static_dir
    if static_dir and static_dir.exists():
        assets = static_dir / "assets"
        if assets.exists():
            app.mount("/assets", StaticFiles(directory=str(assets)), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        def spa(full_path: str) -> FileResponse:
            if full_path.startswith("api/"):
                raise HTTPException(status_code=404, detail="API endpoint not found")
            target = static_dir / full_path
            if full_path and target.exists() and target.is_file():
                if target.name == "index.html":
                    return FileResponse(target, headers=SPA_INDEX_HEADERS)
                return FileResponse(target)
            return FileResponse(static_dir / "index.html", headers=SPA_INDEX_HEADERS)

    return app


app = create_app()


def main() -> None:
    uvicorn.run("mpstats_app.main:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
