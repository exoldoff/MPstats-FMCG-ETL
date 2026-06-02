from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from mpstats_app.api.dependencies import (
    get_catalog_service,
    get_smart_pipeline_service,
    get_smart_plan_service,
    get_workflow_service,
)
from mpstats_app.schemas import (
    AppSettingsPayload,
    CategorySourcePayload,
    ClassifyPayload,
    DownloadPayload,
    MonthlySyncPayload,
    PipelineActionPayload,
    PipelinePlanPayload,
    PipelineSettingsPayload,
    PreviewPayload,
    ProcessPayload,
    SaveToDbPayload,
)
from mpstats_app.services.category_catalog_service import CategoryCatalogService
from mpstats_app.services.smart_plan_service import SmartPlanService
from mpstats_app.services.smart_pipeline_service import SmartPipelineService
from mpstats_app.services.workflow_service import WorkflowService


router = APIRouter(prefix="/api/workflow", tags=["workflow"])


def _handle(call):
    try:
        return call()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/settings")
def get_settings(workflow: WorkflowService = Depends(get_workflow_service)) -> dict[str, object]:
    return workflow.get_settings()


@router.put("/settings")
def put_settings(
    payload: AppSettingsPayload,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    return workflow.save_settings(
        cookie=payload.cookie,
        project_name=payload.project_name,
        workflow_mode=payload.workflow_mode,
        start_year=payload.start_year,
        start_month=payload.start_month,
        end_year=payload.end_year,
        end_month=payload.end_month,
    )


@router.get("/categories")
def list_categories(catalog: CategoryCatalogService = Depends(get_catalog_service)) -> dict[str, object]:
    return {"categories": catalog.list_categories()}


@router.post("/categories/sync")
def sync_categories(catalog: CategoryCatalogService = Depends(get_catalog_service)) -> dict[str, object]:
    source = catalog.find_source()
    if source is None:
        raise HTTPException(status_code=404, detail="Справочник категорий не найден")
    return catalog.import_from_file(source)


@router.get("/categories/source")
def list_category_source(catalog: CategoryCatalogService = Depends(get_catalog_service)) -> dict[str, object]:
    return catalog.list_source_rows()


@router.put("/categories/source")
def put_category_source(
    payload: CategorySourcePayload,
    catalog: CategoryCatalogService = Depends(get_catalog_service),
) -> dict[str, object]:
    try:
        return catalog.save_source_rows([row.model_dump() for row in payload.rows])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/download")
def download(
    payload: DownloadPayload,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    return _handle(
        lambda: workflow.download(
            project_name=payload.project_name,
            cookie=payload.cookie,
            category_ids=payload.category_ids,
            start_year=payload.start_year,
            start_month=payload.start_month,
            end_year=payload.end_year,
            end_month=payload.end_month,
            skip_if_exists=payload.skip_if_exists,
        )
    )


@router.post("/process")
def process(
    payload: ProcessPayload,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    return _handle(lambda: workflow.process(project_name=payload.project_name, max_weight_kg=payload.max_weight_kg))


@router.post("/classify")
def classify(
    payload: ClassifyPayload,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    return _handle(
        lambda: workflow.classify(
            project_name=payload.project_name,
            input_file=payload.input_file,
            overwrite_input=payload.overwrite_input,
            write_xlsx=payload.write_xlsx,
        )
    )


@router.post("/classify-upload")
async def classify_upload(
    request: Request,
    project_name: str = "mpstats",
    filename: str = "external.csv",
    write_xlsx: bool = False,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    content = await request.body()
    return _handle(
        lambda: workflow.classify_uploaded_file(
            project_name=project_name,
            filename=filename,
            content=content,
            write_xlsx=write_xlsx,
        )
    )


@router.post("/preview")
def preview(
    payload: PreviewPayload,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    return _handle(lambda: workflow.preview(project_name=payload.project_name, file_kind=payload.file_kind, file_path=payload.file_path))


@router.post("/save-to-db")
def save_to_db(
    payload: SaveToDbPayload,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> dict[str, object]:
    return _handle(lambda: workflow.save_to_db(project_name=payload.project_name, file_path=payload.file_path))


@router.get("/download-file")
def download_file(
    path: str,
    workflow: WorkflowService = Depends(get_workflow_service),
) -> FileResponse:
    target = _handle(lambda: workflow.resolve_download_file(path))
    return FileResponse(target, filename=target.name)


@router.get("/pipeline/settings")
def get_pipeline_settings(pipeline: SmartPipelineService = Depends(get_smart_pipeline_service)) -> dict[str, object]:
    return pipeline.get_pipeline_settings()


@router.put("/pipeline/settings")
def put_pipeline_settings(
    payload: PipelineSettingsPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return pipeline.save_pipeline_settings(payload.model_dump())


@router.get("/pipeline/runs")
def list_pipeline_runs(
    project_name: str | None = None,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return pipeline.list_runs(project_name=project_name)


@router.post("/pipeline/plans")
def create_pipeline_plan(
    payload: PipelinePlanPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(
        lambda: pipeline.create_plan(
            project_name=payload.project_name,
            run_type=payload.run_type,
            category_ids=payload.category_ids,
            start_year=payload.start_year,
            start_month=payload.start_month,
            end_year=payload.end_year,
            end_month=payload.end_month,
            settings=payload.settings.model_dump() if payload.settings else None,
        )
    )


@router.get("/pipeline/runs/{run_id}")
def get_pipeline_run(
    run_id: str,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.get_run(run_id))


@router.delete("/pipeline/runs/{run_id}")
def delete_pipeline_run(
    run_id: str,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.delete_run(run_id=run_id))


@router.get("/pipeline/runs/{run_id}/tasks")
def list_pipeline_tasks(
    run_id: str,
    task_filter: str = "all",
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.list_tasks(run_id=run_id, task_filter=task_filter))


@router.get("/pipeline/runs/{run_id}/smart-plan")
def get_smart_plan(
    run_id: str,
    status: str = "all",
    smart_plan: SmartPlanService = Depends(get_smart_plan_service),
) -> dict[str, object]:
    return _handle(lambda: smart_plan.get_run_plan(run_id=run_id, status=status))


@router.post("/pipeline/runs/{run_id}/start")
def start_pipeline_run(
    run_id: str,
    payload: PipelineActionPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.start_run(run_id=run_id, wait=payload.wait))


@router.post("/pipeline/runs/{run_id}/pause")
def pause_pipeline_run(
    run_id: str,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.pause_run(run_id=run_id))


@router.post("/pipeline/runs/{run_id}/resume")
def resume_pipeline_run(
    run_id: str,
    payload: PipelineActionPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.resume_run(run_id=run_id, wait=payload.wait))


@router.post("/pipeline/runs/{run_id}/retry-errors")
def retry_pipeline_errors(
    run_id: str,
    payload: PipelineActionPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.retry_errors(run_id=run_id, wait=payload.wait))


@router.post("/pipeline/runs/{run_id}/rebuild-cube")
def rebuild_pipeline_cube(
    run_id: str,
    payload: PipelineActionPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.rebuild_cube(run_id=run_id, wait=payload.wait))


@router.post("/pipeline/runs/{run_id}/reclassify-cube")
def reclassify_pipeline_cube(
    run_id: str,
    payload: PipelineActionPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.reclassify_cube(run_id=run_id, wait=payload.wait))


@router.post("/pipeline/tasks/{task_id}/retry")
def retry_pipeline_task(
    task_id: str,
    payload: PipelineActionPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.retry_task(task_id=task_id, wait=payload.wait))


@router.post("/pipeline/monthly-sync")
def monthly_sync(
    payload: MonthlySyncPayload,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    plan = _handle(
        lambda: pipeline.create_monthly_sync_plan(
            project_name=payload.project_name,
            settings=payload.settings.model_dump() if payload.settings else None,
        )
    )
    if payload.start_immediately:
        return _handle(lambda: pipeline.start_run(run_id=str(plan["id"]), wait=payload.wait))
    return plan


@router.get("/pipeline/files")
def list_pipeline_files(
    project_name: str,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.list_files(project_name=project_name))


@router.delete("/pipeline/files")
def delete_pipeline_file(
    project_name: str,
    path: str,
    delete_cube: bool = False,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.delete_file(project_name=project_name, path=path, delete_cube=delete_cube))


@router.get("/pipeline/cube")
def list_pipeline_cube(
    project_name: str,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.list_cube(project_name=project_name))


@router.delete("/pipeline/cube/{entry_id}")
def delete_pipeline_cube_entry(
    entry_id: str,
    pipeline: SmartPipelineService = Depends(get_smart_pipeline_service),
) -> dict[str, object]:
    return _handle(lambda: pipeline.delete_cube_entry(entry_id=entry_id))
