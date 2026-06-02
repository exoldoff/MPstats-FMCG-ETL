from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from mpstats_app.api.dependencies import get_export_service
from mpstats_app.schemas import ExportBuildPayload, ExportPreviewPayload, ExportTemplatePayload
from mpstats_app.services.export_service import ExportService


router = APIRouter(prefix="/api/exports", tags=["exports"])


def _handle(call):
    try:
        return call()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/options")
def export_options(
    project_name: str = "mpstats",
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(lambda: export_service.options(project_name=project_name))


@router.get("/templates")
def export_templates(
    project_name: str = "mpstats",
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(lambda: export_service.list_templates(project_name=project_name))


@router.post("/templates")
def save_export_template(
    payload: ExportTemplatePayload,
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(
        lambda: export_service.save_template(
            name=payload.name,
            project_name=payload.project_name,
            category_keys=payload.category_keys,
            period_from=payload.period_from,
            period_to=payload.period_to,
            selected_columns=payload.selected_columns,
            filters=[item.model_dump() for item in payload.filters],
            sort_column=payload.sort_column,
            sort_direction=payload.sort_direction,
            split_by_category=payload.split_by_category,
            export_format=payload.export_format,
            output_dir=payload.output_dir,
        )
    )


@router.delete("/templates/{template_id}")
def delete_export_template(
    template_id: str,
    project_name: str = "mpstats",
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(lambda: export_service.delete_template(template_id=template_id, project_name=project_name))


@router.post("/preview")
def export_preview(
    payload: ExportPreviewPayload,
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(
        lambda: export_service.preview(
            project_name=payload.project_name,
            category_keys=payload.category_keys,
            period_from=payload.period_from,
            period_to=payload.period_to,
            selected_columns=payload.selected_columns,
            filters=[item.model_dump() for item in payload.filters],
            excluded_row_hashes=payload.excluded_row_hashes,
            sort_column=payload.sort_column,
            sort_direction=payload.sort_direction,
            split_by_category=payload.split_by_category,
            export_format=payload.export_format,
            limit=payload.limit,
            offset=payload.offset,
        )
    )


@router.post("/build")
def export_build(
    payload: ExportBuildPayload,
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(
        lambda: export_service.build(
            project_name=payload.project_name,
            category_keys=payload.category_keys,
            period_from=payload.period_from,
            period_to=payload.period_to,
            selected_columns=payload.selected_columns,
            filters=[item.model_dump() for item in payload.filters],
            excluded_row_hashes=payload.excluded_row_hashes,
            sort_column=payload.sort_column,
            sort_direction=payload.sort_direction,
            split_by_category=payload.split_by_category,
            output_dir=payload.output_dir,
            confirm_large_export=payload.confirm_large_export,
            export_format=payload.export_format,
        )
    )


@router.post("/build-jobs")
def export_build_job(
    payload: ExportBuildPayload,
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(
        lambda: export_service.start_build_job(
            project_name=payload.project_name,
            category_keys=payload.category_keys,
            period_from=payload.period_from,
            period_to=payload.period_to,
            selected_columns=payload.selected_columns,
            filters=[item.model_dump() for item in payload.filters],
            excluded_row_hashes=payload.excluded_row_hashes,
            sort_column=payload.sort_column,
            sort_direction=payload.sort_direction,
            split_by_category=payload.split_by_category,
            output_dir=payload.output_dir,
            confirm_large_export=payload.confirm_large_export,
            export_format=payload.export_format,
        )
    )


@router.get("/build-jobs/{job_id}")
def export_build_job_status(
    job_id: str,
    export_service: ExportService = Depends(get_export_service),
) -> dict[str, object]:
    return _handle(lambda: export_service.get_build_job(job_id))


@router.get("/download-file")
def download_export_file(
    path: str,
    export_service: ExportService = Depends(get_export_service),
) -> FileResponse:
    target = _handle(lambda: export_service.resolve_export_file(path))
    return FileResponse(target, filename=target.name)
