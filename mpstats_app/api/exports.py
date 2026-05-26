from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from mpstats_app.api.dependencies import get_export_service
from mpstats_app.schemas import ExportBuildPayload, ExportPreviewPayload
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
        )
    )


@router.get("/download-file")
def download_export_file(
    path: str,
    export_service: ExportService = Depends(get_export_service),
) -> FileResponse:
    target = _handle(lambda: export_service.resolve_export_file(path))
    return FileResponse(target, filename=target.name)
