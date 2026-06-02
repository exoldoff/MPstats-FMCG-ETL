from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from mpstats_app.api.dependencies import get_report_service
from mpstats_app.schemas import ReportBuildPayload, ReportPreviewPayload
from mpstats_app.services.report_service import ReportService


router = APIRouter(prefix="/api/reports", tags=["reports"])


def _handle(call):
    try:
        return call()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/options")
def report_options(
    project_name: str = "mpstats",
    report_service: ReportService = Depends(get_report_service),
) -> dict[str, object]:
    return _handle(lambda: report_service.options(project_name=project_name))


@router.post("/preview")
def report_preview(
    payload: ReportPreviewPayload,
    report_service: ReportService = Depends(get_report_service),
) -> dict[str, object]:
    return _handle(
        lambda: report_service.preview(
            project_name=payload.project_name,
            report_type=payload.report_type,
            category_keys=payload.category_keys,
            period_from=payload.period_from,
            period_to=payload.period_to,
            export_format=payload.export_format,
            max_rows=payload.max_rows,
            limit=payload.limit,
            offset=payload.offset,
        )
    )


@router.post("/build")
def report_build(
    payload: ReportBuildPayload,
    report_service: ReportService = Depends(get_report_service),
) -> dict[str, object]:
    return _handle(
        lambda: report_service.build(
            project_name=payload.project_name,
            report_type=payload.report_type,
            category_keys=payload.category_keys,
            period_from=payload.period_from,
            period_to=payload.period_to,
            export_format=payload.export_format,
            output_dir=payload.output_dir,
            max_rows=payload.max_rows,
        )
    )


@router.get("/download-file")
def download_report_file(
    path: str,
    report_service: ReportService = Depends(get_report_service),
) -> FileResponse:
    target = _handle(lambda: report_service.resolve_report_file(path))
    return FileResponse(target, filename=target.name)
