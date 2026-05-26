from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from mpstats_app.api.dependencies import get_data_quality_service
from pipeline.services.data_quality_service import DataQualityService


router = APIRouter(prefix="/api/quality", tags=["quality"])


@router.get("/projects")
def list_quality_projects(service: DataQualityService = Depends(get_data_quality_service)) -> dict[str, object]:
    return service.list_projects()


@router.get("/report")
def get_quality_report(
    project_name: str,
    service: DataQualityService = Depends(get_data_quality_service),
) -> dict[str, object]:
    try:
        return service.build_report(project_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
