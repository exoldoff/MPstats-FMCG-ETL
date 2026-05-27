from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from mpstats_app.api.dependencies import get_project_service
from mpstats_app.schemas import ProjectPayload
from mpstats_app.services.project_service import ProjectService


router = APIRouter(prefix="/api/projects", tags=["projects"])


@router.get("")
def list_projects(projects: ProjectService = Depends(get_project_service)) -> dict[str, object]:
    return projects.list_projects()


@router.post("")
def create_project(
    payload: ProjectPayload,
    projects: ProjectService = Depends(get_project_service),
) -> dict[str, object]:
    try:
        return projects.create_project(project_name=payload.project_name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("")
def delete_project(
    project_name: str,
    delete_files: bool = False,
    projects: ProjectService = Depends(get_project_service),
) -> dict[str, object]:
    try:
        return projects.delete_project(project_name=project_name, delete_files=delete_files)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
