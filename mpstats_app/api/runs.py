from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from mpstats_app.api.dependencies import get_job_service, get_repository
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.schemas import RunCreate
from mpstats_app.services.job_service import JobService, RunRequest


router = APIRouter(prefix="/api/runs", tags=["runs"])


def _run_payload(repository: DuckDbAppRepository, run_id: str) -> dict[str, object]:
    run = repository.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    run["steps_detail"] = repository.list_run_steps(run_id)
    run["events"] = repository.list_run_events(run_id)
    return run


@router.post("")
def create_run(payload: RunCreate, job_service: JobService = Depends(get_job_service)) -> dict[str, object]:
    return job_service.create_run(
        RunRequest(
            project_name=payload.project_name,
            steps=payload.steps,
            write_xlsx=payload.write_xlsx,
            max_weight_kg=payload.max_weight_kg,
            fill_unclassified=payload.fill_unclassified,
        )
    )


@router.get("")
def list_runs(
    limit: int = Query(default=50, ge=1, le=200),
    repository: DuckDbAppRepository = Depends(get_repository),
) -> dict[str, object]:
    return {"runs": repository.list_runs(limit=limit)}


@router.get("/{run_id}")
def get_run(run_id: str, repository: DuckDbAppRepository = Depends(get_repository)) -> dict[str, object]:
    return _run_payload(repository, run_id)


@router.post("/{run_id}/cancel")
def cancel_run(run_id: str, job_service: JobService = Depends(get_job_service)) -> dict[str, object]:
    run = job_service.cancel_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@router.get("/{run_id}/events")
def run_events(
    run_id: str,
    after: str | None = None,
    repository: DuckDbAppRepository = Depends(get_repository),
) -> dict[str, object]:
    if not repository.get_run(run_id):
        raise HTTPException(status_code=404, detail="Run not found")
    return {"events": repository.list_run_events(run_id, after=after)}
