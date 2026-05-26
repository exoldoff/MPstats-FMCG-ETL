from __future__ import annotations

import json
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from mpstats_app.api.dependencies import get_repository
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.schemas import ScheduleCreate, ScheduleUpdate


router = APIRouter(prefix="/api/schedules", tags=["schedules"])


@router.get("")
def list_schedules(repository: DuckDbAppRepository = Depends(get_repository)) -> dict[str, object]:
    return {"schedules": repository.list_schedules()}


@router.post("")
def create_schedule(
    payload: ScheduleCreate,
    repository: DuckDbAppRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.create_schedule(
        schedule_id=uuid4().hex,
        name=payload.name,
        project_name=payload.project_name,
        steps=payload.steps,
        enabled=payload.enabled,
        interval_minutes=payload.interval_minutes,
        next_run_at=payload.resolved_next_run_at(),
        write_xlsx=payload.write_xlsx,
        max_weight_kg=payload.max_weight_kg,
        fill_unclassified=payload.fill_unclassified,
    )


@router.put("/{schedule_id}")
def update_schedule(
    schedule_id: str,
    payload: ScheduleUpdate,
    repository: DuckDbAppRepository = Depends(get_repository),
) -> dict[str, object]:
    schedule = repository.update_schedule(
        schedule_id,
        {
            "name": payload.name,
            "project_name": payload.project_name,
            "steps": payload.steps,
            "enabled": payload.enabled,
            "interval_minutes": payload.interval_minutes,
            "next_run_at": payload.resolved_next_run_at(),
            "write_xlsx": payload.write_xlsx,
            "max_weight_kg": payload.max_weight_kg,
            "fill_unclassified_json": json.dumps(payload.fill_unclassified, ensure_ascii=False)
            if payload.fill_unclassified
            else None,
        },
    )
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return schedule


@router.delete("/{schedule_id}")
def delete_schedule(schedule_id: str, repository: DuckDbAppRepository = Depends(get_repository)) -> dict[str, object]:
    deleted = repository.delete_schedule(schedule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"deleted": True}
