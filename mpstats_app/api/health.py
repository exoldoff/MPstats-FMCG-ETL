from __future__ import annotations

from fastapi import APIRouter, Depends

from mpstats_app.api.dependencies import get_settings
from mpstats_app.config import AppSettings


router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
def health(settings: AppSettings = Depends(get_settings)) -> dict[str, object]:
    return {
        "ok": True,
        "project_root": str(settings.project_root),
        "workdir": str(settings.workdir),
        "db_path": str(settings.db_path),
    }
