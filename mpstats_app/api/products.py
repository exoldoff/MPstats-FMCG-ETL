from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from mpstats_app.api.dependencies import get_repository, get_settings
from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository


router = APIRouter(prefix="/api/products", tags=["products"])


@router.get("")
def search_products(
    query: str | None = None,
    project_name: str | None = None,
    run_id: str | None = None,
    marketplace: str | None = None,
    category: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    repository: DuckDbAppRepository = Depends(get_repository),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, object]:
    return repository.search_products(
        table_name=settings.products_table,
        query_text=query,
        project_name=project_name,
        run_id=run_id,
        marketplace=marketplace,
        category=category,
        limit=limit,
        offset=offset,
    )
