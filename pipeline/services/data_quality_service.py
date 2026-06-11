from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.data_quality import DataQualityConfig, DataQualityRunner
from pipeline.repositories.data_quality_repository import DataQualityRepository


class DataQualityService:
    """Тонкий фасад для web-app.

    Бизнес-логика проверок живёт в `pipeline.data_quality`: сервис только
    находит проектный источник и возвращает готовый JSON-friendly отчёт.
    """

    def __init__(self, repository: DataQualityRepository, *, config: DataQualityConfig | None = None) -> None:
        self.repository = repository
        temp_directory = Path(repository.workdir) / ".data_quality_duckdb_tmp"
        self.runner = DataQualityRunner(config=config, temp_directory=temp_directory)

    def list_projects(self) -> dict[str, object]:
        return {"projects": self.repository.list_projects()}

    def build_report(self, project_name: str) -> dict[str, Any]:
        source = self.repository.resolve_source(project_name)
        return self.runner.run(source)
