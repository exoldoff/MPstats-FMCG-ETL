from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re

import pandas as pd

from pipeline.repositories.file_repository import read_csv_auto
from pipeline.repositories.sql_repository import apply_migrations, connect, quote_identifier


@dataclass(frozen=True)
class QualityDataSource:
    project_name: str
    source_kind: str
    source_scope: str
    paths: tuple[Path, ...]
    fallback_used: bool = False
    table_name: str | None = None
    row_count: int = 0
    slice_count: int = 0

    @property
    def file_count(self) -> int:
        return len(self.paths)

    @property
    def primary_path(self) -> Path | None:
        return self.paths[0] if self.paths else None


class DataQualityRepository:
    def __init__(
        self,
        *,
        project_root: str | Path,
        workdir: str | Path,
        db_path: str | Path | None = None,
        products_table: str = "mpstats_products",
    ) -> None:
        self.project_root = Path(project_root)
        self.workdir = Path(workdir)
        self.db_path = Path(db_path) if db_path else self.project_root / "mpstats.duckdb"
        self.products_table = products_table
        self.projects_root = self.project_root / "data" / "projects"

    def list_projects(self) -> list[dict[str, object]]:
        sources = self._discover_sources()
        return [self._source_payload(source) for source in sorted(sources.values(), key=lambda item: item.project_name.lower())]

    def resolve_source(self, project_name: str) -> QualityDataSource:
        normalized = project_name.strip()
        sources = self._discover_sources()
        if normalized in sources:
            return sources[normalized]

        safe = _safe_segment(normalized)
        if safe in sources:
            return sources[safe]

        raise FileNotFoundError(f"Источник качества для проекта «{project_name}» не найден.")

    def read_dataframe(self, source: QualityDataSource) -> pd.DataFrame:
        if not source.paths:
            raise FileNotFoundError(f"Источник качества для проекта «{source.project_name}» не найден.")

        frames: list[pd.DataFrame] = []
        for path in source.paths:
            frame = read_csv_auto(path, low_memory=False)
            frame["__quality_source_file"] = str(path)
            frames.append(frame)
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, ignore_index=True, sort=False)

    def _discover_sources(self) -> dict[str, QualityDataSource]:
        sources: dict[str, QualityDataSource] = {}
        sources.update(self._discover_cube_sources())

        # Legacy fallback for projects that have not yet been saved to the cube.
        # The web workflow should normally show cube sources; this branch is only
        # for old local data directories.
        for project_dir in self._iter_project_dirs():
            project_name = project_dir.name
            if project_name in sources:
                continue
            source = self._project_files_source(project_name, project_dir)
            if source is not None:
                sources[project_name] = source

        return sources

    def _discover_cube_sources(self) -> dict[str, QualityDataSource]:
        if not self.db_path.exists():
            return {}
        quote_identifier(self.products_table)
        with connect(self.db_path) as con:
            apply_migrations(con)
            table_exists = bool(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name = ?
                    """,
                    [self.products_table],
                ).fetchone()[0]
            )
            if not table_exists:
                return {}
            columns = {
                str(row[1])
                for row in con.execute(f"PRAGMA table_info({quote_identifier(self.products_table)})").fetchall()
            }
            if "__project_name" not in columns:
                return {}
            product_counts = {
                str(row[0]): int(row[1] or 0)
                for row in con.execute(
                    f"""
                    SELECT
                        CAST({quote_name('__project_name')} AS VARCHAR) AS project_name,
                        COUNT(*) AS row_count
                    FROM {quote_identifier(self.products_table)}
                    WHERE {quote_name('__project_name')} IS NOT NULL
                        AND TRIM(CAST({quote_name('__project_name')} AS VARCHAR)) <> ''
                    GROUP BY {quote_name('__project_name')}
                    ORDER BY project_name
                    """
                ).fetchall()
            }
            registry_counts = {
                str(row[0]): int(row[1] or 0)
                for row in con.execute(
                    """
                    SELECT project_name, COUNT(*) AS slice_count
                    FROM cube_registry
                    GROUP BY project_name
                    """
                ).fetchall()
            }
            projects = sorted(set(product_counts) | set(registry_counts), key=str.lower)

        return {
            project_name: QualityDataSource(
                project_name=project_name,
                source_kind="cube",
                source_scope="duckdb",
                paths=(self.db_path,),
                table_name=self.products_table,
                row_count=product_counts.get(project_name, 0),
                slice_count=registry_counts.get(project_name, 0),
            )
            for project_name in projects
        }

    def _iter_project_dirs(self) -> list[Path]:
        if not self.projects_root.exists():
            return []
        return sorted(path for path in self.projects_root.iterdir() if path.is_dir())

    def _project_files_source(self, project_name: str, project_dir: Path) -> QualityDataSource | None:
        processed_dir = project_dir / "processed"
        classified = tuple(sorted(path for path in processed_dir.rglob("*_classified.csv") if path.is_file())) if processed_dir.exists() else ()
        if classified:
            return QualityDataSource(
                project_name=project_name,
                source_kind="classified",
                source_scope="project_files",
                paths=classified,
            )

        merged_dir = project_dir / "merged"
        merged = tuple(sorted(path for path in merged_dir.rglob("*.csv") if path.is_file())) if merged_dir.exists() else ()
        if merged:
            return QualityDataSource(
                project_name=project_name,
                source_kind="merged",
                source_scope="project_files",
                paths=merged,
                fallback_used=True,
            )
        return None

    def _source_payload(self, source: QualityDataSource) -> dict[str, object]:
        updated_at = max((path.stat().st_mtime for path in source.paths if path.exists()), default=0.0)
        primary = source.primary_path
        return {
            "project_name": source.project_name,
            "source_kind": source.source_kind,
            "source_scope": source.source_scope,
            "file_count": source.file_count,
            "path": str(primary) if primary else "",
            "paths": [str(path) for path in source.paths[:10]],
            "fallback_used": source.fallback_used,
            "table_name": source.table_name,
            "row_count": source.row_count,
            "slice_count": source.slice_count,
            "updated_at": datetime.fromtimestamp(updated_at).isoformat(timespec="seconds") if updated_at else None,
        }


def _safe_segment(value: str) -> str:
    segment = re.sub(r"[^\w_.-]+", "_", value.strip(), flags=re.UNICODE)
    return segment.strip("._") or "mpstats"


def quote_name(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
