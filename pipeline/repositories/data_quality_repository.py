from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re

import pandas as pd

from pipeline.repositories.file_repository import read_csv_auto


@dataclass(frozen=True)
class QualityDataSource:
    project_name: str
    source_kind: str
    source_scope: str
    paths: tuple[Path, ...]
    fallback_used: bool = False

    @property
    def file_count(self) -> int:
        return len(self.paths)

    @property
    def primary_path(self) -> Path | None:
        return self.paths[0] if self.paths else None


class DataQualityRepository:
    def __init__(self, *, project_root: str | Path, workdir: str | Path) -> None:
        self.project_root = Path(project_root)
        self.workdir = Path(workdir)
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

        raise FileNotFoundError(f"Итоговый CSV для проекта «{project_name}» не найден.")

    def read_dataframe(self, source: QualityDataSource) -> pd.DataFrame:
        if not source.paths:
            raise FileNotFoundError(f"Итоговый CSV для проекта «{source.project_name}» не найден.")

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

        legacy = self._legacy_sources()
        sources.update(legacy)

        for project_dir in self._iter_project_dirs():
            project_name = project_dir.name
            if project_name in sources:
                continue
            source = self._project_files_source(project_name, project_dir)
            if source is not None:
                sources[project_name] = source

        return sources

    def _legacy_sources(self) -> dict[str, QualityDataSource]:
        found: dict[str, dict[str, Path]] = {}
        if not self.workdir.exists():
            return {}

        for path in self.workdir.glob("03_*_merged*.csv"):
            if not path.is_file():
                continue
            project_name = _legacy_project_name(path.name)
            if not project_name:
                continue
            item = found.setdefault(project_name, {})
            if path.name.endswith("_merged_classified.csv"):
                item["classified"] = path
            elif path.name.endswith("_merged.csv"):
                item["merged"] = path

        out: dict[str, QualityDataSource] = {}
        for project_name, paths in found.items():
            if "classified" in paths:
                out[project_name] = QualityDataSource(
                    project_name=project_name,
                    source_kind="classified",
                    source_scope="legacy",
                    paths=(paths["classified"],),
                )
            elif "merged" in paths:
                out[project_name] = QualityDataSource(
                    project_name=project_name,
                    source_kind="merged",
                    source_scope="legacy",
                    paths=(paths["merged"],),
                    fallback_used=True,
                )
        return out

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
            "updated_at": datetime.fromtimestamp(updated_at).isoformat(timespec="seconds") if updated_at else None,
        }


def _legacy_project_name(filename: str) -> str | None:
    if not filename.startswith("03_"):
        return None
    if filename.endswith("_merged_classified.csv"):
        return filename.removeprefix("03_").removesuffix("_merged_classified.csv")
    if filename.endswith("_merged.csv"):
        return filename.removeprefix("03_").removesuffix("_merged.csv")
    return None


def _safe_segment(value: str) -> str:
    segment = re.sub(r"[^\w_.-]+", "_", value.strip(), flags=re.UNICODE)
    return segment.strip("._") or "mpstats"
