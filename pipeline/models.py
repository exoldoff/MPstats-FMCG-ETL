from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelinePaths:
    project_root: Path
    workdir: Path
    project_name: str

    @classmethod
    def create(
        cls,
        *,
        project_root: str | Path | None = None,
        workdir: str | Path | None = None,
        project_name: str = "mpstats",
    ) -> "PipelinePaths":
        root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parent.parent
        wd = Path(workdir).resolve() if workdir else root / "pipeline"
        return cls(project_root=root, workdir=wd, project_name=project_name)

    @property
    def step1_raw_dir(self) -> Path:
        return self.workdir / "01_step1_raw"

    @property
    def step2_enriched_dir(self) -> Path:
        return self.workdir / "02_step2_enriched"

    @property
    def step3_standardized_dir(self) -> Path:
        return self.workdir / "03_step3_standardized"

    @property
    def step4_parsed_dir(self) -> Path:
        return self.workdir / "04_step4_parsed"

    @property
    def logs_dir(self) -> Path:
        return self.workdir / "logs"

    @property
    def merged_csv(self) -> Path:
        return self.workdir / f"03_{self.project_name}_merged.csv"

    @property
    def classified_csv(self) -> Path:
        return self.merged_csv.with_name(f"{self.merged_csv.stem}_classified.csv")

    @property
    def weighted_xlsx(self) -> Path:
        return self.workdir / f"04_{self.project_name}_weighted.xlsx"

    def ensure_dirs(self) -> None:
        for path in (
            self.workdir,
            self.step1_raw_dir,
            self.step2_enriched_dir,
            self.step3_standardized_dir,
            self.step4_parsed_dir,
            self.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class StepResult:
    name: str
    ok: int = 0
    errors: int = 0
    skipped: int = 0
    rows: int = 0
    output: Path | None = None
    details: list[dict[str, Any]] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.errors > 0

    def add_detail(self, **kwargs: Any) -> None:
        self.details.append(kwargs)
