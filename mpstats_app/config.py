from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppSettings:
    project_root: Path
    workdir: Path
    db_path: Path
    config_path: Path
    rules_path: Path
    products_table: str = "mpstats_products"
    scheduler_poll_seconds: float = 10.0
    static_dir: Path | None = None

    @classmethod
    def create(
        cls,
        *,
        project_root: str | Path | None = None,
        workdir: str | Path | None = None,
        db_path: str | Path | None = None,
        config_path: str | Path | None = None,
        rules_path: str | Path | None = None,
        products_table: str = "mpstats_products",
        scheduler_poll_seconds: float = 10.0,
        static_dir: str | Path | None = None,
    ) -> "AppSettings":
        root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parent.parent
        wd = Path(workdir).resolve() if workdir else root / "pipeline"
        return cls(
            project_root=root,
            workdir=wd,
            db_path=Path(db_path).resolve() if db_path else root / "mpstats.duckdb",
            config_path=Path(config_path).resolve() if config_path else wd / "step1_export_config.json",
            rules_path=Path(rules_path).resolve() if rules_path else root / "classifiers" / "rules.csv",
            products_table=products_table,
            scheduler_poll_seconds=scheduler_poll_seconds,
            static_dir=Path(static_dir).resolve() if static_dir else root / "web" / "dist",
        )
