from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from pipeline.models import PipelinePaths, StepResult
from pipeline.repositories.file_repository import write_json
from pipeline.services.classification_service import classify_file
from pipeline.services.enrich_service import enrich_directory
from pipeline.services.export_service import load_export_settings, run_export
from pipeline.services.merge_service import merge_directory
from pipeline.services.standardize_service import standardize_directory
from pipeline.services.weight_parser_service import parse_weights_directory


def parse_steps(raw: str | Iterable[int]) -> list[int]:
    if not isinstance(raw, str):
        return sorted({int(step) for step in raw})

    steps: set[int] = set()
    for chunk in raw.split(","):
        part = chunk.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            start = int(left.strip())
            end = int(right.strip())
            if start > end:
                raise ValueError(f"Некорректный диапазон шагов: {part}")
            steps.update(range(start, end + 1))
        else:
            steps.add(int(part))

    bad = sorted(step for step in steps if step < 1 or step > 6)
    if bad:
        raise ValueError(f"Поддерживаются шаги 1..6, получено: {bad}")
    return sorted(steps)


def result_to_dict(result: StepResult) -> dict[str, object]:
    payload = asdict(result)
    if result.output is not None:
        payload["output"] = str(result.output)
    return payload


def run_pipeline(
    *,
    paths: PipelinePaths,
    steps: list[int],
    config_path: str | Path,
    rules_path: str | Path,
    write_xlsx: bool = True,
    max_weight_kg: float = 40.0,
    fill_unclassified: dict[str, object] | None = None,
    manual_overrides_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
) -> list[StepResult]:
    paths.ensure_dirs()
    results: list[StepResult] = []

    if 1 in steps:
        settings = load_export_settings(config_path, default_save_dir=paths.step1_raw_dir)
        results.append(run_export(settings, log_dir=paths.logs_dir))
    if 2 in steps:
        results.append(enrich_directory(paths.step1_raw_dir, paths.step2_enriched_dir))
    if 3 in steps:
        results.append(standardize_directory(paths.step2_enriched_dir, paths.step3_standardized_dir))
    if 4 in steps:
        results.append(parse_weights_directory(paths.step3_standardized_dir, paths.step4_parsed_dir, max_weight_kg=max_weight_kg))
    if 5 in steps:
        _, merge_result = merge_directory(paths.step4_parsed_dir, paths.merged_csv)
        results.append(merge_result)
    if 6 in steps:
        _, _, classify_result = classify_file(
            paths.merged_csv,
            paths.classified_csv,
            rules_path=rules_path,
            write_xlsx=write_xlsx,
            fill_unclassified=fill_unclassified,
            manual_overrides_path=manual_overrides_path,
        )
        results.append(classify_result)

    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "project_name": paths.project_name,
        "workdir": str(paths.workdir),
        "steps": steps,
        "config_path": str(config_path),
        "rules_path": str(rules_path),
        "results": [result_to_dict(result) for result in results],
    }
    out_manifest = Path(manifest_path) if manifest_path else paths.logs_dir / f"run_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.json"
    write_json(manifest, out_manifest)
    return results
