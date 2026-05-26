from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Thread
import traceback
from typing import Any
from uuid import uuid4

from pipeline.models import PipelinePaths, StepResult
from pipeline.services.run_service import parse_steps, run_pipeline

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository


STEP_NAMES = {
    1: "export",
    2: "enrich",
    3: "standardize",
    4: "parse_weights",
    5: "merge",
    6: "classify",
}


@dataclass(frozen=True)
class RunRequest:
    project_name: str
    steps: str = "2-6"
    source: str = "manual"
    schedule_id: str | None = None
    write_xlsx: bool = True
    max_weight_kg: float = 40.0
    fill_unclassified: dict[str, Any] | None = None


class JobService:
    def __init__(self, *, settings: AppSettings, repository: DuckDbAppRepository) -> None:
        self.settings = settings
        self.repository = repository
        self._queue: Queue[str | None] = Queue()
        self._stop = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(target=self._worker, name="mpstats-app-jobs", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._queue.put(None)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def create_run(self, request: RunRequest) -> dict[str, Any]:
        steps = ",".join(str(step) for step in parse_steps(request.steps))
        run_id = uuid4().hex
        run = self.repository.create_run(
            run_id=run_id,
            project_name=request.project_name.strip() or "mpstats",
            steps=steps,
            source=request.source,
            schedule_id=request.schedule_id,
            workdir=self.settings.workdir,
            config_path=self.settings.config_path,
            rules_path=self.settings.rules_path,
            db_path=self.settings.db_path,
            products_table=self.settings.products_table,
            write_xlsx=request.write_xlsx,
            max_weight_kg=request.max_weight_kg,
            fill_unclassified=request.fill_unclassified,
        )
        self._queue.put(run_id)
        return run

    def cancel_run(self, run_id: str) -> dict[str, Any] | None:
        run = self.repository.get_run(run_id)
        if not run:
            return None
        self.repository.request_cancel(run_id)
        if run.get("status") == "queued":
            self.repository.finish_run(run_id, "cancelled", error="cancelled before start")
        return self.repository.get_run(run_id)

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                run_id = self._queue.get(timeout=0.5)
            except Empty:
                continue
            if run_id is None:
                continue
            try:
                self._process_run(run_id)
            finally:
                self._queue.task_done()

    def _process_run(self, run_id: str) -> None:
        run = self.repository.get_run(run_id)
        if not run or run.get("status") == "cancelled":
            return
        if self.repository.is_cancel_requested(run_id):
            self.repository.finish_run(run_id, "cancelled", error="cancelled before start")
            return

        self.repository.mark_run_running(run_id)
        self.repository.add_event(run_id, "info", "Прогон запущен", {"project_name": run["project_name"]})

        try:
            steps = parse_steps(str(run["steps"]))
            fill_unclassified = self._load_fill_unclassified(run.get("fill_unclassified_json"))
            paths = PipelinePaths.create(
                project_root=self.settings.project_root,
                workdir=Path(str(run["workdir"])),
                project_name=str(run["project_name"]),
            )
            final_manifest: str | None = None

            for step in steps:
                if self.repository.is_cancel_requested(run_id):
                    self.repository.finish_run(run_id, "cancelled", error="cancelled between steps")
                    self.repository.add_event(run_id, "warning", "Прогон отменён между шагами", {"step": step})
                    return

                step_name = STEP_NAMES.get(step, f"step_{step}")
                self.repository.record_step(
                    run_id=run_id,
                    step_number=step,
                    step_name=step_name,
                    status="running",
                )
                self.repository.add_event(run_id, "info", f"Шаг {step} запущен", {"step_name": step_name})

                manifest_path = paths.logs_dir / f"app_run_{run_id}_step_{step}.json"
                results = run_pipeline(
                    paths=paths,
                    steps=[step],
                    config_path=Path(str(run["config_path"])),
                    rules_path=Path(str(run["rules_path"])),
                    write_xlsx=bool(run["write_xlsx"]),
                    max_weight_kg=float(run["max_weight_kg"]),
                    fill_unclassified=fill_unclassified,
                    manifest_path=manifest_path,
                )
                final_manifest = str(manifest_path)
                result = results[0] if results else StepResult(name=step_name, errors=1)
                status = "failed" if result.failed else "succeeded"
                self.repository.record_step(
                    run_id=run_id,
                    step_number=step,
                    step_name=result.name,
                    status=status,
                    rows=result.rows,
                    ok_count=result.ok,
                    error_count=result.errors,
                    skipped_count=result.skipped,
                    output=str(result.output) if result.output else None,
                    details=result.details,
                    error=None if not result.failed else "step failed",
                    finished=True,
                )
                self.repository.add_event(
                    run_id,
                    "error" if result.failed else "info",
                    f"Шаг {step} завершён: {status}",
                    {"rows": result.rows, "errors": result.errors, "output": str(result.output) if result.output else None},
                )
                if result.failed:
                    self.repository.finish_run(run_id, "failed", error=f"Step {step} failed", manifest_path=final_manifest)
                    return

            if 6 in steps and paths.classified_csv.exists():
                rows = self.repository.import_products_file(
                    run_id=run_id,
                    csv_path=paths.classified_csv,
                    table_name=str(run["products_table"]),
                    project_name=str(run["project_name"]),
                    load_name=f"app_run:{run_id}",
                )
                self.repository.add_event(
                    run_id,
                    "info",
                    "Итоговый CSV загружен в DuckDB",
                    {"rows": rows, "table": run["products_table"]},
                )

            self.repository.finish_run(run_id, "succeeded", manifest_path=final_manifest)
            self.repository.add_event(run_id, "info", "Прогон успешно завершён", None)
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", error=str(exc))
            self.repository.add_event(
                run_id,
                "error",
                "Прогон завершился с ошибкой",
                {"error": str(exc), "trace": traceback.format_exc()},
            )

    @staticmethod
    def _load_fill_unclassified(raw: Any) -> dict[str, Any] | None:
        if not raw:
            return None
        if isinstance(raw, dict):
            return raw
        parsed = json.loads(str(raw))
        return parsed if isinstance(parsed, dict) else None
