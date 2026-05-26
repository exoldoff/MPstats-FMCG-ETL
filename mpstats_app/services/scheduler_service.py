from __future__ import annotations

from datetime import datetime
from threading import Event, Thread
import time

from mpstats_app.config import AppSettings
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.services.job_service import JobService, RunRequest


class SchedulerService:
    def __init__(
        self,
        *,
        settings: AppSettings,
        repository: DuckDbAppRepository,
        job_service: JobService,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.job_service = job_service
        self._stop = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(target=self._loop, name="mpstats-app-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(self.settings.scheduler_poll_seconds)

    def tick(self) -> None:
        now = datetime.now()
        for schedule in self.repository.due_schedules(now):
            interval = max(1, int(schedule["interval_minutes"]))
            self.repository.mark_schedule_triggered(str(schedule["schedule_id"]), now=now, interval_minutes=interval)
            self.job_service.create_run(
                RunRequest(
                    project_name=str(schedule["project_name"]),
                    steps=str(schedule["steps"]),
                    source="schedule",
                    schedule_id=str(schedule["schedule_id"]),
                    write_xlsx=bool(schedule["write_xlsx"]),
                    max_weight_kg=float(schedule["max_weight_kg"]),
                    fill_unclassified=JobService._load_fill_unclassified(schedule.get("fill_unclassified_json")),
                )
            )
            time.sleep(0.05)
