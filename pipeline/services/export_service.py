from __future__ import annotations

import calendar
import csv
from dataclasses import dataclass
from datetime import date, datetime
import io
from pathlib import Path
import random
import re
import time
from typing import Any
import zipfile

import requests

from pipeline.models import StepResult
from pipeline.step1_config import build_runtime_step1_settings, load_step1_config


EXPORT_ENDPOINTS = {
    "oz": "https://mpstats.io/api/oz/get/category",
    "wb": "https://mpstats.io/api/wb/get/category",
    "ym": "https://mpstats.io/api/ym/get/category",
}

BASE_BODY = {
    "dataFilter": {},
    "filterModel": {},
    "sortModel": [{"colId": "sales", "sort": "desc", "sortIndex": 0}],
    "fields": [
        "ag-Grid-AutoColumn",
        "ag-Grid-ControlsColumn",
        "id",
        "brand",
        "name",
        "sales",
        "seller",
        "final_price_average",
        "revenue",
    ],
    "exportFileName": "TEMP",
}

BASE_BODY_YM_FIELDS = [
    "ag-Grid-AutoColumn",
    "ag-Grid-ControlsColumn",
    "name",
    "sku",
    "brand",
    "sales",
    "price",
    "discount",
    "basic_price",
    "revenue",
]


@dataclass(frozen=True)
class ExportSettings:
    export_months_by_year: dict[int, tuple[int, ...]]
    save_dir: Path
    skip_if_exists: bool
    extract_zip: bool
    cookie: str
    tasks: list[dict[str, Any]]


def load_export_settings(config_path: str | Path, *, default_save_dir: str | Path) -> ExportSettings:
    cfg = load_step1_config(config_path)
    runtime = build_runtime_step1_settings(cfg, default_save_dir=default_save_dir)
    return ExportSettings(
        export_months_by_year=runtime["EXPORT_MONTHS_BY_YEAR"],
        save_dir=Path(runtime["SAVE_DIR"]),
        skip_if_exists=bool(runtime["SKIP_IF_EXISTS"]),
        extract_zip=bool(runtime["EXTRACT_ZIP"]),
        cookie=str(runtime["COOKIE"]),
        tasks=list(runtime["TASKS"]),
    )


def month_range(year: int, month: int) -> tuple[date, date]:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def ymd(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def safe_filename(name: str) -> str:
    normalized = re.sub(r'[<>:"/\\|?*]+', "_", name)
    normalized = normalized.strip().strip(".")
    return normalized[:180] if len(normalized) > 180 else normalized


def is_zip_bytes(content: bytes) -> bool:
    return len(content) >= 4 and content[:2] == b"PK"


def pick_csv_from_zip(zip_bytes: bytes) -> tuple[str, bytes] | None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for name in archive.namelist():
            if name.lower().endswith(".csv"):
                return name, archive.read(name)
    return None


def marketplace_file_prefix(mp: str) -> str:
    return {"oz": "Ozon", "wb": "WB", "ym": "YM"}.get(mp, mp.upper())


def export_basename(mp: str, category_path: str, d1s: str, d2s: str, cat: str | None = None) -> str:
    base = safe_filename(
        f"{marketplace_file_prefix(mp)}_-_Категории_-_{category_path.replace('/', '_')}_{d1s}-{d2s}"
    )
    if cat:
        return f"{base}__{cat}"
    return base


def existing_step1_export(
    save_dir: Path,
    mp: str,
    category_path: str,
    d1s: str,
    d2s: str,
    cat: str | None,
) -> tuple[str, Path | None, str | None]:
    base_name = export_basename(mp, category_path, d1s, d2s, cat=cat)
    for suffix in (".csv", ".zip"):
        exact = save_dir / f"{base_name}{suffix}"
        if exact.exists():
            return base_name, exact, "exact"

    category = (cat or "").strip()
    if not category:
        return base_name, None, None

    pattern = re.compile(
        "^"
        + re.escape(marketplace_file_prefix(mp))
        + r"_-_Категории_-_.*_"
        + re.escape(d1s)
        + r"-"
        + re.escape(d2s)
        + r"__"
        + re.escape(category)
        + r"\.(?:csv|zip)$",
        re.IGNORECASE,
    )
    for path in save_dir.iterdir():
        if path.is_file() and pattern.match(path.name):
            return base_name, path, "fuzzy"
    return base_name, None, None


class ExportLogger:
    def __init__(self, log_dir: str | Path) -> None:
        self.path = Path(log_dir) / f"step1_export_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
        self.index = 0

    def init(self) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(
                [
                    "index",
                    "datetime",
                    "file_name",
                    "status",
                    "reason",
                    "mp",
                    "year_month",
                    "category",
                    "http_status",
                    "error_type",
                ]
            )
        return self.path

    def row(
        self,
        *,
        file_name: str,
        status: str,
        reason: str = "",
        mp: str = "",
        year_month: str = "",
        category: str = "",
        http_status: str = "",
        error_type: str = "",
    ) -> None:
        self.index += 1
        safe_reason = (reason or "").replace("\n", " ").replace("\r", "")[:4000]
        with self.path.open("a", newline="", encoding="utf-8-sig") as file:
            csv.writer(file, delimiter=";").writerow(
                [
                    self.index,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    file_name,
                    status,
                    safe_reason,
                    mp,
                    year_month,
                    category,
                    http_status,
                    error_type,
                ]
            )


def error_details(exc: BaseException) -> tuple[str, str, str]:
    reason = str(exc)
    err_type = type(exc).__name__
    http_status = ""
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None) is not None:
        http_status = str(resp.status_code)
    return reason, http_status, err_type


def wait_for_report_ready(
    session: requests.Session,
    endpoint: str,
    params: dict[str, Any],
    body: dict[str, Any],
    *,
    max_wait_sec: int = 240,
    poll_min: float = 2.0,
    poll_max: float = 6.0,
) -> dict[str, Any]:
    started_at = time.time()
    last_data: Any = None

    while True:
        response = session.post(endpoint, params=params, json=body, timeout=120)
        response.raise_for_status()
        data = response.json()
        last_data = data

        if isinstance(data, dict) and isinstance(data.get("path"), str) and data.get("path"):
            return data

        result = data.get("result") if isinstance(data, dict) else None
        if isinstance(result, dict) and isinstance(result.get("path"), str) and result.get("path"):
            data["path"] = result["path"]
            return data

        status = data.get("status") if isinstance(data, dict) else None
        message = (data.get("message") or "") if isinstance(data, dict) else ""
        if status in (2, "2") or "Готовим отчет" in message:
            if time.time() - started_at > max_wait_sec:
                raise RuntimeError(f"Отчёт не успел подготовиться за {max_wait_sec}s. Последний ответ: {last_data}")
            time.sleep(random.uniform(poll_min, poll_max))
            continue

        raise RuntimeError(f"Неожиданный ответ MPStats: {data}")


def build_session(cookie: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Cookie": cookie,
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://mpstats.io",
            "Referer": "https://mpstats.io/",
        }
    )
    return session


def export_one_month(
    session: requests.Session,
    settings: ExportSettings,
    task: dict[str, Any],
    *,
    year: int,
    month: int,
    max_wait_sec: int = 240,
    request_timeout: int = 300,
) -> Path:
    mp = str(task["mp"])
    category_path = str(task["path"])
    fbs = int(task.get("fbs", 0))
    cat = task.get("cat")
    endpoint = EXPORT_ENDPOINTS[mp]

    d1, d2 = month_range(year, month)
    d1s, d2s = ymd(d1), ymd(d2)

    params: dict[str, Any] = {
        "d1": d1s,
        "d2": d2s,
        "path": category_path,
        "type": "csv",
        "salesFields": "0",
        "stocksFields": "0",
        "observeFields": "0",
        "priceFields": "0",
    }
    if fbs:
        params["fbs"] = "1"

    body = dict(BASE_BODY)
    if mp == "ym":
        body["fields"] = list(BASE_BODY_YM_FIELDS)
    if "filterModel" in task:
        body["filterModel"] = task["filterModel"]

    base_name = export_basename(mp, category_path, d1s, d2s, cat=str(cat) if cat else None)
    body["exportFileName"] = base_name

    data = wait_for_report_ready(session, endpoint, params, body, max_wait_sec=max_wait_sec)
    file_url = "https://mpstats.io" + data["path"]
    response = session.get(file_url, timeout=request_timeout)
    response.raise_for_status()
    content = response.content

    is_zip = (
        "zip" in response.headers.get("Content-Type", "").lower()
        or file_url.lower().endswith(".zip")
        or is_zip_bytes(content)
    )

    if is_zip:
        zip_path = settings.save_dir / f"{base_name}.zip"
        zip_path.write_bytes(content)
        if not settings.extract_zip:
            return zip_path

        picked = pick_csv_from_zip(content)
        if picked is None:
            raise RuntimeError(f"ZIP скачался, но внутри не найден CSV: {zip_path}")
        _, csv_bytes = picked
        csv_path = settings.save_dir / f"{base_name}.csv"
        csv_path.write_bytes(csv_bytes)
        return csv_path

    csv_path = settings.save_dir / f"{base_name}.csv"
    csv_path.write_bytes(content)
    return csv_path


def run_export(settings: ExportSettings, *, log_dir: str | Path | None = None) -> StepResult:
    settings.save_dir.mkdir(parents=True, exist_ok=True)
    logger = ExportLogger(log_dir or settings.save_dir.parent / "logs")
    logger_path = logger.init()
    result = StepResult(name="step1_export", output=logger_path)

    if not settings.cookie.strip():
        raise ValueError("В step1 config пустой cookie. Заполните cookie перед выгрузкой MPStats.")

    session = build_session(settings.cookie)
    for task in settings.tasks:
        mp = str(task["mp"])
        category_path = str(task["path"])
        category = str(task.get("cat") or "")

        for year in sorted(settings.export_months_by_year):
            for month in settings.export_months_by_year[year]:
                year_month = f"{year}-{month:02d}"
                try:
                    d1, d2 = month_range(year, int(month))
                    d1s, d2s = ymd(d1), ymd(d2)
                    base_name, existing, match_kind = existing_step1_export(
                        settings.save_dir,
                        mp,
                        category_path,
                        d1s,
                        d2s,
                        category,
                    )
                    if settings.skip_if_exists and existing is not None:
                        result.skipped += 1
                        logger.row(
                            file_name=f"{base_name}.csv",
                            status="пропуск",
                            reason="SKIP_IF_EXISTS: файл уже есть" + (" (МП+период+cat)" if match_kind == "fuzzy" else ""),
                            mp=mp,
                            year_month=year_month,
                            category=category,
                        )
                        result.add_detail(status="skip", mp=mp, year_month=year_month, file=str(existing))
                        continue

                    saved = export_one_month(session, settings, task, year=year, month=int(month))
                    result.ok += 1
                    logger.row(
                        file_name=saved.name,
                        status="получилось",
                        mp=mp,
                        year_month=year_month,
                        category=category,
                    )
                    result.add_detail(status="ok", mp=mp, year_month=year_month, file=str(saved))
                    time.sleep(random.uniform(2.0, 5.0))
                except Exception as exc:
                    result.errors += 1
                    reason, http_status, error_type = error_details(exc)
                    d1, d2 = month_range(year, int(month))
                    base_name = export_basename(mp, category_path, ymd(d1), ymd(d2), category)
                    logger.row(
                        file_name=f"{base_name}.csv",
                        status="не получилось",
                        reason=reason,
                        mp=mp,
                        year_month=year_month,
                        category=category,
                        http_status=http_status,
                        error_type=error_type,
                    )
                    result.add_detail(status="error", mp=mp, year_month=year_month, error=reason)
                    time.sleep(random.uniform(3.0, 8.0))

    return result
