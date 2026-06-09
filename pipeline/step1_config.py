from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_FILENAME = "step1_export_config.json"
DEFAULT_ARCHIVE_FILENAME = "справочник tasks архив.md"


def default_config_path(base_dir: str | Path | None = None) -> Path:
    if base_dir is None:
        return Path(__file__).resolve().parent / DEFAULT_CONFIG_FILENAME
    return Path(base_dir) / DEFAULT_CONFIG_FILENAME


def archive_tasks_path(project_root: str | Path | None = None) -> Path:
    if project_root is None:
        return Path(__file__).resolve().parent.parent / DEFAULT_ARCHIVE_FILENAME
    return Path(project_root) / DEFAULT_ARCHIVE_FILENAME


def _to_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "да"}


def _normalize_months(value: object) -> list[int]:
    if value is None:
        return []

    if isinstance(value, int):
        value = [value]
    elif isinstance(value, range):
        value = list(value)
    elif isinstance(value, str):
        items: list[int] = []
        chunks = [chunk.strip() for chunk in value.split(",")]
        for chunk in chunks:
            if chunk == "":
                continue
            if "-" in chunk:
                left, right = chunk.split("-", 1)
                start = int(left.strip())
                end = int(right.strip())
                step = 1 if start <= end else -1
                items.extend(range(start, end + step, step))
            else:
                items.append(int(chunk))
        value = items
    elif not isinstance(value, (list, tuple, set)):
        raise ValueError(f"Unsupported months value: {value!r}")

    out = sorted({int(month) for month in value if 1 <= int(month) <= 12})
    return out


def normalize_export_months_by_year(raw: object) -> dict[str, list[int]]:
    if not isinstance(raw, dict):
        return {}

    out: dict[str, list[int]] = {}
    for year_raw, months_raw in raw.items():
        year = str(year_raw).strip()
        if year == "":
            continue
        if not year.isdigit():
            raise ValueError(f"Invalid year key in export_months_by_year: {year!r}")
        months = _normalize_months(months_raw)
        if months:
            out[year] = months
    return dict(sorted(out.items(), key=lambda item: int(item[0])))


def _normalize_filter_model(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in ("", None):
        return {}
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return {}
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        raise ValueError("filterModel JSON must be an object")
    raise ValueError(f"Unsupported filterModel value: {value!r}")


def _normalize_source_type(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"subject", "предмет", "по предмету"}:
        return "subject"
    return "category"


def normalize_task(task: object) -> dict[str, Any]:
    if not isinstance(task, dict):
        raise ValueError("Task must be a dict")

    mp = str(task.get("mp", "")).strip().lower()
    path = str(task.get("path", "")).strip()
    cat = str(task.get("cat", "")).strip()
    if mp not in {"oz", "wb", "ym"}:
        raise ValueError(f"Task has invalid mp={mp!r}")
    if path == "":
        raise ValueError("Task has empty path")
    if cat == "":
        raise ValueError("Task has empty cat")

    out: dict[str, Any] = {
        "active": _to_bool(task.get("active"), default=True),
        "mp": mp,
        "path": path,
        "cat": cat,
        "source_type": _normalize_source_type(task.get("source_type")),
    }
    if out["source_type"] == "subject" and mp == "ym":
        out["source_type"] = "category"

    if "fbs" in task and task.get("fbs") not in ("", None):
        out["fbs"] = int(task["fbs"])

    filter_model = _normalize_filter_model(task.get("filterModel"))
    if filter_model:
        out["filterModel"] = filter_model

    return out


def normalize_config(config: object) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError("Step1 config must be a JSON object")

    raw_tasks = config.get("tasks", [])
    if not isinstance(raw_tasks, list):
        raise ValueError("'tasks' must be a list")

    tasks = [normalize_task(task) for task in raw_tasks]

    return {
        "export_months_by_year": normalize_export_months_by_year(
            config.get("export_months_by_year", {})
        ),
        "save_dir": str(config.get("save_dir", "")).strip(),
        "skip_if_exists": _to_bool(config.get("skip_if_exists"), default=True),
        "extract_zip": _to_bool(config.get("extract_zip"), default=True),
        "cookie": str(config.get("cookie", "")),
        "api_token": str(config.get("api_token", "")),
        "tasks": tasks,
    }


def runtime_template_from_legacy(
    *,
    export_months_by_year: object,
    save_dir: str | Path | None,
    skip_if_exists: object,
    extract_zip: object,
    cookie: object,
    tasks: object,
) -> dict[str, Any]:
    raw_tasks = tasks if isinstance(tasks, list) else []
    template = {
        "export_months_by_year": normalize_export_months_by_year(export_months_by_year),
        "save_dir": "" if save_dir is None else str(save_dir),
        "skip_if_exists": _to_bool(skip_if_exists, default=True),
        "extract_zip": _to_bool(extract_zip, default=True),
        "cookie": "" if cookie is None else str(cookie),
        "api_token": "",
        "tasks": [],
    }
    for task in raw_tasks:
        normalized = normalize_task({**task, "active": task.get("active", True)})
        template["tasks"].append(normalized)
    return template


def ensure_config_file(config_path: str | Path, config_data: dict[str, Any]) -> Path:
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        save_config(config_data, path)
    return path


def load_step1_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Step1 config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return normalize_config(raw)


def save_config(config_data: dict[str, Any], config_path: str | Path) -> Path:
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_config(config_data)
    with path.open("w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return path


def build_runtime_step1_settings(
    config_data: dict[str, Any],
    *,
    default_save_dir: str | Path,
) -> dict[str, Any]:
    normalized = normalize_config(config_data)

    export_months: dict[int, tuple[int, ...]] = {
        int(year): tuple(months)
        for year, months in normalized["export_months_by_year"].items()
    }
    active_tasks = []
    for task in normalized["tasks"]:
        if not _to_bool(task.get("active"), default=True):
            continue
        runtime_task = {
            "mp": task["mp"],
            "path": task["path"],
            "cat": task["cat"],
            "source_type": task.get("source_type") or "category",
        }
        if "fbs" in task:
            runtime_task["fbs"] = int(task["fbs"])
        if task.get("filterModel"):
            runtime_task["filterModel"] = task["filterModel"]
        active_tasks.append(runtime_task)

    save_dir_value = normalized["save_dir"] or str(default_save_dir)
    return {
        "EXPORT_MONTHS_BY_YEAR": export_months,
        "SAVE_DIR": Path(save_dir_value),
        "SKIP_IF_EXISTS": normalized["skip_if_exists"],
        "EXTRACT_ZIP": normalized["extract_zip"],
        "COOKIE": normalized["cookie"],
        "API_TOKEN": normalized.get("api_token", ""),
        "TASKS": active_tasks,
    }


def _extract_python_dict_chunks(text: str) -> list[str]:
    chunks: list[str] = []
    start_idx: int | None = None
    depth = 0
    quote: str | None = None
    escaped = False

    for idx, char in enumerate(text):
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        if char in {"'", '"'}:
            quote = char
            continue
        if char == "{":
            if depth == 0:
                start_idx = idx
            depth += 1
            continue
        if char == "}":
            if depth == 0:
                continue
            depth -= 1
            if depth == 0 and start_idx is not None:
                chunks.append(text[start_idx : idx + 1])
                start_idx = None
    return chunks


def load_tasks_from_archive(archive_path: str | Path) -> list[dict[str, Any]]:
    path = Path(archive_path)
    if not path.exists():
        raise FileNotFoundError(f"Archive not found: {path}")
    text = path.read_text(encoding="utf-8")
    chunks = _extract_python_dict_chunks(text)

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for chunk in chunks:
        try:
            parsed = ast.literal_eval(chunk)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        if not {"mp", "path", "cat"}.issubset(parsed.keys()):
            continue
        try:
            normalized = normalize_task({**parsed, "active": True})
        except Exception:
            continue
        key = json.dumps(
            {
                "mp": normalized["mp"],
                "path": normalized["path"],
                "cat": normalized["cat"],
                "source_type": normalized.get("source_type", "category"),
                "fbs": normalized.get("fbs"),
                "filterModel": normalized.get("filterModel", {}),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped
