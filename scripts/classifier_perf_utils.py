from __future__ import annotations

import cProfile
from dataclasses import dataclass, field
import importlib
import io
import json
from pathlib import Path
import pstats
import re
import resource
import sys
import time
from typing import Any, Callable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from classifiers import engine as classifier_engine
from classifiers.engine import CONDITIONS_COLUMN, REQUIRED_RULE_COLUMNS
from pipeline.repositories.file_repository import detect_encoding_and_sep, read_csv_auto, write_semicolon_csv
from pipeline.services.classification_service import (
    postprocess_classified,
    prepare_for_classification,
    read_classification_input,
)


CLASSIFIER_SIZE_ORDER = ("small", "medium", "large")
CLASSIFIER_BENCH_ROWS = {
    "small": 10_000,
    "medium": 100_000,
    "large": 500_000,
}
DEFAULT_WORKDIR = Path("data/classifier_benchmark")
DEFAULT_RULES_PATH = Path("classifiers/rules.csv")
DEFAULT_CLASSIFICATION_COLUMNS = ("Категория", "Подкатегория", "Бренд", "Тип", "Вид мяса")


def classifier_sizes_for_args(*, size: str, all_sizes: bool, include_large: bool) -> list[str]:
    if size not in CLASSIFIER_BENCH_ROWS:
        raise ValueError(f"Unknown classifier benchmark size: {size}")
    if all_sizes:
        sizes = list(CLASSIFIER_SIZE_ORDER)
        if not include_large:
            sizes.remove("large")
        return sizes
    if size == "large" and not include_large:
        raise ValueError("Classifier benchmark size=large запускается только с --include-large.")
    return [size]


def _time_call(timings: dict[str, float], key: str, callback: Callable[[], Any]) -> Any:
    started = time.perf_counter()
    try:
        return callback()
    finally:
        timings[key] = timings.get(key, 0.0) + (time.perf_counter() - started)


def _peak_rss_mb() -> float | None:
    try:
        value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None
    if sys.platform == "darwin":
        return value / 1024 / 1024
    return value / 1024


@dataclass
class MatchInstrumentation:
    match_calls: int = 0
    condition_checks: int = 0
    match_seconds: float = 0.0
    read_rules_seconds: float = 0.0
    prepare_rules_seconds: float = 0.0
    by_match_type_seconds: dict[str, float] = field(default_factory=dict)
    by_match_type_calls: dict[str, int] = field(default_factory=dict)
    by_match_type_checks: dict[str, int] = field(default_factory=dict)

    def record_match(self, match_type: str, rows: int, elapsed: float) -> None:
        key = str(match_type or "").strip().lower() or "unknown"
        self.match_calls += 1
        self.condition_checks += rows
        self.match_seconds += elapsed
        self.by_match_type_seconds[key] = self.by_match_type_seconds.get(key, 0.0) + elapsed
        self.by_match_type_calls[key] = self.by_match_type_calls.get(key, 0) + 1
        self.by_match_type_checks[key] = self.by_match_type_checks.get(key, 0) + rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "match_calls": self.match_calls,
            "condition_checks": self.condition_checks,
            "match_seconds": self.match_seconds,
            "read_rules_seconds": self.read_rules_seconds,
            "prepare_rules_seconds": self.prepare_rules_seconds,
            "by_match_type_seconds": self.by_match_type_seconds,
            "by_match_type_calls": self.by_match_type_calls,
            "by_match_type_checks": self.by_match_type_checks,
        }


class ClassifierInstrumentation:
    def __init__(self) -> None:
        self.stats = MatchInstrumentation()
        self._original_build_match_mask: Callable[..., Any] | None = None
        self._original_load_rules: Callable[..., Any] | None = None
        self._original_validate_rules: Callable[..., Any] | None = None

    def __enter__(self) -> MatchInstrumentation:
        self._original_build_match_mask = classifier_engine._build_match_mask
        self._original_load_rules = classifier_engine.load_rules
        self._original_validate_rules = classifier_engine._validate_and_prepare_rules

        def timed_build_match_mask(series: pd.Series, match_type: str, pattern: str) -> pd.Series:
            started = time.perf_counter()
            try:
                assert self._original_build_match_mask is not None
                return self._original_build_match_mask(series, match_type, pattern)
            finally:
                self.stats.record_match(match_type, len(series), time.perf_counter() - started)

        def timed_load_rules(rules_path: str | Path) -> pd.DataFrame:
            started = time.perf_counter()
            try:
                assert self._original_load_rules is not None
                return self._original_load_rules(rules_path)
            finally:
                self.stats.read_rules_seconds += time.perf_counter() - started

        def timed_validate_rules(rules_df: pd.DataFrame) -> pd.DataFrame:
            started = time.perf_counter()
            try:
                assert self._original_validate_rules is not None
                return self._original_validate_rules(rules_df)
            finally:
                self.stats.prepare_rules_seconds += time.perf_counter() - started

        classifier_engine._build_match_mask = timed_build_match_mask
        classifier_engine.load_rules = timed_load_rules
        classifier_engine._validate_and_prepare_rules = timed_validate_rules
        return self.stats

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._original_build_match_mask is not None:
            classifier_engine._build_match_mask = self._original_build_match_mask
        if self._original_load_rules is not None:
            classifier_engine.load_rules = self._original_load_rules
        if self._original_validate_rules is not None:
            classifier_engine._validate_and_prepare_rules = self._original_validate_rules


def _classification_columns_from_report(report: pd.DataFrame, result_df: pd.DataFrame) -> list[str]:
    columns: list[str] = []
    if "target_column" in report.columns:
        for column in report["target_column"].dropna().astype(str).str.strip().tolist():
            if column and column in result_df.columns and column not in columns:
                columns.append(column)
    for column in DEFAULT_CLASSIFICATION_COLUMNS:
        if column in result_df.columns and column not in columns:
            columns.append(column)
    return columns


def _non_empty_mask(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    if not columns:
        return pd.Series(False, index=frame.index)
    mask = pd.Series(False, index=frame.index)
    for column in columns:
        text = frame[column].astype("string")
        mask |= frame[column].notna() & text.str.strip().fillna("").ne("")
    return mask


def run_instrumented_classification(
    *,
    name: str,
    input_path: Path,
    output_path: Path,
    rules_path: Path,
    enable_cprofile: bool = False,
    fill_unclassified: dict[str, object] | None = None,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    profiler = cProfile.Profile()
    if enable_cprofile:
        profiler.enable()
    total_started = time.perf_counter()
    cprofile_top = ""

    try:
        df = _time_call(timings, "read_input_seconds", lambda: read_classification_input(input_path))
        prepared = _time_call(timings, "prepare_input_seconds", lambda: prepare_for_classification(df))

        def reload_engine() -> None:
            importlib.reload(classifier_engine)

        _time_call(timings, "module_reload_seconds", reload_engine)
        with ClassifierInstrumentation() as instrumentation:
            classified, report = _time_call(
                timings,
                "apply_rules_seconds",
                lambda: classifier_engine.apply_classifiers(
                    prepared,
                    rules_path=rules_path,
                    fill_unclassified=fill_unclassified,
                ),
            )
            instrumentation_payload = instrumentation.to_dict()

        def postprocess() -> pd.DataFrame:
            result, _, _ = postprocess_classified(classified)
            return result

        result_df = _time_call(timings, "postprocess_seconds", postprocess)
        out_path = _time_call(timings, "write_output_seconds", lambda: write_semicolon_csv(result_df, output_path))
    finally:
        total_seconds = time.perf_counter() - total_started
        if enable_cprofile:
            profiler.disable()
            stream = io.StringIO()
            pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats("cumulative").print_stats(20)
            cprofile_top = stream.getvalue()

    active_report = report[report["active"] == True].copy() if "active" in report.columns else report.copy()
    classification_columns = _classification_columns_from_report(report, result_df)
    classified_mask = _non_empty_mask(result_df, classification_columns)
    rows_output = int(len(result_df))
    active_rules = int(len(active_report))
    estimated_rule_checks = rows_output * active_rules
    measured_condition_checks = int(instrumentation_payload.get("condition_checks") or 0)
    rows_per_second = rows_output / total_seconds if total_seconds > 0 else 0.0
    rules_per_second = active_rules / timings.get("apply_rules_seconds", total_seconds) if timings.get("apply_rules_seconds", 0) > 0 else 0.0
    checks_base = measured_condition_checks or estimated_rule_checks
    checks_per_second = checks_base / timings.get("apply_rules_seconds", total_seconds) if timings.get("apply_rules_seconds", 0) > 0 else 0.0
    bottlenecks = {
        "read_input": timings.get("read_input_seconds", 0.0),
        "rule_loading": instrumentation_payload.get("read_rules_seconds", 0.0)
        + instrumentation_payload.get("prepare_rules_seconds", 0.0)
        + timings.get("module_reload_seconds", 0.0),
        "apply_rules": timings.get("apply_rules_seconds", 0.0),
        "string_matching": instrumentation_payload.get("match_seconds", 0.0),
        "write_output": timings.get("write_output_seconds", 0.0),
        "postprocess": timings.get("postprocess_seconds", 0.0),
    }
    top_bottleneck = max(bottlenecks, key=bottlenecks.get)

    return {
        "name": name,
        "input_file": str(input_path),
        "output_file": str(out_path),
        "rules_file": str(rules_path),
        "rows_input": int(len(df)),
        "rows_output": rows_output,
        "rules_count": int(len(report)),
        "active_rules": active_rules,
        "classification_columns": classification_columns,
        "classified_rows": int(classified_mask.sum()),
        "unclassified_rows": int((~classified_mask).sum()),
        "total_seconds": total_seconds,
        "timings": timings,
        "instrumentation": instrumentation_payload,
        "estimated_rule_checks": estimated_rule_checks,
        "measured_condition_checks": measured_condition_checks,
        "rows_per_second": rows_per_second,
        "rules_per_second": rules_per_second,
        "checks_per_second": checks_per_second,
        "top_bottleneck": top_bottleneck,
        "top_bottleneck_seconds": bottlenecks[top_bottleneck],
        "output_file_size_bytes": out_path.stat().st_size if out_path.exists() else 0,
        "peak_rss_mb": _peak_rss_mb(),
        "cprofile_top": cprofile_top,
        "report_applied_rows_sum": int(active_report["applied_rows"].sum()) if "applied_rows" in active_report.columns else None,
    }


MOCK_TEMPLATES = (
    ("Ozon", "Мясо", "Слово Мясника", "Слово Мясника котлеты домашние охлажденные 360 г", "seller-a"),
    ("Ozon", "Мясо", "Слово Мясника", "Слово Мясника шашлык свиной классический в маринаде 1 кг", "seller-a"),
    ("WB", "Мясо", "Слово Мясника", "Сосиски сливочные Слово Мясника 420 г", "seller-b"),
    ("Яндекс Маркет", "Мясо", "No name", "Фарш домашний охлажденный 400 г", "seller-c"),
    ("Ozon", "Мыло", "Чистая линия", "Жидкое крем-мыло ромашка 500 мл", "seller-d"),
    ("WB", "Мыло хозяйственное", "Clean", "Мыло хозяйственное твердое 72 процента", "seller-e"),
    ("Ozon", "Сахар", "Русский сахар", "Сахар белый кусковой 1 кг", "seller-f"),
    ("Яндекс Маркет", "Сахар", "No name", "Песок сахарный белый 900 г", "seller-g"),
)


def generate_mock_classifier_frame(rows: int) -> pd.DataFrame:
    payload: dict[str, list[object]] = {
        "Дата": [],
        "Маркетплейс": [],
        "Категория": [],
        "SKU": [],
        "Бренд": [],
        "Название": [],
        "Продавец": [],
        "Описание": [],
        "Продажи, шт": [],
        "Средняя цена, руб": [],
    }
    for index in range(rows):
        marketplace, category, brand, name, seller = MOCK_TEMPLATES[index % len(MOCK_TEMPLATES)]
        payload["Дата"].append(f"01.{(index % 12) + 1:02d}.2025")
        payload["Маркетплейс"].append(marketplace)
        payload["Категория"].append(category)
        payload["SKU"].append(f"MOCK-{index:09d}")
        payload["Бренд"].append(brand)
        payload["Название"].append(f"{name} партия {index % 1000}")
        payload["Продавец"].append(seller)
        payload["Описание"].append(f"{name}. Тестовое описание для классификации {index % 37}.")
        payload["Продажи, шт"].append((index % 17) + 1)
        payload["Средняя цена, руб"].append(100 + (index % 500))
    return pd.DataFrame(payload)


def write_mock_classifier_input(path: Path, rows: int) -> Path:
    return write_semicolon_csv(generate_mock_classifier_frame(rows), path)


def write_mock_classifier_rules(path: Path) -> Path:
    rows = [
        {
            "active": "1",
            "priority": "10",
            "category": "Мясо",
            "target_column": "Подкатегория",
            "match_field": "Название",
            "match_type": "contains",
            "pattern": "котлет",
            "set_value": "Кулинария",
            "mode": "fill_empty",
            "comment": "cheap contains",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "20",
            "category": "Мясо",
            "target_column": "Подкатегория",
            "match_field": "Название",
            "match_type": "regex",
            "pattern": "шашл|маринад|стейк",
            "set_value": "Маринады",
            "mode": "fill_empty",
            "comment": "simple regex",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "30",
            "category": "Мясо",
            "target_column": "Подкатегория",
            "match_field": "Название",
            "match_type": "regex",
            "pattern": "(сосиск|колбас|сардельк)",
            "set_value": "Колбаски",
            "mode": "fill_empty",
            "comment": "simple regex sausage",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "40",
            "category": "Мясо",
            "target_column": "Бренд",
            "match_field": "Название",
            "match_type": "regex",
            "pattern": "(?=.*слово)(?=.*мясник).*",
            "set_value": "Слово Мясника",
            "mode": "fill_empty",
            "comment": "complex lookahead regex",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "50",
            "category": "*",
            "target_column": "Тип",
            "match_field": "Бренд",
            "match_type": "not_contains",
            "pattern": "No name",
            "set_value": "Бренд указан",
            "mode": "fill_empty",
            "comment": "negative rule",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "60",
            "category": "*",
            "target_column": "Тип",
            "match_field": "SKU",
            "match_type": "equals",
            "pattern": "MOCK-000000001",
            "set_value": "Точный SKU",
            "mode": "overwrite",
            "comment": "exact match",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "70",
            "category": "Мыло",
            "target_column": "Подкатегория",
            "match_field": "Название",
            "match_type": "contains",
            "pattern": "жидк",
            "set_value": "Жидкое",
            "mode": "fill_empty",
            "comment": "contains soap",
            CONDITIONS_COLUMN: json.dumps(
                [
                    {"join_with_prev": "or", "match_field": "Название", "match_type": "contains", "pattern": "крем"},
                ],
                ensure_ascii=False,
            ),
        },
        {
            "active": "1",
            "priority": "80",
            "category": "Мыло хозяйственное",
            "target_column": "Подкатегория",
            "match_field": "Категория",
            "match_type": "contains",
            "pattern": "хозяйств",
            "set_value": "Хозяйственное",
            "mode": "fill_empty",
            "comment": "category filtered",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "90",
            "category": "Сахар",
            "target_column": "Подкатегория",
            "match_field": "Название",
            "match_type": "regex",
            "pattern": "(сахар(ный)?|песок).*?(белый|кусковой)?",
            "set_value": "Белый сахар",
            "mode": "fill_empty",
            "comment": "complex sugar regex",
            CONDITIONS_COLUMN: "",
        },
        {
            "active": "1",
            "priority": "999",
            "category": "*",
            "target_column": "Подкатегория",
            "match_field": "",
            "match_type": "otherwise",
            "pattern": "",
            "set_value": "Прочее",
            "mode": "fill_empty",
            "comment": "fallback",
            CONDITIONS_COLUMN: "",
        },
    ]
    frame = pd.DataFrame(rows, columns=list(REQUIRED_RULE_COLUMNS) + ["comment", CONDITIONS_COLUMN])
    return write_semicolon_csv(frame, path)


def ensure_mock_classifier_files(*, workdir: Path, size: str) -> tuple[Path, Path, int]:
    rows = CLASSIFIER_BENCH_ROWS[size]
    mock_dir = workdir / "mock"
    mock_dir.mkdir(parents=True, exist_ok=True)
    input_path = mock_dir / f"mock_{size}.csv"
    rules_path = mock_dir / "mock_rules.csv"
    if not input_path.exists():
        write_mock_classifier_input(input_path, rows)
    if not rules_path.exists():
        write_mock_classifier_rules(rules_path)
    return input_path, rules_path, rows


def find_real_processed_sample(root: Path = Path("data/projects")) -> Path | None:
    if not root.exists():
        return None
    candidates = [
        path
        for path in root.rglob("*.csv")
        if path.is_file() and "/processed/" in path.as_posix() and not path.name.endswith("_classified.csv")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_size)


def write_real_sample(input_path: Path, output_path: Path, *, rows: int) -> Path:
    enc, sep = detect_encoding_and_sep(input_path)
    frame = pd.read_csv(input_path, sep=sep, encoding=enc, low_memory=False, nrows=rows)
    return write_semicolon_csv(frame, output_path)


def write_real_baseline_sample(input_path: Path, output_path: Path, *, rows: int) -> Path | None:
    baseline = input_path.with_name(f"{input_path.stem}_classified.csv")
    if not baseline.exists():
        return None
    enc, sep = detect_encoding_and_sep(baseline)
    frame = pd.read_csv(baseline, sep=sep, encoding=enc, low_memory=False, nrows=rows)
    return write_semicolon_csv(frame, output_path)


def compare_classified_outputs(
    baseline_path: Path,
    candidate_path: Path,
    *,
    columns: list[str] | None = None,
    max_differences: int = 20,
) -> dict[str, Any]:
    baseline = read_csv_auto(baseline_path, low_memory=False)
    candidate = read_csv_auto(candidate_path, low_memory=False)
    compare_columns = columns or [
        column for column in DEFAULT_CLASSIFICATION_COLUMNS if column in baseline.columns or column in candidate.columns
    ]
    row_count_match = len(baseline) == len(candidate)
    columns_match = list(baseline.columns) == list(candidate.columns)
    diff_counts: dict[str, int | None] = {}
    first_differences: list[dict[str, object]] = []
    shared_rows = min(len(baseline), len(candidate))

    for column in compare_columns:
        if column not in baseline.columns or column not in candidate.columns:
            diff_counts[column] = None
            first_differences.append(
                {
                    "row_index": None,
                    "column": column,
                    "baseline": "<missing>" if column not in baseline.columns else "<present>",
                    "candidate": "<missing>" if column not in candidate.columns else "<present>",
                }
            )
            continue
        left = baseline[column].head(shared_rows).astype("string").fillna("")
        right = candidate[column].head(shared_rows).astype("string").fillna("")
        mask = left.ne(right)
        diff_counts[column] = int(mask.sum())
        for index in mask[mask].index[: max(0, max_differences - len(first_differences))]:
            first_differences.append(
                {
                    "row_index": int(index),
                    "column": column,
                    "baseline": str(left.loc[index]),
                    "candidate": str(right.loc[index]),
                }
            )
        if len(first_differences) >= max_differences:
            break

    baseline_classified = _non_empty_mask(baseline, [c for c in compare_columns if c in baseline.columns])
    candidate_classified = _non_empty_mask(candidate, [c for c in compare_columns if c in candidate.columns])
    return {
        "baseline_file": str(baseline_path),
        "candidate_file": str(candidate_path),
        "rows_baseline": int(len(baseline)),
        "rows_candidate": int(len(candidate)),
        "row_count_match": row_count_match,
        "columns_match": columns_match,
        "compare_columns": compare_columns,
        "classified_baseline": int(baseline_classified.sum()),
        "classified_candidate": int(candidate_classified.sum()),
        "unclassified_baseline": int((~baseline_classified).sum()),
        "unclassified_candidate": int((~candidate_classified).sum()),
        "diff_counts": diff_counts,
        "first_differences": first_differences[:max_differences],
    }


def _regex_looks_expensive(pattern: str) -> bool:
    expensive_markers = (
        r"\.\*.*\.\*",
        r"\([^)]*[+*][^)]*\)[+*]",
        r"\([^)]*\.\*[^)]*\)[+*]",
        r"\.\+.*\.\+",
    )
    return any(re.search(marker, pattern) for marker in expensive_markers)


def audit_rules(rules_path: Path, *, sample_input: Path | None = None, sample_limit: int = 10_000) -> dict[str, Any]:
    raw = classifier_engine.load_rules(rules_path)
    rules = classifier_engine._validate_and_prepare_rules(raw)
    active = rules[rules["active"]].copy()
    suspicious: list[dict[str, Any]] = []

    duplicate_columns = [
        "active",
        "category",
        "target_column",
        "match_field",
        "match_type",
        "pattern",
        "set_value",
        "mode",
        CONDITIONS_COLUMN,
    ]
    duplicate_mask = rules.duplicated(subset=duplicate_columns, keep=False)
    for row in rules[duplicate_mask].itertuples(index=False):
        suspicious.append(
            {
                "rule_id": int(row.row_num),
                "pattern": row.pattern,
                "field": row.match_field,
                "priority": int(row.priority),
                "reason": "duplicate",
            }
        )

    seen_otherwise: set[tuple[str, str]] = set()
    for row in active.itertuples(index=False):
        row_num = int(row.row_num)
        pattern = str(row.pattern)
        match_type = str(row.match_type)
        key = (str(row.category), str(row.target_column))
        if key in seen_otherwise and str(row.mode) == "fill_empty":
            suspicious.append(
                {
                    "rule_id": row_num,
                    "pattern": pattern,
                    "field": row.match_field,
                    "priority": int(row.priority),
                    "reason": "unreachable_after_otherwise",
                }
            )
        if match_type == "otherwise":
            seen_otherwise.add(key)
            suspicious.append(
                {
                    "rule_id": row_num,
                    "pattern": pattern,
                    "field": row.match_field,
                    "priority": int(row.priority),
                    "reason": "too_broad_otherwise",
                }
            )
        if str(row.category).strip() == "*" and match_type in {"contains", "regex", "not_contains"}:
            suspicious.append(
                {
                    "rule_id": row_num,
                    "pattern": pattern,
                    "field": row.match_field,
                    "priority": int(row.priority),
                    "reason": "global_rule_without_category_filter",
                }
            )
        if match_type in {"contains", "equals", "startswith"} and 0 < len(pattern.strip()) < 3:
            suspicious.append(
                {
                    "rule_id": row_num,
                    "pattern": pattern,
                    "field": row.match_field,
                    "priority": int(row.priority),
                    "reason": "too_short_pattern",
                }
            )
        if match_type == "not_contains":
            suspicious.append(
                {
                    "rule_id": row_num,
                    "pattern": pattern,
                    "field": row.match_field,
                    "priority": int(row.priority),
                    "reason": "negative_rule_can_match_most_rows",
                }
            )
        if match_type == "regex":
            try:
                re.compile(pattern)
            except re.error as exc:
                suspicious.append(
                    {
                        "rule_id": row_num,
                        "pattern": pattern,
                        "field": row.match_field,
                        "priority": int(row.priority),
                        "reason": f"invalid_regex: {exc}",
                    }
                )
            if ".*" in pattern or _regex_looks_expensive(pattern):
                suspicious.append(
                    {
                        "rule_id": row_num,
                        "pattern": pattern,
                        "field": row.match_field,
                        "priority": int(row.priority),
                        "reason": "potentially_expensive_regex",
                    }
                )

    sample_report: dict[str, Any] | None = None
    if sample_input is not None and sample_input.exists():
        sample = read_classification_input(sample_input)
        if len(sample) > sample_limit:
            sample = sample.head(sample_limit).copy()
        sample_result, report = classifier_engine.apply_classifiers(prepare_for_classification(sample), rules_path=rules_path)
        del sample_result
        rows = max(len(sample), 1)
        sample_report = {
            "sample_file": str(sample_input),
            "sample_rows": int(len(sample)),
            "per_rule": report.to_dict(orient="records"),
        }
        active_report = report[report["active"] == True].copy() if "active" in report.columns else report.copy()
        for row in active_report.itertuples(index=False):
            candidate_rows = int(getattr(row, "candidate_rows", 0))
            applied_rows = int(getattr(row, "applied_rows", 0))
            if candidate_rows == 0:
                suspicious.append(
                    {
                        "rule_id": int(row.row_num),
                        "pattern": getattr(row, "pattern", ""),
                        "field": getattr(row, "match_field", ""),
                        "priority": int(getattr(row, "priority", 9999)),
                        "reason": "never_matched_sample",
                    }
                )
            if candidate_rows / rows >= 0.8 and str(getattr(row, "match_type", "")) != "otherwise":
                suspicious.append(
                    {
                        "rule_id": int(row.row_num),
                        "pattern": getattr(row, "pattern", ""),
                        "field": getattr(row, "match_field", ""),
                        "priority": int(getattr(row, "priority", 9999)),
                        "reason": f"matches_too_many_rows_sample:{candidate_rows}/{rows}",
                    }
                )
            if candidate_rows > 0 and applied_rows == 0:
                suspicious.append(
                    {
                        "rule_id": int(row.row_num),
                        "pattern": getattr(row, "pattern", ""),
                        "field": getattr(row, "match_field", ""),
                        "priority": int(getattr(row, "priority", 9999)),
                        "reason": "matched_but_never_applied_sample",
                    }
                )

    return {
        "rules_file": str(rules_path),
        "rules_total": int(len(rules)),
        "active_rules": int(len(active)),
        "inactive_rules": int(len(rules) - len(active)),
        "match_type_counts": {str(key): int(value) for key, value in rules["match_type"].value_counts(dropna=False).items()},
        "target_column_counts": {
            str(key): int(value) for key, value in active["target_column"].value_counts(dropna=False).items()
        },
        "match_field_counts": {str(key): int(value) for key, value in active["match_field"].value_counts(dropna=False).items()},
        "category_counts": {str(key): int(value) for key, value in active["category"].value_counts(dropna=False).items()},
        "conditions_rules": int(rules[CONDITIONS_COLUMN].astype(str).str.strip().ne("").sum()),
        "duplicate_rules": int(duplicate_mask.sum()),
        "suspicious_rules": suspicious,
        "sample_report": sample_report,
    }


def render_result_text(result: dict[str, Any]) -> str:
    timings = result["timings"]
    instrumentation = result["instrumentation"]
    lines = [
        f"# Classifier benchmark: {result['name']}",
        "",
        f"Input rows: {result['rows_input']}",
        f"Output rows: {result['rows_output']}",
        f"Rules: {result['rules_count']} total / {result['active_rules']} active",
        f"Classified rows: {result['classified_rows']}",
        f"Unclassified rows: {result['unclassified_rows']}",
        f"Total duration: {result['total_seconds']:.4f}s",
        f"Rows/sec: {result['rows_per_second']:.2f}",
        f"Rules/sec: {result['rules_per_second']:.2f}",
        f"Checks/sec: {result['checks_per_second']:.2f}",
        f"Top bottleneck: {result['top_bottleneck']} ({result['top_bottleneck_seconds']:.4f}s)",
        f"Output size: {result['output_file_size_bytes']} bytes",
        f"Peak RSS: {result['peak_rss_mb']:.2f} MB" if result.get("peak_rss_mb") is not None else "Peak RSS: n/a",
        "",
        "## Timings",
    ]
    for key in sorted(timings):
        lines.append(f"- {key}: {timings[key]:.4f}s")
    lines.extend(
        [
            f"- read_rules_seconds: {instrumentation.get('read_rules_seconds', 0.0):.4f}s",
            f"- prepare_rules_seconds: {instrumentation.get('prepare_rules_seconds', 0.0):.4f}s",
            f"- match_seconds: {instrumentation.get('match_seconds', 0.0):.4f}s",
            "",
            "## Match Types",
        ]
    )
    for match_type, seconds in sorted(instrumentation.get("by_match_type_seconds", {}).items()):
        calls = instrumentation.get("by_match_type_calls", {}).get(match_type, 0)
        checks = instrumentation.get("by_match_type_checks", {}).get(match_type, 0)
        lines.append(f"- {match_type}: {seconds:.4f}s, calls={calls}, checks={checks}")
    if result.get("cprofile_top"):
        lines.extend(["", "## cProfile Top 20", "", "```text", result["cprofile_top"].rstrip(), "```"])
    if result.get("comparison"):
        lines.extend(["", "## Baseline Comparison", render_comparison_text(result["comparison"])])
    return "\n".join(lines) + "\n"


def render_comparison_text(comparison: dict[str, Any]) -> str:
    lines = [
        f"Rows match: {comparison['row_count_match']}",
        f"Columns match: {comparison['columns_match']}",
        f"Classified baseline/candidate: {comparison['classified_baseline']} / {comparison['classified_candidate']}",
        f"Unclassified baseline/candidate: {comparison['unclassified_baseline']} / {comparison['unclassified_candidate']}",
        "Diff counts:",
    ]
    for column, count in comparison["diff_counts"].items():
        lines.append(f"- {column}: {count}")
    if comparison["first_differences"]:
        lines.append("First differences:")
        for diff in comparison["first_differences"]:
            lines.append(
                f"- row={diff['row_index']} column={diff['column']} baseline={diff['baseline']!r} candidate={diff['candidate']!r}"
            )
    return "\n".join(lines)


def render_rules_audit_text(audit: dict[str, Any], *, limit: int = 30) -> str:
    lines = [
        "# Classifier Rules Audit",
        "",
        f"Rules file: {audit['rules_file']}",
        f"Rules total: {audit['rules_total']}",
        f"Active rules: {audit['active_rules']}",
        f"Inactive rules: {audit['inactive_rules']}",
        f"Rules with extra conditions: {audit['conditions_rules']}",
        f"Duplicate rules: {audit['duplicate_rules']}",
        "",
        "## Match Types",
    ]
    for key, value in audit["match_type_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Target Columns"])
    for key, value in audit["target_column_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Match Fields"])
    for key, value in audit["match_field_counts"].items():
        label = key if key else "<empty>"
        lines.append(f"- {label}: {value}")
    lines.extend(["", "## Suspicious Rules"])
    for item in audit["suspicious_rules"][:limit]:
        lines.append(
            f"- rule={item['rule_id']} priority={item['priority']} field={item['field']!r} "
            f"pattern={item['pattern']!r}: {item['reason']}"
        )
    if len(audit["suspicious_rules"]) > limit:
        lines.append(f"- ... {len(audit['suspicious_rules']) - limit} more")
    return "\n".join(lines) + "\n"


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def write_text(path: Path, payload: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return path
