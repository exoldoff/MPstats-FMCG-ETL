from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from pipeline.models import PipelinePaths, StepResult
from pipeline.services.classification_service import classify_file
from pipeline.services.enrich_service import enrich_directory
from pipeline.services.export_service import load_export_settings, run_export
from pipeline.services.merge_service import merge_directory
from pipeline.services.run_service import parse_steps, run_pipeline
from pipeline.services.standardize_service import standardize_directory
from pipeline.services.sql_service import (
    export_sql_to_csv,
    import_csv_to_sql,
    import_directory_to_sql,
    sql_load_history,
    sql_query,
    sql_table,
    sql_tables,
)
from pipeline.services.weight_parser_service import parse_weights_directory


def default_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def build_paths(args: argparse.Namespace) -> PipelinePaths:
    return PipelinePaths.create(
        project_root=args.project_root,
        workdir=args.workdir,
        project_name=args.project_name,
    )


def parse_fill_unclassified(raw: str | None) -> dict[str, object] | None:
    if not raw:
        return None
    path = Path(raw)
    if path.exists():
        text = path.read_text(encoding="utf-8")
    else:
        text = raw
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("--fill-unclassified должен быть JSON object или путь к JSON-файлу.")
    return parsed


def print_result(result: StepResult) -> None:
    output = f" output={result.output}" if result.output else ""
    print(
        f"[{result.name}] ok={result.ok} skipped={result.skipped} "
        f"errors={result.errors} rows={result.rows}{output}"
    )


def add_common_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project-root", type=Path, default=default_project_root(), help="Корень проекта.")
    parser.add_argument("--workdir", type=Path, default=default_project_root() / "pipeline", help="Рабочая папка pipeline.")
    parser.add_argument("--project-name", default="mpstats", help="Имя прогона для итоговых файлов.")


def add_common_files(parser: argparse.ArgumentParser) -> None:
    root = default_project_root()
    parser.add_argument("--config", type=Path, default=root / "pipeline" / "step1_export_config.json", help="JSON-конфиг шага 1.")
    parser.add_argument("--rules", type=Path, default=root / "classifiers" / "rules.csv", help="CSV/XLSX с правилами классификации.")


def add_common_sql(parser: argparse.ArgumentParser) -> None:
    root = default_project_root()
    parser.add_argument("--db", type=Path, default=root / "mpstats.duckdb", help="DuckDB-файл.")
    parser.add_argument("--table", default="mpstats_products", help="SQL-таблица для данных.")


def cmd_doctor(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    paths.ensure_dirs()
    print("[doctor] project_root:", paths.project_root)
    print("[doctor] workdir:", paths.workdir)
    print("[doctor] project_name:", paths.project_name)
    print("[doctor] step1 raw:", paths.step1_raw_dir)
    print("[doctor] step2 enriched:", paths.step2_enriched_dir)
    print("[doctor] step3 standardized:", paths.step3_standardized_dir)
    print("[doctor] step4 parsed:", paths.step4_parsed_dir)
    print("[doctor] merged:", paths.merged_csv)
    print("[doctor] classified:", paths.classified_csv)
    print("[doctor] ok")
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    settings = load_export_settings(args.config, default_save_dir=paths.step1_raw_dir)
    result = run_export(settings, log_dir=paths.logs_dir)
    print_result(result)
    return 1 if result.failed else 0


def cmd_enrich(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    result = enrich_directory(args.input or paths.step1_raw_dir, args.output or paths.step2_enriched_dir)
    print_result(result)
    return 1 if result.failed else 0


def cmd_standardize(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    result = standardize_directory(args.input or paths.step2_enriched_dir, args.output or paths.step3_standardized_dir)
    print_result(result)
    return 1 if result.failed else 0


def cmd_parse_weights(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    result = parse_weights_directory(
        args.input or paths.step3_standardized_dir,
        args.output or paths.step4_parsed_dir,
        max_weight_kg=args.max_weight_kg,
    )
    print_result(result)
    return 1 if result.failed else 0


def cmd_merge(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    _, result = merge_directory(
        args.input or paths.step4_parsed_dir,
        args.output or paths.merged_csv,
        min_sales=args.min_sales,
        max_sales=args.max_sales,
    )
    print_result(result)
    return 1 if result.failed else 0


def cmd_classify(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    _, _, result = classify_file(
        args.input or paths.merged_csv,
        args.output or paths.classified_csv,
        rules_path=args.rules,
        write_xlsx=args.write_xlsx,
        fill_unclassified=parse_fill_unclassified(args.fill_unclassified),
    )
    print_result(result)
    return 1 if result.failed else 0


def default_sql_input(paths: PipelinePaths) -> Path:
    return paths.classified_csv if paths.classified_csv.exists() else paths.merged_csv


def cmd_sql_import(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    input_file = args.input or default_sql_input(paths)
    result = import_csv_to_sql(
        input_file,
        db_path=args.db,
        table_name=args.table,
        mode=args.mode,
        load_name=args.load_name,
        project_name=paths.project_name,
    )
    print_result(result)
    return 1 if result.failed else 0


def cmd_sql_import_dir(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    result = import_directory_to_sql(
        args.input or paths.step4_parsed_dir,
        db_path=args.db,
        table_name=args.table,
        mode=args.mode,
        load_name=args.load_name,
        project_name=paths.project_name,
    )
    print_result(result)
    return 1 if result.failed else 0


def cmd_sql_export(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    output = args.output or (paths.workdir / f"sql_{paths.project_name}_export.csv")
    result = export_sql_to_csv(
        db_path=args.db,
        output_file=output,
        table_name=args.table if not args.query else None,
        query=args.query,
    )
    print_result(result)
    return 1 if result.failed else 0


def cmd_sql_query(args: argparse.Namespace) -> int:
    if args.history:
        df = sql_load_history(args.db)
    elif args.tables:
        df = sql_tables(args.db, include_internal=args.include_internal)
    elif args.query:
        df = sql_query(args.db, args.query)
    else:
        df = sql_table(args.db, args.table, limit=args.limit)
    print(df.head(args.limit).to_string(index=False))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    paths = build_paths(args)
    results = run_pipeline(
        paths=paths,
        steps=parse_steps(args.steps),
        config_path=args.config,
        rules_path=args.rules,
        write_xlsx=args.write_xlsx,
        max_weight_kg=args.max_weight_kg,
        fill_unclassified=parse_fill_unclassified(args.fill_unclassified),
    )
    failed = False
    for result in results:
        print_result(result)
        failed = failed or result.failed
    return 1 if failed else 0


def cmd_gui_config(args: argparse.Namespace) -> int:
    root = Path(args.project_root)
    config = Path(args.config)
    archive = root / "справочник tasks архив.md"
    subprocess.Popen(
        [sys.executable, "-m", "pipeline.step1_gui", "--config", str(config), "--archive", str(archive)],
        cwd=str(root),
    )
    print("[gui-config] launched")
    return 0


def cmd_gui_rules(args: argparse.Namespace) -> int:
    root = Path(args.project_root)
    subprocess.Popen([sys.executable, "-m", "classifiers.gui"], cwd=str(root))
    print("[gui-rules] launched")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mpstats-pipeline", description="MPStats pipeline CLI.")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Проверить пути и создать рабочие директории.")
    add_common_paths(doctor)
    doctor.set_defaults(func=cmd_doctor)

    run = sub.add_parser("run", help="Запустить несколько шагов, например --steps 2-6.")
    add_common_paths(run)
    add_common_files(run)
    run.add_argument("--steps", default="2-6", help="Шаги: 1,2,3 или диапазон 2-6.")
    run.add_argument("--max-weight-kg", type=float, default=40.0)
    run.add_argument("--fill-unclassified", default=None, help="JSON object или путь к JSON-файлу.")
    run.add_argument("--write-xlsx", action=argparse.BooleanOptionalAction, default=True)
    run.set_defaults(func=cmd_run)

    export = sub.add_parser("export", help="Шаг 1: выгрузка MPStats.")
    add_common_paths(export)
    add_common_files(export)
    export.set_defaults(func=cmd_export)

    enrich = sub.add_parser("enrich", help="Шаг 2: добавить дату, маркетплейс, категорию.")
    add_common_paths(enrich)
    enrich.add_argument("--input", type=Path, default=None)
    enrich.add_argument("--output", type=Path, default=None)
    enrich.set_defaults(func=cmd_enrich)

    standardize = sub.add_parser("standardize", help="Шаг 3: привести CSV к канону колонок.")
    add_common_paths(standardize)
    standardize.add_argument("--input", type=Path, default=None)
    standardize.add_argument("--output", type=Path, default=None)
    standardize.set_defaults(func=cmd_standardize)

    parse_weights = sub.add_parser("parse-weights", help="Шаг 4: распарсить вес и объём.")
    add_common_paths(parse_weights)
    parse_weights.add_argument("--input", type=Path, default=None)
    parse_weights.add_argument("--output", type=Path, default=None)
    parse_weights.add_argument("--max-weight-kg", type=float, default=40.0)
    parse_weights.set_defaults(func=cmd_parse_weights)

    merge = sub.add_parser("merge", help="Шаг 5: склеить CSV.")
    add_common_paths(merge)
    merge.add_argument("--input", type=Path, default=None)
    merge.add_argument("--output", type=Path, default=None)
    merge.add_argument("--min-sales", type=float, default=0)
    merge.add_argument("--max-sales", type=float, default=40_000)
    merge.set_defaults(func=cmd_merge)

    classify = sub.add_parser("classify", help="Шаг 6: применить правила классификации.")
    add_common_paths(classify)
    add_common_files(classify)
    classify.add_argument("--input", type=Path, default=None)
    classify.add_argument("--output", type=Path, default=None)
    classify.add_argument("--fill-unclassified", default=None, help="JSON object или путь к JSON-файлу.")
    classify.add_argument("--write-xlsx", action=argparse.BooleanOptionalAction, default=True)
    classify.set_defaults(func=cmd_classify)

    sql_import = sub.add_parser("sql-import", help="Загрузить итоговый CSV в DuckDB.")
    add_common_paths(sql_import)
    add_common_sql(sql_import)
    sql_import.add_argument("--input", type=Path, default=None, help="CSV для загрузки. По умолчанию classified или merged.")
    sql_import.add_argument("--mode", choices=("append", "replace"), default="append")
    sql_import.add_argument("--load-name", default=None, help="Человеческая метка загрузки.")
    sql_import.set_defaults(func=cmd_sql_import)

    sql_import_dir = sub.add_parser("sql-import-dir", help="Загрузить все CSV из папки в одну DuckDB-таблицу.")
    add_common_paths(sql_import_dir)
    add_common_sql(sql_import_dir)
    sql_import_dir.add_argument("--input", type=Path, default=None, help="Папка CSV. По умолчанию 04_step4_parsed.")
    sql_import_dir.add_argument("--mode", choices=("append", "replace"), default="append")
    sql_import_dir.add_argument("--load-name", default=None, help="Человеческая метка загрузки.")
    sql_import_dir.set_defaults(func=cmd_sql_import_dir)

    sql_export = sub.add_parser("sql-export", help="Выгрузить SQL-таблицу или запрос обратно в CSV.")
    add_common_paths(sql_export)
    add_common_sql(sql_export)
    sql_export.add_argument("--output", type=Path, default=None)
    sql_export.add_argument("--query", default=None, help="SQL-запрос. Если не задан, экспортируется --table.")
    sql_export.set_defaults(func=cmd_sql_export)

    sql_query_parser = sub.add_parser("sql-query", help="Посмотреть таблицы, историю загрузок или результат SQL-запроса.")
    add_common_sql(sql_query_parser)
    sql_query_parser.add_argument("--query", default=None)
    sql_query_parser.add_argument("--tables", action="store_true")
    sql_query_parser.add_argument("--history", action="store_true")
    sql_query_parser.add_argument("--include-internal", action="store_true")
    sql_query_parser.add_argument("--limit", type=int, default=20)
    sql_query_parser.set_defaults(func=cmd_sql_query)

    gui_config = sub.add_parser("gui-config", help="Открыть GUI настроек выгрузки.")
    add_common_paths(gui_config)
    add_common_files(gui_config)
    gui_config.set_defaults(func=cmd_gui_config)

    gui_rules = sub.add_parser("gui-rules", help="Открыть GUI правил классификации.")
    add_common_paths(gui_rules)
    gui_rules.set_defaults(func=cmd_gui_rules)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func")
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
