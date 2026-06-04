from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import time
from typing import Callable

import duckdb
import pandas as pd

from duckdb_mock_data import DEFAULT_SIZES, generate_mock_csv


BENCH_SIZE_ORDER = ("small", "medium", "large")
MOCK_SCHEMA = """
{
    'period': 'VARCHAR',
    'date': 'DATE',
    'category': 'VARCHAR',
    'network': 'VARCHAR',
    'brand': 'VARCHAR',
    'sku': 'VARCHAR',
    'price': 'DOUBLE',
    'volume': 'DOUBLE',
    'stores_count': 'INTEGER',
    'region': 'VARCHAR'
}
"""


@dataclass(frozen=True)
class BenchResult:
    operation: str
    old_method: str
    new_method: str
    old_seconds: float | None
    new_seconds: float | None
    rows: int | None
    comment: str
    risk: str
    old_file_size_bytes: int | None = None
    new_file_size_bytes: int | None = None

    @property
    def speedup(self) -> float | None:
        if self.old_seconds is None or self.new_seconds is None or self.new_seconds <= 0:
            return None
        return self.old_seconds / self.new_seconds


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def timed(fn: Callable[[], int | None]) -> tuple[float, int | None]:
    start = time.perf_counter()
    rows = fn()
    return time.perf_counter() - start, rows


def file_size(path: Path) -> int | None:
    return path.stat().st_size if path.exists() else None


def connect(db_path: Path, *, threads: int | None = None, memory_limit: str | None = None, temp_directory: Path | None = None) -> duckdb.DuckDBPyConnection:
    config: dict[str, str] = {}
    if threads is not None:
        config["threads"] = str(max(1, int(threads)))
    if memory_limit:
        config["memory_limit"] = memory_limit
    if temp_directory:
        temp_directory.mkdir(parents=True, exist_ok=True)
        config["temp_directory"] = str(temp_directory)
    return duckdb.connect(str(db_path), config=config or None)


def create_dim_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_category AS
        SELECT *
        FROM (
            VALUES
                ('sugar', 'food'),
                ('soap', 'home'),
                ('lemon_acid', 'food'),
                ('tea', 'food'),
                ('coffee', 'food'),
                ('pasta', 'food'),
                ('cereal', 'food'),
                ('sauce', 'food')
        ) AS t(category, category_group)
        """
    )


def duckdb_csv_scan(csv_path: Path) -> str:
    return (
        "read_csv("
        f"{sql_literal(str(csv_path))}, "
        "delim=';', header=true, dateformat='%Y-%m-%d', "
        f"columns={MOCK_SCHEMA}"
        ")"
    )


def stage_select(csv_path: Path) -> str:
    scan = duckdb_csv_scan(csv_path)
    return f"""
        SELECT
            period,
            date,
            category,
            network,
            brand,
            sku,
            price,
            volume,
            stores_count,
            region,
            hash(period, date, category, network, brand, sku, price, volume, stores_count, region)::VARCHAR AS row_hash
        FROM {scan}
        WHERE price > 0 AND volume > 0
    """


GROUP_QUERY = """
    SELECT
        period,
        category,
        network,
        COUNT(*) AS rows_count,
        SUM(volume) AS total_volume,
        SUM(price * volume) AS revenue,
        AVG(price) AS avg_price
    FROM products_new
    WHERE period BETWEEN '2024-06' AND '2025-06'
      AND category IN ('sugar', 'tea', 'coffee')
      AND network IN ('Ozon', 'Wildberries')
    GROUP BY period, category, network
    ORDER BY period, category, network
"""

REPORT_CSV_QUERY = """
    SELECT
        period,
        category,
        network,
        brand,
        sku,
        COUNT(*) AS rows_count,
        SUM(volume) AS total_volume,
        SUM(price * volume) AS revenue,
        AVG(price) AS avg_price
    FROM products_new
    WHERE period BETWEEN '2024-01' AND '2025-12'
    GROUP BY period, category, network, brand, sku
    ORDER BY revenue DESC NULLS LAST, period, category, network, brand, sku
"""

JOIN_QUERY = """
    SELECT
        p.period,
        d.category_group,
        p.network,
        COUNT(*) AS rows_count,
        SUM(p.volume) AS total_volume
    FROM products_new p
    JOIN dim_category d USING (category)
    WHERE p.period >= '2024-06'
    GROUP BY p.period, d.category_group, p.network
"""


def run_benchmark(
    *,
    workdir: Path,
    size: str,
    rows: int,
    threads: int | None,
    memory_limit: str | None,
    skip_excel: bool,
) -> tuple[list[BenchResult], dict[str, object]]:
    workdir.mkdir(parents=True, exist_ok=True)
    csv_path = workdir / f"mock_{size}_{rows}.csv"
    if not csv_path.exists():
        generate_mock_csv(csv_path, rows=rows)

    old_db = workdir / f"old_{size}.duckdb"
    new_db = workdir / f"new_{size}.duckdb"
    for path in (old_db, new_db):
        if path.exists():
            path.unlink()

    results: list[BenchResult] = []

    with connect(old_db, threads=threads, memory_limit=memory_limit, temp_directory=workdir / "tmp_old") as con:
        def old_load() -> int:
            df = pd.read_csv(csv_path, sep=";", low_memory=False)
            con.register("mock_df", df)
            con.execute("CREATE OR REPLACE TABLE products_old AS SELECT * FROM mock_df")
            con.unregister("mock_df")
            return int(con.execute("SELECT COUNT(*) FROM products_old").fetchone()[0])

        old_load_seconds, old_rows = timed(old_load)

    with connect(new_db, threads=threads, memory_limit=memory_limit, temp_directory=workdir / "tmp_new") as con:
        create_dim_tables(con)

        def new_load() -> int:
            con.execute(f"CREATE OR REPLACE TABLE products_new AS {stage_select(csv_path)}")
            return int(con.execute("SELECT COUNT(*) FROM products_new").fetchone()[0])

        new_load_seconds, new_rows = timed(new_load)
        results.append(
            BenchResult(
                "load_csv_to_duckdb",
                "pandas.read_csv -> register -> CTAS",
                "DuckDB read_csv -> CTAS with explicit schema",
                old_load_seconds,
                new_load_seconds,
                new_rows,
                "DuckDB filters zero-volume rows during load.",
                "CSV dialect is fixed to semicolon/UTF-8 for this benchmark.",
            )
        )

        def idempotent_rerun() -> int:
            con.execute("CREATE OR REPLACE TEMP TABLE stage_products AS " + stage_select(csv_path))
            before = int(con.execute("SELECT COUNT(*) FROM products_new").fetchone()[0])
            con.execute(
                """
                INSERT INTO products_new
                SELECT s.*
                FROM stage_products s
                WHERE NOT EXISTS (
                    SELECT 1 FROM products_new p WHERE p.row_hash = s.row_hash
                )
                """
            )
            after = int(con.execute("SELECT COUNT(*) FROM products_new").fetchone()[0])
            return after - before

        rerun_seconds, inserted_again = timed(idempotent_rerun)
        results.append(
            BenchResult(
                "idempotent_rerun",
                "append can duplicate rows",
                "stage table + anti-join by row_hash",
                None,
                rerun_seconds,
                inserted_again,
                "Expected inserted_again=0.",
                "row_hash key must match production business key policy.",
            )
        )

        def group_by() -> int:
            return len(con.execute(GROUP_QUERY).fetchall())

        group_seconds, group_rows = timed(group_by)
        results.append(
            BenchResult(
                "group_by_report",
                "fetch raw rows to pandas, then aggregate",
                "DuckDB GROUP BY SQL",
                None,
                group_seconds,
                group_rows,
                "Heavy aggregation stays inside DuckDB.",
                "No pandas baseline is run here because it scales poorly by design.",
            )
        )

        def join_query() -> int:
            return len(con.execute(JOIN_QUERY).fetchall())

        join_seconds, join_rows = timed(join_query)
        results.append(
            BenchResult(
                "join_with_dictionary",
                "pandas merge after fetching raw data",
                "DuckDB SQL join",
                None,
                join_seconds,
                join_rows,
                "Dictionary join stays inside DuckDB.",
                "Representative mock dictionary only.",
            )
        )

        def filtered_count() -> int:
            return int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM products_new
                    WHERE period BETWEEN '2024-06' AND '2025-06'
                      AND category = 'sugar'
                      AND network = 'Ozon'
                    """
                ).fetchone()[0]
            )

        filter_seconds, filter_rows = timed(filtered_count)
        results.append(
            BenchResult(
                "filter_period_category_network",
                "pandas boolean mask after fetch",
                "DuckDB WHERE filter",
                None,
                filter_seconds,
                filter_rows,
                "Filters are pushed before result materialization.",
                "No indexes: speed depends on DuckDB scan and table ordering.",
            )
        )

        old_csv = workdir / f"report_old_{size}.csv"
        new_csv = workdir / f"report_new_{size}.csv"

        def old_csv_export() -> int:
            df = con.execute(REPORT_CSV_QUERY).fetchdf()
            df.to_csv(old_csv, sep=";", index=False, encoding="utf-8-sig")
            return len(df)

        old_csv_seconds, old_csv_rows = timed(old_csv_export)

        def new_csv_export() -> int:
            row = con.execute(f"COPY ({REPORT_CSV_QUERY}) TO {sql_literal(str(new_csv))} WITH (FORMAT csv, DELIMITER ';', HEADER true)").fetchone()
            return int(row[0]) if row else 0

        new_csv_seconds, new_csv_rows = timed(new_csv_export)
        results.append(
            BenchResult(
                "report_csv_export",
                "fetchdf -> df.to_csv",
                "COPY (SELECT ...) TO CSV",
                old_csv_seconds,
                new_csv_seconds,
                new_csv_rows,
                "Direct report CSV export avoids pandas materialization.",
                "COPY output has simpler formatting controls.",
                old_file_size_bytes=file_size(old_csv),
                new_file_size_bytes=file_size(new_csv),
            )
        )

        if not skip_excel:
            old_xlsx = workdir / f"report_old_{size}.xlsx"
            new_xlsx = workdir / f"report_new_{size}.xlsx"

            def old_xlsx_export() -> int:
                df = con.execute(GROUP_QUERY).fetchdf()
                df.to_excel(old_xlsx, index=False)
                return len(df)

            old_xlsx_seconds, old_xlsx_rows = timed(old_xlsx_export)
            try:
                def new_xlsx_export() -> int:
                    con.execute("INSTALL excel")
                    con.execute("LOAD excel")
                    con.execute(f"COPY ({GROUP_QUERY}) TO {sql_literal(str(new_xlsx))} WITH (FORMAT xlsx, HEADER true)")
                    return int(new_xlsx.exists() and new_xlsx.stat().st_size > 0)

                new_xlsx_seconds, _ = timed(new_xlsx_export)
                xlsx_comment = "DuckDB Excel extension works for plain XLSX."
                xlsx_risk = "Use openpyxl for styled/multi-sheet workbooks."
            except Exception as exc:
                new_xlsx_seconds = None
                xlsx_comment = f"DuckDB XLSX export unavailable: {exc}"
                xlsx_risk = "Extension install/load may be unavailable offline."
            results.append(
                BenchResult(
                    "export_xlsx",
                    "fetchdf -> pandas.to_excel",
                    "COPY (SELECT ...) TO XLSX via DuckDB excel extension",
                    old_xlsx_seconds,
                    new_xlsx_seconds,
                    old_xlsx_rows,
                    xlsx_comment,
                    xlsx_risk,
                )
            )

        quality = {
            "source_csv": str(csv_path),
            "source_size_bytes": csv_path.stat().st_size,
            "requested_rows": rows,
            "loaded_rows": new_rows,
            "duplicate_row_hashes": int(
                con.execute(
                    """
                    SELECT COALESCE(SUM(cnt - 1), 0)
                    FROM (
                        SELECT row_hash, COUNT(*) AS cnt
                        FROM products_new
                        GROUP BY row_hash
                        HAVING COUNT(*) > 1
                    )
                    """
                ).fetchone()[0]
            ),
            "null_required_rows": int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM products_new
                    WHERE period IS NULL OR date IS NULL OR category IS NULL OR network IS NULL OR sku IS NULL
                    """
                ).fetchone()[0]
            ),
            "date_min": str(con.execute("SELECT MIN(date) FROM products_new").fetchone()[0]),
            "date_max": str(con.execute("SELECT MAX(date) FROM products_new").fetchone()[0]),
            "idempotent_inserted_again": inserted_again,
            "csv_export_exists": new_csv.exists() and new_csv.stat().st_size > 0,
            "report_csv_rows_old": old_csv_rows,
            "report_csv_rows_new": new_csv_rows,
            "report_csv_old_size_bytes": file_size(old_csv),
            "report_csv_new_size_bytes": file_size(new_csv),
            "memory": "not_measured",
        }

    shutil.rmtree(workdir / "tmp_old", ignore_errors=True)
    shutil.rmtree(workdir / "tmp_new", ignore_errors=True)
    return results, quality


def markdown_table(results: list[BenchResult]) -> str:
    lines = [
        "| operation | old method | new method | before, s | after, s | speedup | rows | old bytes | new bytes | comment | risk |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for item in results:
        before = "-" if item.old_seconds is None else f"{item.old_seconds:.4f}"
        after = "-" if item.new_seconds is None else f"{item.new_seconds:.4f}"
        speedup = "-" if item.speedup is None else f"{item.speedup:.2f}x"
        rows = "-" if item.rows is None else str(item.rows)
        old_bytes = "-" if item.old_file_size_bytes is None else str(item.old_file_size_bytes)
        new_bytes = "-" if item.new_file_size_bytes is None else str(item.new_file_size_bytes)
        lines.append(
            "| "
            + " | ".join(
                [
                    item.operation,
                    item.old_method,
                    item.new_method,
                    before,
                    after,
                    speedup,
                    rows,
                    old_bytes,
                    new_bytes,
                    item.comment.replace("|", "/"),
                    item.risk.replace("|", "/"),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark old pandas-heavy paths against DuckDB SQL paths on mock data.")
    parser.add_argument("--workdir", type=Path, default=Path("data/duckdb_benchmark"), help="Directory for generated data and benchmark outputs.")
    parser.add_argument("--size", choices=sorted(DEFAULT_SIZES), default="small", help="Named mock dataset size.")
    parser.add_argument("--rows", type=int, default=None, help="Override row count.")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads setting.")
    parser.add_argument("--memory-limit", default=None, help="DuckDB memory_limit setting, e.g. 4GB.")
    parser.add_argument("--skip-excel", action="store_true", help="Skip XLSX export benchmarks.")
    parser.add_argument("--all-sizes", action="store_true", help="Run small, medium and large sizes in one pass.")
    return parser.parse_args()


def write_outputs(*, workdir: Path, size: str, rows: int, results: list[BenchResult], quality: dict[str, object]) -> str:
    payload = {
        "size": size,
        "rows": rows,
        "quality": quality,
        "results": [
            {
                **item.__dict__,
                "speedup": item.speedup,
            }
            for item in results
        ],
    }
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / f"benchmark_{size}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = markdown_table(results)
    (workdir / f"benchmark_{size}.md").write_text(report + "\n", encoding="utf-8")
    return report


def main() -> None:
    args = parse_args()
    if args.all_sizes and args.rows is not None:
        raise ValueError("--rows нельзя использовать вместе с --all-sizes.")
    sizes = [size for size in BENCH_SIZE_ORDER if size in DEFAULT_SIZES] if args.all_sizes else [args.size]
    for size in sizes:
        rows = int(DEFAULT_SIZES[size] if args.all_sizes else args.rows if args.rows is not None else DEFAULT_SIZES[size])
        results, quality = run_benchmark(
            workdir=args.workdir,
            size=size,
            rows=rows,
            threads=args.threads,
            memory_limit=args.memory_limit,
            skip_excel=args.skip_excel,
        )
        report = write_outputs(workdir=args.workdir, size=size, rows=rows, results=results, quality=quality)
        print(f"\n## {size}: {rows:,} rows")
        print(report)
        print("\nQuality:")
        print(json.dumps(quality, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
