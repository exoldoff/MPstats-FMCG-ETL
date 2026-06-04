# DuckDB optimization report

## Scope

DuckDB remains the main local analytical engine. The current production workflow is the local web-app (`mpstats_app/` + `web/`). Legacy standalone SQL-service code was removed from the active codebase because it duplicated the web DuckDB layer and loaded CSV into DuckDB through pandas.

## Audit summary

| Area | Finding | Impact | First action |
| --- | --- | --- | --- |
| Legacy SQL service | `pipeline/services/sql_service.py` imported CSV through `pandas.read_csv` and registered the whole DataFrame in DuckDB. | Slow and memory-heavy on large exports. | Removed as non-web legacy path. |
| Web DB import | `DuckDbAppRepository.import_products_file_idempotent` still reads classified CSV into pandas and calculates row hashes in Python. | High memory use and slow CPU path on 1M+ rows. | Next step: replace with DuckDB staging SQL. |
| Idempotency | Smart pipeline has `cube_registry` skip and `__row_hash` anti-join. | Good baseline, but the old append import can still duplicate rows. | Next step: remove/replace old append import usages in web services. |
| Transactions | `DELETE`/`INSERT`/load history/cube registry updates are not consistently grouped. | Partial updates are possible if a save fails mid-flow. | Next step: wrap DB save in explicit transaction. |
| CSV export | Raw export already uses DuckDB `COPY`. Report CSV still fetches aggregate to pandas before writing. | Unneeded pandas materialization for report CSV. | Later step: add direct report `COPY`. |
| XLSX export | openpyxl write-only mode is safe for formatted/sheet-controlled output. DuckDB Excel extension can write simple XLSX. | pandas/openpyxl should stay only on final aggregates or controlled batches. | Benchmark includes both simple DuckDB XLSX and pandas/openpyxl baseline. |
| DB settings | Connections use default `duckdb.connect(path)`. | Fine by default, but no central place for `threads`, `memory_limit`, `temp_directory`. | Later step: managed connection helper with optional settings. |

## Benchmark stand

Artifacts:

- `scripts/duckdb_mock_data.py` generates deterministic mock CSV data.
- `scripts/duckdb_benchmark.py` compares pandas-heavy legacy style with DuckDB SQL style.

Mock schema:

- `period`
- `date`
- `category`
- `network`
- `brand`
- `sku`
- `price`
- `volume`
- `stores_count`
- `region`

Dataset sizes:

- `small`: 10,000 rows
- `medium`: 500,000 rows
- `large`: 2,000,000 rows

Run examples:

```bash
python3 scripts/duckdb_benchmark.py --size small
python3 scripts/duckdb_benchmark.py --size medium --threads 4 --memory-limit 4GB
python3 scripts/duckdb_benchmark.py --size large --threads 4 --memory-limit 6GB --skip-excel
```

Outputs are written to `data/duckdb_benchmark/`:

- generated mock CSV
- old/new `.duckdb` files
- CSV/XLSX exports
- `benchmark_<size>.json`
- `benchmark_<size>.md`

## Benchmark results

Local runs:

- `python3 scripts/duckdb_benchmark.py --size small --workdir data/duckdb_benchmark --skip-excel`
- `python3 scripts/duckdb_benchmark.py --size medium --workdir data/duckdb_benchmark --skip-excel`

### small: 10,000 rows

| operation | old method | new method | before, s | after, s | speedup | comment | risk |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| load_csv_to_duckdb | pandas.read_csv -> register -> CTAS | DuckDB read_csv -> CTAS with explicit schema | 0.0628 | 0.0429 | 1.46x | DuckDB filters zero-volume rows during load. | CSV dialect is fixed to semicolon/UTF-8 for this benchmark. |
| idempotent_rerun | append can duplicate rows | stage table + anti-join by row_hash | - | 0.0417 | - | Expected inserted_again=0. | row_hash key must match production business key policy. |
| group_by_report | fetch raw rows to pandas, then aggregate | DuckDB GROUP BY SQL | - | 0.0045 | - | Heavy aggregation stays inside DuckDB. | No pandas baseline is run here because it scales poorly by design. |
| join_with_dictionary | pandas merge after fetching raw data | DuckDB SQL join | - | 0.0034 | - | Dictionary join stays inside DuckDB. | Representative mock dictionary only. |
| filter_period_category_network | pandas boolean mask after fetch | DuckDB WHERE filter | - | 0.0005 | - | Filters are pushed before result materialization. | No indexes: speed depends on DuckDB scan and table ordering. |
| export_csv | fetchdf -> df.to_csv | COPY (SELECT ...) TO CSV | 0.0031 | 0.0020 | 1.60x | Direct export avoids pandas materialization. | COPY output has simpler formatting controls. |

Quality:

- requested rows: 10,000
- loaded rows after SQL filter: 9,989
- duplicate row hashes: 0
- null required rows: 0
- date range: 2024-01-01..2025-12-01
- idempotent rerun inserted rows: 0
- CSV export exists and is non-empty: true

### medium: 500,000 rows

| operation | old method | new method | before, s | after, s | speedup | comment | risk |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| load_csv_to_duckdb | pandas.read_csv -> register -> CTAS | DuckDB read_csv -> CTAS with explicit schema | 0.9336 | 0.5894 | 1.58x | DuckDB filters zero-volume rows during load. | CSV dialect is fixed to semicolon/UTF-8 for this benchmark. |
| idempotent_rerun | append can duplicate rows | stage table + anti-join by row_hash | - | 0.2546 | - | Expected inserted_again=0. | row_hash key must match production business key policy. |
| group_by_report | fetch raw rows to pandas, then aggregate | DuckDB GROUP BY SQL | - | 0.0065 | - | Heavy aggregation stays inside DuckDB. | No pandas baseline is run here because it scales poorly by design. |
| join_with_dictionary | pandas merge after fetching raw data | DuckDB SQL join | - | 0.0059 | - | Dictionary join stays inside DuckDB. | Representative mock dictionary only. |
| filter_period_category_network | pandas boolean mask after fetch | DuckDB WHERE filter | - | 0.0010 | - | Filters are pushed before result materialization. | No indexes: speed depends on DuckDB scan and table ordering. |
| export_csv | fetchdf -> df.to_csv | COPY (SELECT ...) TO CSV | 0.0084 | 0.0059 | 1.42x | Direct export avoids pandas materialization. | COPY output has simpler formatting controls. |

Quality:

- requested rows: 500,000
- loaded rows after SQL filter: 499,496
- duplicate row hashes: 0
- null required rows: 0
- date range: 2024-01-01..2025-12-01
- idempotent rerun inserted rows: 0
- CSV export exists and is non-empty: true

## Manual load/export checks

Current web flow remains unchanged:

1. Start local web-app.
2. Run smart pipeline and save slices to DuckDB.
3. Open exports/reports from the web UI.
4. For raw CSV exports, the app already uses DuckDB `COPY`.
5. For raw XLSX exports, the app keeps Excel row-limit guards and writes in batches.

Next implementation step:

1. Move `import_products_file_idempotent` from pandas DataFrame loading to DuckDB SQL staging.
2. Add row count, duplicate, null, date-range and exported-file checks.
3. Add focused tests for idempotent rerun and CSV direct load.
