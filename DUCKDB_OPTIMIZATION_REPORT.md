# DuckDB optimization report

## Scope

DuckDB remains the main local analytical engine. The current production workflow is the local web-app (`mpstats_app/` + `web/`). Legacy standalone SQL-service code was removed from the active codebase because it duplicated the web DuckDB layer and loaded CSV into DuckDB through pandas.

## Audit summary

| Area | Finding | Impact | First action |
| --- | --- | --- | --- |
| Legacy SQL service | `pipeline/services/sql_service.py` imported CSV through `pandas.read_csv` and registered the whole DataFrame in DuckDB. | Slow and memory-heavy on large exports. | Removed as non-web legacy path. |
| Web DB import | `DuckDbAppRepository.import_products_file_idempotent` previously read classified CSV into pandas and calculated row hashes in Python. | High memory use and slow CPU path on 1M+ rows. | Done: replaced with DuckDB staging SQL. |
| Idempotency | Smart pipeline has `cube_registry` skip. The old Python `__row_hash` and the new DuckDB SQL hash do not match byte-for-byte. | Anti-join by new hash is not safe for deduping already existing legacy rows. | Decision B adopted: smart import replaces the whole slice by project/period/marketplace/category inside one transaction. |
| Transactions | Import table changes and load history insert were not consistently grouped. | Partial updates were possible if a save failed mid-flow. | Done for products import; cube registry updates remain a separate service operation. |
| CSV export | Raw export already uses DuckDB `COPY`. Report CSV now uses direct DuckDB `COPY` for plain SQL reports. | Avoids pandas materialization for aggregated CSV files. | Done for report CSV; XLSX remains unchanged. |
| CSV merge | Step 5 merge previously used `pandas.read_csv` for every parsed CSV, then `pd.concat(...).drop_duplicates()`. | Memory-heavy on many large parsed/classified CSV files. | Done: merge CSV via DuckDB temp tables and SQL dedup. |
| XLSX export | openpyxl write-only mode is safe for formatted/sheet-controlled output. DuckDB Excel extension can write simple XLSX. | pandas/openpyxl should stay only on final aggregates or controlled batches. | Benchmark includes both simple DuckDB XLSX and pandas/openpyxl baseline. |
| DB settings | Connections were created through several local helpers and one direct in-memory merge connection. | No central place for `threads`, `memory_limit`, `temp_directory`, read-only mode or operation timing. | Done: managed connection helper, transaction helper, timing logs and settings benchmark. |
| Flat exports | Raw XLSX and report XLSX still used openpyxl/Python materialization, while CSV used DuckDB `COPY`. | CSV/XLSX could diverge and large flat XLSX pulled rows through Python. | Done: unified flat CSV/XLSX helper with DuckDB `COPY`, row-limit guard and openpyxl fallback only when Excel extension is unavailable. |

## Implemented changes

### Step 1: cleanup and benchmark

- Removed legacy standalone `pipeline/services/sql_service.py`.
- Removed its pandas-heavy import/query test.
- Kept `pipeline/repositories/sql_repository.py` as a small shared DuckDB infrastructure helper for web-app.
- Added reproducible benchmark scripts and this report.

### Step 2: web import via DuckDB staging

- `DuckDbAppRepository.import_products_file` and `import_products_file_idempotent` now load CSV through DuckDB `read_csv` into temp staging tables.
- Positive sales/volume filters run in DuckDB SQL before rows reach the products table.
- `__row_hash` is calculated in DuckDB SQL with `sha1(...)`.
- Because old Python hashes and new SQL hashes do not match 1:1, `import_products_file_idempotent` does not rely on hash anti-join for existing slices. It deletes the target slice by metadata and inserts the staged slice in the same transaction.
- Load table changes and `pipeline_loads` insert are wrapped in an explicit transaction.

DB impact:

- No migration was added in this step.
- Existing tables are preserved.
- For newly created products tables, source CSV columns are loaded as `VARCHAR`; metadata columns keep typed SQL expressions (`TIMESTAMP`, integer metadata, hash text). Existing numeric columns are preserved where possible, and text-containing source columns are widened to `VARCHAR` when needed.
- Existing rows keep their old `__row_hash` values until their slice is replaced. New rows use SQL-computed `sha1` hashes; smart pipeline also protects existing slices through `cube_registry`.

Hash compatibility check:

- Mock CSV: old Python filtered rows `2`, new SQL filtered rows `2`, matching hashes `0`.
- Real classified CSV `data/projects/mpstats/processed/2024-01/oz/f9022660368318fe_classified.csv`: old Python filtered rows `2,216`, new SQL filtered rows `2,216`, matching hashes `0`.
- Reason: the previous formula hashed JSON produced from pandas values; the new formula hashes SQL string concatenation from DuckDB `read_csv(all_varchar=true)`. These are intentionally treated as different hash generations.

Quality checks added:

- Regression test: repeating the same period/category import leaves exactly one slice in the table and duplicate `__row_hash` count stays `0`.
- Regression test: duplicate `__row_hash` count remains `0`.
- Existing tests still cover positive sales/volume filtering, garbage sales/volume filtering, required metadata and text widening.

### Step 3: report CSV via DuckDB COPY

- Added repository helper `export_query_to_csv(query, output_path, params=None, delimiter=';', header=True)`.
- The helper creates the parent directory, writes through DuckDB `COPY (SELECT ...) TO CSV`, returns `ExportResult` with path, file size, duration, status and `row_count` when DuckDB returns it.
- Report CSV build now calls `DuckDbAppRepository.export_report_to_csv` instead of `fetch_report_dataframe(...).to_csv(...)`.
- Report preview and report XLSX still use pandas/openpyxl paths:
  - preview needs a small DataFrame to serialize JSON rows;
  - XLSX uses openpyxl workbook writing, freeze panes and filters;
  - raw XLSX batching is outside this step and was not changed.
- CSV output keeps the current user-facing contract: delimiter `;`, header row and `utf-8-sig` BOM for Excel-friendly opening.

Row count policy:

- CSV build does not run `SELECT COUNT(*) FROM (<aggregate query>)` before `COPY`, because that doubles the heavy aggregate work.
- `row_count` is read from DuckDB `COPY` result when available. On local DuckDB 1.5.2 this returns the exported row count.
- `source_total` for CSV build is therefore the number of exported rows. Preview still returns the full counted total for pagination.
- If CSV `COPY` exports 0 rows, the service removes the created file and returns the existing "Нет строк для отчёта" error. The lower-level helper itself can still create a header-only CSV for direct repository use.

DB impact:

- No tables, columns or migrations were added.
- No import/smart pipeline SQL was changed.
- No XLSX export path was changed.

### Step 4: merge CSV via DuckDB

Current merge path audit:

- File: `pipeline/services/merge_service.py`.
- Callers:
  - CLI pipeline step 5: `pipeline/services/run_service.py`;
  - local web-app process action: `mpstats_app/services/workflow_service.py`.
- Inputs: semicolon CSV files from `PipelinePaths.step4_parsed_dir` (`04_step4_parsed`).
- Output: merged semicolon CSV at `PipelinePaths.merged_csv`, used by `classification_service.classify_file`.
- Old path:
  - `read_semicolon_csv(...)` for each file;
  - `pd.concat(frames, ignore_index=True)`;
  - column aliases: `Продажи` -> `Продажи, шт`, `Средняя цена` -> `Средняя цена, руб`, `Выручка` -> `Выручка, руб` when target column is absent;
  - sales normalization through string cleanup and `pd.to_numeric(...).fillna(0)`;
  - sales filter: `Продажи, шт > min_sales` and `< max_sales`;
  - `drop_duplicates()` by all normalized columns, `keep='first'`;
  - `to_csv(..., sep=';', encoding='utf-8-sig')`.
- Old order semantics: first file order, then row order inside each file; duplicates keep the first occurrence.

Implemented DuckDB path:

- Added `merge_csv_files_with_duckdb(...) -> MergeResult`.
- CSV bodies are read through DuckDB `read_csv(..., all_varchar=true, parallel=false)`, not pandas.
- Each file scan adds `__source_file_index` and `__source_row_number`.
- Dedup uses `ROW_NUMBER() OVER (PARTITION BY <dedup columns> ORDER BY __source_file_index, __source_row_number)`.
- Default dedup columns are all normalized output columns, matching old `drop_duplicates()`.
- Output order is `ORDER BY __source_file_index, __source_row_number`, matching old `keep='first'` order.
- Output is written by DuckDB `COPY ... TO CSV` with delimiter `;`, header and `utf-8-sig` BOM.
- `merge_directory` now returns `MergeResult` instead of a full merged DataFrame, so the web process action reads row count from `MergeResult.rows_out` and does not materialize the merged output in memory.

DB impact:

- No new persistent DuckDB tables were added.
- The helper uses in-memory DuckDB temp tables only.
- Import/smart pipeline DB import functions were not changed.
- XLSX and report CSV exports were not changed in this step.

### Step 5: managed DuckDB connection/settings

Connect audit:

- `pipeline/repositories/sql_repository.py`:
  - found `connect(db_path) -> duckdb.connect(str(path))`;
  - replaced with `get_duckdb_connection(...)`, `duckdb_connection(...)`, `duckdb_transaction(...)`, `measure_duckdb_operation(...)`;
  - kept public `connect(...)` API for existing repositories/tests.
- `mpstats_app/repositories/duckdb_repository.py`:
  - no direct `duckdb.connect(...)`;
  - all DB operations opened short-lived connections through `pipeline.repositories.sql_repository.connect`;
  - active heavy import/report/export paths now pass through the managed helper with optional temp dir and timing logs;
  - report/export/product preview reads and CSV `COPY` use `read_only=True` where they do not need DB writes;
  - app writes, migrations, runs, tasks, schedules, cube registry and import paths keep write connections.
- `pipeline/services/merge_service.py`:
  - found direct `duckdb.connect(":memory:")`;
  - replaced with `duckdb_connection(":memory:", ...)`;
  - merge temp tables remain in-memory and use a controlled temp dir for spills.
- `scripts/duckdb_benchmark.py`:
  - found local benchmark `connect(...)` wrapper with direct `duckdb.connect(...)`;
  - replaced with the managed helper and added a separate settings benchmark.
- Tests:
  - `tests/test_web_api.py` imports `connect`; API stayed compatible.
- Left in place:
  - the only active `duckdb.connect(...)` call is inside `pipeline/repositories/sql_repository.get_duckdb_connection`;
  - docs/report text may mention old calls for historical audit context.

Connection lifetime/frequency:

- No global singleton connection was added.
- Existing repository calls are short-lived and use context managers, so connections are closed promptly.
- `DuckDbAppRepository` still opens connections frequently in composed methods such as options/summary builders. This is a known tradeoff kept for minimal diff; consolidating those calls would be a separate repository refactor.
- Import and CSV export paths now log operation timing and rows/file metadata when known.

Write/read-only classification:

- Write connections required:
  - migrations and `ensure_ready`;
  - app run/task/schedule/cube registry mutations;
  - product import staging plus insert/delete;
  - `mark_reports_built`;
  - destructive project/cube cleanup flows.
- Read-only connections used:
  - product preview count/page query;
  - report count/preview query;
  - export count/preview query;
  - report/raw CSV `COPY` reads that only write an external file.

Managed settings:

- `DUCKDB_THREADS`: optional positive integer. If unset, DuckDB keeps its default.
- `DUCKDB_MEMORY_LIMIT`: optional DuckDB memory limit string, for example `2GB`, `4GB`, `6GB`. If unset, no manual limit is applied.
- `DUCKDB_TEMP_DIRECTORY`: optional controlled temp/spill directory. If unset:
  - generic `connect(...)` does not force a temp dir;
  - web import/report/export heavy paths use `data/duckdb_tmp` under the project root;
  - merge CSV uses `.duckdb_tmp` next to the merge output.
- `read_only` is a connection flag, not an env setting, because it depends on the operation.

Transaction/timing:

- Added `duckdb_transaction(con)`:
  - `BEGIN`;
  - `COMMIT` on success;
  - `ROLLBACK` on exception;
  - exception is re-raised.
- `import_products_file` and `import_products_file_idempotent` now use this helper instead of manual `BEGIN/COMMIT/ROLLBACK`.
- Added `measure_duckdb_operation(name, metadata)` logger context:
  - logs operation name, duration, DB path, file path, file size, row metadata when known and success/error status;
  - does not add DB tables.

DB impact:

- No schema changes.
- No migrations were added.
- Persistent data layout is unchanged.
- New temp directories may appear under `data/duckdb_tmp`, merge output directories or the user-provided `DUCKDB_TEMP_DIRECTORY`.

Writer-lock risk:

- DuckDB still has a single-writer constraint for one `.duckdb` file.
- The app repository keeps an in-process `RLock`, which reduces conflicts inside one local web-app process.
- Two separate app processes or external scripts writing the same `mpstats.duckdb` can still conflict.
- Read-only report/export queries reduce accidental writer use, but they do not make concurrent external writes safe.

### Step 6: flat CSV/XLSX export via DuckDB COPY

Export path audit:

- Raw CSV export:
  - already used `DuckDbAppRepository.export_products_to_csv(...)`;
  - now routes through `export_products_flat(..., format="csv")` and `export_flat_query(...)`;
  - stays DuckDB `COPY TO CSV` with delimiter `;` and BOM/header behavior preserved.
- Raw XLSX export:
  - previously used `ExportService._write_xlsx(...)` with openpyxl write-only batching and repeated `fetch_export_products_dataframe(...)`;
  - now uses the same raw export SQL as raw CSV and writes through DuckDB `COPY ... FORMAT xlsx`;
  - openpyxl is only fallback if `INSTALL/LOAD excel` or `COPY FORMAT xlsx` fails.
- Report CSV export:
  - already used `DuckDbAppRepository.export_report_to_csv(...)`;
  - now routes through the same `export_flat_query(...)` helper.
- Report XLSX export:
  - previously used `fetch_report_dataframe(...)` and `ReportService._write_xlsx(...)`;
  - now uses the same report SQL as report CSV and writes through DuckDB `COPY ... FORMAT xlsx`.
- Preview paths:
  - still fetch a small DataFrame for JSON rows in the web UI;
  - not part of the large flat file export path.
- Non-flat/simple-data paths left alone:
  - classifier upload/classification XLSX handling;
  - category/rules CSV writes;
  - pipeline enrich/classification CSV generation;
  - benchmark legacy baselines.

Unified helper:

- Added `DuckDbAppRepository.export_flat_query(...)`.
- Inputs:
  - `query`;
  - `output_path`;
  - `format`: `csv` or `xlsx`;
  - optional params, delimiter, header and sheet name.
- Outputs:
  - path;
  - format;
  - duration seconds;
  - file size;
  - row count when known;
  - status;
  - fallback/error reason when fallback was used.

CSV policy:

- CSV stays direct DuckDB `COPY`.
- Delimiter remains `;`.
- Header+BOM behavior is preserved through the existing `PREFIX` strategy, so Excel-friendly opening stays the same.
- CSV remains the recommended format above the Excel row limit.

XLSX policy:

- Main path:
  - `INSTALL excel`;
  - `LOAD excel`;
  - `COPY (<query>) TO '<file.xlsx>' WITH (FORMAT xlsx, HEADER true, SHEET 'Data')`.
- Fallback:
  - if the DuckDB Excel extension path fails, the helper logs a warning and writes the same query with openpyxl streaming;
  - fallback status is `fallback`, with the extension/COPY error stored in `ExportResult.error`;
  - fallback is not the normal large export path.
- No styles, freeze panes, filters, formulas, merged cells, extra sheets or formatting are added.

Row limit policy:

- Excel has 1,048,576 rows per sheet.
- With a header row, the app allows up to 1,048,575 data rows in one XLSX.
- If the flat XLSX query would export more rows, the helper raises: `XLSX row limit exceeded (... rows), use CSV.`
- The app no longer creates automatic row-split XLSX parts. `Разными файлами по категориям` can still create separate flat files per selected category.

Type casting policy for Excel pivots:

- No DB schema changes.
- Raw export query casts only the exported SELECT:
  - sales/revenue/volume/price-like columns are `DOUBLE`;
  - stores-count-like columns are `BIGINT`;
  - date-like columns are `DATE`;
  - timestamp-like columns are `TIMESTAMP`;
  - text dimensions remain `VARCHAR`.
- Report query already emits numeric aggregates (`SUM`, `COUNT`, arithmetic metrics) and year/month integers.
- CSV and XLSX raw exports use the same typed SELECT, so row sets match across formats.

### Step 7: CSV decimal comma formatting

- Flat CSV export now applies decimal comma formatting inside the export SQL query before DuckDB `COPY` writes the file.
- XLSX export is not wrapped and keeps DuckDB/openpyxl numeric cells as numbers.
- CSV delimiter remains `;`.
- Header and `utf-8-sig` BOM behavior are preserved through the existing `PREFIX` strategy.
- Protected dimension/text columns are not formatted: `ДАТА`, `SKU`, `продавец`, `категория`, `бренд`, `год`, `месяц`, `подкатегория`, `тип`. Column-name matching is case-insensitive and trims surrounding spaces.
- Non-protected columns are cast to `VARCHAR` only for CSV output and use `regexp_replace(..., '([0-9])\.([0-9])', '\1,\2', 'g')`, so decimal points between digits become commas without running a blind replace over the finished CSV file.
- Power Query in Russian locale no longer needs manual replacement of `.` with `,` for flat CSV exports.
- Filters, sorting, selected columns and row-count semantics stay on the original query; the formatting wrapper is applied after the source query result.

DB impact:

- No schema changes.
- No migrations were added.
- No new persistent DuckDB tables.
- XLSX export may install/load the DuckDB Excel extension in the local DuckDB extension directory.

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
python3 scripts/duckdb_benchmark.py --all-sizes --skip-excel
python3 scripts/duckdb_benchmark.py --merge-only --all-sizes
python3 scripts/duckdb_benchmark.py --merge-only --size large --include-large-merge
python3 scripts/duckdb_benchmark.py --settings-only --size small --rows 50000 --settings-merge-size medium --skip-excel
python3 scripts/duckdb_benchmark.py --flat-export-only --all-sizes
python3 scripts/duckdb_benchmark.py --flat-export-only --size large --include-large-flat-export
```

Outputs are written to `data/duckdb_benchmark/`:

- generated mock CSV
- old/new `.duckdb` files
- CSV/XLSX exports
- `benchmark_<size>.json`
- `benchmark_<size>.md`
- `settings_benchmark.json`
- `settings_benchmark.md`
- `flat_export_benchmark.json`
- `flat_export_benchmark.md`

## Benchmark results

Step 3 local run:

- `python3 scripts/duckdb_benchmark.py --all-sizes --workdir data/duckdb_benchmark_step3 --skip-excel`

Memory:

- Per-operation memory was not measured. The project has no existing simple RSS/heap benchmark helper, and this step avoids adding a separate profiler.

### Step 3 report CSV results

The report CSV benchmark uses a top-SKU-like aggregate query grouped by period/category/network/brand/SKU. It compares:

- old path: DuckDB aggregate query -> `fetchdf()` -> `df.to_csv(..., sep=';', encoding='utf-8-sig')`;
- new path: `COPY (aggregate query) TO CSV`.

| size | input rows | exported report rows | old duration, s | new duration, s | speedup | old file bytes | new file bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 10,000 | 9,989 | 0.0344 | 0.0100 | 3.44x | 799,834 | 799,831 |
| medium | 500,000 | 499,496 | 1.4445 | 0.1374 | 10.52x | 39,974,646 | 39,974,643 |
| large | 2,000,000 | 750,000 | 2.5734 | 0.2568 | 10.02x | 68,347,072 | 68,349,535 |

Notes:

- Row counts match old/new in all three runs.
- File sizes are close but not byte-identical because pandas and DuckDB serialize floating-point values slightly differently.
- Excel benchmarks were skipped for Step 3 by design.

### COPY CSV limitations

DuckDB `COPY` is used only for simple report CSV exports where the result is a normal SQL result set.

Pandas/openpyxl should remain for cases that need:

- XLSX workbook structure, freeze panes, filters, styles or formulas;
- several logical tables in one artifact;
- Python post-processing before writing;
- small preview data that must be converted to JSON rows.

Current report CSV does not need these features, so it is a type A export and now goes through direct `COPY`.

### Step 4 merge CSV results

Step 4 local run:

- `python3 scripts/duckdb_benchmark.py --merge-only --all-sizes --workdir data/duckdb_merge_benchmark_step4`

Large merge benchmark:

- Not run by default.
- `--merge-only --all-sizes` runs only `small` and `medium`.
- `large` requires `--include-large-merge`.

Memory:

- Per-operation memory was not measured. The project still has no lightweight RSS/heap helper, and this step avoids adding a profiler.

The merge benchmark compares:

- old path: pandas `read_csv` for every file -> `pd.concat` -> sales filter -> `drop_duplicates` -> `to_csv`;
- new path: DuckDB `read_csv` scans -> temp table -> SQL sales filter -> SQL window dedup -> `COPY TO CSV`.

| size | input files | input rows | old output rows | new output rows | old dup removed | new dup removed | old duration, s | new duration, s | speedup | old file bytes | new file bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 3 | 30,000 | 24,947 | 24,947 | 4,990 | 4,990 | 0.1070 | 0.0777 | 1.38x | 1,444,353 | 1,494,247 |
| medium | 5 | 2,500,000 | 1,995,978 | 1,995,978 | 498,992 | 498,992 | 8.2827 | 2.1138 | 3.92x | 115,555,637 | 119,547,593 |

Notes:

- Output row counts match old/new.
- Duplicate removal counts match old/new.
- File sizes differ because DuckDB and pandas serialize numeric values differently, but downstream CSV reads get equivalent columns and values.
- Small datasets can still be dominated by DuckDB startup/query overhead; the value of Step 4 is avoiding pandas materialization on large multi-file merges.

### DuckDB merge limitations

DuckDB merge is used for the standard semicolon CSV step 5 path.

Pandas merge helpers are left in place for:

- small direct unit tests of `merge_dataframes`;
- ad hoc legacy callers that already pass DataFrames;
- future nonstandard cases where Python post-processing is explicitly required.

Known constraints:

- The optimized path expects a normal header row and semicolon CSV dialect.
- `utf-8-sig` input is scanned as UTF-8 by DuckDB; the output still includes the BOM.
- Exact byte-for-byte CSV equality with pandas is not guaranteed for numeric formatting, but schema, order, row count and dedup semantics are preserved.

### Step 5 settings benchmark

Step 5 local run:

- `python3 scripts/duckdb_benchmark.py --settings-only --size small --rows 50000 --settings-merge-size medium --workdir data/duckdb_settings_benchmark_step5_medium --skip-excel`

Benchmark operations:

- load CSV into DuckDB through `read_csv` + CTAS;
- report aggregate query;
- report CSV `COPY`;
- merge CSV medium: 5 files x 500,000 rows.

| variant | threads | memory_limit | temp dir | load CSV, s | report aggregate, s | report CSV COPY, s | merge CSV, s | total, s | speedup vs default | rows loaded | report rows | merge rows |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default | - | - | no | 0.1435 | 0.0131 | 0.0627 | 2.2797 | 2.4990 | 1.00x | 49,949 | 49,949 | 1,995,978 |
| threads=1 | 1 | - | no | 0.1011 | 0.0057 | 0.0526 | 3.4303 | 3.5898 | 0.70x | 49,949 | 49,949 | 1,995,978 |
| threads=4 | 4 | - | no | 0.1052 | 0.0029 | 0.0450 | 1.9908 | 2.1438 | 1.17x | 49,949 | 49,949 | 1,995,978 |
| memory_limit=2GB | - | 2GB | no | 0.1067 | 0.0025 | 0.0423 | 2.1276 | 2.2790 | 1.10x | 49,949 | 49,949 | 1,995,978 |
| memory_limit=4GB | - | 4GB | no | 0.1059 | 0.0026 | 0.0434 | 2.1258 | 2.2776 | 1.10x | 49,949 | 49,949 | 1,995,978 |
| temp_directory | - | - | yes | 0.1141 | 0.0037 | 0.0561 | 2.2401 | 2.4140 | 1.04x | 49,949 | 49,949 | 1,995,978 |

Notes:

- `threads=4` was the clearest useful setting for the medium merge workload: `2.2797s -> 1.9908s`.
- `threads=1` made medium merge slower: `2.2797s -> 3.4303s`.
- `memory_limit=2GB/4GB` looked slightly faster in this run, but the workload did not prove a memory-pressure win; treat it mainly as a guardrail, not a performance default.
- Explicit `temp_directory` did not materially improve this run. It is still useful for controlled spill location on larger joins/sorts or low-memory machines.
- Small load/report timings are noisy at this scale; recommendations should be based on repeated local runs on the actual project data.

Recommended local settings:

- Safe default: leave `DUCKDB_THREADS` and `DUCKDB_MEMORY_LIMIT` unset.
- Try `DUCKDB_THREADS=4` for large merge/report sessions on a machine with spare CPU; do not hardcode it globally without local measurement.
- Use `DUCKDB_MEMORY_LIMIT=4GB` or `6GB` only when you need a memory cap. It is not enabled by default.
- Use `DUCKDB_TEMP_DIRECTORY=/absolute/path/to/project/data/duckdb_tmp` when running large workloads and you want spills away from random system temp locations.
- Do not run two write-capable app/script processes against the same `.duckdb` file.

### Step 6 flat CSV/XLSX export results

Step 6 local runs:

- `python3 scripts/duckdb_benchmark.py --flat-export-only --size small --workdir data/duckdb_flat_export_benchmark_step6`
- `python3 scripts/duckdb_benchmark.py --flat-export-only --size medium --workdir data/duckdb_flat_export_benchmark_step6_medium`

Large flat export:

- Not run by default.
- Use `--flat-export-only --size large --include-large-flat-export` for the explicit 1,000,000-row XLSX benchmark.

Benchmark operations:

- old XLSX: openpyxl write-only streaming with `LIMIT/OFFSET` batches;
- new XLSX: DuckDB `COPY ... FORMAT xlsx`;
- CSV: DuckDB `COPY ... FORMAT csv`;
- row-limit check: `range(1048577)` must refuse XLSX before creating a file.

| size | rows | old XLSX openpyxl, s | new XLSX DuckDB COPY, s | XLSX speedup | CSV COPY, s | old XLSX rows | new XLSX rows | CSV rows | old XLSX bytes | new XLSX bytes | CSV bytes | xlsx opens | headers match | numeric cells | CSV rows match XLSX | row-limit error |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- |
| small | 9,989 | 0.8094 | 0.1082 | 7.48x | 0.0094 | 9,989 | 9,989 | 9,989 | 577,141 | 574,336 | 925,367 | true | true | true | true | XLSX row limit exceeded (1048577 rows), use CSV. |
| medium | 499,496 | 33.7077 | 3.0476 | 11.06x | 0.1548 | 499,496 | 499,496 | 499,496 | 28,617,491 | 28,648,257 | 46,252,306 | true | true | true | true | XLSX row limit exceeded (1048577 rows), use CSV. |

Notes:

- DuckDB XLSX COPY is the clear winner for simple flat XLSX: `7.48x` on small and `11.06x` on medium.
- CSV COPY remains much faster than XLSX and did not regress in the benchmark.
- XLSX files open with openpyxl, headers match the SQL query and numeric cells read back as numeric Python values.
- CSV and XLSX exported row counts match on the same query.
- The row-limit guard refuses `1,048,577` rows and points users to CSV.

Earlier Step 1/2 local runs:

- `python3 scripts/duckdb_benchmark.py --size small --workdir data/duckdb_benchmark --skip-excel`
- `python3 scripts/duckdb_benchmark.py --size medium --workdir data/duckdb_benchmark --skip-excel`

### small: 10,000 rows

Note: benchmark `idempotent_rerun` measures a synthetic anti-join strategy. Production smart import now uses slice replacement because old/new hash formulas are not compatible.

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

Note: benchmark `idempotent_rerun` measures a synthetic anti-join strategy. Production smart import now uses slice replacement because old/new hash formulas are not compatible.

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

Current verification focus:

1. Keep web import on DuckDB SQL staging.
2. Keep idempotency as slice replacement for smart pipeline imports.
3. Keep raw CSV export on DuckDB `COPY`.
4. Keep Excel export unchanged in Step 5.
5. Use direct DuckDB `COPY` for plain report CSV exports.
6. Use DuckDB temp-table merge for standard step 5 parsed CSV merging.
7. Keep DuckDB connection settings optional: no required env vars for default local run.
8. Use one flat DuckDB `COPY` helper for raw/report CSV and XLSX.
9. Keep XLSX simple: one sheet, header row, typed values, no styling/filtering.

Verification smoke:

- Save classified CSV slice to DuckDB: `2` rows inserted.
- Repeat save of the same slice: slice replaced, final DB row count stays `2`.
- Cube API opens with `1` item.
- Products API returns `2` rows.
- Raw CSV export job succeeds and creates a file.
- Raw XLSX export succeeds and creates a file.
- Report CSV export succeeds and does not call pandas `fetchdf()` for the final file.
- Step 5 merge CSV succeeds and does not call pandas `concat()` for the directory merge path.
- Optional `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, `DUCKDB_TEMP_DIRECTORY` settings are picked up by the shared connection helper.
- Raw XLSX export succeeds without calling `fetch_export_products_dataframe()` for file writing.
- Report XLSX export succeeds without calling `fetch_report_dataframe()` for file writing.
- XLSX files open in Excel/openpyxl and numeric columns can be summed in pivots.
- XLSX over the row limit is rejected with `XLSX row limit exceeded, use CSV`.
