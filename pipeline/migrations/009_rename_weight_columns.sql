-- DuckDB does not support conditional UPDATE IF EXISTS in plain SQL migrations.
-- The data-preserving rename is applied by DuckDbAppRepository.ensure_ready(),
-- which can check the configured products table before altering it.
SELECT 1;
