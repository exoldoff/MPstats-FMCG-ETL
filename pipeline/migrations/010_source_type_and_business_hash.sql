ALTER TABLE app_category_catalog ADD COLUMN IF NOT EXISTS source_type VARCHAR DEFAULT 'category';
UPDATE app_category_catalog
SET source_type = 'category'
WHERE source_type IS NULL OR TRIM(CAST(source_type AS VARCHAR)) = '';

ALTER TABLE download_tasks ADD COLUMN IF NOT EXISTS source_type VARCHAR DEFAULT 'category';
UPDATE download_tasks
SET source_type = 'category'
WHERE source_type IS NULL OR TRIM(CAST(source_type AS VARCHAR)) = '';

ALTER TABLE cube_registry ADD COLUMN IF NOT EXISTS source_type VARCHAR DEFAULT 'category';
UPDATE cube_registry
SET source_type = 'category'
WHERE source_type IS NULL OR TRIM(CAST(source_type AS VARCHAR)) = '';
