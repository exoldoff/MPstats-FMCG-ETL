ALTER TABLE cube_registry ADD COLUMN IF NOT EXISTS exported_at TIMESTAMP;

UPDATE cube_registry
SET exported_at = saved_to_db_at
WHERE exported_at IS NULL;
