ALTER TABLE cube_registry ADD COLUMN IF NOT EXISTS days_loaded INTEGER;
ALTER TABLE cube_registry ADD COLUMN IF NOT EXISTS days_in_month INTEGER;
ALTER TABLE cube_registry ADD COLUMN IF NOT EXISTS data_actual_until DATE;

UPDATE cube_registry
SET days_in_month = CASE month
    WHEN 1 THEN 31
    WHEN 2 THEN CASE
        WHEN year % 400 = 0 OR (year % 4 = 0 AND year % 100 <> 0) THEN 29
        ELSE 28
    END
    WHEN 3 THEN 31
    WHEN 4 THEN 30
    WHEN 5 THEN 31
    WHEN 6 THEN 30
    WHEN 7 THEN 31
    WHEN 8 THEN 31
    WHEN 9 THEN 30
    WHEN 10 THEN 31
    WHEN 11 THEN 30
    WHEN 12 THEN 31
    ELSE NULL
END
WHERE days_in_month IS NULL;

UPDATE cube_registry
SET days_loaded = CASE
    WHEN year * 12 + month < CAST(EXTRACT(year FROM saved_to_db_at) AS INTEGER) * 12 + CAST(EXTRACT(month FROM saved_to_db_at) AS INTEGER)
        THEN days_in_month
    WHEN year * 12 + month = CAST(EXTRACT(year FROM saved_to_db_at) AS INTEGER) * 12 + CAST(EXTRACT(month FROM saved_to_db_at) AS INTEGER)
        THEN LEAST(CAST(EXTRACT(day FROM saved_to_db_at) AS INTEGER), days_in_month)
    ELSE 0
END
WHERE days_loaded IS NULL AND saved_to_db_at IS NOT NULL;

UPDATE cube_registry
SET data_actual_until = CASE
    WHEN days_loaded > 0 THEN CAST(printf('%04d-%02d-%02d', year, month, days_loaded) AS DATE)
    ELSE NULL
END
WHERE data_actual_until IS NULL AND days_loaded IS NOT NULL;
