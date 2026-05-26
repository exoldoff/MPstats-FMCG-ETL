CREATE TABLE IF NOT EXISTS pipeline_loads (
    loaded_at TIMESTAMP DEFAULT current_timestamp,
    table_name VARCHAR NOT NULL,
    source_file VARCHAR,
    load_name VARCHAR,
    project_name VARCHAR,
    mode VARCHAR NOT NULL,
    rows_loaded BIGINT NOT NULL
);
