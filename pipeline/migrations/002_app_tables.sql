CREATE TABLE IF NOT EXISTS app_runs (
    run_id VARCHAR PRIMARY KEY,
    project_name VARCHAR NOT NULL,
    steps VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    schedule_id VARCHAR,
    workdir VARCHAR NOT NULL,
    config_path VARCHAR NOT NULL,
    rules_path VARCHAR NOT NULL,
    db_path VARCHAR NOT NULL,
    products_table VARCHAR NOT NULL,
    write_xlsx BOOLEAN NOT NULL,
    max_weight_kg DOUBLE NOT NULL,
    fill_unclassified_json VARCHAR,
    requested_cancel BOOLEAN DEFAULT false,
    manifest_path VARCHAR,
    error VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_run_steps (
    run_id VARCHAR NOT NULL,
    step_number INTEGER NOT NULL,
    step_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    rows_loaded BIGINT DEFAULT 0,
    ok_count BIGINT DEFAULT 0,
    error_count BIGINT DEFAULT 0,
    skipped_count BIGINT DEFAULT 0,
    output VARCHAR,
    details_json VARCHAR,
    error VARCHAR,
    started_at TIMESTAMP DEFAULT current_timestamp,
    finished_at TIMESTAMP,
    PRIMARY KEY (run_id, step_number)
);

CREATE TABLE IF NOT EXISTS app_run_events (
    event_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    message VARCHAR NOT NULL,
    payload_json VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS app_schedules (
    schedule_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    project_name VARCHAR NOT NULL,
    steps VARCHAR NOT NULL,
    enabled BOOLEAN NOT NULL,
    interval_minutes BIGINT NOT NULL,
    next_run_at TIMESTAMP NOT NULL,
    last_run_at TIMESTAMP,
    write_xlsx BOOLEAN NOT NULL,
    max_weight_kg DOUBLE NOT NULL,
    fill_unclassified_json VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp,
    updated_at TIMESTAMP DEFAULT current_timestamp
);
