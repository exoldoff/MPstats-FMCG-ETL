CREATE TABLE IF NOT EXISTS app_settings (
    key VARCHAR PRIMARY KEY,
    value VARCHAR,
    updated_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS app_category_catalog (
    category_id VARCHAR PRIMARY KEY,
    category_name VARCHAR NOT NULL,
    marketplace VARCHAR NOT NULL,
    mp_code VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    filter_json VARCHAR,
    source_file VARCHAR,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT current_timestamp,
    updated_at TIMESTAMP DEFAULT current_timestamp
);
