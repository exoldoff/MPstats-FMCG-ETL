ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS stop_requested BOOLEAN DEFAULT false;
