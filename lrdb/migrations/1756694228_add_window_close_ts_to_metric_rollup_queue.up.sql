-- Clear existing data before schema change since we're adding NOT NULL without default
TRUNCATE TABLE metric_rollup_queue;

-- Add window_close_ts column to track when rollup windows can safely be processed
ALTER TABLE metric_rollup_queue 
ADD COLUMN window_close_ts TIMESTAMPTZ NOT NULL;

-- Add index for efficient querying by window close time
CREATE INDEX idx_mrq_window_close ON metric_rollup_queue(window_close_ts) 
WHERE claimed_at IS NULL;