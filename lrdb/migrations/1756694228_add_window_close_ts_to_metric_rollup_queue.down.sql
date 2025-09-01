-- Drop the index first
DROP INDEX IF EXISTS idx_mrq_window_close;

-- Remove the window_close_ts column
ALTER TABLE metric_rollup_queue 
DROP COLUMN IF EXISTS window_close_ts;