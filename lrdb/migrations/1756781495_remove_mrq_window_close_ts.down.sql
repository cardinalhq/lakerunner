-- Add back window_close_ts column to metric_rollup_queue
ALTER TABLE metric_rollup_queue ADD COLUMN window_close_ts TIMESTAMP WITH TIME ZONE;