-- NOTE: metric_rollup_queue and metric_compaction_queue tables have since been dropped

-- Remove window_close_ts column from metric_rollup_queue
-- ALTER TABLE metric_rollup_queue DROP COLUMN window_close_ts;
-- ALTER TABLE metric_rollup_queue ALTER COLUMN frequency_ms TYPE integer USING frequency_ms::integer;
-- ALTER TABLE metric_compaction_queue ALTER COLUMN frequency_ms TYPE integer USING frequency_ms::integer;
ALTER TABLE metric_pack_estimate ALTER COLUMN frequency_ms TYPE integer USING frequency_ms::integer;
