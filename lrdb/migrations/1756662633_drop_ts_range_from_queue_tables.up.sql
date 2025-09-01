-- 1756662633_drop_ts_range_from_queue_tables.up.sql

-- Drop ts_range columns from queue tables - they're causing more problems than they solve
-- We only need the primary key fields and bundling info like record_count

ALTER TABLE metric_rollup_queue DROP COLUMN IF EXISTS ts_range;
ALTER TABLE metric_compaction_queue DROP COLUMN IF EXISTS ts_range;