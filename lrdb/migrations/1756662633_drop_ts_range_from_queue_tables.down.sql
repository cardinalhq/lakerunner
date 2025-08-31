-- 1756662633_drop_ts_range_from_queue_tables.down.sql

-- Re-add ts_range columns to queue tables
ALTER TABLE metric_rollup_queue ADD COLUMN ts_range TSTZRANGE;
ALTER TABLE metric_compaction_queue ADD COLUMN ts_range TSTZRANGE;