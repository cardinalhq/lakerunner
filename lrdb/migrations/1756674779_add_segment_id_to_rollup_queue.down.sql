-- 1756674779_add_segment_id_to_rollup_queue.down.sql

-- Remove segment_id column from metric_rollup_queue

DROP INDEX IF EXISTS idx_mrq_segment_id;
ALTER TABLE metric_rollup_queue DROP COLUMN IF EXISTS segment_id;