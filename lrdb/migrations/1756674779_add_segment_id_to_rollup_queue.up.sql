-- 1756674779_add_segment_id_to_rollup_queue.up.sql

-- Add segment_id column to metric_rollup_queue to enable precise segment targeting
-- This allows rollup work items to specify exactly which segments to process,
-- similar to how metric_compaction_queue works.

ALTER TABLE metric_rollup_queue ADD COLUMN segment_id BIGINT NOT NULL DEFAULT 0;

-- Remove the default after adding the column
ALTER TABLE metric_rollup_queue ALTER COLUMN segment_id DROP DEFAULT;

-- Create index for efficient lookups by segment_id
CREATE INDEX IF NOT EXISTS idx_mrq_segment_id 
ON metric_rollup_queue (segment_id);