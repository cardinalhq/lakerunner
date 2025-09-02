-- 1756809100_optimize_queue_indexes.down.sql

-- Recreate previous indexes for queue tables

-- Drop new compaction queue index
DROP INDEX IF EXISTS mcq_ready_group_idx;

-- Restore original compaction queue indexes
CREATE INDEX IF NOT EXISTS idx_mcq_ready_global
  ON metric_compaction_queue (priority DESC, queue_ts, id)
  WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mcq_ready_bigrow
  ON metric_compaction_queue (record_count DESC, priority DESC, queue_ts, id)
  WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mcq_ready_group
  ON metric_compaction_queue (organization_id, dateint, frequency_ms, instance_num, priority DESC, queue_ts, id)
  WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mcq_claimed_at_nonnull
  ON metric_compaction_queue (claimed_at) WHERE claimed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mcq_heartbeated_at_nonnull
  ON metric_compaction_queue (heartbeated_at) WHERE heartbeated_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS mcq_claimed_hb_idx
  ON metric_compaction_queue (heartbeated_at) WHERE claimed_by <> -1;

-- Drop new rollup queue indexes
DROP INDEX IF EXISTS mrq_ready_idx;
DROP INDEX IF EXISTS mrq_ready_group_idx;
DROP INDEX IF EXISTS idx_mrq_expired_heartbeat;

-- Restore original rollup queue indexes
CREATE INDEX IF NOT EXISTS idx_mrq_ready_global
  ON metric_rollup_queue (priority DESC, queue_ts, id)
  WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mrq_ready_group
  ON metric_rollup_queue (organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group, priority DESC, queue_ts, id)
  WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mrq_claimed_at_nonnull
  ON metric_rollup_queue (claimed_at) WHERE claimed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS mrq_claimed_hb_idx
  ON metric_rollup_queue (heartbeated_at) WHERE claimed_by <> -1;
