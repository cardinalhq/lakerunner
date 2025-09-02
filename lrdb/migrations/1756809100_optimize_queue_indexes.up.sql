-- 1756809100_optimize_queue_indexes.up.sql

-- Optimize queue indexes for ready and heartbeat queries

-- Drop obsolete compaction queue indexes
DROP INDEX IF EXISTS idx_mcq_ready_global;
DROP INDEX IF EXISTS idx_mcq_ready_bigrow;
DROP INDEX IF EXISTS idx_mcq_ready_group;
DROP INDEX IF EXISTS idx_mcq_claimed_at_nonnull;
DROP INDEX IF EXISTS idx_mcq_heartbeated_at_nonnull;
DROP INDEX IF EXISTS mcq_claimed_hb_idx;

-- Create improved compaction queue ready index scoped to group
CREATE INDEX IF NOT EXISTS mcq_ready_group_idx
ON public.metric_compaction_queue (
    organization_id,
    dateint,
    frequency_ms,
    instance_num,
    eligible_at,
    queue_ts,
    id
) INCLUDE (segment_id, record_count)
WHERE claimed_by = -1;

-- Drop obsolete rollup queue indexes
DROP INDEX IF EXISTS idx_mrq_ready_global;
DROP INDEX IF EXISTS idx_mrq_ready_group;
DROP INDEX IF EXISTS idx_mrq_claimed_at_nonnull;
DROP INDEX IF EXISTS idx_mrq_segment_id;
DROP INDEX IF EXISTS mrq_claimed_hb_idx;

-- Create optimized rollup queue ready indexes
CREATE INDEX IF NOT EXISTS mrq_ready_idx
ON public.metric_rollup_queue (
    priority,
    eligible_at,
    queue_ts
) INCLUDE (
    organization_id,
    dateint,
    frequency_ms,
    instance_num,
    slot_id,
    slot_count,
    rollup_group,
    segment_id,
    record_count
) WHERE claimed_by = -1;

CREATE INDEX IF NOT EXISTS mrq_ready_group_idx
ON public.metric_rollup_queue (
    organization_id,
    dateint,
    frequency_ms,
    instance_num,
    slot_id,
    slot_count,
    rollup_group,
    eligible_at,
    queue_ts,
    id
) INCLUDE (segment_id, record_count)
WHERE claimed_by = -1;

-- Index to efficiently reclaim timed out rollup work
CREATE INDEX IF NOT EXISTS idx_mrq_expired_heartbeat
ON public.metric_rollup_queue (heartbeated_at, claimed_by)
WHERE claimed_by <> -1 AND heartbeated_at IS NOT NULL;
