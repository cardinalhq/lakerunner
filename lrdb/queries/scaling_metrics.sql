-- name: InqueueScalingDepth :one
-- Get queue depth for ingest scaling by signal
SELECT COALESCE(COUNT(*), 0) AS count
FROM inqueue 
WHERE signal = @signal 
  AND claimed_at IS NULL;

-- name: WorkQueueScalingDepth :one  
-- Get queue depth for work queue scaling by signal and action
SELECT COALESCE(COUNT(*), 0) AS count
FROM work_queue 
WHERE needs_run = true 
  AND runnable_at <= now() 
  AND signal = $1 
  AND action = $2;

-- name: MetricCompactionQueueScalingDepth :one
-- Get queue depth for metric compaction scaling
SELECT COALESCE(COUNT(*), 0) AS count
FROM metric_compaction_queue 
WHERE claimed_at IS NULL;

-- name: MetricRollupQueueScalingDepth :one
-- Get queue depth for metric rollup scaling
SELECT COALESCE(COUNT(*), 0) AS count
FROM metric_rollup_queue 
WHERE claimed_at IS NULL;