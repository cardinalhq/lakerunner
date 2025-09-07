-- name: WorkQueueScalingDepth :one
-- Get queue depth for work queue scaling by signal and action
SELECT COALESCE(COUNT(*), 0) AS count
FROM work_queue
WHERE needs_run = true
  AND runnable_at <= now()
  AND signal = $1
  AND action = $2;

-- name: MetricRollupQueueScalingDepth :one
-- Get queue depth for metric rollup scaling
SELECT COALESCE(COUNT(*), 0) AS count
FROM metric_rollup_queue
WHERE claimed_at IS NULL;
