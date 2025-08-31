-- name: InqueueScalingDepth :one
-- Get queue depth for ingest scaling by telemetry type
SELECT COALESCE(COUNT(*), 0) AS count
FROM inqueue 
WHERE telemetry_type = $1 
  AND claimed_at IS NULL;

-- name: WorkQueueScalingDepth :one  
-- Get queue depth for work queue scaling by signal and action
SELECT COALESCE(COUNT(*), 0) AS count
FROM work_queue 
WHERE needs_run = true 
  AND runnable_at <= now() 
  AND signal = $1 
  AND action = $2;