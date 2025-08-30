-- name: CleanupMetricCompactionWork :many
UPDATE metric_compaction_queue
SET claimed_by = -1, claimed_at = NULL, heartbeated_at = NULL
WHERE claimed_by <> -1
  AND heartbeated_at IS NOT NULL 
  AND heartbeated_at < @cutoff_time
RETURNING *;