-- name: TouchMetricCompactionWork :exec
UPDATE metric_compaction_queue
SET
  claimed_at = NOW(),
  heartbeated_at = NOW()
WHERE
  id = ANY(@ids::bigint[])
  AND claimed_by = @claimed_by;