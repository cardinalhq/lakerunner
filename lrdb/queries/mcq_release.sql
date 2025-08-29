-- name: ReleaseMetricCompactionWork :exec
UPDATE metric_compaction_queue
SET
  claimed_by = -1,
  claimed_at = NULL,
  queue_ts = NOW() + INTERVAL '5 second',
  tries = tries + 1
WHERE
  id = @id
  AND claimed_by = @claimed_by;