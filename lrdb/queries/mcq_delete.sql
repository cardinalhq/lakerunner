-- name: DeleteMetricCompactionWork :exec
DELETE FROM metric_compaction_queue
WHERE id = @id
  AND claimed_by = @claimed_by;