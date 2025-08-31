-- name: DeleteMetricRollupWork :exec
DELETE FROM metric_rollup_queue
WHERE id = @id AND claimed_by = @claimed_by;