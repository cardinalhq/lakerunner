-- name: ReleaseMetricRollupWork :exec
UPDATE metric_rollup_queue
SET claimed_by = -1,
    claimed_at = NULL,
    heartbeated_at = NULL,
    tries = tries + 1
WHERE id = @id AND claimed_by = @claimed_by;