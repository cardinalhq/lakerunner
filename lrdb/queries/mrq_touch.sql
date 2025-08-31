-- name: TouchMetricRollupWork :exec
UPDATE metric_rollup_queue
SET heartbeated_at = COALESCE(sqlc.narg(now_ts)::timestamptz, now())
WHERE id = ANY(@ids::bigint[]) AND claimed_by = @claimed_by;