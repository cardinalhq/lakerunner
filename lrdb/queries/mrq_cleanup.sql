-- name: CleanupMetricRollupWork :one
WITH deleted AS (
  DELETE FROM metric_rollup_queue
  WHERE claimed_at IS NOT NULL
    AND heartbeated_at < (now() - make_interval(secs => @timeout_seconds))
  RETURNING 1
)
SELECT COUNT(*) FROM deleted;