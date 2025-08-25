-- name: InqueueSummary :many
SELECT count(*) AS count, telemetry_type
FROM inqueue
WHERE claimed_at IS NULL
GROUP BY telemetry_type
ORDER BY telemetry_type;