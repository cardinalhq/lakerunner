-- name: InqueueSummary :many
SELECT count(*) AS count, signal
FROM inqueue
WHERE claimed_at IS NULL
GROUP BY signal
ORDER BY signal;