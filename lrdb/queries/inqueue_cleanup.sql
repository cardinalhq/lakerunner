-- name: CleanupInqueueWork :many
UPDATE inqueue
SET claimed_by = -1, claimed_at = NULL
WHERE claimed_at IS NOT NULL
  AND claimed_at < @cutoff_time
RETURNING *;