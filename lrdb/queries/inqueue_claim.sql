-- name: ClaimInqueueWork :one
UPDATE inqueue AS i
SET
  claimed_by = @claimed_by,
  claimed_at = NOW()
WHERE i.id = (
  SELECT ii.id
  FROM inqueue ii
  WHERE ii.claimed_at IS NULL
    AND ii.telemetry_type = @telemetry_type
  ORDER BY ii.priority DESC, ii.queue_ts
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;