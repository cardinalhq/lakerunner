-- name: TouchInqueueWork :exec
UPDATE inqueue
SET
  claimed_at = NOW(),
  heartbeated_at = NOW()
WHERE
  id = ANY(@ids::uuid[])
  AND claimed_by = @claimed_by;