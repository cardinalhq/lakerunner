-- name: TouchInqueueWork :exec
UPDATE inqueue
SET
  claimed_at = NOW()
WHERE
  id IN (@ids::uuid[])
  AND claimed_by = @claimed_by;