-- name: DeleteInqueueWork :exec
DELETE FROM inqueue
WHERE
  id = @id
  AND claimed_by = @claimed_by;