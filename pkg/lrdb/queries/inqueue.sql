-- name: ClaimInqueueWork :one
UPDATE inqueue AS i
SET
  claimed_by = @claimed_by,
  claimed_at = NOW()
WHERE i.id = (
  SELECT ii.id
  FROM inqueue ii
  WHERE (ii.claimed_at IS NULL
         OR ii.claimed_at < NOW() - INTERVAL '20 minutes')
    AND ii.telemetry_type = @telemetry_type
  ORDER BY ii.priority DESC, ii.queue_ts
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: ReleaseInqueueWork :exec
UPDATE inqueue
SET
  claimed_by = -1,
  claimed_at = NULL,
  queue_ts = NOW() + INTERVAL '5 second',
  tries = tries + 1
WHERE
  id = @id
  AND claimed_by = @claimed_by;

-- name: TouchInqueueWork :exec
UPDATE inqueue
SET
  claimed_at = NOW()
WHERE
  id IN (@ids::uuid[])
  AND claimed_by = @claimed_by;

-- name: DeleteInqueueWork :exec
DELETE FROM inqueue
WHERE
  id = @id
  AND claimed_by = @claimed_by;

-- name: PutInqueueWork :exec
INSERT INTO inqueue (organization_id, collector_name, instance_num, bucket, object_id, telemetry_type, priority)
VALUES (@organization_id, @collector_name, @instance_num, @bucket, @object_id, @telemetry_type, @priority);
