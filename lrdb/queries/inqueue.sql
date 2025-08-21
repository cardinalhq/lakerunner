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

-- name: ClaimInqueueWorkBatch :many
UPDATE inqueue
SET
  claimed_by = @claimed_by,
  claimed_at = NOW()
WHERE id IN (
  SELECT i.id
  FROM inqueue i
  WHERE i.claimed_at IS NULL
    AND i.telemetry_type = @telemetry_type
    AND i.organization_id = (
      SELECT ii.organization_id 
      FROM inqueue ii 
      WHERE ii.claimed_at IS NULL AND ii.telemetry_type = @telemetry_type 
      ORDER BY ii.priority DESC, ii.queue_ts 
      LIMIT 1
    )
    AND i.instance_num = (
      SELECT ii.instance_num 
      FROM inqueue ii 
      WHERE ii.claimed_at IS NULL AND ii.telemetry_type = @telemetry_type 
      ORDER BY ii.priority DESC, ii.queue_ts 
      LIMIT 1
    )
  ORDER BY i.priority DESC, i.queue_ts
  LIMIT @max_batch_size
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
INSERT INTO inqueue (organization_id, collector_name, instance_num, bucket, object_id, telemetry_type, priority, file_size)
VALUES (@organization_id, @collector_name, @instance_num, @bucket, @object_id, @telemetry_type, @priority, @file_size);

-- name: CleanupInqueueWork :exec
UPDATE inqueue
SET claimed_by = -1, claimed_at = NULL
WHERE claimed_at IS NOT NULL
  AND claimed_at < NOW() - INTERVAL '5 minutes';

-- name: InqueueSummary :many
SELECT count(*) AS count, telemetry_type
FROM inqueue
WHERE claimed_at IS NULL
GROUP BY telemetry_type
ORDER BY telemetry_type;
