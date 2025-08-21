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