-- name: ObjectCleanupAdd :exec
INSERT INTO obj_cleanup (
  organization_id,
  instance_num,
  bucket_id,
  object_id
) VALUES (
  @organization_id,
  @instance_num,
  @bucket_id,
  @object_id
) ON CONFLICT (organization_id, instance_num, bucket_id, object_id) DO NOTHING;

-- name: ObjectCleanupComplete :exec
DELETE FROM obj_cleanup WHERE id = @id;

-- name: ObjectCleanupGet :many
UPDATE obj_cleanup
SET delete_at = NOW() + INTERVAL '30 minutes'
WHERE id IN (
  SELECT id
  FROM obj_cleanup
  WHERE tries < 10
  ORDER BY delete_at DESC
  LIMIT 20
)
RETURNING
  id,
  organization_id,
  instance_num,
  bucket_id,
  object_id;

-- name: ObjectCleanupFail :exec
UPDATE obj_cleanup
SET tries = tries + 1
WHERE id = @id;
