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
SELECT
  id,
  organization_id,
  instance_num,
  bucket_id,
  object_id
FROM obj_cleanup
WHERE delete_at < NOW()
  AND tries < 10
ORDER BY delete_at ASC
LIMIT 1000;

-- name: ObjectCleanupFail :exec
UPDATE obj_cleanup
SET tries = tries + 1,
    delete_at = NOW() + INTERVAL '5 minutes'
WHERE id = @id;
