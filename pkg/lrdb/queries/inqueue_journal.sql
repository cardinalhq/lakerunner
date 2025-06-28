-- name: InqueueJournalUpsert :one
INSERT INTO inqueue_journal (organization_id, bucket, object_id)
VALUES (@organization_id, @bucket, @object_id)
ON CONFLICT (organization_id, bucket, object_id)
  DO UPDATE SET updated_at = clock_timestamp()
RETURNING (updated_at = created_at) AS is_new;

-- name: InqueueJournalDelete :exec
DELETE FROM inqueue_journal
WHERE organization_id = @organization_id
  AND bucket = @bucket
  AND object_id = @object_id;
