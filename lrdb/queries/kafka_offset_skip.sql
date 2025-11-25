-- name: InsertKafkaOffsetSkip :exec
INSERT INTO kafka_offset_skip (
  consumer_group,
  topic,
  partition_id,
  skip_to_offset,
  created_at
) VALUES (
  @consumer_group,
  @topic,
  @partition_id,
  @skip_to_offset,
  COALESCE(sqlc.narg('created_at')::timestamptz, now())
)
ON CONFLICT (consumer_group, topic, partition_id)
DO UPDATE SET
  skip_to_offset = EXCLUDED.skip_to_offset,
  created_at = EXCLUDED.created_at;

-- name: GetKafkaOffsetSkips :many
SELECT consumer_group, topic, partition_id, skip_to_offset, created_at
FROM kafka_offset_skip
WHERE consumer_group = @consumer_group
  AND topic = @topic;

-- name: GetKafkaOffsetSkipsForGroup :many
SELECT consumer_group, topic, partition_id, skip_to_offset, created_at
FROM kafka_offset_skip
WHERE consumer_group = @consumer_group;

-- name: DeleteKafkaOffsetSkip :exec
DELETE FROM kafka_offset_skip
WHERE consumer_group = @consumer_group
  AND topic = @topic
  AND partition_id = @partition_id;

-- name: DeleteKafkaOffsetSkipsForGroup :execrows
DELETE FROM kafka_offset_skip
WHERE consumer_group = @consumer_group;

-- name: CleanupKafkaOffsetSkipsByAge :execrows
DELETE FROM kafka_offset_skip
WHERE created_at < @created_before;
