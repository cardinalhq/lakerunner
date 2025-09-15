-- name: KafkaOffsetsAfter :one
SELECT
  COALESCE(
    ARRAY(
      SELECT o
      FROM kafka_offset_tracker kot
      CROSS JOIN LATERAL unnest(kot.offsets) AS o
      WHERE kot.consumer_group = @consumer_group
        AND kot.topic          = @topic
        AND kot.partition_id   = @partition_id
        AND kot.max_offset     >= @min_offset
        AND o                 >= @min_offset
      ORDER BY o
    ),
    ARRAY[]::bigint[]
  )::bigint[] AS offsets;

-- name: InsertKafkaOffsets :exec
INSERT INTO kafka_offset_tracker (
  consumer_group,
  topic,
  partition_id,
  bin_id,
  min_offset,
  max_offset,
  offsets,
  created_at
) VALUES (
  @consumer_group,
  @topic,
  @partition_id,
  @bin_id,
  (SELECT MIN(o) FROM unnest(@offsets::bigint[]) AS o),
  (SELECT MAX(o) FROM unnest(@offsets::bigint[]) AS o),
  @offsets,
  COALESCE(sqlc.narg('created_at')::timestamptz, now())
);

-- name: CleanupKafkaOffsets :execrows
DELETE FROM kafka_offset_tracker
WHERE consumer_group = @consumer_group
  AND topic = @topic
  AND partition_id = @partition_id
  AND max_offset <= @max_offset;

-- name: CleanupKafkaOffsetsByAge :execrows
DELETE FROM kafka_offset_tracker
WHERE created_at < @created_before;