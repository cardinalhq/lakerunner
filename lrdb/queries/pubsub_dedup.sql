-- name: PubSubMessageHistoryInsert :execresult
INSERT INTO pubsub_message_history (
    bucket, object_id, source
) VALUES (
    @bucket, @object_id, @source
) ON CONFLICT (bucket, object_id, source) DO NOTHING;

-- name: PubSubMessageHistoryCleanup :execresult
DELETE FROM pubsub_message_history
WHERE (bucket, object_id, source) IN (
    SELECT pmh.bucket, pmh.object_id, pmh.source
    FROM pubsub_message_history pmh
    WHERE pmh.received_at < @age_threshold
    ORDER BY pmh.received_at
    LIMIT @batch_size
);

-- name: PubSubMessageHistoryCount :one
SELECT COUNT(*) as total_count
FROM pubsub_message_history;

