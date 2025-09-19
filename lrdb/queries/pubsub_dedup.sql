-- name: PubSubMessageHistoryInsert :execresult
INSERT INTO pubsub_message_history (
    organization_id, instance_num, bucket, object_id, source
) VALUES (
    @organization_id, @instance_num, @bucket, @object_id, @source
) ON CONFLICT (organization_id, instance_num, bucket, object_id, source) DO NOTHING;

-- name: PubSubMessageHistoryCleanup :execresult
DELETE FROM pubsub_message_history
WHERE (organization_id, instance_num, bucket, object_id, source) IN (
    SELECT pmh.organization_id, pmh.instance_num, pmh.bucket, pmh.object_id, pmh.source
    FROM pubsub_message_history pmh
    WHERE pmh.received_at < @age_threshold
    ORDER BY pmh.received_at
    LIMIT @batch_size
);

-- name: PubSubMessageHistoryCount :one
SELECT COUNT(*) as total_count
FROM pubsub_message_history;

