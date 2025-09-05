-- name: KafkaJournalGetLastProcessed :one
-- Get the last processed offset for a specific consumer group, topic, and partition
SELECT last_processed_offset 
FROM kafka_offset_journal 
WHERE consumer_group = @consumer_group AND topic = @topic AND partition = @partition;

-- name: KafkaJournalUpsert :exec
-- Insert or update the last processed offset for a consumer group, topic, and partition
INSERT INTO kafka_offset_journal (consumer_group, topic, partition, last_processed_offset, updated_at)
VALUES (@consumer_group, @topic, @partition, @last_processed_offset, NOW())
ON CONFLICT (consumer_group, topic, partition)
DO UPDATE SET 
    last_processed_offset = EXCLUDED.last_processed_offset,
    updated_at = NOW();

-- name: GetKafkaOffsetsByConsumerGroup :many
-- Get all offset entries for a specific consumer group (useful for monitoring)
SELECT consumer_group, topic, partition, last_processed_offset, updated_at
FROM kafka_offset_journal 
WHERE consumer_group = @consumer_group
ORDER BY topic, partition;

-- name: DeleteOldKafkaOffsets :exec
-- Clean up old offset entries (older than specified timestamp)
DELETE FROM kafka_offset_journal 
WHERE updated_at < @cutoff_time;