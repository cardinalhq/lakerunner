-- name: KafkaJournalGetLastProcessed :one
-- Get the last processed offset for a specific consumer group, topic, and partition
SELECT last_processed_offset
FROM kafka_offset_journal
WHERE consumer_group = @consumer_group AND topic = @topic AND partition = @partition;

-- name: KafkaJournalUpsert :exec
-- Insert or update the last processed offset for a consumer group, topic, and partition
-- Only updates if the new offset is greater than the existing one
INSERT INTO kafka_offset_journal (consumer_group, topic, partition, last_processed_offset, updated_at)
VALUES (@consumer_group, @topic, @partition, @last_processed_offset, NOW())
ON CONFLICT (consumer_group, topic, partition)
DO UPDATE SET
    last_processed_offset = EXCLUDED.last_processed_offset,
    updated_at = NOW()
WHERE kafka_offset_journal.last_processed_offset < EXCLUDED.last_processed_offset;

-- name: KafkaJournalBatchUpsert :batchexec
-- Insert or update multiple Kafka journal entries in a single batch operation
-- Only updates if the new offset is greater than the existing one
INSERT INTO kafka_offset_journal (consumer_group, topic, partition, last_processed_offset, organization_id, instance_num, updated_at)
VALUES (@consumer_group, @topic, @partition, @last_processed_offset, @organization_id, @instance_num, NOW())
ON CONFLICT (consumer_group, topic, partition, organization_id, instance_num)
DO UPDATE SET
    last_processed_offset = EXCLUDED.last_processed_offset,
    updated_at = NOW()
WHERE kafka_offset_journal.last_processed_offset < EXCLUDED.last_processed_offset;

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

-- name: KafkaJournalGetLastProcessedWithOrgInstance :one
-- Get the last processed offset for a specific consumer group, topic, partition, organization, and instance
SELECT last_processed_offset
FROM kafka_offset_journal
WHERE consumer_group = @consumer_group
  AND topic = @topic
  AND partition = @partition
  AND organization_id = @organization_id
  AND instance_num = @instance_num;

-- name: KafkaJournalUpsertWithOrgInstance :exec
-- Insert or update the last processed offset for a consumer group, topic, partition, organization, and instance
-- Only updates if the new offset is greater than the existing one
INSERT INTO kafka_offset_journal (consumer_group, topic, partition, organization_id, instance_num, last_processed_offset, updated_at)
VALUES (@consumer_group, @topic, @partition, @organization_id, @instance_num, @last_processed_offset, NOW())
ON CONFLICT (consumer_group, topic, partition, organization_id, instance_num)
DO UPDATE SET
    last_processed_offset = EXCLUDED.last_processed_offset,
    updated_at = NOW()
WHERE kafka_offset_journal.last_processed_offset < EXCLUDED.last_processed_offset;
