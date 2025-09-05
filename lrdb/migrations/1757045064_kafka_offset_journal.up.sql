-- Create kafka_offset_journal table to track processed Kafka message offsets
-- This enables exactly-once processing by recording the last successfully processed
-- offset per consumer group, topic, and partition combination.
CREATE TABLE kafka_offset_journal (
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL, 
    partition INTEGER NOT NULL,
    last_processed_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (consumer_group, topic, partition)
);

-- Index for monitoring and cleanup queries
CREATE INDEX idx_kafka_offset_journal_updated_at 
ON kafka_offset_journal (updated_at);

-- Comments for documentation
COMMENT ON TABLE kafka_offset_journal IS 'Tracks the last successfully processed Kafka message offset per consumer group/topic/partition to enable exactly-once processing semantics';
COMMENT ON COLUMN kafka_offset_journal.consumer_group IS 'Kafka consumer group name (e.g., lakerunner.ingest.metrics)';
COMMENT ON COLUMN kafka_offset_journal.topic IS 'Kafka topic name (e.g., lakerunner.objstore.ingest.metrics)';
COMMENT ON COLUMN kafka_offset_journal.partition IS 'Kafka partition number within the topic';
COMMENT ON COLUMN kafka_offset_journal.last_processed_offset IS 'Last Kafka message offset that was successfully processed and committed to storage';
COMMENT ON COLUMN kafka_offset_journal.updated_at IS 'Timestamp when this offset was last updated';