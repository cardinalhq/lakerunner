-- Drop the kafka_offset_journal table and its index
DROP INDEX IF EXISTS idx_kafka_offset_journal_updated_at;
DROP TABLE IF EXISTS kafka_offset_journal;