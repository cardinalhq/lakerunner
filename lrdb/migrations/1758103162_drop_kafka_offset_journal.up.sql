-- Drop the old kafka_offset_journal table that has been replaced by kafka_offset_tracker
DROP TABLE IF EXISTS kafka_offset_journal;