-- Add debugging columns to segment_journal for better compaction and rollup analysis
-- Clear existing data since we're adding NOT NULL columns without defaults
TRUNCATE TABLE segment_journal;

-- Drop the old redundant frequency_ms column since we now have explicit source/dest frequencies
ALTER TABLE segment_journal DROP COLUMN frequency_ms;

-- Add columns for time window debugging (especially useful for rollups)
ALTER TABLE segment_journal ADD COLUMN source_min_timestamp BIGINT NOT NULL;
ALTER TABLE segment_journal ADD COLUMN source_max_timestamp BIGINT NOT NULL;
ALTER TABLE segment_journal ADD COLUMN dest_min_timestamp BIGINT NOT NULL;
ALTER TABLE segment_journal ADD COLUMN dest_max_timestamp BIGINT NOT NULL;

-- Add columns for frequency context (source vs target frequency)
ALTER TABLE segment_journal ADD COLUMN source_frequency_ms INTEGER NOT NULL;
ALTER TABLE segment_journal ADD COLUMN dest_frequency_ms INTEGER NOT NULL;

-- Add check constraints
ALTER TABLE segment_journal ADD CONSTRAINT segment_journal_source_min_timestamp_check CHECK (source_min_timestamp >= 0);
ALTER TABLE segment_journal ADD CONSTRAINT segment_journal_source_max_timestamp_check CHECK (source_max_timestamp >= source_min_timestamp);
ALTER TABLE segment_journal ADD CONSTRAINT segment_journal_dest_min_timestamp_check CHECK (dest_min_timestamp >= 0);
ALTER TABLE segment_journal ADD CONSTRAINT segment_journal_dest_max_timestamp_check CHECK (dest_max_timestamp >= dest_min_timestamp);
ALTER TABLE segment_journal ADD CONSTRAINT segment_journal_source_frequency_ms_check CHECK (source_frequency_ms > 0);
ALTER TABLE segment_journal ADD CONSTRAINT segment_journal_dest_frequency_ms_check CHECK (dest_frequency_ms > 0);

-- Update table comment to reflect new debugging capabilities
COMMENT ON TABLE segment_journal IS 'Enhanced debugging journal for segment operations. Tracks record counts, time windows, and frequency changes for compaction and rollup debugging.';
COMMENT ON COLUMN segment_journal.source_min_timestamp IS 'Minimum timestamp (ms) in source data';
COMMENT ON COLUMN segment_journal.source_max_timestamp IS 'Maximum timestamp (ms) in source data';
COMMENT ON COLUMN segment_journal.dest_min_timestamp IS 'Minimum timestamp (ms) in destination data';
COMMENT ON COLUMN segment_journal.dest_max_timestamp IS 'Maximum timestamp (ms) in destination data';
COMMENT ON COLUMN segment_journal.source_frequency_ms IS 'Source frequency in milliseconds';
COMMENT ON COLUMN segment_journal.dest_frequency_ms IS 'Destination frequency in milliseconds';