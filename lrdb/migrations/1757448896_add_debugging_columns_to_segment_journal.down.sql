-- Reverse the changes made in the up migration
ALTER TABLE segment_journal DROP CONSTRAINT segment_journal_dest_frequency_ms_check;
ALTER TABLE segment_journal DROP CONSTRAINT segment_journal_source_frequency_ms_check;
ALTER TABLE segment_journal DROP CONSTRAINT segment_journal_dest_max_timestamp_check;
ALTER TABLE segment_journal DROP CONSTRAINT segment_journal_dest_min_timestamp_check;
ALTER TABLE segment_journal DROP CONSTRAINT segment_journal_source_max_timestamp_check;
ALTER TABLE segment_journal DROP CONSTRAINT segment_journal_source_min_timestamp_check;

ALTER TABLE segment_journal DROP COLUMN dest_frequency_ms;
ALTER TABLE segment_journal DROP COLUMN source_frequency_ms;
ALTER TABLE segment_journal DROP COLUMN dest_max_timestamp;
ALTER TABLE segment_journal DROP COLUMN dest_min_timestamp;
ALTER TABLE segment_journal DROP COLUMN source_max_timestamp;
ALTER TABLE segment_journal DROP COLUMN source_min_timestamp;

-- Restore the original frequency_ms column
ALTER TABLE segment_journal ADD COLUMN frequency_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE segment_journal ADD CONSTRAINT seg_log_frequency_ms_check CHECK (frequency_ms > 0);

-- Restore original table comment
COMMENT ON TABLE segment_journal IS 'Debugging journal for segment operations across all signals (logs, metrics, traces) and actions (ingest, compaction, rollups). Not typically enabled in production.';