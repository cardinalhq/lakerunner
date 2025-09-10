-- Rename seg_log table to segment_journal for better clarity
ALTER TABLE seg_log RENAME TO segment_journal;

-- Rename all indexes to match new table name
ALTER INDEX idx_seg_log_created_at RENAME TO idx_segment_journal_created_at;
ALTER INDEX idx_seg_log_org RENAME TO idx_segment_journal_org;

-- Update table comment
COMMENT ON TABLE segment_journal IS 'Debugging journal for segment operations across all signals (logs, metrics, traces) and actions (ingest, compaction, rollups). Not typically enabled in production.';
