-- Rename segment_journal table back to seg_log
ALTER TABLE segment_journal RENAME TO seg_log;

-- Rename all indexes back to original names
ALTER INDEX idx_segment_journal_created_at RENAME TO idx_seg_log_created_at;
ALTER INDEX idx_segment_journal_org RENAME TO idx_seg_log_org;

-- Update table comment back
COMMENT ON TABLE seg_log IS 'Debugging log for segment operations across all signals (logs, metrics, traces) and actions (ingest, compaction, rollups). Not typically enabled in production.';