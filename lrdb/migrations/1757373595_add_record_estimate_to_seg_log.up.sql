-- Add record_estimate field to seg_log table for tracking compaction estimates
ALTER TABLE seg_log 
ADD COLUMN record_estimate BIGINT NOT NULL DEFAULT 0 CHECK (record_estimate >= 0);

-- Add comment explaining the field
COMMENT ON COLUMN seg_log.record_estimate IS 'Estimated record count used for compaction planning and comparison with actual results';