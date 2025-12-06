-- Remove stream_ids column and index
DROP INDEX IF EXISTS idx_log_seg_stream_ids;
ALTER TABLE log_seg DROP COLUMN IF EXISTS stream_ids;
