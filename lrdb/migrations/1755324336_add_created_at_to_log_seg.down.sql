DROP INDEX IF EXISTS idx_log_seg_created_at_segment_id;
ALTER TABLE log_seg DROP COLUMN IF EXISTS created_at;