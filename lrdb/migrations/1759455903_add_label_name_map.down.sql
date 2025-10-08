-- Rollback label_name_map column addition

DROP INDEX IF EXISTS idx_log_seg_label_name_map;
ALTER TABLE log_seg DROP COLUMN IF EXISTS label_name_map;
