-- Remove compacted field from log_seg table
ALTER TABLE log_seg DROP COLUMN compacted;
ALTER TABLE log_seg DROP COLUMN published;
