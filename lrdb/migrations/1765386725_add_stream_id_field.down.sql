-- Drop the consistency constraint first
ALTER TABLE log_seg DROP CONSTRAINT IF EXISTS stream_id_consistency;

-- Drop the stream_id_field column
ALTER TABLE log_seg DROP COLUMN IF EXISTS stream_id_field;
