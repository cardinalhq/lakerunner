-- Add stream_ids column to log_seg for tracking unique stream identifiers per segment
ALTER TABLE log_seg ADD COLUMN IF NOT EXISTS stream_ids TEXT[] DEFAULT NULL;

-- Create GIN index for fast stream_id lookups (e.g., "what streams exist for org X in time range Y")
CREATE INDEX IF NOT EXISTS idx_log_seg_stream_ids ON log_seg USING gin (stream_ids);
