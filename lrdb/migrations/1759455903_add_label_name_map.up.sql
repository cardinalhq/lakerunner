-- Add label_name_map column to log_seg table for legacy API support
-- This column stores the mapping from underscored label names (used in LogQL)
-- to dotted label names (used in the legacy Scala API)

ALTER TABLE log_seg ADD COLUMN label_name_map JSONB;

-- Create GIN index for efficient lookup of label mappings
CREATE INDEX idx_log_seg_label_name_map ON log_seg USING gin(label_name_map);
