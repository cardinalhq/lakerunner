-- Add label_name_map column to metric_seg table for v2 tag queries and legacy API support
-- This column stores all label/tag column names from the Parquet schema, with mappings
-- from underscored names to dotted names where applicable (for legacy API compatibility)

ALTER TABLE metric_seg ADD COLUMN label_name_map JSONB;

-- Create GIN index for efficient lookup of label mappings
CREATE INDEX idx_metric_seg_label_name_map ON metric_seg USING gin(label_name_map);

ALTER TABLE trace_seg ADD COLUMN label_name_map JSONB;

-- Create GIN index for efficient lookup of label mappings
CREATE INDEX idx_trace_seg_label_name_map ON trace_seg USING gin(label_name_map);
