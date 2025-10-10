-- Drop label_name_map column from metric_seg table

DROP INDEX IF EXISTS idx_metric_seg_label_name_map;
ALTER TABLE metric_seg DROP COLUMN IF EXISTS label_name_map;

DROP INDEX IF EXISTS idx_trace_seg_label_name_map;
ALTER TABLE trace_seg DROP COLUMN IF EXISTS label_name_map;
