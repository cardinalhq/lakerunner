DROP INDEX IF EXISTS idx_metric_seg_metric_names;
ALTER TABLE metric_seg DROP COLUMN IF EXISTS metric_names;
