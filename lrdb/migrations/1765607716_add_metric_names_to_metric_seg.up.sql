-- Add metric_names column to metric_seg for fast metric name lookups
ALTER TABLE metric_seg ADD COLUMN IF NOT EXISTS metric_names TEXT[] DEFAULT NULL;

-- GIN index for fast array element lookups
CREATE INDEX IF NOT EXISTS idx_metric_seg_metric_names ON metric_seg USING gin (metric_names);
