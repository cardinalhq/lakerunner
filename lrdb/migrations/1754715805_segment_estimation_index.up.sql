-- 1754715805_segment_estimation_index.up.sql

CREATE INDEX IF NOT EXISTS log_estimator
  ON log_seg (organization_id, instance_num)
  INCLUDE (record_count, file_size)
  WHERE record_count > 100;

CREATE INDEX IF NOT EXISTS metric_estimator
  ON metric_seg (organization_id, instance_num)
  INCLUDE (record_count, file_size)
  WHERE record_count > 100;

ALTER TABLE log_seg
  ADD COLUMN IF NOT EXISTS created_by TEXT;

ALTER TABLE metric_seg
  ADD COLUMN IF NOT EXISTS created_by TEXT;
