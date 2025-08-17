-- 1755447293_metric_estimator_frequency_index.down.sql

-- Restore the original metric_estimator index without frequency_ms

DROP INDEX IF EXISTS metric_estimator;

CREATE INDEX IF NOT EXISTS metric_estimator
  ON metric_seg (organization_id, instance_num)
  INCLUDE (record_count, file_size)
  WHERE record_count > 100;