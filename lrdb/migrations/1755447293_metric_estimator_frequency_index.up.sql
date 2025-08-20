-- 1755447293_metric_estimator_frequency_index.up.sql

-- Replace the existing metric_estimator index to include frequency_ms for improved estimation
-- This enhances the existing estimator index to support frequency-aware metric estimation queries

DROP INDEX IF EXISTS metric_estimator;

CREATE INDEX IF NOT EXISTS metric_estimator
  ON metric_seg (frequency_ms, instance_num)
  INCLUDE (record_count, file_size)
  WHERE record_count > 100;