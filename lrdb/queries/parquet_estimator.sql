-- name: MetricSegEstimator :many
-- Returns an estimate of the number of metric segments, average bytes, average records,
-- and average bytes per record for metric segments in the last hour per organization, instance, and frequency.
-- Uses frequency_ms to provide more accurate estimates based on collection frequency.
WITH params AS (
  SELECT 1_000_000::float8 AS target_bytes
),
bpr AS (
  SELECT
    organization_id,
    frequency_ms,
    (sum(file_size)::float8 / NULLIF(sum(record_count), 0))::float8 AS avg_bpr
  FROM metric_seg
  WHERE
      record_count > 100
      AND dateint IN (@dateint_low, @dateint_high)
      AND ts_range && int8range(@ms_low, @ms_high, '[)')
  GROUP BY organization_id, frequency_ms
)
SELECT
  b.organization_id,
  b.frequency_ms,
  CEIL(p.target_bytes / NULLIF(b.avg_bpr, 0))::bigint AS estimated_records
FROM bpr b
CROSS JOIN params p;

-- name: LogSegEstimator :many
-- Returns an estimate of the number of log segments, average bytes, average records,
-- and average bytes per record for log segments in the last hour per organization and instance.
-- This query is basically identical to the MetricSegEstimator, but for log segments.
WITH params AS (
  SELECT 1_000_000::float8 AS target_bytes
),
bpr AS (
  SELECT
    organization_id,
    (sum(file_size)::float8 / NULLIF(sum(record_count), 0))::float8 AS avg_bpr
  FROM log_seg
  WHERE
      record_count > 100
      AND dateint IN (@dateint_low, @dateint_high)
      AND ts_range && int8range(@ms_low, @ms_high, '[)')
  GROUP BY organization_id
)
SELECT
  b.organization_id,
  CEIL(p.target_bytes / NULLIF(b.avg_bpr, 0))::bigint AS estimated_records
FROM bpr b
CROSS JOIN params p;
