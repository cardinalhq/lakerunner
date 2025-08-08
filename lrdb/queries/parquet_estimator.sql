-- name: MetricSegEstimator :many
-- Returns an estimate of the number of metric segments, average bytes, average records,
-- and average bytes per record for metric segments in the last hour per organization and instance.
-- This query is basically identical to the LogSegEstimator, but for metric segments.
SELECT
  organization_id,
  instance_num,
  (sum(file_size)::float8 / sum(record_count))::float8 AS avg_bpr
FROM metric_seg
WHERE
  record_count > 100
  AND dateint IN (@dateint_low, @dateint_high)
  AND ts_range && int8range(@ms_low, @ms_high, '[)')
GROUP BY
  organization_id,
  instance_num
ORDER BY
  organization_id,
  instance_num;

-- name: LogSegEstimator :many
-- Returns an estimate of the number of log segments, average bytes, average records,
-- and average bytes per record for log segments in the last hour per organization and instance.
-- This query is basically identical to the MetricSegEstimator, but for log segments.
SELECT
  organization_id,
  instance_num,
  (sum(file_size)::float8 / sum(record_count))::float8 AS avg_bpr
FROM log_seg
WHERE
  record_count > 100
  AND dateint IN (@dateint_low, @dateint_high)
  AND ts_range && int8range(@ms_low, @ms_high, '[)')
GROUP BY
  organization_id,
  instance_num
ORDER BY
  organization_id,
  instance_num;
