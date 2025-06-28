-- name: MetricSegEstimator :many
-- Returns an estimate of the number of metric segments, average bytes, average records,
-- and average bytes per record for metric segments in the last hour per organization and instance.
-- This query is basically identical to the LogSegEstimator, but for metric segments.
WITH params AS (
  SELECT
    (EXTRACT(EPOCH FROM now() - INTERVAL '6 hour') * 1000)::bigint AS low_ms,
    (EXTRACT(EPOCH FROM now())              * 1000)::bigint AS high_ms
)
SELECT
  organization_id,
  instance_num,
  (sum(file_size)::float8 / sum(record_count))::float8 AS avg_bpr
FROM metric_seg
CROSS JOIN params
WHERE
    record_count > 100
  AND dateint IN (
    (to_char(now(),                     'YYYYMMDD'))::int,
    (to_char(now() - INTERVAL '6 hour', 'YYYYMMDD'))::int
  )
  AND ts_range && int8range(params.low_ms, params.high_ms, '[)')
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
WITH params AS (
  SELECT
    (EXTRACT(EPOCH FROM now() - INTERVAL '6 hour') * 1000)::bigint AS low_ms,
    (EXTRACT(EPOCH FROM now())              * 1000)::bigint AS high_ms
)
SELECT
  organization_id,
  instance_num,
  (sum(file_size)::float8 / sum(record_count))::float8 AS avg_bpr
FROM log_seg
CROSS JOIN params
WHERE
    record_count > 100
  AND dateint IN (
    (to_char(now(),                     'YYYYMMDD'))::int,
    (to_char(now() - INTERVAL '6 hour', 'YYYYMMDD'))::int
  )
  AND ts_range && int8range(params.low_ms, params.high_ms, '[)')
GROUP BY
  organization_id,
  instance_num
ORDER BY
  organization_id,
  instance_num;
