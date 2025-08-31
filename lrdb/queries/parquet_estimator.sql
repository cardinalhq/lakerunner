-- name: MetricSegEstimator :many
-- Returns an estimate of the number of metric segments, accounting for per-file overhead.
-- Uses frequency_ms to provide more accurate estimates based on collection frequency.
WITH params AS (
  SELECT 
    1_000_000::float8 AS target_bytes,
    15_000::float8 AS estimated_overhead_per_file  -- Based on observed 10-18K overhead
),
stats AS (
  SELECT
    organization_id,
    frequency_ms,
    COUNT(*) AS file_count,
    SUM(file_size) AS total_bytes,
    SUM(record_count) AS total_records
  FROM metric_seg
  WHERE
      record_count > 100
      AND dateint IN (@dateint_low, @dateint_high)
      AND ts_range && int8range(@ms_low, @ms_high, '[)')
  GROUP BY organization_id, frequency_ms
),
estimates AS (
  SELECT
    organization_id,
    frequency_ms,
    -- Estimate bytes per record excluding overhead
    CASE 
      WHEN total_records > 0 AND file_count > 0 THEN
        GREATEST(1, (total_bytes::float8 - (file_count * p.estimated_overhead_per_file)) / total_records)
      ELSE 100  -- fallback bytes per record
    END AS bytes_per_record
  FROM stats s
  CROSS JOIN params p
)
SELECT
  e.organization_id,
  e.frequency_ms,
  -- Calculate records needed: (target_bytes - overhead) / bytes_per_record
  GREATEST(1000, 
    CEIL((p.target_bytes - p.estimated_overhead_per_file) / NULLIF(e.bytes_per_record, 0))
  )::bigint AS estimated_records
FROM estimates e
CROSS JOIN params p;

-- name: LogSegEstimator :many
-- Returns an estimate of the number of log segments, accounting for per-file overhead.
WITH params AS (
  SELECT 
    1_000_000::float8 AS target_bytes,
    15_000::float8 AS estimated_overhead_per_file  -- Based on observed 10-18K overhead
),
stats AS (
  SELECT
    organization_id,
    COUNT(*) AS file_count,
    SUM(file_size) AS total_bytes,
    SUM(record_count) AS total_records
  FROM log_seg
  WHERE
      record_count > 100
      AND dateint IN (@dateint_low, @dateint_high)
      AND ts_range && int8range(@ms_low, @ms_high, '[)')
  GROUP BY organization_id
),
estimates AS (
  SELECT
    organization_id,
    -- Estimate bytes per record excluding overhead
    CASE 
      WHEN total_records > 0 AND file_count > 0 THEN
        GREATEST(1, (total_bytes::float8 - (file_count * p.estimated_overhead_per_file)) / total_records)
      ELSE 100  -- fallback bytes per record
    END AS bytes_per_record
  FROM stats s
  CROSS JOIN params p
)
SELECT
  e.organization_id,
  -- Calculate records needed: (target_bytes - overhead) / bytes_per_record
  GREATEST(1000, 
    CEIL((p.target_bytes - p.estimated_overhead_per_file) / NULLIF(e.bytes_per_record, 0))
  )::bigint AS estimated_records
FROM estimates e
CROSS JOIN params p;

-- name: TraceSegEstimator :many
-- Returns an estimate of the number of trace segments, accounting for per-file overhead.
WITH params AS (
  SELECT 
    1_000_000::float8 AS target_bytes,
    15_000::float8 AS estimated_overhead_per_file  -- Based on observed 10-18K overhead
),
stats AS (
  SELECT
    organization_id,
    COUNT(*) AS file_count,
    SUM(file_size) AS total_bytes,
    SUM(record_count) AS total_records
  FROM trace_seg
  WHERE
      record_count > 100
      AND dateint IN (@dateint_low, @dateint_high)
      AND ts_range && int8range(@ms_low, @ms_high, '[)')
  GROUP BY organization_id
),
estimates AS (
  SELECT
    organization_id,
    -- Estimate bytes per record excluding overhead
    CASE 
      WHEN total_records > 0 AND file_count > 0 THEN
        GREATEST(1, (total_bytes::float8 - (file_count * p.estimated_overhead_per_file)) / total_records)
      ELSE 100  -- fallback bytes per record
    END AS bytes_per_record
  FROM stats s
  CROSS JOIN params p
)
SELECT
  e.organization_id,
  -- Calculate records needed: (target_bytes - overhead) / bytes_per_record
  GREATEST(1000, 
    CEIL((p.target_bytes - p.estimated_overhead_per_file) / NULLIF(e.bytes_per_record, 0))
  )::bigint AS estimated_records
FROM estimates e
CROSS JOIN params p;
