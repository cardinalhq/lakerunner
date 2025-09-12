-- name: MetricSegmentCleanupGet :many
SELECT
  organization_id,
  dateint,
  frequency_ms,
  segment_id,
  instance_num,
  file_size,
  lower(ts_range)::bigint as ts_range_lower
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND published = false
  AND created_at < @age_threshold
LIMIT @max_rows;

-- name: MetricSegmentCleanupDelete :exec
DELETE FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND segment_id = @segment_id
  AND instance_num = @instance_num;

-- name: MetricSegmentCleanupBatchDelete :batchexec
DELETE FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND segment_id = @segment_id
  AND instance_num = @instance_num;
