-- name: TraceSegmentCleanupGet :many
SELECT
  organization_id,
  dateint,
  segment_id,
  instance_num,
  file_size,
  lower(ts_range)::bigint as ts_range_lower
FROM trace_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND published = false
  AND created_at < @age_threshold
LIMIT @max_rows;

-- name: TraceSegmentCleanupDelete :exec
DELETE FROM trace_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND segment_id = @segment_id
  AND instance_num = @instance_num;

-- name: TraceSegmentCleanupBatchDelete :batchexec
DELETE FROM trace_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND segment_id = @segment_id
  AND instance_num = @instance_num;
