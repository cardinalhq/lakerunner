-- name: GetMetricSegmentsForDownload :many
SELECT *
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_time, @end_time, '[)')
  AND published = true
ORDER BY dateint, segment_id;

-- name: GetLogSegmentsForDownload :many
SELECT *
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_time, @end_time, '[)')
ORDER BY dateint, segment_id;

-- name: GetTraceSegmentsForDownload :many
SELECT *
FROM trace_seg
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_time, @end_time, '[)')
ORDER BY dateint, segment_id;