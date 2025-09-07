-- name: GetMetricSegByPrimaryKey :one
-- Fetch a single metric segment by its complete primary key
SELECT * FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND segment_id = @segment_id
  AND instance_num = @instance_num
  AND slot_id = @slot_id
  AND slot_count = @slot_count;