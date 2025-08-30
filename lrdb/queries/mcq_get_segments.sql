-- name: GetMetricSegsForCompactionWork :many
SELECT *
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND instance_num = @instance_num
  AND segment_id = ANY(@segment_ids::bigint[])
ORDER BY segment_id;