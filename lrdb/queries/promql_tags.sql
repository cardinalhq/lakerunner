-- name: ListPromMetrics :many
SELECT DISTINCT
  metric_name,
  metric_type
FROM lrdb_exemplar_metrics
WHERE organization_id = $1
ORDER BY metric_name;

-- name: GetMetricType :one
SELECT metric_type
FROM lrdb_exemplar_metrics
WHERE organization_id = $1
  AND metric_name = $2
ORDER BY 1
LIMIT 1;

-- name: ListPromMetricTags :many
-- Extract tag keys from flat exemplar format
-- Only return keys that start with _cardinalhq_, resource_, scope_, or metric_
SELECT DISTINCT key::text AS tag_key
FROM lrdb_exemplar_metrics,
     LATERAL jsonb_object_keys(exemplar) AS key
WHERE organization_id = $1
  AND metric_name = $2
  AND key ~ '^(_cardinalhq_|resource_|scope_|metric_)'
ORDER BY tag_key;