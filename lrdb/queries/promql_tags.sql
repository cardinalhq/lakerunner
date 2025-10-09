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
-- Extract tag keys from label_name_map in metric_seg table for a specific metric
-- Filters by metric fingerprint to return tags only for the requested metric
-- Returns all keys from label_name_map (for v2 APIs)
-- Handler code can filter by non-empty values for v1 legacy API support
SELECT DISTINCT key::text AS tag_key
FROM metric_seg,
     LATERAL jsonb_object_keys(label_name_map) AS key
WHERE organization_id = @organization_id
  AND @metric_fingerprint = ANY(fingerprints)
  AND label_name_map IS NOT NULL
ORDER BY tag_key;