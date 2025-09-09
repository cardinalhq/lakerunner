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
WITH src AS (
  SELECT exemplar
  FROM lrdb_exemplar_metrics
  WHERE organization_id = $1
    AND metric_name = $2
),
res_keys AS (
  SELECT DISTINCT (
    CASE
      WHEN (attr->>'key') ~ '^_cardinalhq\.' THEN (attr->>'key')
      ELSE 'resource.' || (attr->>'key')
    END
  ) AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(exemplar->'resourceMetrics','[]'::jsonb)) rm
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(rm->'resource'->'attributes','[]'::jsonb)) attr
),
dp_keys AS (
  SELECT DISTINCT (
    CASE
      WHEN (attr->>'key') ~ '^_cardinalhq\.' THEN (attr->>'key')
      ELSE 'metric.' || (attr->>'key')
    END
  ) AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(exemplar->'resourceMetrics','[]'::jsonb)) rm
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(rm->'scopeMetrics','[]'::jsonb)) sm
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(sm->'metrics','[]'::jsonb)) m
  CROSS JOIN LATERAL jsonb_array_elements(
    coalesce(
      m->'gauge'->'dataPoints',
      m->'sum'->'dataPoints',
      m->'histogram'->'dataPoints',
      m->'summary'->'dataPoints',
      '[]'::jsonb
    )
  ) dp
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(dp->'attributes','[]'::jsonb)) attr
)
SELECT k AS tag_key
FROM (
  SELECT k FROM res_keys
  UNION
  SELECT k FROM dp_keys
) u
ORDER BY k;