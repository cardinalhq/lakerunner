-- name: ListLogQLTags :many
WITH src AS (
  SELECT exemplar::jsonb AS exemplar
  FROM exemplar_logs
  WHERE organization_id = sqlc.arg(organization_id)
),
res_keys AS (
  SELECT DISTINCT ('resource.' || (attr->>'key'))::text AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceLogs','[]'::jsonb)) rl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl->'resource'->'attributes','[]'::jsonb)) attr
),
log_keys AS (
  SELECT DISTINCT ('log.' || (attr->>'key'))::text AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceLogs','[]'::jsonb)) rl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl->'scopeLogs','[]'::jsonb)) sl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(sl->'logRecords','[]'::jsonb)) rec
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rec->'attributes','[]'::jsonb)) attr
)
SELECT k AS tag_key
FROM (
  SELECT k FROM res_keys
  UNION
  SELECT k FROM log_keys
) all_keys
ORDER BY k;
