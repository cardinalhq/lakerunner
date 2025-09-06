-- name: ListLogQLTags :many
WITH src AS (
  SELECT exemplar::jsonb AS exemplar
  FROM lrdb_exemplar_logs
  WHERE organization_id = sqlc.arg(organization_id)
),
res_keys AS (
  SELECT DISTINCT (
    CASE
      WHEN (attr->>'key') ~ '^_cardinalhq\.' THEN (attr->>'key')
      ELSE 'resource.' || (attr->>'key')
    END
  ) AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceLogs','[]'::jsonb)) rl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl->'resource'->'attributes','[]'::jsonb)) attr
),
log_attr_keys AS (
  SELECT DISTINCT (
    CASE
      WHEN (attr->>'key') ~ '^_cardinalhq\.' THEN (attr->>'key')
      ELSE 'log.' || (attr->>'key')
    END
  ) AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceLogs','[]'::jsonb)) rl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl->'scopeLogs','[]'::jsonb)) sl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(sl->'logRecords','[]'::jsonb)) rec
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rec->'attributes','[]'::jsonb)) attr
),
body_key AS (
  SELECT DISTINCT '_cardinalhq.message'::text AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceLogs','[]'::jsonb)) rl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl->'scopeLogs','[]'::jsonb)) sl
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(sl->'logRecords','[]'::jsonb)) rec
  WHERE rec ? 'body'                               
    AND jsonb_typeof(rec->'body') IN ('object')   
    AND (
      (rec->'body'->>'stringValue') IS NOT NULL OR
      (rec->'body'->>'intValue')    IS NOT NULL OR
      (rec->'body'->>'doubleValue') IS NOT NULL OR
      (rec->'body'->>'boolValue')   IS NOT NULL OR
      (rec->'body'->'bytesValue')   IS NOT NULL OR
      (rec->'body'->'kvlistValue')  IS NOT NULL OR
      (rec->'body'->'arrayValue')   IS NOT NULL
    )
)
SELECT k AS tag_key
FROM (
  SELECT k FROM res_keys
  UNION
  SELECT k FROM log_attr_keys
  UNION
  SELECT k FROM body_key
) all_keys
ORDER BY k;
