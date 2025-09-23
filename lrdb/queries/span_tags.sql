-- name: ListSpanTags :many
WITH src AS (
  SELECT exemplar::jsonb AS exemplar
  FROM lrdb_exemplar_traces
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
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceSpans','[]'::jsonb)) rs
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rs->'resource'->'attributes','[]'::jsonb)) attr
),
span_attr_keys AS (
  SELECT DISTINCT (
    CASE
      WHEN (attr->>'key') ~ '^_cardinalhq\.' THEN (attr->>'key')
      ELSE 'span.' || (attr->>'key')
    END
  ) AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceSpans','[]'::jsonb)) rs
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rs->'scopeSpans','[]'::jsonb)) ss
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ss->'spans','[]'::jsonb)) span
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(span->'attributes','[]'::jsonb)) attr
),
span_name_key AS (
  SELECT DISTINCT '_cardinalhq.span_name'::text AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceSpans','[]'::jsonb)) rs
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rs->'scopeSpans','[]'::jsonb)) ss
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ss->'spans','[]'::jsonb)) span
  WHERE span ? 'name'
),
span_kind_key AS (
  SELECT DISTINCT '_cardinalhq.span_kind'::text AS k
  FROM src
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(exemplar->'resourceSpans','[]'::jsonb)) rs
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rs->'scopeSpans','[]'::jsonb)) ss
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ss->'spans','[]'::jsonb)) span
  WHERE span ? 'kind'
)
SELECT k AS tag_key
FROM (
  SELECT k FROM res_keys
  UNION
  SELECT k FROM span_attr_keys
  UNION
  SELECT k FROM span_name_key
  UNION
  SELECT k FROM span_kind_key
) all_keys
ORDER BY k;