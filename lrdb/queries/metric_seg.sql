-- name: insertMetricSegDirect :exec
INSERT INTO metric_seg (
  organization_id,
  dateint,
  frequency_ms,
  segment_id,
  instance_num,
  ts_range,
  record_count,
  file_size,
  created_by,
  published,
  rolledup,
  fingerprints,
  sort_version,
  compacted,
  label_name_map,
  metric_names,
  metric_types
)
VALUES (
  @organization_id,
  @dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @created_by,
  @published,
  @rolledup,
  @fingerprints::bigint[],
  @sort_version,
  @compacted,
  @label_name_map,
  @metric_names::text[],
  @metric_types::smallint[]
);

-- name: GetMetricSegsByIds :many
SELECT *
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND instance_num = @instance_num
  AND segment_id = ANY(@segment_ids::bigint[])
ORDER BY segment_id;

-- name: insertMetricSegsDirect :batchexec
INSERT INTO metric_seg (
  organization_id,
  dateint,
  frequency_ms,
  segment_id,
  instance_num,
  ts_range,
  record_count,
  file_size,
  published,
  created_by,
  rolledup,
  fingerprints,
  sort_version,
  compacted,
  label_name_map,
  metric_names,
  metric_types
)
VALUES (
  @organization_id,
  @dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @published,
  @created_by,
  @rolledup,
  @fingerprints::bigint[],
  @sort_version,
  @compacted,
  @label_name_map,
  @metric_names::text[],
  @metric_types::smallint[]
)
ON CONFLICT (organization_id, dateint, frequency_ms, segment_id, instance_num)
DO NOTHING;

-- name: BatchMarkMetricSegsRolledup :batchexec
UPDATE public.metric_seg
   SET rolledup = true
 WHERE organization_id = @organization_id
   AND dateint         = @dateint
   AND frequency_ms    = @frequency_ms
   AND segment_id      = @segment_id
   AND instance_num    = @instance_num
;

-- name: BatchDeleteMetricSegs :batchexec
DELETE FROM public.metric_seg
 WHERE organization_id = @organization_id
   AND dateint         = @dateint
   AND frequency_ms    = @frequency_ms
   AND segment_id      = @segment_id
   AND instance_num    = @instance_num
;

-- name: ListMetricSegmentsForQuery :many
SELECT
    instance_num,
    segment_id,
    lower(ts_range)::bigint AS start_ts,
    (upper(ts_range) - 1)::bigint AS end_ts
FROM metric_seg
WHERE ts_range && int8range(@start_ts, @end_ts, '[)')
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND organization_id = @organization_id
  AND fingerprints && @fingerprints::BIGINT[]
  AND published = true;

-- name: GetMetricSeg :one
SELECT *
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND segment_id = @segment_id
  AND instance_num = @instance_num;

-- name: MarkMetricSegsCompactedByKeys :exec
UPDATE metric_seg
SET compacted = true
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND frequency_ms    = @frequency_ms
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: markMetricSegsCompactedUnpublishedByKeys :exec
UPDATE metric_seg
SET compacted = true, published = false
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND frequency_ms    = @frequency_ms
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;


-- name: MarkMetricSegsRolledupByKeys :exec
UPDATE metric_seg
SET rolledup = true
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND frequency_ms    = @frequency_ms
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND rolledup        = false;

-- name: GetMetricLabelNameMaps :many
SELECT
    segment_id,
    label_name_map
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND segment_id = ANY(@segment_ids::BIGINT[])
  AND label_name_map IS NOT NULL;

-- name: ListMetricNames :many
-- Returns distinct metric names for an organization within a time range
SELECT DISTINCT
    unnest(metric_names)::text AS metric_name
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_ts, @end_ts, '[)')
  AND published = true
  AND metric_names IS NOT NULL;

-- name: ListMetricNamesWithTypes :many
-- Returns distinct (metric_name, metric_type) pairs for an organization within a time range
-- Uses WITH ORDINALITY to properly join parallel arrays
SELECT DISTINCT
    n.name::text AS metric_name,
    t.type::smallint AS metric_type
FROM metric_seg
CROSS JOIN LATERAL unnest(metric_names) WITH ORDINALITY AS n(name, idx)
INNER JOIN LATERAL unnest(metric_types) WITH ORDINALITY AS t(type, idx) ON n.idx = t.idx
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_ts, @end_ts, '[)')
  AND published = true
  AND metric_names IS NOT NULL
  AND metric_types IS NOT NULL;
