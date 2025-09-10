-- name: insertMetricSegDirect :exec
INSERT INTO metric_seg (
  organization_id,
  dateint,
  ingest_dateint,
  frequency_ms,
  segment_id,
  instance_num,
  slot_id,
  ts_range,
  record_count,
  file_size,
  created_by,
  published,
  rolledup,
  fingerprints,
  sort_version,
  slot_count,
  compacted
)
VALUES (
  @organization_id,
  @dateint,
  @ingest_dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  @slot_id,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @created_by,
  @published,
  @rolledup,
  @fingerprints::bigint[],
  @sort_version,
  @slot_count,
  @compacted
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
  ingest_dateint,
  frequency_ms,
  segment_id,
  instance_num,
  slot_id,
  ts_range,
  record_count,
  file_size,
  published,
  created_by,
  rolledup,
  fingerprints,
  sort_version,
  slot_count,
  compacted
)
VALUES (
  @organization_id,
  @dateint,
  @ingest_dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  @slot_id,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @published,
  @created_by,
  @rolledup,
  @fingerprints::bigint[],
  @sort_version,
  @slot_count,
  @compacted
)
ON CONFLICT (organization_id, dateint, frequency_ms, segment_id, instance_num, slot_id, slot_count)
DO NOTHING;

-- name: BatchMarkMetricSegsRolledup :batchexec
UPDATE public.metric_seg
   SET rolledup = true
 WHERE organization_id = @organization_id
   AND dateint         = @dateint
   AND frequency_ms    = @frequency_ms
   AND segment_id      = @segment_id
   AND instance_num    = @instance_num
   AND slot_id         = @slot_id
;

-- name: BatchDeleteMetricSegs :batchexec
DELETE FROM public.metric_seg
 WHERE organization_id = @organization_id
   AND dateint         = @dateint
   AND frequency_ms    = @frequency_ms
   AND segment_id      = @segment_id
   AND instance_num    = @instance_num
   AND slot_id         = @slot_id
;

-- name: ListMetricSegmentsForQuery :many
SELECT
    instance_num,
    segment_id,
    lower(ts_range)::bigint AS start_ts,
    (upper(ts_range) - 1)::bigint AS end_ts
FROM metric_seg
WHERE ts_range && int8range($1, $2, '[)')
  AND dateint = $3
  AND frequency_ms = $4
  AND organization_id = $5
  AND fingerprints && @fingerprints::BIGINT[]
  AND published = true;

-- name: GetMetricSeg :one
SELECT *
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND segment_id = @segment_id
  AND instance_num = @instance_num
  AND slot_id = @slot_id
  AND slot_count = @slot_count;

-- name: MarkMetricSegsCompactedByKeys :exec
UPDATE metric_seg
SET compacted = true,
    published = false
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
