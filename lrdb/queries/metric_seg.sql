-- name: InsertMetricSegmentDirect :exec
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
  @fingerprints::bigint[],
  @sort_version,
  @slot_count,
  @compacted
);

-- name: GetMetricSegsForCompaction :many
SELECT *
FROM metric_seg
WHERE
  organization_id = @organization_id AND
  dateint = @dateint AND
  frequency_ms = @frequency_ms AND
  instance_num = @instance_num AND
  slot_id = @slot_id AND
  ts_range && int8range(@start_ts, @end_ts, '[)') AND
  file_size <= @max_file_size AND
  (created_at, segment_id) > (@cursor_created_at, @cursor_segment_id::bigint)
ORDER BY
  created_at, segment_id
LIMIT @maxrows;

-- name: GetMetricSegsForRollup :many
SELECT *
FROM metric_seg
WHERE
  organization_id = @organization_id AND
  dateint = @dateint AND
  frequency_ms = @frequency_ms AND
  instance_num = @instance_num AND
  slot_id = @slot_id
ORDER BY
  ts_range;

-- name: GetMetricSegsByIds :many
SELECT *
FROM metric_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND frequency_ms = @frequency_ms
  AND instance_num = @instance_num
  AND segment_id = ANY(@segment_ids::bigint[])
ORDER BY segment_id;

-- name: BatchInsertMetricSegs :batchexec
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
);

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
