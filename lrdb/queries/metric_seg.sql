-- name: InsertMetricSegmentDirect :exec
INSERT INTO metric_seg (
  organization_id,
  dateint,
  ingest_dateint,
  frequency_ms,
  segment_id,
  instance_num,
  tid_partition,
  ts_range,
  record_count,
  file_size,
  tid_count,
  created_by,
  published
)
VALUES (
  @organization_id,
  @dateint,
  @ingest_dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  @tid_partition,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @tid_count,
  @created_by,
  @published
);

-- name: GetMetricSegs :many
SELECT *
FROM metric_seg
WHERE
  organization_id = @organization_id AND
  dateint = @dateint AND
  frequency_ms = @frequency_ms AND
  instance_num = @instance_num
  AND ts_range && int8range(@start_ts, @end_ts, '[)')
ORDER BY
  ts_range;

-- name: BatchInsertMetricSegs :batchexec
INSERT INTO metric_seg (
  organization_id,
  dateint,
  ingest_dateint,
  frequency_ms,
  segment_id,
  instance_num,
  tid_partition,
  ts_range,
  record_count,
  file_size,
  tid_count,
  published,
  created_by,
  rolledup
)
VALUES (
  @organization_id,
  @dateint,
  @ingest_dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  @tid_partition,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @tid_count,
  @published,
  @created_by,
  @rolledup
);

-- name: BatchMarkMetricSegsRolledup :batchexec
UPDATE public.metric_seg
   SET rolledup = true
 WHERE organization_id = @organization_id
   AND dateint         = @dateint
   AND frequency_ms    = @frequency_ms
   AND segment_id      = @segment_id
   AND instance_num    = @instance_num
   AND tid_partition   = @tid_partition
;

-- name: BatchDeleteMetricSegs :batchexec
DELETE FROM public.metric_seg
 WHERE organization_id = @organization_id
   AND dateint         = @dateint
   AND frequency_ms    = @frequency_ms
   AND segment_id      = @segment_id
   AND instance_num    = @instance_num
   AND tid_partition   = @tid_partition
;

-- name: ListSegmentsForQuery :many
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
  AND published = true;
