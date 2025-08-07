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
  AND (
    (@start_ts::BIGINT = 0 AND @end_ts::BIGINT = 0)
    OR
    (ts_range && int8range(@start_ts, @end_ts, '[)'))
  )
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
