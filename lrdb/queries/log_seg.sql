-- name: InsertLogSegmentDirect :exec
INSERT INTO log_seg (
  organization_id,
  dateint,
  ingest_dateint,
  segment_id,
  instance_num,
  ts_range,
  record_count,
  file_size,
  fingerprints
)
VALUES (
  @organization_id,
  @dateint,
  @ingest_dateint,
  @segment_id,
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @fingerprints::bigint[]
);

-- name: GetLogSegmentsForCompaction :many
SELECT
  segment_id,
  lower(ts_range)::bigint AS start_ts,
  upper(ts_range)::bigint AS end_ts,
  file_size,
  record_count,
  ingest_dateint
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND instance_num    = @instance_num
  AND file_size > 0
  AND record_count > 0
ORDER BY lower(ts_range);

-- name: CompactLogSegments :exec
WITH
  all_fp AS (
    SELECT unnest(fingerprints) AS fp
      FROM log_seg
     WHERE organization_id = @organization_id
       AND dateint        = @dateint
       AND instance_num   = @instance_num
       AND segment_id     = ANY(@old_segment_ids::bigint[])
  ),
  fingerprint_array AS (
    SELECT coalesce(
      array_agg(DISTINCT fp ORDER BY fp),
      '{}'::bigint[]
    ) AS fingerprints
    FROM all_fp
  ),
  deleted_seg AS (
    DELETE FROM log_seg
     WHERE organization_id = @organization_id
       AND dateint        = @dateint
       AND instance_num   = @instance_num
       AND segment_id     = ANY(@old_segment_ids::bigint[])
  )
INSERT INTO log_seg (
  organization_id,
  dateint,
  ingest_dateint,
  segment_id,
  instance_num,
  record_count,
  file_size,
  ts_range,
  fingerprints
)
SELECT
  @organization_id,
  @dateint,
  @ingest_dateint,
  @new_segment_id,
  @instance_num,
  @new_record_count,
  @new_file_size,
  int8range(@new_start_ts, @new_end_ts, '[)'),
  fa.fingerprints
FROM fingerprint_array AS fa;
