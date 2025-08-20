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
  created_by,
  fingerprints
)
VALUES (
  @organization_id,
  @dateint,
  @ingest_dateint,
  @segment_id,
  1,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @created_by,
  @fingerprints::bigint[]
);

-- name: GetLogSegmentsForCompaction :many
SELECT
  segment_id,
  lower(ts_range)::bigint AS start_ts,
  upper(ts_range)::bigint AS end_ts,
  file_size,
  record_count,
  ingest_dateint,
  created_at
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND file_size > 0
  AND record_count > 0
  AND file_size <= @max_file_size
  AND (created_at, segment_id) > (@cursor_created_at, @cursor_segment_id::bigint)
ORDER BY created_at, segment_id
LIMIT @maxrows;

-- name: CompactLogSegments :exec
WITH
  all_fp AS (
    SELECT unnest(fingerprints) AS fp
      FROM log_seg
     WHERE organization_id = @organization_id
       AND dateint        = @dateint
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
  created_by,
  fingerprints
)
SELECT
  @organization_id,
  @dateint,
  @ingest_dateint,
  @new_segment_id,
  1,
  @new_record_count,
  @new_file_size,
  int8range(@new_start_ts, @new_end_ts, '[)'),
  @created_by,
  fa.fingerprints
FROM fingerprint_array AS fa;
