-- name: insertLogSegmentDirect :exec
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
  fingerprints,
  published,
  compacted
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
  @created_by,
  @fingerprints::bigint[],
  @published,
  @compacted
);

-- name: GetLogSeg :one
SELECT *
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND segment_id = @segment_id
  AND instance_num = @instance_num;

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
  created_by,
  fingerprints,
  published,
  compacted
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
  @created_by,
  fa.fingerprints,
  @published,
  @compacted
FROM fingerprint_array AS fa;

-- name: batchInsertLogSegsDirect :batchexec
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
  fingerprints,
  published,
  compacted
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
  @created_by,
  @fingerprints::bigint[],
  @published,
  @compacted
);

-- name: MarkLogSegsCompactedByKeys :exec
UPDATE log_seg
SET compacted = true, published = false
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: ListLogSegmentsForQuery :many
SELECT
    t.fp::bigint                    AS fingerprint,
    s.instance_num,
    s.segment_id,
    lower(s.ts_range)::bigint        AS start_ts,
    (upper(s.ts_range) - 1)::bigint  AS end_ts
FROM log_seg AS s
         CROSS JOIN LATERAL
    unnest(s.fingerprints) AS t(fp)
WHERE
    s.organization_id = @organization_id
  AND s.dateint       = @dateint
  AND s.published     = true
  AND s.fingerprints && @fingerprints::BIGINT[]
  AND t.fp            = ANY(@fingerprints::BIGINT[])
  AND ts_range && int8range(@s, @e, '[)');
