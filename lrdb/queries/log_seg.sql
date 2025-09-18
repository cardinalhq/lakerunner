-- name: insertLogSegmentDirect :exec
INSERT INTO log_seg (
  organization_id,
  dateint,
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

-- name: batchInsertLogSegsDirect :batchexec
INSERT INTO log_seg (
  organization_id,
  dateint,
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
SET compacted = true
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: markLogSegsCompactedUnpublishedByKeys :exec
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
