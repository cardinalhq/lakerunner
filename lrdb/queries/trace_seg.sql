-- name: insertTraceSegmentDirect :exec
INSERT INTO trace_seg (
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

-- name: batchInsertTraceSegsDirect :batchexec
INSERT INTO trace_seg (
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

-- name: GetTraceSeg :one
SELECT *
FROM trace_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND segment_id = @segment_id
  AND instance_num = @instance_num;

-- name: MarkTraceSegsCompactedByKeys :exec
UPDATE trace_seg
SET compacted = true, published = false
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;
