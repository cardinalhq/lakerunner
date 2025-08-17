-- name: InsertTraceSegmentDirect :exec
INSERT INTO trace_seg (
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
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @created_by,
  @fingerprints::bigint[]
);
