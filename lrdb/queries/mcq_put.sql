-- name: PutMetricCompactionWork :exec
INSERT INTO metric_compaction_queue (
  organization_id,
  dateint,
  frequency_ms,
  segment_id,
  instance_num,
  record_count,
  priority
)
VALUES (
  @organization_id,
  @dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  @record_count,
  @priority
);