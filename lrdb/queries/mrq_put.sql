-- name: PutMetricRollupWork :exec
INSERT INTO metric_rollup_queue (
  organization_id,
  dateint,
  frequency_ms,
  instance_num,
  slot_id,
  slot_count,
  segment_id,
  priority
)
VALUES (
  @organization_id,
  @dateint,
  @frequency_ms,
  @instance_num,
  @slot_id,
  @slot_count,
  @segment_id,
  @priority
);