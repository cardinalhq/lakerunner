-- name: PutMetricRollupWork :exec
INSERT INTO metric_rollup_queue (
  organization_id,
  dateint,
  frequency_ms,
  instance_num,
  slot_id,
  slot_count,
  segment_id,
  record_count,
  rollup_group,
  priority,
  window_close_ts
)
VALUES (
  @organization_id,
  @dateint,
  @frequency_ms,
  @instance_num,
  @slot_id,
  @slot_count,
  @segment_id,
  @record_count,
  @rollup_group,
  @priority,
  @window_close_ts
);