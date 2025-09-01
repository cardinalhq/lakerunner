-- name: MarkMetricSegsCompactedByKeys :exec
UPDATE metric_seg
SET compacted = true,
    published = false
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND frequency_ms    = @frequency_ms
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: SetMetricSegCompacted :exec
UPDATE metric_seg
SET compacted = true
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND frequency_ms    = @frequency_ms
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: MarkMetricSegsRolledupByKeys :exec
UPDATE metric_seg
SET rolledup = true
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND frequency_ms    = @frequency_ms
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND rolledup        = false;

-- name: InsertCompactedMetricSeg :batchexec
INSERT INTO metric_seg (
  organization_id, dateint, frequency_ms, segment_id, instance_num,
  ts_range, record_count, file_size, ingest_dateint,
  published, rolledup, created_at, created_by, slot_id,
  fingerprints, sort_version, slot_count, compacted
)
VALUES (
  @organization_id,
  @dateint,
  @frequency_ms,
  @segment_id,
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),  -- half-open; change to '[]' if you store inclusive end
  @record_count,
  @file_size,
  @e_ingest_dateint,
  @published,            -- typically true for new compacted output
  @rolledup,             -- pass as needed
  now(),
  @created_by,
  @slot_id,
  @fingerprints::bigint[],  -- per-row bigint[]; bind as []int64
  @sort_version,
  @slot_count,
  false                  -- new segments are not compacted
)
ON CONFLICT (organization_id, dateint, frequency_ms, segment_id, instance_num, slot_id, slot_count)
DO NOTHING;               -- idempotent replays
