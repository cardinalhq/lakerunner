-- name: MrqPickHead :one
SELECT id, organization_id, dateint, frequency_ms, instance_num,
       slot_id, slot_count, rollup_group, segment_id, record_count, queue_ts
FROM public.metric_rollup_queue
WHERE claimed_by = -1
  AND eligible_at <= now()
ORDER BY priority ASC, eligible_at ASC, queue_ts ASC
FOR UPDATE
LIMIT 1;

-- name: MrqFetchCandidates :many
SELECT id, organization_id, dateint, frequency_ms, instance_num,
       slot_id, slot_count, rollup_group, segment_id, record_count, queue_ts
FROM public.metric_rollup_queue
WHERE claimed_by = -1
  AND eligible_at <= now()
  AND organization_id = @organization_id
  AND dateint        = @dateint
  AND frequency_ms   = @frequency_ms
  AND instance_num   = @instance_num
  AND slot_id        = @slot_id
  AND slot_count     = @slot_count
  AND rollup_group   = @rollup_group
ORDER BY queue_ts ASC, id ASC
FOR UPDATE
LIMIT @max_rows;

-- name: MrqClaimBundle :exec
UPDATE public.metric_rollup_queue
SET claimed_by = @worker_id, claimed_at = now(), heartbeated_at = now()
WHERE id = ANY(@ids::bigint[]);

-- name: MrqDeferItems :exec
UPDATE public.metric_rollup_queue
SET eligible_at = now() + @push::interval
WHERE claimed_by = -1
  AND id = ANY(@ids::bigint[]);

-- name: MrqHeartbeat :execrows
UPDATE public.metric_rollup_queue
SET heartbeated_at = now()
WHERE claimed_by = @worker_id AND id = ANY(@ids::bigint[]);

-- name: MrqCompleteDelete :exec
DELETE FROM public.metric_rollup_queue
WHERE claimed_by = @worker_id AND id = ANY(@ids::bigint[]);

-- name: MrqReclaimTimeouts :execrows
WITH stale AS (
  SELECT id
  FROM public.metric_rollup_queue
  WHERE claimed_by <> -1
    AND heartbeated_at < (now() - @max_age::interval)
  LIMIT @max_rows
)
UPDATE public.metric_rollup_queue q
SET claimed_by = -1, claimed_at = NULL, heartbeated_at = NULL, tries = q.tries + 1
FROM stale s
WHERE q.id = s.id;

-- name: MrqRelease :exec
UPDATE public.metric_rollup_queue
SET claimed_by = -1, claimed_at = NULL, heartbeated_at = NULL, tries = tries + 1
WHERE claimed_by = @worker_id AND id = ANY(@ids::bigint[]);

-- name: MrqQueueWork :exec
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
  eligible_at
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
  @eligible_at
);

-- name: MrqClaimSingleRow :one
WITH candidate AS (
  SELECT id, organization_id, dateint, frequency_ms, instance_num,
         slot_id, slot_count, rollup_group, segment_id, record_count, queue_ts
  FROM public.metric_rollup_queue
  WHERE claimed_by = -1
  ORDER BY priority ASC, queue_ts ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE public.metric_rollup_queue q
SET claimed_by = @worker_id, claimed_at = @now::timestamptz, heartbeated_at = @now::timestamptz
FROM candidate c
WHERE q.id = c.id
RETURNING c.id, c.organization_id, c.dateint, c.frequency_ms, c.instance_num,
          c.slot_id, c.slot_count, c.rollup_group, c.segment_id, c.record_count, c.queue_ts;

-- name: MrqClaimBatch :many
WITH candidates AS (
  SELECT id, organization_id, dateint, frequency_ms, instance_num,
         slot_id, slot_count, rollup_group, segment_id, record_count, queue_ts
  FROM public.metric_rollup_queue
  WHERE claimed_by = -1
    AND eligible_at <= now()
  ORDER BY priority ASC, eligible_at ASC, queue_ts ASC
  FOR UPDATE SKIP LOCKED
  LIMIT @batch_limit
)
UPDATE public.metric_rollup_queue q
SET claimed_by = @worker_id, claimed_at = @now::timestamptz, heartbeated_at = @now::timestamptz
FROM candidates c
WHERE q.id = c.id
RETURNING c.id, c.organization_id, c.dateint, c.frequency_ms, c.instance_num,
          c.slot_id, c.slot_count, c.rollup_group, c.segment_id, c.record_count, c.queue_ts;
