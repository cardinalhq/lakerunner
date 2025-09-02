-- name: McqPickHead :one
SELECT id, organization_id, dateint, frequency_ms, instance_num, segment_id, record_count, queue_ts
FROM public.metric_compaction_queue
WHERE claimed_by = -1
  AND eligible_at <= now()
ORDER BY priority ASC, eligible_at ASC, queue_ts ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- name: McqFetchCandidates :many
SELECT id, organization_id, dateint, frequency_ms, instance_num, segment_id, record_count, queue_ts
FROM public.metric_compaction_queue
WHERE claimed_by = -1
  AND eligible_at <= now()
  AND organization_id = @organization_id
  AND dateint        = @dateint
  AND frequency_ms   = @frequency_ms
  AND instance_num   = @instance_num
ORDER BY queue_ts ASC, id ASC
LIMIT @max_rows
FOR UPDATE SKIP LOCKED;

-- name: McqClaimBundle :exec
UPDATE public.metric_compaction_queue
SET claimed_by = @worker_id, claimed_at = now(), heartbeated_at = now()
WHERE id = ANY(@ids::bigint[]);

-- name: McqDeferKey :exec
WITH to_defer AS (
  SELECT q.id
  FROM public.metric_compaction_queue q
  WHERE q.claimed_by = -1
    AND q.organization_id = @organization_id
    AND q.dateint        = @dateint
    AND q.frequency_ms   = @frequency_ms
    AND q.instance_num   = @instance_num
  FOR UPDATE SKIP LOCKED
)
UPDATE public.metric_compaction_queue
SET eligible_at = now() + @push::interval
WHERE id IN (SELECT id FROM to_defer);

-- name: McqHeartbeat :execrows
UPDATE public.metric_compaction_queue
SET heartbeated_at = now()
WHERE claimed_by = @worker_id
  AND id = ANY(@ids::bigint[]);

-- name: McqCompleteDelete :exec
DELETE FROM public.metric_compaction_queue
WHERE claimed_by = @worker_id
  AND id = ANY(@ids::bigint[]);


-- name: McqReclaimTimeouts :execrows
WITH stale AS (
  SELECT id
  FROM public.metric_compaction_queue
  WHERE claimed_by <> -1
    AND heartbeated_at < (now() - @max_age::interval)
  LIMIT @max_rows
)
UPDATE public.metric_compaction_queue AS q
SET claimed_by = -1,
    claimed_at = NULL,
    heartbeated_at = NULL,
    tries = q.tries + 1
FROM stale s
WHERE q.id = s.id;

-- name: McqRelease :exec
UPDATE public.metric_compaction_queue
SET claimed_by = -1, claimed_at = NULL, heartbeated_at = NULL, tries = tries + 1
WHERE claimed_by = @worker_id AND id = ANY(@ids::bigint[]);

-- name: McqQueueWork :exec
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


-- name: McqCleanupExpired :many
UPDATE metric_compaction_queue
SET claimed_by = -1, claimed_at = NULL, heartbeated_at = NULL
WHERE claimed_by <> -1
  AND heartbeated_at IS NOT NULL
  AND heartbeated_at < @cutoff_time
RETURNING *;
