-- name: WorkQueueGlobalLock :exec
SELECT pg_advisory_xact_lock(hashtext('work_queue_global')::bigint);

-- name: WorkQueueAddDirect :exec
SELECT public.work_queue_add(
  @org_id      :: UUID,
  @instance    :: SMALLINT,
  @dateint     :: INTEGER,
  @frequency   :: INTEGER,
  @signal      :: signal_enum,
  @action      :: action_enum,
  @ts_range    :: TSTZRANGE,
  @runnable_at :: TIMESTAMPTZ,
  @priority    :: INTEGER
);


-- name: WorkQueueFailDirect :exec
WITH params AS (
  SELECT
    NOW()                                         AS v_now,
    (SELECT value::interval
       FROM public.settings
      WHERE key = 'work_fail_requeue_ttl')        AS requeue_ttl,
    (SELECT value::int
       FROM public.settings
      WHERE key = 'max_retries')                  AS max_retries
),
old AS (
  SELECT w.tries
  FROM public.work_queue w
  WHERE w.id         = @id::BIGINT
    AND w.claimed_by = @worker_id
),
updated AS (
  UPDATE public.work_queue w
  SET
    claimed_by     = -1,
    claimed_at     = NULL,
    heartbeated_at = (SELECT v_now FROM params),
    tries =
      CASE
        WHEN o.tries IS NULL THEN 1
        ELSE o.tries + 1
      END,
    runnable_at =
      CASE
        WHEN o.tries + 1 <= (SELECT max_retries FROM params)
          THEN (SELECT v_now FROM params) + (SELECT requeue_ttl FROM params)
        ELSE w.runnable_at
      END,
    needs_run =
      CASE
        WHEN o.tries + 1 <= (SELECT max_retries FROM params) THEN TRUE
        ELSE FALSE
      END
  FROM old o
  WHERE w.id         = @id::BIGINT
    AND w.claimed_by = @worker_id
)
DELETE FROM public.signal_locks sl
WHERE sl.work_id    = @id::BIGINT
  AND sl.claimed_by = @worker_id;


-- name: WorkQueueCompleteDirect :exec
WITH updated AS (
  UPDATE public.work_queue w
  SET
    claimed_by     = -1,
    claimed_at     = NULL,
    heartbeated_at = NOW(),
    needs_run      = FALSE,
    runnable_at    = NOW(),
    tries          = 0
  WHERE w.id         = @id::BIGINT
    AND w.claimed_by = @worker_id
  RETURNING id
)
DELETE FROM public.signal_locks sl
USING updated u
WHERE sl.work_id    = u.id
  AND sl.claimed_by = @worker_id;


-- name: WorkQueueHeartbeatDirect :exec
WITH params AS (
  SELECT
    NOW() AS v_now,
    (SELECT value::interval
       FROM public.settings
      WHERE key = 'lock_ttl') AS lock_ttl
)
-- 1) heart-beat the work_queue
UPDATE public.work_queue w
SET heartbeated_at = p.v_now
FROM params p
WHERE w.id            = ANY(@ids::BIGINT[])
  AND w.claimed_by    = @worker_id
  AND w.heartbeated_at >= p.v_now - p.lock_ttl;

-- 2) heart-beat the signal_locks
UPDATE public.signal_locks sl
SET heartbeated_at = p.v_now
FROM params p
WHERE sl.work_id     = ANY(@ids::BIGINT[])
  AND sl.claimed_by  = @worker_id
  AND sl.heartbeated_at >= p.v_now - p.lock_ttl;


-- name: WorkQueueCleanupDirect :many
WITH params AS (
  SELECT
    NOW() AS v_now,
    (SELECT value::interval
       FROM public.settings
      WHERE key = 'lock_ttl_dead') AS dead_ttl
),
expired AS (
  UPDATE public.work_queue w
  SET
    claimed_by     = -1,
    claimed_at     = NULL,
    heartbeated_at = params.v_now,
    needs_run      = TRUE
  FROM params
  WHERE
    w.claimed_by <> -1
    AND w.heartbeated_at < params.v_now - params.dead_ttl
  RETURNING w.*
),
deleted_locks AS (
  DELETE FROM public.signal_locks sl
  USING expired e
  WHERE sl.work_id = e.id
  RETURNING sl.id
)
SELECT
  e.*,
  (SELECT COUNT(*) FROM deleted_locks) AS locks_removed
FROM expired e;
