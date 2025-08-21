-- name: WorkQueueGlobalLock :exec
SELECT pg_advisory_xact_lock(hashtext('work_queue_global')::bigint);

-- name: WorkQueueAddDirect :exec
SELECT public.work_queue_add(
  @org_id      :: UUID,
  9999         :: SMALLINT,
  @dateint     :: INTEGER,
  @frequency   :: INTEGER,
  @signal      :: signal_enum,
  @action      :: action_enum,
  @ts_range    :: TSTZRANGE,
  @runnable_at :: TIMESTAMPTZ,
  @priority    :: INTEGER,
  @slot_id     :: INTEGER
);


-- name: WorkQueueFailDirect :exec
WITH params AS (
  SELECT
    NOW()                                         AS v_now,
    @requeue_ttl::INTERVAL                        AS requeue_ttl,
    @max_retries::INTEGER                         AS max_retries
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
    @lock_ttl::INTERVAL AS lock_ttl
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
    @lock_ttl_dead::INTERVAL AS dead_ttl
),
expired AS (
  UPDATE public.work_queue w
  SET
    claimed_by     = -1,
    claimed_at     = NULL,
    heartbeated_at = params.v_now,
    needs_run      = TRUE,
    tries          = 0
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


-- name: WorkQueueGC :one
WITH doomed AS (
  SELECT w.id
  FROM public.work_queue AS w
  WHERE w.claimed_by = -1
    AND NOT w.needs_run
    AND w.runnable_at < @cutoff
  ORDER BY w.runnable_at
  LIMIT @maxrows
  FOR UPDATE SKIP LOCKED
),
del_wq AS (
  DELETE FROM public.work_queue AS w
  USING doomed AS d
  WHERE w.id = d.id
  RETURNING 1
)
SELECT COALESCE(COUNT(*), 0)::int AS deleted
FROM del_wq;


-- name: WorkQueueDeleteDirect :exec
DELETE FROM public.work_queue
WHERE id = @id::BIGINT
  AND claimed_by = @worker_id;

-- name: WorkQueueSummary :many
SELECT count(*) AS count, signal, action
FROM work_queue
WHERE needs_run = true AND runnable_at <= now()
GROUP BY signal, action
ORDER BY signal, action;

-- name: WorkQueueOrphanedSignalLockCleanup :one
WITH params AS (
  SELECT pg_advisory_xact_lock(hashtext('work_queue_global')::bigint) AS locked
),
orphaned AS (
  SELECT sl.id
  FROM public.signal_locks sl
  LEFT JOIN public.work_queue wq ON sl.work_id = wq.id
  WHERE wq.id IS NULL
  ORDER BY sl.id
  LIMIT @maxrows
),
deleted AS (
  DELETE FROM public.signal_locks sl
  USING orphaned o
  WHERE sl.id = o.id
  RETURNING 1
)
SELECT COALESCE(COUNT(*), 0)::int AS deleted
FROM deleted;
