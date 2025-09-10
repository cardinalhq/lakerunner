-- name: WorkQueueGlobalLock :exec
SELECT pg_advisory_xact_lock(hashtext('work_queue_global')::bigint);

-- name: WorkQueueClaimDirect :one
WITH
  v_now AS (
    SELECT NOW() AS ts
  ),

  target_freqs AS (
    SELECT unnest(@target_freqs::INTEGER[]) AS freq
  ),

  -- Only used for rollup actions (compact doesn't expand)
  rollup_sources(parent_freq_ms, child_freq_ms) AS (
    VALUES
      (60000,    10000),
      (300000,   60000),
      (1200000,  300000),
      (3600000, 1200000)
  ),

  sl_small AS MATERIALIZED (
    SELECT *
    FROM public.signal_locks sl
    WHERE
      sl.signal = @signal
      AND sl.frequency_ms = ANY (
        ARRAY(SELECT freq FROM target_freqs)
        || COALESCE(
             (SELECT array_agg(child_freq_ms)
              FROM rollup_sources
              WHERE parent_freq_ms = ANY(SELECT freq FROM target_freqs)
                AND @action::action_enum = 'rollup'),
             '{}'
           )
      )
  ),

  -- Find a candidate work item that:
  -- - matches the requested signal, action, and target frequencies
  -- - has priority >= min_priority
  -- - is runnable now
  -- - is not blocked by an existing lock on the same lock key
  -- - needs to run
  candidate AS (
    SELECT w.*
    FROM public.work_queue w
    LEFT JOIN sl_small sl
      ON sl.organization_id = w.organization_id
     AND sl.signal          = w.signal
     AND sl.slot_id         = w.slot_id
     AND sl.ts_range        && w.ts_range
     AND sl.work_id         <> w.id
    WHERE
      w.frequency_ms = ANY (SELECT freq FROM target_freqs)
      AND w.priority       >= @min_priority
      AND w.signal         = @signal
      AND w.action         = @action
      AND w.runnable_at   <= (SELECT ts FROM v_now)
      AND sl.id IS NULL
      AND w.needs_run
    ORDER BY
      w.needs_run DESC,
      w.priority  DESC,
      w.runnable_at,
      w.id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  ),

  -- Lock the candidate's own frequency, and (for rollup) its child frequency too.
  lock_map AS (
    SELECT c.frequency_ms AS lock_freq_ms
    FROM candidate c

    UNION ALL

    SELECT rs.child_freq_ms AS lock_freq_ms
    FROM candidate c
    JOIN rollup_sources rs
      ON c.frequency_ms = rs.parent_freq_ms
    WHERE @action = 'rollup'
  ),

  -- Clear any stale locks tied to this work_id (idempotence).
  cleanup_locks AS (
    DELETE FROM public.signal_locks sl
    USING candidate c
    WHERE sl.work_id = c.id
  ),

  -- Insert fresh locks bound to the candidate's slot.
  new_locks AS (
    INSERT INTO public.signal_locks (
      organization_id, instance_num, dateint,
      frequency_ms,    signal,       claimed_by,
      claimed_at,      ts_range,     work_id,
      slot_id
    )
    SELECT
      c.organization_id,
      c.instance_num,
      c.dateint,
      lm.lock_freq_ms,
      c.signal,
      @worker_id,
      (SELECT ts FROM v_now),
      c.ts_range,
      c.id,
      c.slot_id
    FROM candidate c
    CROSS JOIN lock_map lm
    ORDER BY lm.lock_freq_ms
  ),

  -- Claim the work item.
  updated AS (
    UPDATE public.work_queue w
    SET
      claimed_by     = @worker_id,
      claimed_at     = (SELECT ts FROM v_now),
      heartbeated_at = (SELECT ts FROM v_now),
      needs_run      = FALSE,
      tries          = w.tries + 1
    FROM candidate c
    WHERE w.id = c.id
    RETURNING w.*
  )

SELECT * FROM updated;

-- name: WorkQueueAddDirect :exec
SELECT public.work_queue_add(
    @org_id,
    @instance,
    @dateint,
    @frequency,
    @signal,
    @action,
    @ts_range,
    @runnable_at,
    @priority,
    @slot_id
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
-- 1) heart-beat the work_queue
UPDATE public.work_queue w
SET heartbeated_at = NOW()
WHERE w.id         = ANY(@ids::BIGINT[])
  AND w.claimed_by = @worker_id;

-- 2) heart-beat the signal_locks
UPDATE public.signal_locks sl
SET heartbeated_at = NOW()
WHERE sl.work_id    = ANY(@ids::BIGINT[])
  AND sl.claimed_by = @worker_id;


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

-- name: WorkQueueExtendedStatus :many
WITH unclaimed_summary AS (
  SELECT 
    signal, 
    action,
    count(*) AS unclaimed_count
  FROM work_queue
  WHERE needs_run = true 
    AND runnable_at <= now() 
    AND claimed_by = -1
  GROUP BY signal, action
),
claimed_details AS (
  SELECT 
    signal,
    action,
    ts_range,
    claimed_by,
    claimed_at AT TIME ZONE 'UTC' AS claimed_at_utc,
    heartbeated_at AT TIME ZONE 'UTC' AS heartbeated_at_utc,
    EXTRACT(EPOCH FROM (now() - heartbeated_at)) AS age_seconds,
    CASE 
      WHEN heartbeated_at < now() - INTERVAL '2.5 minutes' THEN true
      ELSE false
    END AS is_stale
  FROM work_queue
  WHERE claimed_by > 0
)
-- First, return unclaimed summaries
SELECT 
  signal,
  action,
  'unclaimed'::text AS row_type,
  unclaimed_count::bigint AS count_or_claimed_by,
  NULL::tstzrange AS ts_range,
  NULL::timestamptz AS claimed_at_utc,
  NULL::timestamptz AS heartbeated_at_utc,
  NULL::double precision AS age_seconds,
  false AS is_stale
FROM unclaimed_summary
UNION ALL
-- Then, return claimed details
SELECT 
  signal,
  action,
  'claimed'::text AS row_type,
  claimed_by AS count_or_claimed_by,
  ts_range,
  claimed_at_utc,
  heartbeated_at_utc,
  age_seconds,
  is_stale
FROM claimed_details
ORDER BY signal, action, row_type DESC;

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
