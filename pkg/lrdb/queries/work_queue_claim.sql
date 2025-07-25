-- name: WorkQueueClaimDirect :one
WITH params AS (
    SELECT
      NOW() AS v_now,
      (SELECT value::interval FROM public.settings WHERE key = 'lock_ttl') AS v_lock_ttl
  ),

  target_freqs AS (
    SELECT unnest(@target_freqs::INTEGER[]) AS freq
  ),

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
      sl.signal       = @signal
      AND sl.frequency_ms = ANY (
        -- the “own” frequencies
        ARRAY(SELECT freq FROM target_freqs)
        -- plus, if rollup, the child freqs
        || COALESCE(
             (SELECT array_agg(child_freq_ms)
              FROM rollup_sources
              WHERE parent_freq_ms = ANY(SELECT freq FROM target_freqs)
                AND @action::action_enum = 'rollup'),
             '{}'
           )
      )
  ),

candidate AS (
  SELECT w.*
  FROM public.work_queue w
  LEFT JOIN sl_small sl
    ON sl.organization_id = w.organization_id
   AND sl.instance_num    = w.instance_num
   AND sl.signal          = w.signal
   AND sl.ts_range && w.ts_range
   AND sl.work_id <> w.id
  WHERE
    w.frequency_ms = ANY (SELECT freq FROM target_freqs)
    AND w.priority >= @min_priority
    AND w.signal     = @signal
    AND w.action     = @action
    AND w.runnable_at <= (SELECT v_now FROM params)
    AND sl.id IS NULL
    AND w.needs_run
  ORDER BY w.needs_run DESC, w.priority DESC, w.runnable_at, w.id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
),

  lock_map AS (
    -- always insert a lock at the candidate’s own frequency
    SELECT c.frequency_ms AS lock_freq_ms
    FROM candidate c

    UNION ALL

    -- if this is a rollup action, also insert a lock at the child-frequency
    SELECT rs.child_freq_ms AS lock_freq_ms
    FROM candidate c
    JOIN rollup_sources rs
      ON c.frequency_ms = rs.parent_freq_ms
    WHERE @action = 'rollup'
  ),

  cleanup_locks AS (
    DELETE FROM public.signal_locks sl
    USING candidate c
    WHERE sl.work_id = c.id
  ),

  new_locks AS (
    INSERT INTO public.signal_locks (
      organization_id, instance_num, dateint,
      frequency_ms,    signal,       claimed_by,
      claimed_at,      ts_range,     work_id
    )
    SELECT
      c.organization_id,
      c.instance_num,
      c.dateint,
      lm.lock_freq_ms,
      c.signal,
      @worker_id,
      (SELECT v_now FROM params),
      c.ts_range,
      c.id
    FROM candidate c
    CROSS JOIN lock_map lm
    ORDER BY lm.lock_freq_ms
  ),

  updated AS (
    UPDATE public.work_queue w
    SET
      claimed_by     = @worker_id,
      claimed_at     = (SELECT v_now FROM params),
      heartbeated_at = (SELECT v_now FROM params),
      needs_run      = FALSE,
      tries          = w.tries + 1
    FROM candidate c
    WHERE w.id = c.id
    RETURNING w.*
  )

SELECT * FROM updated;
