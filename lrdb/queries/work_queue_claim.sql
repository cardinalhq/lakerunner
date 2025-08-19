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
