-- 1751059100_initial_work_queue.up.sql

CREATE TABLE IF NOT EXISTS work_queue (
  id              BIGSERIAL       PRIMARY KEY,
  priority        INTEGER         NOT NULL DEFAULT 0,
  runnable_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  organization_id UUID            NOT NULL,
  instance_num    SMALLINT        NOT NULL,
  dateint         INTEGER         NOT NULL,
  frequency_ms    INTEGER         NOT NULL,
  signal          signal_enum     NOT NULL,
  action          action_enum     NOT NULL,
  needs_run       BOOLEAN         NOT NULL DEFAULT TRUE,
  tries           INTEGER         NOT NULL DEFAULT 0,
  ts_range        tstzrange       NOT NULL,
  claimed_by      BIGINT          NOT NULL DEFAULT -1,
  claimed_at      TIMESTAMPTZ,
  heartbeated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  CONSTRAINT work_queue_sa_check CHECK (
    (signal = 'metrics' AND action IN ('rollup', 'compact'))
    OR (signal <> 'metrics' AND action = 'compact')
  ),
  CONSTRAINT work_queue_tsrange_not_empty CHECK (NOT isempty(ts_range)),
  CONSTRAINT work_queue_conflict EXCLUDE USING GIST (
    organization_id WITH =,
    instance_num    WITH =,
    frequency_ms    WITH =,
    signal          WITH =,
    action          WITH =,
    ts_range        WITH &&
  )
);

CREATE INDEX IF NOT EXISTS idx_work_queue_full_order
  ON public.work_queue (
    frequency_ms,
    signal,
    action,
    needs_run DESC,
    priority   DESC,
    runnable_at,
    id
  )
  INCLUDE (
    claimed_by,
    heartbeated_at,
    ts_range,
    organization_id,
    instance_num,
    dateint
  );

-- work queue add

CREATE OR REPLACE FUNCTION public.work_queue_add(
    p_org_id      UUID,
    p_instance    SMALLINT,
    p_dateint     INTEGER,
    p_frequency   INTEGER,
    p_signal      signal_enum,
    p_action      action_enum,
    p_ts_range    TSTZRANGE,
    p_runnable_at TIMESTAMPTZ,
    p_priority    INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    lower_ts TIMESTAMPTZ := lower(p_ts_range);
    upper_ts TIMESTAMPTZ := upper(p_ts_range);
BEGIN
    IF upper_ts <= lower_ts THEN
        RAISE EXCEPTION
          'work_queue_add: ts_range upper (%) must be > lower (%)',
          upper_ts, lower_ts;
    END IF;

    BEGIN
        INSERT INTO public.work_queue (
            organization_id,
            instance_num,
            dateint,
            frequency_ms,
            signal,
            action,
            tries,
            claimed_by,
            claimed_at,
            needs_run,
            ts_range,
            heartbeated_at,
            runnable_at,
            priority
        ) VALUES (
            p_org_id,
            p_instance,
            p_dateint,
            p_frequency,
            p_signal,
            p_action,
            0,
            -1,
            NULL,
            TRUE,
            p_ts_range,
            NOW(),
            p_runnable_at,
            p_priority
        );
        RETURN;
    EXCEPTION
        WHEN SQLSTATE '23P01' THEN
            UPDATE public.work_queue w
               SET needs_run   = TRUE,
                   runnable_at = CASE
                                   WHEN w.needs_run THEN w.runnable_at
                                   ELSE p_runnable_at
                                  END,
                   priority    = GREATEST(w.priority, p_priority)
             WHERE w.organization_id = p_org_id
               AND w.instance_num    = p_instance
               AND w.frequency_ms    = p_frequency
               AND w.signal          = p_signal
               AND w.action          = p_action
               AND w.ts_range && p_ts_range;
            RETURN;
    END;
END;
$$;

-- ===========================================================================
-- Function: work_queue_fail
--
-- Release a task and indicate if it should be retried.
--
-- Inputs:
--   p_id        BIGINT
--   p_worker_id BIGINT
--
-- Returns:
--   BOOLEAN – TRUE if the task will be retried; FALSE otherwise.
-- ===========================================================================

CREATE OR REPLACE FUNCTION public.work_queue_fail(
    p_id        BIGINT,
    p_worker_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_now          TIMESTAMPTZ := NOW();
    v_requeue_ttl  INTERVAL;
    v_max_retries  INTEGER;
    v_old_tries    INTEGER;
    v_new_tries    INTEGER;
BEGIN
    -- Fetch TTL and max retries
    SELECT value::interval INTO v_requeue_ttl
      FROM public.settings
     WHERE key = 'work_fail_requeue_ttl';

    SELECT value::int INTO v_max_retries
      FROM public.settings
     WHERE key = 'max_retries';

    -- Retrieve current tries and ownership
    SELECT tries
      INTO v_old_tries
      FROM public.work_queue
     WHERE id = p_id
       AND claimed_by = p_worker_id;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    v_new_tries := v_old_tries + 1;

    UPDATE public.work_queue
       SET claimed_by     = -1,
           claimed_at     = NULL,
           heartbeated_at = v_now,
           tries          = v_new_tries,
           runnable_at    = CASE WHEN v_new_tries <= v_max_retries THEN v_now + v_requeue_ttl ELSE runnable_at END,
           needs_run      = (v_new_tries <= v_max_retries)
     WHERE id = p_id
       AND claimed_by = p_worker_id;

    RETURN (v_new_tries <= v_max_retries);
END;
$$;


-- ===========================================================================
-- Function: work_queue_complete
--
-- Complete a task by setting claimed_by = -1, needs_run = FALSE,
-- and runnable_at = NOW(), only if owned by caller.
--
-- Inputs:
--   p_id        BIGINT
--   p_worker_id BIGINT
--
-- Returns:
--   BOOLEAN
-- ===========================================================================

CREATE OR REPLACE FUNCTION public.work_queue_complete(
    p_id        BIGINT,
    p_worker_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
BEGIN
    UPDATE public.work_queue
       SET claimed_by     = -1,
           claimed_at     = NULL,
           heartbeated_at = v_now,
           needs_run      = FALSE,
           runnable_at    = v_now,
           tries          = 0
     WHERE id = p_id
       AND claimed_by = p_worker_id;

    IF FOUND THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$;


-- ===========================================================================
-- Function: work_queue_heartbeat
--
-- Extend a claim by updating heartbeated_at = NOW(), only if owned by caller
-- and the lock has not already expired.
-- Inputs:
--   p_ids       BIGINT[]
--   p_worker_id BIGINT
--
-- Returns:
--   BOOLEAN
-- ===========================================================================

CREATE OR REPLACE FUNCTION public.work_queue_heartbeat(
    p_ids        BIGINT[],
    p_worker_id  BIGINT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_now      TIMESTAMPTZ := NOW();
    v_lock_ttl INTERVAL;
    v_count    INTEGER;
BEGIN
    SELECT value::interval
      INTO v_lock_ttl
      FROM public.settings
     WHERE key = 'lock_ttl';

    UPDATE public.work_queue
       SET heartbeated_at = v_now
     WHERE id = ANY(p_ids)
       AND claimed_by = p_worker_id
       AND heartbeated_at >= v_now - v_lock_ttl
    RETURNING 1
    INTO v_count;

    RETURN COALESCE(v_count, 0);
END;
$$;


-- ===========================================================================
-- Function: work_queue_cleanup
--
-- For tasks whose heartbeat expired, mark them unclaimed and needs_run = TRUE.
-- Also, tasks that exceeded max_retries remain ineligible for fetch_next.
--
-- Returns:
--   INTEGER – number of rows updated
-- ===========================================================================

CREATE OR REPLACE FUNCTION public.work_queue_cleanup()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_now        TIMESTAMPTZ := NOW();
    v_dead_ttl   INTERVAL;
    v_count      INTEGER;
BEGIN
    SELECT value::interval INTO v_dead_ttl
      FROM public.settings
     WHERE key = 'lock_ttl_dead';

    UPDATE public.work_queue
       SET claimed_by     = -1,
           claimed_at     = NULL,
           heartbeated_at = v_now,
           needs_run      = TRUE
     WHERE claimed_by <> -1
       AND heartbeated_at < v_now - v_dead_ttl
    RETURNING 1
    INTO v_count;

    RETURN COALESCE(v_count, 0);
END;
$$;
